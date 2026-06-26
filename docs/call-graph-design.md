# Call Graph: a stream-based, mid-call-reconfigurable model for queues (and beyond)

Status: PROPOSAL / RFC
Branch context: replacing the imperative inbound-queue procedure
(`execute_queue` → `dial_queue_sequential` → `try_single_target`).

## 1. Why the current queue is fragile

The inbound queue is a single long-lived imperative async fn. It detects
caller hangup by **polling** `server_dialog.state().is_terminated()` on a 100 ms
`tokio::time::interval` tick (`src/proxy/proxy_call/sip_session.rs:4386`). It is
not driven by caller-leg events.

Observed production symptoms, all explained by that one design choice:

| Symptom | Root cause |
|---|---|
| Caller hangs up but agent keeps ringing | CANCEL only fires when the 100 ms poll observes `terminated`; if the BYE/CANCEL isn't pumped concurrently or arrives on an un-answered early dialog, the poll never trips and the pending INVITE future is never dropped. |
| Agent rejects → next agent dials slowly | Reject falls through to the next loop iteration behind ring-timeout / playback awaits; nothing fast-paths a 4xx/6xx into "advance now." |
| Silence, "not dialing", target appears de-registered | If `resolve_custom_targets` yields no contact (registration expired), we silently jump to fallback → no dial, silence. |
| In-dialog BYE never lands on the callee | Bridge/teardown is entangled with the imperative flow; the callee dialog's remote target is the NAT'd response Contact. |

> Terminology: in this (proxy callee-leg) path the dialed unit is a
> `crate::call::Location` — a resolved AoR+contact, called a **target** in the
> code (`DialStrategy::Sequential(Vec<Location>)`, `try_single_target`). It is
> NOT the contact-center "agent" of `src/call/app/agent_registry.rs`; that
> `AgentRegistry`/`AgentRecord` model belongs to the app-runtime queue
> (`src/call/app/queue.rs`) we are not using. "Agent = ext 1000" was operator
> shorthand. This doc uses **CalleeLeg / target** throughout.

These are not four bugs; they are four faces of "imperative procedure + polling
bolted onto an event-driven SIP/media stack."

## 2. The model: a directed media graph reduced by events

A call is a **directed graph** owned by one controller task:

- **Nodes** = media endpoints / processors. Each node owns a `MediaPeer`
  (`src/proxy/proxy_call/media_peer.rs`) and, for SIP nodes, a dialog handle.
  - `CallerLeg` — the inbound SIP dialog.
  - `CalleeLeg(n)` — an outbound dialog to a resolved `Location`/target (one per hunt attempt).
  - `Player` — hold music / prompts (file track).
  - `Bridge` / `Mixer` — N×N audio routing (`MediaMixer`).
  - `Recorder`, `IVR`, `Fallback` — as needed.
- **Edges** = audio routes. Realized as `MixerRoute { input_id → {output_id: gain} }`
  on a `MediaMixer`, or a direct P2P bridge for the 2-party fast path.
- **Controller (reducer)** = a single task that `select!`s over a unified event
  stream and applies a pure-ish transition function `(graph, event) -> Vec<Effect>`.
  No polling. Effects are things like "send INVITE", "send CANCEL", "set route",
  "remove node", "start timer".

### Event sources (all first-class, all `select!`ed — never polled)
- SIP dialog events for every leg: `Ringing`, `EarlyMedia`, `Answered(sdp)`,
  `Rejected(code)`, `Bye`, `Cancel`, `Failed`.
- Timers: per-agent `RingTimeout`, queue `WaitTimeout`.
- Registration / resolution outcomes: `AgentUnreachable` (no contact) as an
  explicit event, not a silent skip.
- Control commands injected at runtime: the existing `CallCommand` enum
  (`Bridge`, `Hold`, `Transfer`, `SupervisorBarge`, `LegAdd`, …).
- DTMF, recording lifecycle, etc.

### Transition examples (queue)
- `CalleeAnswered(sdp)` → add `Bridge` edges Caller↔Callee, remove `Player`(hold)
  node and its edges, cancel all other pending `CalleeLeg` INVITEs, set callee
  node's correct remote target.
- `CalleeRejected | RingTimeout | TargetUnreachable` → remove that `CalleeLeg`
  node; if more candidates, add next `CalleeLeg`; else emit `TargetsExhausted`.
- `CallerBye | CallerCancel` → tear down all downstream nodes, emit CANCEL to any
  ringing `CalleeLeg`, stop players, end graph. **Immediate and deterministic.**
- `TargetsExhausted` → add `Fallback` node (dial+bridge PSTN/internal, see §5).

## 3. Why this fixes the bugs structurally

- Caller hangup is an **event the controller is already awaiting**, so CANCEL /
  teardown is immediate — no 100 ms race, no "rings forever."
- Reject/timeout/unreachable are **events** → immediate, uniform transition to
  the next candidate or fallback. No hidden waits.
- Registration failure is an **explicit `TargetUnreachable` event** with logging,
  not a silent fallthrough → no mystery silence.
- Bridge is a media-plane edge; SIP teardown is a per-node effect driven by the
  controller with the node's correct remote target recorded at confirm time.

## 4. What we reuse (most of it already exists)

| Concern | Existing primitive | File |
|---|---|---|
| Control vocabulary | `CallCommand` (+ `LegConnected`/`LegFailed`) | `src/call/domain/command.rs` |
| Command dispatch | `command_dispatch.rs`, `command_executor.rs` | `src/call/runtime/` |
| Media routing (edges) | `MediaMixer` `set_route`/`clear_route`/`add_input` | `src/media/mixer.rs` |
| Endpoint media (nodes) | `MediaPeer`, `MediaStream`, `Track` | `media_peer.rs`, `media/mod.rs` |
| Outbound dial | `try_single_target` internals (INVITE/SDP to a `Location`) — refactored into a node | `sip_session.rs` |
| Leg identity/state | `LegId`, `LegState` | `src/call/domain/leg.rs` |

The genuinely new piece is the **graph controller + transition function**. It
replaces both the imperative `execute_queue` and the half-wired app-runtime
queue (whose dynamic `LegAdd` legs were never wired into media forwarding — that
gap is exactly what a node's `MediaPeer` wiring closes).

## 5. Fallback (PSTN/internal) becomes a node

`execute_queue_fallback`'s REFER (broken for Twilio/PSTN) is replaced by adding a
`Fallback` `CalleeLeg`-style node that dials the trunk target via the same
outbound-dial node code and bridges — identical machinery to a normal callee.

**Status: implemented, incl. PSTN-via-trunk.** `FallbackPlan::DialBridge` makes
exhaustion dial the fallback target as one more callee node
(`NodeId::fallback()`), reusing the same dial → ring-timer → answer → bridge
path. Two resolution strategies, chosen by whether the locator yields a
concrete `destination`:
* **Registered / internal / external-realm** → dialed directly from the
  locator-resolved `Location`. Proven by `test_queue_graph_fallback_dial_bridge`.
* **PSTN** (locator passes the URI through with `destination: None`) → routed
  through the proxy's OUTBOUND routing (`match_invite`, `DialDirection::Outbound`)
  so the matching trunk's destination is applied, then bridged. Proven by
  `test_queue_graph_fallback_via_trunk` (gateway TestUa behind an outbound trunk
  → two-way RTP).

`FallbackPlan::Hangup(code)` covers explicit failure-code fallbacks. **Remaining:**
re-queue / skill-group fallbacks are a clean busy in this version.

Ringback: when the caller is neither answered immediately nor on hold music, a
callee `180` relays a `RelayCallerRinging` effect so the caller hears ringback
(`server_dialog.ringing`). In-band `183` early-media passthrough is a follow-up.

## 6. Migration / proving plan (respects the "prove before surgery" rule)

1. **DONE** — `model.rs` + `reducer.rs` (`src/call/graph/`): pure data +
   unit-testable `QueueGraph`. 15 transition tests green (caller-hangup-while-
   ringing, reject→next, unreachable→next, exhausted→fallback,
   answer→bridge+cancel-others, parallel forks, stale-event inertness).
2. **DONE** — `executor.rs`: `QueueController` (owns event loop + ring timers) +
   `QueueBackend` port trait. 3 tokio tests with real timers, fake backend.
3. **IN PROGRESS** — `SipSession` adapter implementing `QueueBackend` +
   `DialogState`→`GraphEvent` translation, in `sip_session/queue_graph.rs`.
   Keep the e2e harness (`test_queue_media_e2e.rs`) green; add a regression per
   real-world symptom (divergent callee Contact → teardown reaches INVITE src).
4. Route inbound queue through the controller behind a `ProxyConfig` flag
   (default off); run e2e + new regressions; then a real prod call + sipflow
   trace. Remove the imperative path once parity is proven.

The reducer being pure means every prod symptom we saw becomes a deterministic
unit test before any shared SIP code is touched.

## 6b. Prior art: how Asterisk and FreeSWITCH structure queues/routing

This design is not novel — it is the conclusion both major open-source PBXs
converged on. We are porting their event-driven model in-process.

### Asterisk has *two* models, and we're replacing the old one with the new one
* **Classic `app_queue`** (`apps/app_queue.c`) runs the `Queue()` dialplan
  application **on the caller's channel thread as an imperative loop**: join →
  wait-in-loop-until-our-turn → call a member → if not bridged, do between-call
  options (announcements/MOH) → retry until an exit condition. Ring strategies:
  `ringall`/`linear`/`leastrecent`/`fewestcalls`/`rrmemory`/`random`; `autofill`
  toggles serial vs parallel head-caller connection. **This is the same
  imperative-loop shape as rustpbx's broken `execute_queue`.** It only works
  because Asterisk's *core* is event-driven beneath it.
* **ARI / Stasis** (the modern model) hands a channel to an **external
  event-driven controller** over WebSocket. Primitives:
  * **Bridge** = "a container for channels that form paths of communication";
    mixing bridges pass media between members.
  * **Dial** = `POST /channels` (originate) → then *add both channels to a
    mixing bridge*.
  * **Channel state** (Down/Up) + an event stream drive everything;
    `externalMedia` channels splice in recording/AI.

  Our model is ARI 1:1: **Bridge/Mixer node = ARI bridge, CalleeLeg = channel,
  `DialTarget` then `Bridge` effects = originate-then-add-to-bridge, the reducer
  = the external Stasis app.** `MediaMixer` is our mixing bridge.

### FreeSWITCH `mod_callcenter` — and the bugs that validate single-owner state
DB-backed (`queues`, `agents`, `tiers`), with an explicit agent presence state
machine (Ready / In-call / Wrap-up / Standby / No-answer / Offering) and
strategies like `top-down`, `longest-idle-agent`, `round-robin`. It runs
**separate agent and member threads**, and its tracker shows *exactly our
failure classes*:
* "Stale members listed as answered" — a **race between agent and member
  threads**.
* "top-down: if the last agent is unavailable, distribution stalls."

Its core (`switch_core_state_machine.c`) is a per-channel state machine + event
bus, with `mod_event_socket` for external async control (FS's ARI analogue).

### Lessons baked into this design
1. **Two-tier pattern is universal:** event-driven core (channel state machines
   + bridges + event bus) with a routing policy on top. rustpbx's bug was
   implementing the queue as a pure imperative app **without** a concurrently
   pumped event core — forcing the 100 ms poll. The controller owns both event
   streams, fixing this at the root.
2. **We map onto proven primitives, not invented ones:** ARI bridge ≈ our
   `MediaMixer` node; originate-then-add-to-bridge ≈ `DialTarget`+`Bridge`.
3. **Single-owner reducer avoids FreeSWITCH's documented races.** Their
   stale-answered and top-down-stall bugs come from two threads mutating shared
   member state. Our state has exactly one owner (the reducer) reacting to
   events serially — those races are structurally impossible.
4. **Separate "route a call" (graph) from "select an agent" (policy).** Both
   PBXs separate the queue mechanism from member selection/presence. The graph
   deals in **target/CalleeLeg**; presence/skills (rustpbx's `AgentRegistry`)
   are a separate policy that feeds the candidate `Location` list — never baked
   into the controller.

## 7. Open questions for the user
- Scope: queue-only first, or design the controller to also subsume normal
  1:1 calls / transfers / conference (they already speak `CallCommand`)?
- Do we keep `CallCommand` as the external injection API (recommended — RWI/HTTP
  already emit it) and add an internal `GraphEvent` for SIP/timer events?
- Anchored media only, or must the 2-party fast path stay direct-P2P for codec
  passthrough?
