# Continuation Prompt — RustPBX `SipSession` God-Object Refactor

## Context

Working on RustPBX (production PBX). Repo: `/home/kieran/git/rustpbx`. Fork
`v0l/rustpbx`, upstream `restsend/rustpbx` (remote `origin` points at
restsend; remote `fork` = v0l). GitHub auth as `v0l`. Branch:
`fix/queue-via-callee-leg` (HEAD `950f32c9`, rebased on upstream `1514e4eb`).
Push with `git push fork fix/queue-via-callee-leg`. **Do not open a PR unless
explicitly told** (PR #228 was closed; we push the branch only).

Prod access:
`export SSH_AUTH_SOCK=/tmp/ssh-1sBcEUCNgyog/agent.309300` then
`ssh debian@pbx.axst.io`. Container `debian-pbx-1`, image `rustpbx:queue-graph`,
compose at `/home/debian/docker-compose.yml`. Config (root-owned, use `sudo`):
`/home/debian/pbx/config/config.toml`; queue config
`/home/debian/pbx/config/queue/queues.generated.toml` (regenerated from DB by
the Queue Manager addon on restart — hand edits to it don't stick). DB:
`/home/debian/pbx/db/rustpbx.sqlite3` (no `sqlite3` binary — use
`sudo python3` + `sqlite3` module). Custom sounds live in
`/home/debian/pbx/config/sounds/` (container `/app/config/sounds/`).

## Hard rules (from the user — keep these)

- Every identified bug MUST get a regression test.
- Prove fixes with unit tests or the e2e harness BEFORE surgery on shared
  critical code. Don't charge ahead on shared paths unvalidated.
- Prefer extracting testable units over editing the god object in place.

## What's already done (this branch, all prod-proven)

A complete event-driven, hook-extensible **queue call-graph engine** that
replaced the imperative polling queue. Lives in `src/call/graph/`
(`model.rs`, `reducer.rs`, `executor.rs`) + the SipSession adapter
`src/proxy/proxy_call/sip_session/queue_graph.rs`. Behind `ProxyConfig`
flag `queue_graph_engine` (currently **on** in prod). 22 pure unit tests +
12 real SIP+RTP e2e tests (`src/proxy/tests/test_queue_media_e2e.rs`),
all green. Validated live on prod: greeting + no-answer prompts, caller-hangup
cancels ringing agent (~47ms), BYE cascade both ways, PSTN/trunk + skill-group
dial+bridge fallback.

Key commits: `028a3608` (engine), `2d8d54f4` (PSTN-trunk fallback),
`fc352424` (parity: greeting/enricher/delegated fallback), `675ad891`
(playback codec + non-blocking CANCEL + BYE-answered-caller), `ccca2f25`
(phase-hook architecture), `8efa47e2` (audio path resolution), `950f32c9`
(skill-group fallback dial+bridge). Design + parity audit in
`docs/call-graph-design.md`.

## The problem to solve now

`src/proxy/proxy_call/sip_session.rs` is an **11,625-line god object**:
one `SipSession` struct with 34 fields and ~270 methods spanning signaling
(dialogs, INVITE/BYE/CANCEL), media (the bridge, anchored RTP, transcoding,
playback/codec), legs, session timers, recording, app runtime, transfers,
conference, supervisor, and codec negotiation — all coupled through `self`.

This coupling is the **root cause** of the last round of production
firefighting: the silent-playback codec bug (offer-first vs negotiated codec),
the blocking-CANCEL stall, the reject-on-confirmed stuck call — all lived in
this file and were only discoverable via real prod calls because nothing in it
is unit-testable in isolation. The user's own "prove before surgery" rule
exists precisely because this file is too coupled to touch safely.

## The approach (already validated on the queue)

Strangler-fig extraction behind **ports & adapters** — the exact pattern the
queue engine proved works:
- pure/typed core logic in its own module with exhaustive unit tests,
- a narrow port trait for the side effects,
- `SipSession` shrinks to a thin adapter implementing the port.

Each extraction turns a class of prod-only bugs into unit tests.

## First target: a `CallerPlayback` / media-playback service

This is where the last three prod bugs lived, so it has the highest
bug-to-LOC payoff and is the most self-contained.

Concretely:
- `handle_play` (sip_session.rs, ~line 8800) + `resolve_audio_file_path`
  (~line 6979) + the codec selection (now `self.media.answer`-first, fixed
  in `675ad891`) + `play_audio_file` + `stop_playback_track` +
  `prepare_queue_playback_media`.
- The media bridge file-source egress (`src/media/bridge.rs`,
  `replace_output_with_file`, the caller/callee sender codec, the
  `caller_gate`).

Extract a small service with **testable codec-selection logic** (the unit test
that would have caught the PCMA-pinned silence: "given caller offer `0 8 101`
and negotiated answer `8`, the playback payload type must be 8, not 0"), a port
for "play file to leg / stop / resolve path", and have `SipSession` implement
the port. Keep `play_audio_file`'s public signature stable (it's called from
many places incl. the queue adapter, IVR, voicemail).

Suggested follow-on extractions (in rough priority): the outbound **Dialer**
(`try_single_target` / `do_invite_async` / CANCEL — the blocking-CANCEL class),
then **caller-leg lifecycle** (answer/reject/BYE on `server_dialog` — the
reject-on-confirmed class).

## Build / test commands

- Lib build: `cargo build --lib` (~45s).
- Graph + queue tests: `cargo test --lib graph::` and
  `cargo test --lib test_queue_media_e2e`.
- Segment builds (`commerce`/`wholesale`/`contact-center`) pull **private
  submodules** from `cnb.cool/miuda.ai/*` needing `CNB_USERNAME`/`CNB_TOKEN`
  — they only build in CI (`.github/workflows/e2e.yml`), not locally. Keep
  changes in core, feature-gate-free, and don't touch `QueuePlan` /
  `AgentRegistry` / addon APIs so the segment builds stay safe.
- Known pre-existing failure: `tests/e2e_queue_comprehensive_test.rs::
  test_queue_cdr_generation` fails on upstream too (env/timing, asserts the
  test harness gives NO bidir RTP). Not ours; ignore.

## Deploy flow (when asked)

Build release locally (`cargo build --release --bin rustpbx --bin sipflow`,
~1m30s) → `mkdir -p bin/amd64 && cp target/release/{rustpbx,sipflow} bin/amd64/`
→ `sed 's/FROM debian:bookworm-slim/FROM debian:trixie-slim/' Dockerfile >
Dockerfile.trixie` (trixie for glibc 2.41 — local is Debian 13, bookworm is too
old) → `docker build -f Dockerfile.trixie --build-arg TARGETARCH=amd64 -t
rustpbx:queue-graph .` → `docker save | gzip` → scp to prod `/tmp/` →
`docker load` → `docker compose up -d`. Verify the running image id matches the
loaded one. **Clean up `bin/` and `Dockerfile.trixie` before git.** Rollback:
config/compose backups `*.bak-*` on prod + the prior image.

## First action in the new session

Read `docs/call-graph-design.md` (the ports/adapters pattern + parity audit),
then read `handle_play` and `resolve_audio_file_path` in `sip_session.rs` and
`replace_output_with_file` + the gate/codec logic in `src/media/bridge.rs`.
Propose the `CallerPlayback` port + pure codec-selection unit and confirm the
boundary with the user before extracting.
