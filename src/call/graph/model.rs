//! Pure data types for the call graph reducer.
//!
//! Nothing here performs I/O. Identifiers are plain strings/indices so the
//! reducer can be tested without SIP or media infrastructure.

use std::time::Duration;

/// Identifier for a node in the call graph (a leg, player, bridge, …).
///
/// Stable, human-readable strings keep effects and assertions legible.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(s: impl Into<String>) -> Self {
        NodeId(s.into())
    }

    /// Stable id for the inbound caller leg.
    pub fn caller() -> Self {
        NodeId("caller".to_string())
    }

    /// Stable id for the callee leg dialing candidate `idx`.
    pub fn callee(idx: usize) -> Self {
        NodeId(format!("callee-{idx}"))
    }

    /// Stable id for the fallback callee leg.
    pub fn fallback() -> Self {
        NodeId("fallback".to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Lifecycle points in the queue graph that the reducer announces and to which
/// behaviours (voice prompts, hold music, future hooks) bind. The reducer is
/// agnostic to what — if anything — is attached: it always emits the point and
/// the backend runs whatever is registered (a no-op if nothing).
///
/// Adding a new prompt is therefore a *data* change (bind audio to a point),
/// not a change to the reducer or executor. New points are added here only when
/// a genuinely new moment in the lifecycle is needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HookPoint {
    /// Caller has entered the queue, before any target is dialed. Blocking:
    /// the hunt waits for this to finish (e.g. a greeting / transfer prompt).
    Greeting,
    /// Begin background hold media while hunting (looping, non-blocking).
    HoldStart,
    /// Stop background hold media.
    HoldStop,
    /// All targets exhausted, before a hangup fallback. Blocking (e.g. a
    /// no-answer / busy prompt).
    NoAnswer,
    /// Before dialing a fallback destination. Blocking (e.g. a
    /// final-destination prompt).
    BeforeFallbackDial,
}

/// What to do once all candidate targets are exhausted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackPlan {
    /// Hang up the caller with this status code (default: 486 Busy Here).
    Hangup(u16),
    /// Dial a fallback target as one more callee leg and bridge on answer.
    DialBridge,
    /// Hand the fallback back to the host (re-enqueue / IVR / skill-group /
    /// play-then-hangup): the controller ends in `Fallback` phase and the
    /// caller's host runs the existing fallback machinery.
    Delegate,
}

/// Hunt strategy for the candidate targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    /// Try one target at a time, advancing on reject/timeout/unreachable.
    Sequential,
    /// Ring all targets at once; first to answer wins.
    Parallel,
}

/// Static configuration the reducer needs to drive a queue call.
///
/// This is derived from the `QueuePlan` by the executor; the reducer only sees
/// counts and flags, never SIP `Location`s.
#[derive(Debug, Clone)]
pub struct GraphConfig {
    pub strategy: Strategy,
    /// Number of resolved candidate targets to hunt through.
    pub target_count: usize,
    /// Per-target ring timeout. `None` = wait indefinitely.
    pub ring_timeout: Option<Duration>,
    /// Whether to answer (200 OK) the caller immediately on entry.
    pub accept_immediately: bool,
    /// What to do when all candidate targets are exhausted.
    pub fallback: FallbackPlan,
}

/// Lifecycle phase of the whole queue graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphPhase {
    /// Not started yet.
    Idle,
    /// Hunting through candidate targets.
    Hunting,
    /// A callee answered; caller is bridged to it.
    Connected,
    /// All targets exhausted; running the fallback action.
    Fallback,
    /// Terminal — nothing more will happen.
    Ended,
}

/// Inputs to the reducer. These are produced by the executor from real SIP
/// dialog events, timers, and target-resolution outcomes.
#[derive(Debug, Clone, PartialEq)]
pub enum GraphEvent {
    /// Caller leg has been answered (200 OK sent). Only meaningful when
    /// `accept_immediately` is set or after early/answer bridging.
    CallerAnswered,
    /// A candidate callee leg is ringing (180).
    CalleeRinging { node: NodeId },
    /// A candidate callee leg produced early media (183 + SDP).
    CalleeEarlyMedia { node: NodeId },
    /// A candidate callee leg answered (200 OK + SDP).
    CalleeAnswered { node: NodeId },
    /// A connected callee leg hung up (in-dialog BYE from the callee).
    CalleeBye { node: NodeId },
    /// A candidate callee leg rejected the call (4xx/5xx/6xx).
    CalleeRejected { node: NodeId, code: u16 },
    /// A candidate callee leg failed (transport/timeout-from-stack/etc.).
    CalleeFailed { node: NodeId },
    /// The per-target ring timer fired without an answer.
    RingTimeout { node: NodeId },
    /// Target resolution produced no reachable contact (e.g. de-registered).
    TargetUnreachable { idx: usize },
    /// The caller hung up an answered call (BYE).
    CallerBye,
    /// The caller cancelled an un-answered call (CANCEL).
    CallerCancel,
}

/// Outputs from the reducer. The executor performs these against real APIs.
///
/// Effects are intentionally fine-grained and idempotent-friendly so the
/// executor can apply them without re-deriving state.
#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    /// Send INVITE to candidate target `idx`, tracked as `node`.
    DialTarget { node: NodeId, idx: usize },
    /// Cancel a still-ringing (un-answered) callee INVITE.
    CancelInvite { node: NodeId },
    /// Send BYE to a connected callee dialog.
    HangupCallee { node: NodeId },
    /// Answer the caller leg (send 200 OK).
    AnswerCaller,
    /// Hang up the caller leg with a status code.
    HangupCaller { code: u16 },
    /// Arm the per-target ring timer.
    StartRingTimer { node: NodeId, timeout: Duration },
    /// Disarm the per-target ring timer.
    CancelRingTimer { node: NodeId },
    /// Run whatever behaviour is bound to a lifecycle point (voice prompt,
    /// hold music, …). The backend resolves the point to an action.
    RunHook { point: HookPoint },
    /// Add a bidirectional audio bridge edge between two nodes.
    Bridge { a: NodeId, b: NodeId },
    /// Remove all audio edges touching `node`.
    ClearRoutes { node: NodeId },
    /// Dial the configured fallback target as a callee leg, tracked as `node`.
    DialFallback { node: NodeId },
    /// Relay a 180 Ringing to the (un-answered) caller so it hears ringback.
    RelayCallerRinging,
    /// The graph has reached a terminal state; the executor should tear down.
    EndGraph,
}
