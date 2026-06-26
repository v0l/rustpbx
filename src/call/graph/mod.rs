//! Stream-based call graph for queue (and, later, general) call control.
//!
//! This module is split into two layers:
//!
//! * [`model`] + [`reducer`] — a **pure**, side-effect-free state machine. It
//!   consumes [`GraphEvent`]s and produces [`Effect`]s. It owns no SIP dialogs,
//!   no media, no I/O, and is therefore exhaustively unit-testable. Every
//!   production symptom (caller-hangup-while-ringing, reject→next,
//!   target-unreachable→next, exhausted→fallback, answer→bridge+cancel-others)
//!   is expressed as a deterministic transition here.
//!
//! * The executor layer (added later) maps [`Effect`]s onto the real
//!   `MediaPeer` / dialog / `MediaMixer` APIs and feeds real SIP/timer events
//!   back in as [`GraphEvent`]s.
//!
//! See `docs/call-graph-design.md` for the full design rationale.

pub mod executor;
pub mod model;
pub mod reducer;

pub use executor::{QueueBackend, QueueController, QueueEventTx};
pub use model::{
    Effect, FallbackPlan, GraphConfig, GraphEvent, GraphPhase, HookPoint, NodeId, Strategy,
};
pub use reducer::QueueGraph;
