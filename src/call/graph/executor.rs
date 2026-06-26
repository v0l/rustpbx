//! Event-driven controller that drives the pure [`QueueGraph`] reducer against
//! a side-effecting backend.
//!
//! ## Separation of concerns
//!
//! * [`QueueGraph`](super::reducer::QueueGraph) — pure decision logic.
//! * [`QueueController`] — owns the event loop and ring timers, translates
//!   reducer [`Effect`]s into [`QueueBackend`] calls. No SIP, no media.
//! * [`QueueBackend`] — the *port*: the minimal set of side effects the queue
//!   needs. Implemented by a fake (tests) and by a `SipSession` adapter (prod).
//!
//! The controller is the higher layer the design calls for: it owns *both* the
//! caller and callee event streams (fed in as [`GraphEvent`]s) plus its own
//! timers, so caller-hangup / reject / timeout are all first-class events —
//! never polled.

use super::model::{Effect, GraphEvent, GraphPhase, HookPoint, NodeId};
use super::reducer::QueueGraph;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// The side effects the queue controller needs from the outside world.
///
/// Timers are intentionally *not* here — the controller owns them and surfaces
/// expiry as a [`GraphEvent::RingTimeout`]. Likewise `EndGraph` is handled by
/// the controller (it stops the loop), not the backend.
#[async_trait]
pub trait QueueBackend: Send {
    /// Send INVITE to candidate target `idx`, tracked under `node`.
    async fn dial_target(&mut self, node: &NodeId, idx: usize) -> Result<()>;
    /// Send INVITE to the configured fallback target, tracked under `node`.
    async fn dial_fallback(&mut self, node: &NodeId) -> Result<()>;
    /// Relay a 180 Ringing to the (un-answered) caller.
    async fn relay_caller_ringing(&mut self);
    /// Cancel a still-ringing (un-answered) callee INVITE.
    async fn cancel_invite(&mut self, node: &NodeId);
    /// Send BYE to a connected callee dialog.
    async fn hangup_callee(&mut self, node: &NodeId);
    /// Answer the caller leg (200 OK).
    async fn answer_caller(&mut self);
    /// Release the caller leg (BYE if answered, else final response `code`).
    async fn hangup_caller(&mut self, code: u16);
    /// Run whatever behaviour is bound to a lifecycle point (voice prompt, hold
    /// music, …). A no-op if nothing is bound. Blocking points (greeting,
    /// no-answer, before-fallback) play to completion before returning.
    async fn run_hook(&mut self, point: HookPoint);
    /// Bridge two nodes' audio bidirectionally.
    async fn bridge(&mut self, a: &NodeId, b: &NodeId);
    /// Remove all audio edges touching `node`.
    async fn clear_routes(&mut self, node: &NodeId);
}

/// Handle used to feed external (SIP) events into a running controller.
#[derive(Clone)]
pub struct QueueEventTx(mpsc::UnboundedSender<GraphEvent>);

impl QueueEventTx {
    pub fn send(&self, event: GraphEvent) {
        let _ = self.0.send(event);
    }
}

/// Source of *external* events (SIP dialog state, caller hangup, …).
///
/// This is the clean seam between the generic controller and the SIP world: a
/// translator implements it by reading the caller/callee dialog channels and
/// mapping `DialogState` → [`GraphEvent`], **without** borrowing the session
/// the backend mutates. Returning `None` means "no more external events" (the
/// controller then runs only on its internal timers until the graph ends).
#[async_trait]
pub trait EventSource: Send {
    async fn next(&mut self) -> Option<GraphEvent>;
}

/// An [`EventSource`] backed by an mpsc receiver — used by tests and as a
/// generic adapter when a translator prefers to push rather than pull.
pub struct ChannelEventSource(pub mpsc::UnboundedReceiver<GraphEvent>);

#[async_trait]
impl EventSource for ChannelEventSource {
    async fn next(&mut self) -> Option<GraphEvent> {
        self.0.recv().await
    }
}

/// Drives a [`QueueGraph`] against a [`QueueBackend`], owning ring timers and
/// the unified event loop.
pub struct QueueController<B: QueueBackend> {
    graph: QueueGraph,
    backend: B,
    event_tx: mpsc::UnboundedSender<GraphEvent>,
    event_rx: mpsc::UnboundedReceiver<GraphEvent>,
    ring_timers: HashMap<NodeId, CancellationToken>,
    ended: bool,
}

impl<B: QueueBackend> QueueController<B> {
    pub fn new(graph: QueueGraph, backend: B) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        Self {
            graph,
            backend,
            event_tx,
            event_rx,
            ring_timers: HashMap::new(),
            ended: false,
        }
    }

    /// A sender external producers (the SIP adapter) use to inject events.
    pub fn event_tx(&self) -> QueueEventTx {
        QueueEventTx(self.event_tx.clone())
    }

    /// Run to completion, drawing external events from `source` and internal
    /// ring-timer expiries from the controller's own timer channel.
    ///
    /// Returns when the graph reaches a terminal state (`EndGraph`). If the
    /// external source dries up before then, the controller keeps running on
    /// timers alone so an in-flight hunt can still time out cleanly.
    pub async fn run(mut self, mut source: impl EventSource) -> Result<GraphPhase> {
        let fx = self.graph.start();
        self.apply(fx).await;

        let mut source_done = false;
        while !self.ended {
            let event = tokio::select! {
                ext = source.next(), if !source_done => match ext {
                    Some(ev) => ev,
                    None => { source_done = true; continue; }
                },
                Some(ev) = self.event_rx.recv() => ev,
            };
            debug!(?event, phase = ?self.graph.phase(), "queue graph event");
            let fx = self.graph.on_event(event);
            self.apply(fx).await;
        }
        // Cancel any lingering timers.
        for (_, token) in self.ring_timers.drain() {
            token.cancel();
        }
        Ok(self.graph.phase())
    }

    async fn apply(&mut self, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::DialTarget { node, idx } => {
                    if let Err(e) = self.backend.dial_target(&node, idx).await {
                        warn!(%node, idx, error = %e, "dial_target failed; surfacing as failure");
                        // Treat a dial-setup failure as a lost target so the hunt
                        // advances instead of stalling.
                        self.event_tx
                            .send(GraphEvent::CalleeFailed { node })
                            .ok();
                    }
                }
                Effect::CancelInvite { node } => self.backend.cancel_invite(&node).await,
                Effect::HangupCallee { node } => self.backend.hangup_callee(&node).await,
                Effect::AnswerCaller => self.backend.answer_caller().await,
                Effect::HangupCaller { code } => self.backend.hangup_caller(code).await,
                Effect::StartRingTimer { node, timeout } => self.arm_ring_timer(node, timeout),
                Effect::CancelRingTimer { node } => {
                    if let Some(token) = self.ring_timers.remove(&node) {
                        token.cancel();
                    }
                }
                Effect::RunHook { point } => self.backend.run_hook(point).await,
                Effect::Bridge { a, b } => self.backend.bridge(&a, &b).await,
                Effect::ClearRoutes { node } => self.backend.clear_routes(&node).await,
                Effect::DialFallback { node } => {
                    if let Err(e) = self.backend.dial_fallback(&node).await {
                        warn!(%node, error = %e, "dial_fallback failed; surfacing as failure");
                        self.event_tx.send(GraphEvent::CalleeFailed { node }).ok();
                    }
                }
                Effect::RelayCallerRinging => self.backend.relay_caller_ringing().await,
                Effect::EndGraph => self.ended = true,
            }
        }
    }

    fn arm_ring_timer(&mut self, node: NodeId, timeout: Duration) {
        // Replace any existing timer for this node.
        if let Some(old) = self.ring_timers.remove(&node) {
            old.cancel();
        }
        let token = CancellationToken::new();
        self.ring_timers.insert(node.clone(), token.clone());
        let tx = self.event_tx.clone();
        let timer_node = node.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = token.cancelled() => {}
                _ = tokio::time::sleep(timeout) => {
                    tx.send(GraphEvent::RingTimeout { node: timer_node }).ok();
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::call::graph::model::{GraphConfig, Strategy};
    use std::sync::{Arc, Mutex};

    /// A backend that records every call so we can assert the orchestration.
    #[derive(Clone, Default)]
    struct FakeBackend {
        log: Arc<Mutex<Vec<String>>>,
    }

    impl FakeBackend {
        fn calls(&self) -> Vec<String> {
            self.log.lock().unwrap().clone()
        }
        fn push(&self, s: impl Into<String>) {
            self.log.lock().unwrap().push(s.into());
        }
    }

    #[async_trait]
    impl QueueBackend for FakeBackend {
        async fn dial_target(&mut self, node: &NodeId, idx: usize) -> Result<()> {
            self.push(format!("dial {node} idx={idx}"));
            Ok(())
        }
        async fn cancel_invite(&mut self, node: &NodeId) {
            self.push(format!("cancel {node}"));
        }
        async fn hangup_callee(&mut self, node: &NodeId) {
            self.push(format!("hangup_callee {node}"));
        }
        async fn answer_caller(&mut self) {
            self.push("answer_caller");
        }
        async fn hangup_caller(&mut self, code: u16) {
            self.push(format!("hangup_caller {code}"));
        }
        async fn run_hook(&mut self, point: HookPoint) {
            self.push(format!("hook {point:?}"));
        }
        async fn bridge(&mut self, a: &NodeId, b: &NodeId) {
            self.push(format!("bridge {a} {b}"));
        }
        async fn clear_routes(&mut self, node: &NodeId) {
            self.push(format!("clear_routes {node}"));
        }
        async fn dial_fallback(&mut self, node: &NodeId) -> Result<()> {
            self.push(format!("dial_fallback {node}"));
            Ok(())
        }
        async fn relay_caller_ringing(&mut self) {
            self.push("relay_caller_ringing");
        }
    }

    fn cfg(strategy: Strategy, n: usize, ring: Option<Duration>) -> GraphConfig {
        GraphConfig {
            strategy,
            target_count: n,
            ring_timeout: ring,
            accept_immediately: true,
            fallback: crate::call::graph::model::FallbackPlan::Hangup(486),
        }
    }

    fn external() -> (QueueEventTx, ChannelEventSource) {
        let (tx, rx) = mpsc::unbounded_channel();
        (QueueEventTx(tx), ChannelEventSource(rx))
    }

    #[tokio::test]
    async fn answer_then_caller_hangup_drives_full_lifecycle() {
        let backend = FakeBackend::default();
        let graph = QueueGraph::new(cfg(Strategy::Sequential, 2, None));
        let controller = QueueController::new(graph, backend.clone());
        let (tx, source) = external();

        let handle = tokio::spawn(controller.run(source));

        // Let start() run, then drive: callee 0 answers, caller hangs up.
        tx.send(GraphEvent::CalleeAnswered {
            node: NodeId::callee(0),
        });
        tx.send(GraphEvent::CallerBye);

        handle.await.unwrap().unwrap();

        let calls = backend.calls();
        assert_eq!(
            calls,
            vec![
                "answer_caller".to_string(),
                "hook Greeting".to_string(),
                "hook HoldStart".to_string(),
                "dial callee-0 idx=0".to_string(),
                "hook HoldStop".to_string(),
                "bridge caller callee-0".to_string(),
                "hangup_callee callee-0".to_string(),
                "hook HoldStop".to_string(),
            ]
        );
    }

    /// THE prod bug as an end-to-end orchestration test: the controller's own
    /// ring timer fires, which must CANCEL the ringing invite and dial the next
    /// target — with no external event other than the passage of time.
    #[tokio::test]
    async fn ring_timer_fires_and_advances_without_external_event() {
        let backend = FakeBackend::default();
        let graph = QueueGraph::new(cfg(
            Strategy::Sequential,
            2,
            Some(Duration::from_millis(40)),
        ));
        let controller = QueueController::new(graph, backend.clone());
        let (tx, source) = external();
        let handle = tokio::spawn(controller.run(source));

        // Wait past the ring timeout; the controller's timer should fire on its
        // own and advance to target 1, which has no timeout-driven follow-up.
        tokio::time::sleep(Duration::from_millis(120)).await;
        // End the call so run() returns.
        tx.send(GraphEvent::CallerCancel);
        handle.await.unwrap().unwrap();

        let calls = backend.calls();
        assert!(calls.contains(&"dial callee-0 idx=0".to_string()));
        assert!(
            calls.contains(&"cancel callee-0".to_string()),
            "ring timeout must cancel the ringing invite: {calls:?}"
        );
        assert!(
            calls.contains(&"dial callee-1 idx=1".to_string()),
            "ring timeout must advance to next target: {calls:?}"
        );
    }

    /// A dial setup failure self-heals into a CalleeFailed event and advances.
    #[tokio::test]
    async fn dial_failure_advances_hunt() {
        #[derive(Clone, Default)]
        struct FailFirst {
            log: Arc<Mutex<Vec<String>>>,
        }
        #[async_trait]
        impl QueueBackend for FailFirst {
            async fn dial_target(&mut self, node: &NodeId, idx: usize) -> Result<()> {
                self.log.lock().unwrap().push(format!("dial {node}"));
                if idx == 0 {
                    anyhow::bail!("no contact");
                }
                Ok(())
            }
            async fn cancel_invite(&mut self, _n: &NodeId) {}
            async fn hangup_callee(&mut self, _n: &NodeId) {}
            async fn answer_caller(&mut self) {}
            async fn hangup_caller(&mut self, _c: u16) {}
            async fn run_hook(&mut self, _p: HookPoint) {}
            async fn bridge(&mut self, _a: &NodeId, _b: &NodeId) {}
            async fn clear_routes(&mut self, _n: &NodeId) {}
            async fn dial_fallback(&mut self, _n: &NodeId) -> Result<()> {
                Ok(())
            }
            async fn relay_caller_ringing(&mut self) {}
        }

        let backend = FailFirst::default();
        let log = backend.log.clone();
        let graph = QueueGraph::new(cfg(Strategy::Sequential, 2, None));
        let controller = QueueController::new(graph, backend);
        let (tx, source) = external();
        let handle = tokio::spawn(controller.run(source));
        tokio::time::sleep(Duration::from_millis(30)).await;
        tx.send(GraphEvent::CallerCancel);
        handle.await.unwrap().unwrap();

        let calls = log.lock().unwrap().clone();
        assert_eq!(
            calls,
            vec!["dial callee-0".to_string(), "dial callee-1".to_string()],
            "dial failure on target 0 must advance to target 1"
        );
    }
}
