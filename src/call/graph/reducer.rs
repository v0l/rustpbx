//! Pure, event-driven reducer for a queue call graph.
//!
//! [`QueueGraph`] owns the state machine; [`QueueGraph::start`] produces the
//! initial effects and [`QueueGraph::on_event`] reduces each subsequent
//! [`GraphEvent`] into a list of [`Effect`]s. It performs no I/O.
//!
//! Design contract (the reason this exists):
//! * Caller hangup is an **event**, not a poll — teardown is immediate and
//!   deterministic regardless of where in the hunt we are.
//! * Reject / ring-timeout / unreachable are uniform events that advance the
//!   hunt with no hidden waits.
//! * Exhausting all targets transitions explicitly to fallback (or a clean
//!   busy hangup), never to silence.

use super::model::{
    Effect, FallbackPlan, GraphConfig, GraphEvent, GraphPhase, NodeId, PlayerKind, Strategy,
};

/// Status code used for the default "all targets unavailable" hangup (BusyHere).
const BUSY_HERE: u16 = 486;

/// Event-driven queue call graph.
#[derive(Debug, Clone)]
pub struct QueueGraph {
    config: GraphConfig,
    phase: GraphPhase,
    /// Next candidate index to dial (sequential cursor).
    cursor: usize,
    /// Currently ringing/pending callee nodes (≥1 for parallel, ≤1 sequential).
    active: Vec<NodeId>,
    /// The connected callee node, once one answers.
    connected: Option<NodeId>,
    /// Whether the caller leg has been answered (200 OK sent). When false and
    /// still dialing, a callee ringing relays 180 to the caller.
    caller_answered: bool,
}

impl QueueGraph {
    pub fn new(config: GraphConfig) -> Self {
        Self {
            config,
            phase: GraphPhase::Idle,
            cursor: 0,
            active: Vec::new(),
            connected: None,
            caller_answered: false,
        }
    }

    /// True while we are dialing candidates or the fallback target.
    fn dialing(&self) -> bool {
        matches!(self.phase, GraphPhase::Hunting | GraphPhase::Fallback)
    }

    pub fn phase(&self) -> GraphPhase {
        self.phase
    }

    pub fn connected(&self) -> Option<&NodeId> {
        self.connected.as_ref()
    }

    /// Produce the initial effects: answer/hold, then start the hunt.
    pub fn start(&mut self) -> Vec<Effect> {
        debug_assert_eq!(self.phase, GraphPhase::Idle, "start() called twice");
        let mut fx = Vec::new();
        self.phase = GraphPhase::Hunting;

        if self.config.accept_immediately {
            fx.push(Effect::AnswerCaller);
        }
        if self.config.has_hold_music {
            fx.push(Effect::StartPlayer {
                kind: PlayerKind::Hold,
            });
        }
        // Both an immediate answer and hold music answer the caller leg, so a
        // 180 ringback relay is only meaningful when neither is configured.
        self.caller_answered = self.config.accept_immediately || self.config.has_hold_music;

        if self.config.target_count == 0 {
            self.exhaust(&mut fx);
            return fx;
        }

        match self.config.strategy {
            Strategy::Sequential => {
                self.dial(self.cursor, &mut fx);
                self.cursor += 1;
            }
            Strategy::Parallel => {
                for idx in 0..self.config.target_count {
                    self.dial(idx, &mut fx);
                }
                self.cursor = self.config.target_count;
            }
        }
        fx
    }

    /// Reduce a single event into effects.
    pub fn on_event(&mut self, event: GraphEvent) -> Vec<Effect> {
        let mut fx = Vec::new();
        match event {
            // Provisional / informational — no state change here. The executor
            // relays 180/183 to the caller leg directly.
            GraphEvent::CallerAnswered | GraphEvent::CalleeEarlyMedia { .. } => {}

            GraphEvent::CalleeRinging { .. } => {
                if self.dialing() && !self.caller_answered {
                    fx.push(Effect::RelayCallerRinging);
                }
            }

            GraphEvent::CalleeAnswered { node } => {
                if self.dialing() && self.is_active(&node) {
                    self.phase = GraphPhase::Connected;
                    self.caller_answered = true;
                    fx.push(Effect::CancelRingTimer { node: node.clone() });
                    // Cancel every other still-ringing fork (parallel hunt).
                    for other in self.active.iter().filter(|n| **n != node) {
                        fx.push(Effect::CancelRingTimer {
                            node: (*other).clone(),
                        });
                        fx.push(Effect::CancelInvite {
                            node: (*other).clone(),
                        });
                    }
                    if self.config.has_hold_music {
                        fx.push(Effect::StopPlayer {
                            kind: PlayerKind::Hold,
                        });
                    }
                    fx.push(Effect::Bridge {
                        a: NodeId::caller(),
                        b: node.clone(),
                    });
                    self.active.clear();
                    self.connected = Some(node);
                }
            }

            GraphEvent::CalleeRejected { node, .. } | GraphEvent::CalleeFailed { node } => {
                if self.dialing() && self.is_active(&node) {
                    fx.push(Effect::CancelRingTimer { node: node.clone() });
                    self.remove_active(&node);
                    self.after_target_lost(&mut fx);
                }
            }

            GraphEvent::RingTimeout { node } => {
                if self.dialing() && self.is_active(&node) {
                    // The INVITE is still ringing; cancel it.
                    fx.push(Effect::CancelInvite { node: node.clone() });
                    self.remove_active(&node);
                    self.after_target_lost(&mut fx);
                }
            }

            GraphEvent::TargetUnreachable { idx } => {
                if self.dialing() {
                    let node = NodeId::callee(idx);
                    if self.is_active(&node) {
                        fx.push(Effect::CancelRingTimer { node: node.clone() });
                        self.remove_active(&node);
                    }
                    self.after_target_lost(&mut fx);
                }
            }

            GraphEvent::CalleeBye { node } => {
                // The connected callee hung up → cascade to the caller.
                if self.phase == GraphPhase::Connected && self.connected.as_ref() == Some(&node) {
                    fx.push(Effect::ClearRoutes { node: node.clone() });
                    fx.push(Effect::HangupCaller { code: BUSY_HERE });
                    fx.push(Effect::EndGraph);
                    self.phase = GraphPhase::Ended;
                    self.connected = None;
                }
            }

            GraphEvent::CallerBye | GraphEvent::CallerCancel => {
                if self.phase != GraphPhase::Ended {
                    self.teardown_caller_gone(&mut fx);
                    self.phase = GraphPhase::Ended;
                }
            }
        }
        fx
    }

    // --- internals ---------------------------------------------------------

    fn dial(&mut self, idx: usize, fx: &mut Vec<Effect>) {
        let node = NodeId::callee(idx);
        fx.push(Effect::DialTarget {
            node: node.clone(),
            idx,
        });
        if let Some(timeout) = self.config.ring_timeout {
            fx.push(Effect::StartRingTimer {
                node: node.clone(),
                timeout,
            });
        }
        self.active.push(node);
    }

    /// A target was lost (reject/timeout/unreachable). Advance the hunt, or
    /// — if we were already dialing the fallback — give up for good.
    fn after_target_lost(&mut self, fx: &mut Vec<Effect>) {
        if self.phase == GraphPhase::Fallback {
            // The fallback target itself failed; nothing left to try.
            self.phase = GraphPhase::Ended;
            fx.push(Effect::HangupCaller { code: BUSY_HERE });
            fx.push(Effect::EndGraph);
            return;
        }
        match self.config.strategy {
            Strategy::Sequential => {
                if self.cursor < self.config.target_count {
                    let idx = self.cursor;
                    self.dial(idx, fx);
                    self.cursor += 1;
                } else {
                    self.exhaust(fx);
                }
            }
            Strategy::Parallel => {
                if self.active.is_empty() {
                    self.exhaust(fx);
                }
            }
        }
    }

    /// All candidate targets are gone — run fallback or hang up.
    fn exhaust(&mut self, fx: &mut Vec<Effect>) {
        if self.config.has_hold_music {
            fx.push(Effect::StopPlayer {
                kind: PlayerKind::Hold,
            });
        }
        match self.config.fallback {
            FallbackPlan::Hangup(code) => {
                self.phase = GraphPhase::Ended;
                fx.push(Effect::HangupCaller { code });
                fx.push(Effect::EndGraph);
            }
            FallbackPlan::DialBridge => {
                self.phase = GraphPhase::Fallback;
                let node = NodeId::fallback();
                fx.push(Effect::DialFallback { node: node.clone() });
                if let Some(timeout) = self.config.ring_timeout {
                    fx.push(Effect::StartRingTimer {
                        node: node.clone(),
                        timeout,
                    });
                }
                self.active.push(node);
            }
        }
    }

    /// The caller went away. Cancel ringing forks, hang up a connected callee,
    /// stop players, end the graph — immediately and deterministically.
    fn teardown_caller_gone(&mut self, fx: &mut Vec<Effect>) {
        let active = std::mem::take(&mut self.active);
        for node in active {
            fx.push(Effect::CancelRingTimer { node: node.clone() });
            fx.push(Effect::CancelInvite { node });
        }
        if let Some(node) = self.connected.take() {
            fx.push(Effect::HangupCallee { node });
        }
        if self.config.has_hold_music && self.phase == GraphPhase::Hunting {
            fx.push(Effect::StopPlayer {
                kind: PlayerKind::Hold,
            });
        }
        fx.push(Effect::EndGraph);
    }

    fn is_active(&self, node: &NodeId) -> bool {
        self.active.iter().any(|n| n == node)
    }

    fn remove_active(&mut self, node: &NodeId) {
        self.active.retain(|n| n != node);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn cfg(strategy: Strategy, n: usize) -> GraphConfig {
        GraphConfig {
            strategy,
            target_count: n,
            ring_timeout: Some(Duration::from_secs(20)),
            accept_immediately: true,
            has_hold_music: true,
            fallback: FallbackPlan::Hangup(486),
        }
    }

    fn callee(i: usize) -> NodeId {
        NodeId::callee(i)
    }

    #[test]
    fn start_answers_holds_and_dials_first_sequential() {
        let mut g = QueueGraph::new(cfg(Strategy::Sequential, 3));
        let fx = g.start();
        assert_eq!(
            fx,
            vec![
                Effect::AnswerCaller,
                Effect::StartPlayer {
                    kind: PlayerKind::Hold
                },
                Effect::DialTarget {
                    node: callee(0),
                    idx: 0
                },
                Effect::StartRingTimer {
                    node: callee(0),
                    timeout: Duration::from_secs(20)
                },
            ]
        );
        assert_eq!(g.phase(), GraphPhase::Hunting);
    }

    #[test]
    fn answer_bridges_stops_hold_and_cancels_timer() {
        let mut g = QueueGraph::new(cfg(Strategy::Sequential, 3));
        g.start();
        let fx = g.on_event(GraphEvent::CalleeAnswered { node: callee(0) });
        assert_eq!(
            fx,
            vec![
                Effect::CancelRingTimer { node: callee(0) },
                Effect::StopPlayer {
                    kind: PlayerKind::Hold
                },
                Effect::Bridge {
                    a: NodeId::caller(),
                    b: callee(0)
                },
            ]
        );
        assert_eq!(g.phase(), GraphPhase::Connected);
        assert_eq!(g.connected(), Some(&callee(0)));
    }

    /// Reject must advance to the *next* target immediately — no waiting.
    #[test]
    fn reject_advances_to_next_target_sequential() {
        let mut g = QueueGraph::new(cfg(Strategy::Sequential, 3));
        g.start();
        let fx = g.on_event(GraphEvent::CalleeRejected {
            node: callee(0),
            code: 486,
        });
        assert_eq!(
            fx,
            vec![
                Effect::CancelRingTimer { node: callee(0) },
                Effect::DialTarget {
                    node: callee(1),
                    idx: 1
                },
                Effect::StartRingTimer {
                    node: callee(1),
                    timeout: Duration::from_secs(20)
                },
            ]
        );
        assert_eq!(g.phase(), GraphPhase::Hunting);
    }

    /// Ring timeout must CANCEL the still-ringing INVITE and advance.
    #[test]
    fn ring_timeout_cancels_invite_and_advances() {
        let mut g = QueueGraph::new(cfg(Strategy::Sequential, 2));
        g.start();
        let fx = g.on_event(GraphEvent::RingTimeout { node: callee(0) });
        assert_eq!(
            fx,
            vec![
                Effect::CancelInvite { node: callee(0) },
                Effect::DialTarget {
                    node: callee(1),
                    idx: 1
                },
                Effect::StartRingTimer {
                    node: callee(1),
                    timeout: Duration::from_secs(20)
                },
            ]
        );
    }

    /// De-registered target → explicit unreachable → advance, never silence.
    #[test]
    fn target_unreachable_advances() {
        let mut g = QueueGraph::new(cfg(Strategy::Sequential, 2));
        g.start();
        let fx = g.on_event(GraphEvent::TargetUnreachable { idx: 0 });
        assert!(fx.iter().any(|e| matches!(
            e,
            Effect::DialTarget { idx: 1, .. }
        )));
    }

    /// THE prod bug: caller hangs up while the callee is still ringing.
    /// The reducer must CANCEL the ringing INVITE immediately.
    #[test]
    fn caller_hangup_while_ringing_cancels_invite_now() {
        let mut g = QueueGraph::new(cfg(Strategy::Sequential, 3));
        g.start();
        let fx = g.on_event(GraphEvent::CallerCancel);
        assert_eq!(
            fx,
            vec![
                Effect::CancelRingTimer { node: callee(0) },
                Effect::CancelInvite { node: callee(0) },
                Effect::StopPlayer {
                    kind: PlayerKind::Hold
                },
                Effect::EndGraph,
            ]
        );
        assert_eq!(g.phase(), GraphPhase::Ended);
    }

    /// Caller hangs up an answered+bridged call → callee must get a BYE.
    #[test]
    fn caller_hangup_after_bridge_hangs_up_callee() {
        let mut g = QueueGraph::new(cfg(Strategy::Sequential, 3));
        g.start();
        g.on_event(GraphEvent::CalleeAnswered { node: callee(0) });
        let fx = g.on_event(GraphEvent::CallerBye);
        assert_eq!(
            fx,
            vec![
                Effect::HangupCallee { node: callee(0) },
                Effect::EndGraph,
            ]
        );
        assert_eq!(g.phase(), GraphPhase::Ended);
    }

    /// Connected callee hangs up → cascade releases the caller.
    #[test]
    fn callee_bye_releases_caller() {
        let mut g = QueueGraph::new(cfg(Strategy::Sequential, 3));
        g.start();
        g.on_event(GraphEvent::CalleeAnswered { node: callee(0) });
        let fx = g.on_event(GraphEvent::CalleeBye { node: callee(0) });
        assert_eq!(
            fx,
            vec![
                Effect::ClearRoutes { node: callee(0) },
                Effect::HangupCaller { code: 486 },
                Effect::EndGraph,
            ]
        );
        assert_eq!(g.phase(), GraphPhase::Ended);
    }

    #[test]
    fn exhausted_without_fallback_hangs_up_busy() {
        let mut g = QueueGraph::new(cfg(Strategy::Sequential, 1));
        g.start();
        let fx = g.on_event(GraphEvent::CalleeRejected {
            node: callee(0),
            code: 480,
        });
        assert_eq!(
            fx,
            vec![
                Effect::CancelRingTimer { node: callee(0) },
                Effect::StopPlayer {
                    kind: PlayerKind::Hold
                },
                Effect::HangupCaller { code: 486 },
                Effect::EndGraph,
            ]
        );
        assert_eq!(g.phase(), GraphPhase::Ended);
    }

    #[test]
    fn exhausted_with_dialbridge_fallback_dials_fallback() {
        let mut c = cfg(Strategy::Sequential, 1);
        c.fallback = FallbackPlan::DialBridge;
        let mut g = QueueGraph::new(c);
        g.start();
        let fx = g.on_event(GraphEvent::CalleeFailed { node: callee(0) });
        assert_eq!(
            fx,
            vec![
                Effect::CancelRingTimer { node: callee(0) },
                Effect::StopPlayer {
                    kind: PlayerKind::Hold
                },
                Effect::DialFallback {
                    node: NodeId::fallback()
                },
                Effect::StartRingTimer {
                    node: NodeId::fallback(),
                    timeout: Duration::from_secs(20)
                },
            ]
        );
        assert_eq!(g.phase(), GraphPhase::Fallback);
    }

    #[test]
    fn fallback_target_answers_and_bridges() {
        let mut c = cfg(Strategy::Sequential, 1);
        c.fallback = FallbackPlan::DialBridge;
        let mut g = QueueGraph::new(c);
        g.start();
        g.on_event(GraphEvent::CalleeFailed { node: callee(0) });
        let fx = g.on_event(GraphEvent::CalleeAnswered {
            node: NodeId::fallback(),
        });
        assert!(fx.contains(&Effect::Bridge {
            a: NodeId::caller(),
            b: NodeId::fallback()
        }));
        assert_eq!(g.phase(), GraphPhase::Connected);
    }

    #[test]
    fn fallback_target_failure_hangs_up_for_good() {
        let mut c = cfg(Strategy::Sequential, 1);
        c.fallback = FallbackPlan::DialBridge;
        let mut g = QueueGraph::new(c);
        g.start();
        g.on_event(GraphEvent::CalleeFailed { node: callee(0) });
        let fx = g.on_event(GraphEvent::CalleeRejected {
            node: NodeId::fallback(),
            code: 480,
        });
        assert!(fx.contains(&Effect::HangupCaller { code: 486 }));
        assert!(fx.contains(&Effect::EndGraph));
        assert_eq!(g.phase(), GraphPhase::Ended);
    }

    /// With no answer and no immediate-answer/hold, a ringing callee relays 180.
    #[test]
    fn ringing_relays_180_when_caller_unanswered() {
        let mut c = cfg(Strategy::Sequential, 1);
        c.accept_immediately = false;
        c.has_hold_music = false;
        let mut g = QueueGraph::new(c);
        g.start();
        let fx = g.on_event(GraphEvent::CalleeRinging { node: callee(0) });
        assert_eq!(fx, vec![Effect::RelayCallerRinging]);
    }

    /// When the caller is already answered (hold music), no 180 relay.
    #[test]
    fn ringing_no_relay_when_caller_answered() {
        let mut g = QueueGraph::new(cfg(Strategy::Sequential, 1));
        g.start();
        let fx = g.on_event(GraphEvent::CalleeRinging { node: callee(0) });
        assert!(fx.is_empty());
    }

    // --- parallel hunt ----------------------------------------------------

    #[test]
    fn parallel_dials_all_targets() {
        let mut g = QueueGraph::new(cfg(Strategy::Parallel, 3));
        let fx = g.start();
        let dials: Vec<_> = fx
            .iter()
            .filter(|e| matches!(e, Effect::DialTarget { .. }))
            .collect();
        assert_eq!(dials.len(), 3);
    }

    #[test]
    fn parallel_answer_cancels_other_forks() {
        let mut g = QueueGraph::new(cfg(Strategy::Parallel, 3));
        g.start();
        let fx = g.on_event(GraphEvent::CalleeAnswered { node: callee(1) });
        // Winner's timer cancelled, hold stopped, bridged; losers cancelled.
        assert!(fx.contains(&Effect::CancelInvite { node: callee(0) }));
        assert!(fx.contains(&Effect::CancelInvite { node: callee(2) }));
        assert!(fx.contains(&Effect::Bridge {
            a: NodeId::caller(),
            b: callee(1)
        }));
        assert!(!fx.contains(&Effect::CancelInvite { node: callee(1) }));
        assert_eq!(g.phase(), GraphPhase::Connected);
    }

    #[test]
    fn parallel_exhausts_only_when_all_forks_lost() {
        let mut g = QueueGraph::new(cfg(Strategy::Parallel, 2));
        g.start();
        let fx = g.on_event(GraphEvent::CalleeRejected {
            node: callee(0),
            code: 486,
        });
        // One fork still ringing → no exhaust yet.
        assert!(!fx.iter().any(|e| matches!(e, Effect::HangupCaller { .. })));
        assert_eq!(g.phase(), GraphPhase::Hunting);

        let fx = g.on_event(GraphEvent::CalleeRejected {
            node: callee(1),
            code: 486,
        });
        assert!(fx.contains(&Effect::HangupCaller { code: 486 }));
        assert_eq!(g.phase(), GraphPhase::Ended);
    }

    #[test]
    fn no_targets_goes_straight_to_exhaust() {
        let mut g = QueueGraph::new(cfg(Strategy::Sequential, 0));
        let fx = g.start();
        assert!(fx.contains(&Effect::HangupCaller { code: 486 }));
        assert_eq!(g.phase(), GraphPhase::Ended);
    }

    /// Stale events after termination must be inert.
    #[test]
    fn events_after_end_are_ignored() {
        let mut g = QueueGraph::new(cfg(Strategy::Sequential, 1));
        g.start();
        g.on_event(GraphEvent::CallerBye);
        assert_eq!(g.phase(), GraphPhase::Ended);
        let fx = g.on_event(GraphEvent::CalleeAnswered { node: callee(0) });
        assert!(fx.is_empty());
        let fx = g.on_event(GraphEvent::RingTimeout { node: callee(0) });
        assert!(fx.is_empty());
    }
}
