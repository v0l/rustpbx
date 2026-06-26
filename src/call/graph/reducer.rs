//! Pure, event-driven reducer for a queue call graph.
//!
//! [`QueueGraph`] owns the state machine; [`QueueGraph::start`] produces the
//! initial effects and [`QueueGraph::on_event`] reduces each subsequent
//! [`GraphEvent`] into a list of [`Effect`]s. It performs no I/O.
//!
//! Lifecycle behaviours (voice prompts, hold music) are **not** hard-coded
//! here. The reducer only announces [`HookPoint`]s at lifecycle transitions;
//! the backend runs whatever is bound to each point (a no-op if nothing). That
//! keeps prompts a data concern \u2014 adding one never touches the reducer.

use super::model::{
    Effect, FallbackPlan, GraphConfig, GraphEvent, GraphPhase, HookPoint, NodeId, Strategy,
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
    /// Currently ringing/pending callee nodes (\u22651 for parallel, \u22641 sequential).
    active: Vec<NodeId>,
    /// The connected callee node, once one answers.
    connected: Option<NodeId>,
}

impl QueueGraph {
    pub fn new(config: GraphConfig) -> Self {
        Self {
            config,
            phase: GraphPhase::Idle,
            cursor: 0,
            active: Vec::new(),
            connected: None,
        }
    }

    pub fn phase(&self) -> GraphPhase {
        self.phase
    }

    pub fn connected(&self) -> Option<&NodeId> {
        self.connected.as_ref()
    }

    /// True while we are dialing candidates or the fallback target.
    fn dialing(&self) -> bool {
        matches!(self.phase, GraphPhase::Hunting | GraphPhase::Fallback)
    }

    fn hook(fx: &mut Vec<Effect>, point: HookPoint) {
        fx.push(Effect::RunHook { point });
    }

    /// Produce the initial effects: answer/greeting/hold, then start the hunt.
    pub fn start(&mut self) -> Vec<Effect> {
        debug_assert_eq!(self.phase, GraphPhase::Idle, "start() called twice");
        let mut fx = Vec::new();
        self.phase = GraphPhase::Hunting;

        if self.config.accept_immediately {
            fx.push(Effect::AnswerCaller);
        }
        // Announce lifecycle points; the backend plays whatever is bound. The
        // greeting is blocking (the hunt waits for it); hold is background.
        Self::hook(&mut fx, HookPoint::Greeting);
        Self::hook(&mut fx, HookPoint::HoldStart);

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
            GraphEvent::CallerAnswered | GraphEvent::CalleeEarlyMedia { .. } => {}

            GraphEvent::CalleeRinging { .. } => {
                // Always relay; the backend suppresses the 180 if the caller is
                // already answered (greeting / hold / accept_immediately).
                if self.dialing() {
                    fx.push(Effect::RelayCallerRinging);
                }
            }

            GraphEvent::CalleeAnswered { node } => {
                if self.dialing() && self.is_active(&node) {
                    self.phase = GraphPhase::Connected;
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
                    Self::hook(&mut fx, HookPoint::HoldStop);
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
    /// \u2014 if we were already dialing the fallback \u2014 give up for good.
    fn after_target_lost(&mut self, fx: &mut Vec<Effect>) {
        if self.phase == GraphPhase::Fallback {
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

    /// All candidate targets are gone \u2014 run fallback or hang up.
    fn exhaust(&mut self, fx: &mut Vec<Effect>) {
        Self::hook(fx, HookPoint::HoldStop);
        match self.config.fallback {
            FallbackPlan::Hangup(code) => {
                // Play the no-answer / busy prompt (if bound) before hanging up.
                // The caller is still answered from the greeting / hold, so it
                // is audible; the hook plays to completion before the BYE.
                Self::hook(fx, HookPoint::NoAnswer);
                self.phase = GraphPhase::Ended;
                fx.push(Effect::HangupCaller { code });
                fx.push(Effect::EndGraph);
            }
            FallbackPlan::DialBridge => {
                self.phase = GraphPhase::Fallback;
                Self::hook(fx, HookPoint::BeforeFallbackDial);
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
            FallbackPlan::Delegate => {
                // Hand control back to the host, which runs the existing
                // fallback machinery (prompts / re-enqueue / IVR / skill-group).
                self.phase = GraphPhase::Fallback;
                fx.push(Effect::EndGraph);
            }
        }
    }

    /// The caller went away. Cancel ringing forks, hang up a connected callee,
    /// stop hold, end the graph \u2014 immediately and deterministically.
    fn teardown_caller_gone(&mut self, fx: &mut Vec<Effect>) {
        let active = std::mem::take(&mut self.active);
        for node in active {
            fx.push(Effect::CancelRingTimer { node: node.clone() });
            fx.push(Effect::CancelInvite { node });
        }
        if let Some(node) = self.connected.take() {
            fx.push(Effect::HangupCallee { node });
        }
        Self::hook(fx, HookPoint::HoldStop);
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
            fallback: FallbackPlan::Hangup(486),
        }
    }

    fn callee(i: usize) -> NodeId {
        NodeId::callee(i)
    }

    fn hook(p: HookPoint) -> Effect {
        Effect::RunHook { point: p }
    }

    #[test]
    fn start_answers_greeting_hold_and_dials_first_sequential() {
        let mut g = QueueGraph::new(cfg(Strategy::Sequential, 3));
        let fx = g.start();
        assert_eq!(
            fx,
            vec![
                Effect::AnswerCaller,
                hook(HookPoint::Greeting),
                hook(HookPoint::HoldStart),
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
                hook(HookPoint::HoldStop),
                Effect::Bridge {
                    a: NodeId::caller(),
                    b: callee(0)
                },
            ]
        );
        assert_eq!(g.phase(), GraphPhase::Connected);
        assert_eq!(g.connected(), Some(&callee(0)));
    }

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
    }

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

    #[test]
    fn target_unreachable_advances() {
        let mut g = QueueGraph::new(cfg(Strategy::Sequential, 2));
        g.start();
        let fx = g.on_event(GraphEvent::TargetUnreachable { idx: 0 });
        assert!(fx.iter().any(|e| matches!(e, Effect::DialTarget { idx: 1, .. })));
    }

    /// THE prod bug: caller hangs up while the callee is still ringing.
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
                hook(HookPoint::HoldStop),
                Effect::EndGraph,
            ]
        );
        assert_eq!(g.phase(), GraphPhase::Ended);
    }

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
                hook(HookPoint::HoldStop),
                Effect::EndGraph,
            ]
        );
        assert_eq!(g.phase(), GraphPhase::Ended);
    }

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

    /// No-answer hangup must play the no-answer hook before hanging up.
    #[test]
    fn exhausted_without_fallback_plays_noanswer_then_hangs_up() {
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
                hook(HookPoint::HoldStop),
                hook(HookPoint::NoAnswer),
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
                hook(HookPoint::HoldStop),
                hook(HookPoint::BeforeFallbackDial),
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

    #[test]
    fn exhausted_with_delegate_ends_in_fallback_phase() {
        let mut c = cfg(Strategy::Sequential, 1);
        c.fallback = FallbackPlan::Delegate;
        let mut g = QueueGraph::new(c);
        g.start();
        let fx = g.on_event(GraphEvent::CalleeRejected {
            node: callee(0),
            code: 480,
        });
        assert!(!fx.iter().any(|e| matches!(e, Effect::HangupCaller { .. })));
        assert!(!fx.iter().any(|e| matches!(e, Effect::DialFallback { .. })));
        assert!(fx.contains(&Effect::EndGraph));
        assert_eq!(g.phase(), GraphPhase::Fallback);
    }

    /// Ringing always emits a relay; the backend decides whether to send 180.
    #[test]
    fn ringing_emits_relay() {
        let mut g = QueueGraph::new(cfg(Strategy::Sequential, 1));
        g.start();
        let fx = g.on_event(GraphEvent::CalleeRinging { node: callee(0) });
        assert_eq!(fx, vec![Effect::RelayCallerRinging]);
    }

    // --- parallel hunt ----------------------------------------------------

    #[test]
    fn parallel_dials_all_targets() {
        let mut g = QueueGraph::new(cfg(Strategy::Parallel, 3));
        let fx = g.start();
        let dials = fx
            .iter()
            .filter(|e| matches!(e, Effect::DialTarget { .. }))
            .count();
        assert_eq!(dials, 3);
    }

    #[test]
    fn parallel_answer_cancels_other_forks() {
        let mut g = QueueGraph::new(cfg(Strategy::Parallel, 3));
        g.start();
        let fx = g.on_event(GraphEvent::CalleeAnswered { node: callee(1) });
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

    #[test]
    fn events_after_end_are_ignored() {
        let mut g = QueueGraph::new(cfg(Strategy::Sequential, 1));
        g.start();
        g.on_event(GraphEvent::CallerBye);
        assert_eq!(g.phase(), GraphPhase::Ended);
        assert!(g
            .on_event(GraphEvent::CalleeAnswered { node: callee(0) })
            .is_empty());
        assert!(g
            .on_event(GraphEvent::RingTimeout { node: callee(0) })
            .is_empty());
    }
}
