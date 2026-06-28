//! The call reducer — the pure decision core above the switch.
//!
//! `reduce(event) -> [effect]` is a pure state machine: it reads the dialplan
//! config, tracks where it is in executing it, consumes [`Event`]s (what
//! happened on the legs / from the caller), and emits [`Effect`]s (what the
//! switch should do). No I/O, no sessions, no media — just the logic, so it is
//! exhaustively unit-testable. The executor (later) maps each `Effect` onto
//! `SipSession`/`CallSwitch`/`Mixer` calls.
//!
//! This first cut covers the foundational `Targets` flow (sequential +
//! parallel) — the nucleus the rest of the dialplan (queue fallback,
//! application) extends. See `docs/call-port-design.md` §7.2.

/// A callee target, identified by its position in the dialplan's target list.
pub type TargetIdx = usize;

/// How to try the targets — the config the reducer reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    /// Try one target at a time, in order, until one answers.
    Sequential,
    /// Ring all targets at once; first to answer wins, cancel the rest.
    Parallel,
}

/// What happened — the reducer's input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    /// Begin executing the flow.
    Start,
    /// A callee target answered (200 OK).
    CalleeAnswered { target: TargetIdx },
    /// A callee target failed (rejected / transport error).
    CalleeFailed { target: TargetIdx },
    /// A callee target rang past its timeout without answering.
    RingTimeout { target: TargetIdx },
    /// The caller hung up an answered call (BYE).
    CallerHangup,
    /// The caller cancelled before the call was answered (CANCEL).
    CallerCancel,
}

/// What the switch should do — the reducer's output. The executor performs
/// these against real `Session`/`Mixer` APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    /// Originate an INVITE to target `target`.
    Dial { target: TargetIdx },
    /// Cancel a still-ringing (un-answered) callee INVITE.
    CancelDial { target: TargetIdx },
    /// Send BYE to a connected callee.
    HangupCallee { target: TargetIdx },
    /// Answer the caller leg (200 OK).
    AnswerCaller,
    /// Hang up the caller leg with a SIP status.
    HangupCaller { code: u16 },
    /// Bridge the caller with the connected callee `target`.
    Bridge { target: TargetIdx },
    /// The flow has reached a terminal state; tear the call down.
    End,
}

/// SIP status used when every target is exhausted.
const NO_TARGET_AVAILABLE: u16 = 486; // Busy Here

#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    /// Not started yet.
    Idle,
    /// Sequential: ringing exactly this target.
    Dialing(TargetIdx),
    /// Parallel: these targets are still outstanding (ringing).
    Forking(Vec<TargetIdx>),
    /// Connected: caller bridged with this target.
    Bridged(TargetIdx),
    /// Done.
    Ended,
}

/// Pure reducer for the `Targets` dialplan flow.
#[derive(Debug, Clone)]
pub struct TargetsReducer {
    strategy: Strategy,
    count: usize,
    phase: Phase,
}

impl TargetsReducer {
    pub fn new(strategy: Strategy, count: usize) -> Self {
        Self {
            strategy,
            count,
            phase: Phase::Idle,
        }
    }

    /// Build from a real `DialStrategy` (the dialplan's target list).
    pub fn from_dial_strategy(strategy: &crate::call::DialStrategy) -> Self {
        use crate::call::DialStrategy as D;
        match strategy {
            D::Sequential(t) => Self::new(Strategy::Sequential, t.len()),
            D::Parallel(t) => Self::new(Strategy::Parallel, t.len()),
        }
    }

    /// Whether the flow has terminated.
    pub fn is_ended(&self) -> bool {
        self.phase == Phase::Ended
    }

    /// Advance the state machine by one event, returning the effects to apply.
    pub fn reduce(&mut self, event: Event) -> Vec<Effect> {
        match (&self.phase, event) {
            // ── Start ────────────────────────────────────────────────────────
            (Phase::Idle, Event::Start) => {
                if self.count == 0 {
                    self.phase = Phase::Ended;
                    return vec![Effect::HangupCaller { code: NO_TARGET_AVAILABLE }, Effect::End];
                }
                match self.strategy {
                    Strategy::Sequential => {
                        self.phase = Phase::Dialing(0);
                        vec![Effect::Dial { target: 0 }]
                    }
                    Strategy::Parallel => {
                        let all: Vec<TargetIdx> = (0..self.count).collect();
                        self.phase = Phase::Forking(all.clone());
                        all.into_iter().map(|target| Effect::Dial { target }).collect()
                    }
                }
            }

            // ── A target answered ────────────────────────────────────────────
            (Phase::Dialing(t), Event::CalleeAnswered { target }) if *t == target => {
                self.phase = Phase::Bridged(target);
                vec![Effect::AnswerCaller, Effect::Bridge { target }]
            }
            (Phase::Forking(outstanding), Event::CalleeAnswered { target })
                if outstanding.contains(&target) =>
            {
                // Winner answers: bridge it, cancel every other still-ringing leg.
                let losers: Vec<TargetIdx> =
                    outstanding.iter().copied().filter(|&o| o != target).collect();
                self.phase = Phase::Bridged(target);
                let mut effects = vec![Effect::AnswerCaller, Effect::Bridge { target }];
                effects.extend(losers.into_iter().map(|target| Effect::CancelDial { target }));
                effects
            }

            // ── A target failed / timed out ──────────────────────────────────
            (Phase::Dialing(t), Event::CalleeFailed { target })
            | (Phase::Dialing(t), Event::RingTimeout { target })
                if *t == target =>
            {
                let next = target + 1;
                if next < self.count {
                    self.phase = Phase::Dialing(next);
                    vec![Effect::Dial { target: next }]
                } else {
                    self.phase = Phase::Ended;
                    vec![Effect::HangupCaller { code: NO_TARGET_AVAILABLE }, Effect::End]
                }
            }
            (Phase::Forking(outstanding), Event::CalleeFailed { target })
            | (Phase::Forking(outstanding), Event::RingTimeout { target })
                if outstanding.contains(&target) =>
            {
                let remaining: Vec<TargetIdx> =
                    outstanding.iter().copied().filter(|&o| o != target).collect();
                if remaining.is_empty() {
                    self.phase = Phase::Ended;
                    vec![Effect::HangupCaller { code: NO_TARGET_AVAILABLE }, Effect::End]
                } else {
                    self.phase = Phase::Forking(remaining);
                    vec![] // keep waiting on the others
                }
            }

            // ── Caller leaves ────────────────────────────────────────────────
            (Phase::Bridged(t), Event::CallerHangup) => {
                let target = *t;
                self.phase = Phase::Ended;
                vec![Effect::HangupCallee { target }, Effect::End]
            }
            (Phase::Dialing(t), Event::CallerHangup)
            | (Phase::Dialing(t), Event::CallerCancel) => {
                let target = *t;
                self.phase = Phase::Ended;
                vec![Effect::CancelDial { target }, Effect::End]
            }
            (Phase::Forking(outstanding), Event::CallerHangup)
            | (Phase::Forking(outstanding), Event::CallerCancel) => {
                let cancels: Vec<Effect> = outstanding
                    .iter()
                    .copied()
                    .map(|target| Effect::CancelDial { target })
                    .collect();
                self.phase = Phase::Ended;
                cancels.into_iter().chain(std::iter::once(Effect::End)).collect()
            }

            // ── Anything else is a no-op (stale/duplicate event) ─────────────
            _ => vec![],
        }
    }
}

// ===========================================================================
// FlowReducer — chains dial stages (Queue agents → fallback `next` → …)
// ===========================================================================

/// One dialling stage: a set of targets tried with a strategy. A `Queue` flow's
/// agents are one stage; its `next` fallback is the following stage(s).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stage {
    pub strategy: Strategy,
    pub count: usize,
}

/// Reduces a *chain* of dial stages: try this stage's targets; on exhaustion,
/// fall through to the next stage instead of giving up. Each stage's targets
/// occupy a contiguous slice of the global target index space, so the executor
/// still dials by a single flat index.
#[derive(Debug, Clone)]
pub struct FlowReducer {
    stages: Vec<Stage>,
    cursor: usize,
    /// Global base index of the current stage's targets.
    offset: usize,
    inner: TargetsReducer,
    ended: bool,
}

/// Shift a stage's local-index effects into the global index space.
fn globalize(effects: Vec<Effect>, base: usize) -> Vec<Effect> {
    effects
        .into_iter()
        .map(|effect| match effect {
            Effect::Dial { target } => Effect::Dial { target: target + base },
            Effect::CancelDial { target } => Effect::CancelDial { target: target + base },
            Effect::HangupCallee { target } => Effect::HangupCallee { target: target + base },
            Effect::Bridge { target } => Effect::Bridge { target: target + base },
            other => other,
        })
        .collect()
}

impl FlowReducer {
    pub fn new(stages: Vec<Stage>) -> Self {
        let first = stages.first().copied().unwrap_or(Stage {
            strategy: Strategy::Sequential,
            count: 0,
        });
        Self {
            stages,
            cursor: 0,
            offset: 0,
            inner: TargetsReducer::new(first.strategy, first.count),
            ended: false,
        }
    }

    /// Build the stage chain from a real dialplan flow. `Queue { plan, next }`
    /// becomes the queue's agents stage followed by `next`'s stages.
    /// (Application is not a dial stage; it is handled separately — TODO.)
    pub fn from_flow(flow: &crate::call::DialplanFlow) -> Self {
        use crate::call::{DialStrategy as D, DialplanFlow as F};
        fn stage_of(s: &D) -> Stage {
            match s {
                D::Sequential(t) => Stage {
                    strategy: Strategy::Sequential,
                    count: t.len(),
                },
                D::Parallel(t) => Stage {
                    strategy: Strategy::Parallel,
                    count: t.len(),
                },
            }
        }
        let mut stages = Vec::new();
        let mut current = flow;
        loop {
            match current {
                F::Targets(s) => {
                    stages.push(stage_of(s));
                    break;
                }
                F::Queue { plan, next } => {
                    if let Some(s) = &plan.dial_strategy {
                        stages.push(stage_of(s));
                    }
                    current = next;
                }
                F::Application { .. } => break, // TODO: App stage
            }
        }
        Self::new(stages)
    }

    pub fn is_ended(&self) -> bool {
        self.ended
    }

    /// Translate a global event into the current stage's local index space,
    /// dropping callee events that belong to a different (already-past) stage.
    fn localize(&self, event: Event) -> Option<Event> {
        let count = self.stages.get(self.cursor).map(|s| s.count).unwrap_or(0);
        let in_range = |t: usize| t >= self.offset && t < self.offset + count;
        match event {
            Event::CalleeAnswered { target } if in_range(target) => Some(Event::CalleeAnswered {
                target: target - self.offset,
            }),
            Event::CalleeFailed { target } if in_range(target) => Some(Event::CalleeFailed {
                target: target - self.offset,
            }),
            Event::RingTimeout { target } if in_range(target) => Some(Event::RingTimeout {
                target: target - self.offset,
            }),
            // A callee event for a different stage is stale → ignore.
            Event::CalleeAnswered { .. } | Event::CalleeFailed { .. } | Event::RingTimeout { .. } => {
                None
            }
            // Start / CallerHangup / CallerCancel pass through unchanged.
            passthrough => Some(passthrough),
        }
    }

    pub fn reduce(&mut self, event: Event) -> Vec<Effect> {
        if self.ended {
            return vec![];
        }
        let Some(local) = self.localize(event) else {
            return vec![];
        };
        let base = self.offset;
        let effects = self.inner.reduce(local);

        // Exhausting the current stage (HangupCaller) falls through to the next
        // stage rather than ending the call.
        let exhausted = effects
            .iter()
            .any(|e| matches!(e, Effect::HangupCaller { .. }));
        if exhausted && self.cursor + 1 < self.stages.len() {
            self.offset += self.stages[self.cursor].count;
            self.cursor += 1;
            let stage = self.stages[self.cursor];
            self.inner = TargetsReducer::new(stage.strategy, stage.count);
            let start = self.inner.reduce(Event::Start);
            return globalize(start, self.offset);
        }

        if self.inner.is_ended() {
            self.ended = true;
        }
        globalize(effects, base)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(count: usize) -> TargetsReducer {
        TargetsReducer::new(Strategy::Sequential, count)
    }
    fn par(count: usize) -> TargetsReducer {
        TargetsReducer::new(Strategy::Parallel, count)
    }

    // ── Sequential ────────────────────────────────────────────────────────

    #[test]
    fn sequential_start_dials_first() {
        let mut r = seq(3);
        assert_eq!(r.reduce(Event::Start), vec![Effect::Dial { target: 0 }]);
    }

    #[test]
    fn sequential_answer_bridges() {
        let mut r = seq(3);
        r.reduce(Event::Start);
        assert_eq!(
            r.reduce(Event::CalleeAnswered { target: 0 }),
            vec![Effect::AnswerCaller, Effect::Bridge { target: 0 }]
        );
    }

    #[test]
    fn sequential_failure_advances_to_next() {
        let mut r = seq(3);
        r.reduce(Event::Start);
        assert_eq!(
            r.reduce(Event::CalleeFailed { target: 0 }),
            vec![Effect::Dial { target: 1 }]
        );
        assert_eq!(
            r.reduce(Event::RingTimeout { target: 1 }),
            vec![Effect::Dial { target: 2 }]
        );
    }

    #[test]
    fn sequential_last_failure_hangs_up_caller() {
        let mut r = seq(2);
        r.reduce(Event::Start);
        r.reduce(Event::CalleeFailed { target: 0 });
        assert_eq!(
            r.reduce(Event::CalleeFailed { target: 1 }),
            vec![Effect::HangupCaller { code: 486 }, Effect::End]
        );
        assert!(r.is_ended());
    }

    #[test]
    fn sequential_caller_hangup_after_answer_releases_callee() {
        let mut r = seq(2);
        r.reduce(Event::Start);
        r.reduce(Event::CalleeAnswered { target: 0 });
        assert_eq!(
            r.reduce(Event::CallerHangup),
            vec![Effect::HangupCallee { target: 0 }, Effect::End]
        );
    }

    #[test]
    fn sequential_caller_cancel_while_ringing_cancels_dial() {
        let mut r = seq(2);
        r.reduce(Event::Start);
        assert_eq!(
            r.reduce(Event::CallerCancel),
            vec![Effect::CancelDial { target: 0 }, Effect::End]
        );
    }

    #[test]
    fn no_targets_hangs_up_immediately() {
        let mut r = seq(0);
        assert_eq!(
            r.reduce(Event::Start),
            vec![Effect::HangupCaller { code: 486 }, Effect::End]
        );
    }

    // ── Parallel ──────────────────────────────────────────────────────────

    #[test]
    fn parallel_start_forks_all() {
        let mut r = par(3);
        assert_eq!(
            r.reduce(Event::Start),
            vec![
                Effect::Dial { target: 0 },
                Effect::Dial { target: 1 },
                Effect::Dial { target: 2 },
            ]
        );
    }

    #[test]
    fn parallel_first_answer_bridges_and_cancels_losers() {
        let mut r = par(3);
        r.reduce(Event::Start);
        assert_eq!(
            r.reduce(Event::CalleeAnswered { target: 1 }),
            vec![
                Effect::AnswerCaller,
                Effect::Bridge { target: 1 },
                Effect::CancelDial { target: 0 },
                Effect::CancelDial { target: 2 },
            ]
        );
    }

    #[test]
    fn parallel_one_failure_keeps_waiting() {
        let mut r = par(3);
        r.reduce(Event::Start);
        assert_eq!(r.reduce(Event::CalleeFailed { target: 0 }), vec![]);
        assert!(!r.is_ended());
        // a survivor can still answer
        assert_eq!(
            r.reduce(Event::CalleeAnswered { target: 2 }),
            vec![
                Effect::AnswerCaller,
                Effect::Bridge { target: 2 },
                Effect::CancelDial { target: 1 },
            ]
        );
    }

    #[test]
    fn parallel_all_failures_hang_up_caller() {
        let mut r = par(2);
        r.reduce(Event::Start);
        assert_eq!(r.reduce(Event::CalleeFailed { target: 0 }), vec![]);
        assert_eq!(
            r.reduce(Event::CalleeFailed { target: 1 }),
            vec![Effect::HangupCaller { code: 486 }, Effect::End]
        );
    }

    #[test]
    fn parallel_caller_cancel_cancels_all_outstanding() {
        let mut r = par(3);
        r.reduce(Event::Start);
        assert_eq!(
            r.reduce(Event::CallerCancel),
            vec![
                Effect::CancelDial { target: 0 },
                Effect::CancelDial { target: 1 },
                Effect::CancelDial { target: 2 },
                Effect::End,
            ]
        );
    }

    #[test]
    fn stale_events_after_bridge_are_noops() {
        let mut r = par(3);
        r.reduce(Event::Start);
        r.reduce(Event::CalleeAnswered { target: 0 });
        // A loser's late failure/answer after we've already bridged: ignored.
        assert_eq!(r.reduce(Event::CalleeFailed { target: 1 }), vec![]);
        assert_eq!(r.reduce(Event::CalleeAnswered { target: 2 }), vec![]);
    }

    // ── FlowReducer (stage chaining) ──────────────────────────────────────

    fn stage(strategy: Strategy, count: usize) -> Stage {
        Stage { strategy, count }
    }

    #[test]
    fn first_stage_answer_does_not_fall_through() {
        let mut r = FlowReducer::new(vec![
            stage(Strategy::Sequential, 1),
            stage(Strategy::Sequential, 1),
        ]);
        assert_eq!(r.reduce(Event::Start), vec![Effect::Dial { target: 0 }]);
        assert_eq!(
            r.reduce(Event::CalleeAnswered { target: 0 }),
            vec![Effect::AnswerCaller, Effect::Bridge { target: 0 }]
        );
    }

    #[test]
    fn exhausting_a_stage_falls_through_to_the_next() {
        let mut r = FlowReducer::new(vec![
            stage(Strategy::Sequential, 1),
            stage(Strategy::Sequential, 1),
        ]);
        r.reduce(Event::Start);
        // Stage 0's only target fails → fall through to stage 1 (global index 1),
        // NOT a caller hangup.
        assert_eq!(
            r.reduce(Event::CalleeFailed { target: 0 }),
            vec![Effect::Dial { target: 1 }]
        );
        assert!(!r.is_ended());
        // Stage 1 answers → bridge at the global index.
        assert_eq!(
            r.reduce(Event::CalleeAnswered { target: 1 }),
            vec![Effect::AnswerCaller, Effect::Bridge { target: 1 }]
        );
    }

    #[test]
    fn all_stages_exhausted_hangs_up_caller() {
        let mut r = FlowReducer::new(vec![
            stage(Strategy::Sequential, 1),
            stage(Strategy::Sequential, 1),
        ]);
        r.reduce(Event::Start);
        r.reduce(Event::CalleeFailed { target: 0 }); // -> stage 1
        assert_eq!(
            r.reduce(Event::CalleeFailed { target: 1 }),
            vec![Effect::HangupCaller { code: 486 }, Effect::End]
        );
        assert!(r.is_ended());
    }

    #[test]
    fn parallel_first_stage_then_sequential_fallback_offsets() {
        // Stage 0: parallel over 2 (global 0,1). Stage 1: sequential 1 (global 2).
        let mut r = FlowReducer::new(vec![
            stage(Strategy::Parallel, 2),
            stage(Strategy::Sequential, 1),
        ]);
        assert_eq!(
            r.reduce(Event::Start),
            vec![Effect::Dial { target: 0 }, Effect::Dial { target: 1 }]
        );
        assert_eq!(r.reduce(Event::CalleeFailed { target: 0 }), vec![]); // one loser
        // Last of the parallel stage fails → fall through to the sequential
        // fallback at global index 2.
        assert_eq!(
            r.reduce(Event::CalleeFailed { target: 1 }),
            vec![Effect::Dial { target: 2 }]
        );
        assert_eq!(
            r.reduce(Event::CalleeAnswered { target: 2 }),
            vec![Effect::AnswerCaller, Effect::Bridge { target: 2 }]
        );
    }

    #[test]
    fn caller_cancel_does_not_fall_through() {
        let mut r = FlowReducer::new(vec![
            stage(Strategy::Sequential, 1),
            stage(Strategy::Sequential, 1),
        ]);
        r.reduce(Event::Start);
        // Caller gives up while stage 0 is ringing: cancel + end, no fallback.
        assert_eq!(
            r.reduce(Event::CallerCancel),
            vec![Effect::CancelDial { target: 0 }, Effect::End]
        );
        assert!(r.is_ended());
    }
}
