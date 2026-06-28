//! Drives a [`CallGraph`](super::graph) to completion: apply the effects of the
//! node under the cursor, wait for the next event, `step`, repeat.
//!
//! The switch-facing operations are a [`CallActions`] seam — `play`, `collect`,
//! `dial`, `bridge`, `hangup` — so the *traversal* (which is pure config the
//! visual designer produces) is decoupled from *execution* (player taps, DTMF,
//! the dialer, the mixer bridge). The port feeds events back (a prompt finished,
//! a digit arrived, a dial answered, a timeout) into the runner's event stream.
//!
//! This is the convergence point: one engine walks one graph, and `Targets`,
//! `Queue`, and IVR/voicemail are all just different node subgraphs.

use async_trait::async_trait;

use super::DtmfDigit;
use super::graph::{CallGraph, GraphEffect, GraphEvent, PromptId};
use super::reducer::TargetIdx;
use crate::call::domain::CallCommand;
use crate::call::runtime::CommandResult;
use tokio::sync::{mpsc, oneshot};

/// An external [`CallCommand`] (the stable RWI/console/AMI API) delivered to a
/// live call, paired with a reply channel for its [`CommandResult`].
pub type GraphCommand = (CallCommand, oneshot::Sender<CommandResult>);

/// The side effects a graph needs performed on the live call. Each starts an
/// operation; its completion is reported back as a [`GraphEvent`] on the
/// runner's event channel (so the port owns the event sender).
#[async_trait]
pub trait CallActions: Send {
    /// Begin playing a prompt; report [`GraphEvent::PromptFinished`] when done.
    async fn play(&mut self, prompt: PromptId);
    /// Begin collecting DTMF; digits arrive as [`GraphEvent::Dtmf`], and a lapse
    /// as [`GraphEvent::Timeout`].
    async fn collect(&mut self, max_digits: u8, timeout_ms: u64, terminator: Option<DtmfDigit>);
    /// Begin recording the caller; report [`GraphEvent::Timeout`] when the
    /// max duration elapses (the terminator digit ends it via the graph).
    async fn record(&mut self, max_duration_ms: u64, terminator: Option<DtmfDigit>);
    /// Dial a target; report [`GraphEvent::DialAnswered`]/[`DialFailed`].
    async fn dial(&mut self, target: TargetIdx);
    /// Bridge the answered callee with the caller (terminal-ish).
    async fn bridge(&mut self);
    /// Hang the call up.
    async fn hangup(&mut self);
    /// Handle an external [`CallCommand`] mid-call. Default: not supported, so a
    /// port opts in (only [`GraphCall`](super::graph_call::GraphCall) does).
    async fn dispatch(&mut self, _cmd: CallCommand) -> CommandResult {
        CommandResult::not_supported("this port does not handle external commands")
    }
}

/// Walk `graph` to completion, executing each effect on `port` and advancing on
/// events from `events`. Returns the port so its owner can keep the (now
/// connected) call alive — the graph being "done" means traversal finished, not
/// that the bridged call should be torn down.
pub async fn run<P: CallActions>(
    mut graph: CallGraph,
    mut port: P,
    mut events: mpsc::UnboundedReceiver<GraphEvent>,
    mut commands: mpsc::UnboundedReceiver<GraphCommand>,
) -> (P, mpsc::UnboundedReceiver<GraphCommand>) {
    apply(&mut port, graph.step(GraphEvent::Enter)).await;
    // Walk the graph, servicing external commands as they arrive. When traversal
    // finishes (e.g. a Bridge), return the port AND the command receiver so the
    // owner can keep the now-connected call alive and still service commands
    // (the bridged call outlives the walk).
    let mut commands_open = true;
    while !graph.is_done() {
        tokio::select! {
            event = events.recv() => match event {
                Some(event) => apply(&mut port, graph.step(event)).await,
                None => break, // event sources gone
            },
            cmd = commands.recv(), if commands_open => match cmd {
                Some((command, reply)) => {
                    let result = port.dispatch(command).await;
                    let _ = reply.send(result);
                }
                None => commands_open = false, // channel closed; keep running on events
            },
        }
    }
    (port, commands)
}

async fn apply(port: &mut impl CallActions, effects: Vec<GraphEffect>) {
    for effect in effects {
        match effect {
            GraphEffect::Play(p) => port.play(p).await,
            GraphEffect::Collect {
                max_digits,
                timeout_ms,
                terminator,
            } => port.collect(max_digits, timeout_ms, terminator).await,
            GraphEffect::Record {
                max_duration_ms,
                terminator,
            } => port.record(max_duration_ms, terminator).await,
            GraphEffect::Dial(t) => port.dial(t).await,
            GraphEffect::Bridge => port.bridge().await,
            GraphEffect::Hangup => port.hangup().await,
            GraphEffect::Done => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::graph::{Node, NodeId};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// What the port was asked to do, for assertions.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Op {
        Play(u32),
        Collect(u8),
        Record,
        Dial(usize),
        Bridge,
        Hangup,
        Command,
    }

    /// A fake port that records operations and lets the test drive the event
    /// stream (standing in for prompt completion / DTMF / dial results).
    struct FakePort {
        ops: Arc<Mutex<Vec<Op>>>,
    }

    #[async_trait]
    impl CallActions for FakePort {
        async fn play(&mut self, prompt: PromptId) {
            self.ops.lock().unwrap().push(Op::Play(prompt.0));
        }
        async fn collect(&mut self, max: u8, _t: u64, _term: Option<DtmfDigit>) {
            self.ops.lock().unwrap().push(Op::Collect(max));
        }
        async fn record(&mut self, _max: u64, _term: Option<DtmfDigit>) {
            self.ops.lock().unwrap().push(Op::Record);
        }
        async fn dial(&mut self, target: TargetIdx) {
            self.ops.lock().unwrap().push(Op::Dial(target));
        }
        async fn bridge(&mut self) {
            self.ops.lock().unwrap().push(Op::Bridge);
        }
        async fn hangup(&mut self) {
            self.ops.lock().unwrap().push(Op::Hangup);
        }
        async fn dispatch(&mut self, _cmd: CallCommand) -> CommandResult {
            self.ops.lock().unwrap().push(Op::Command);
            CommandResult::success()
        }
    }

    // greeting Play(100) -> Collect(1) -> {1: Dial -> Bridge}
    fn ivr() -> CallGraph {
        let (greeting, collect, dial, bridge) = (NodeId(1), NodeId(2), NodeId(3), NodeId(4));
        let mut n = HashMap::new();
        n.insert(greeting, Node::Play { prompt: PromptId(100), next: collect });
        n.insert(
            collect,
            Node::Collect {
                max_digits: 1,
                timeout_ms: 5000,
                terminator: None,
                branches: vec![(vec![DtmfDigit::D1], dial)],
                default: bridge, // (any non-1 just bridges, for the test)
            },
        );
        n.insert(dial, Node::Dial { target: 0, on_answer: bridge, on_fail: bridge });
        n.insert(bridge, Node::Bridge);
        CallGraph::new(n, greeting)
    }

    #[tokio::test]
    async fn runner_drives_ivr_play_collect_dial_bridge() {
        let ops = Arc::new(Mutex::new(Vec::new()));
        let port = FakePort { ops: ops.clone() };
        let (tx, rx) = mpsc::unbounded_channel();
        let (_cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let handle = tokio::spawn(run(ivr(), port, rx, cmd_rx));

        // The runner emits Play on Enter; drive the rest of the conversation.
        tokio::task::yield_now().await;
        tx.send(GraphEvent::PromptFinished).unwrap(); // greeting done -> collect
        tx.send(GraphEvent::Dtmf(DtmfDigit::D1)).unwrap(); // press 1 -> dial
        tx.send(GraphEvent::DialAnswered).unwrap(); // agent answers -> bridge
        let (_port, _cmds) = handle.await.unwrap();

        assert_eq!(
            *ops.lock().unwrap(),
            vec![Op::Play(100), Op::Collect(1), Op::Dial(0), Op::Bridge],
            "the runner walked the IVR graph, executing each node's effect"
        );
    }

    #[tokio::test]
    async fn runner_services_external_commands_on_the_live_call() {
        use crate::call::domain::HangupCommand;

        let ops = Arc::new(Mutex::new(Vec::new()));
        let port = FakePort { ops: ops.clone() };
        let (tx, rx) = mpsc::unbounded_channel();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let handle = tokio::spawn(run(ivr(), port, rx, cmd_rx));
        tokio::task::yield_now().await;

        // Send a CallCommand into the live call and await its result.
        let (reply_tx, reply_rx) = oneshot::channel();
        cmd_tx
            .send((CallCommand::Hangup(HangupCommand::all(None, None)), reply_tx))
            .unwrap();
        let result = reply_rx.await.unwrap();
        assert!(result.success, "the command was dispatched to the port");

        drop(tx); // end the call (graph still mid-traversal -> events-close returns)
        let (_port, _cmds) = handle.await.unwrap();
        assert!(
            ops.lock().unwrap().contains(&Op::Command),
            "the port handled the external command mid-call"
        );
    }
}
