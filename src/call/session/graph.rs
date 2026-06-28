//! The call **graph**: a directed graph of nodes with a **cursor** marking where
//! the call currently is. This is the general form of [`reducer`](super::reducer)
//! — the `FlowReducer`'s linear dial stages are a degenerate path; this adds
//! branching and nested subgraphs (an IVR menu is a subgraph; each menu option
//! is a node).
//!
//! A call enters at a routing node; routing inserts nodes per config (a Queue
//! subgraph, an IVR subgraph, a Dial node, …). There are no high-level "menu"
//! or "IVR" node kinds — those are *compositions* of primitives: an IVR menu is
//! just `Play → Collect → {per-input edges}`, voicemail is `Play → Record`, etc.
//! Traversal is one pure step:
//!
//! ```text
//!   step(event) : move the cursor along the matching edge, emit effects
//! ```
//!
//! The cursor is a pointer-like [`NodeId`] into the node table — "the call's
//! position on the graph." The executor applies the emitted [`GraphEffect`]s to
//! the live switch (play a prompt on a player tap, collect DTMF, dial, bridge).
//!
//! This module is the **pure** traversal core (no I/O), so the whole flow —
//! including IVR menus and re-prompts — is testable in isolation.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::DtmfDigit;
use super::reducer::TargetIdx;

/// Pointer-like reference to a node — the call's cursor type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId(pub u32);

/// A prompt/announcement resource (a file the player tap will stream).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PromptId(pub u32);

/// A node in the call graph. Composite behaviours (queue, IVR) are expressed as
/// subgraphs of these; there is no separate "app runtime". `Serialize`/
/// `Deserialize` so a graph is plain config — the format a future visual node
/// designer reads and writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Node {
    /// Play a prompt, then advance to `next` when it finishes.
    Play { prompt: PromptId, next: NodeId },
    /// Collect DTMF input, then route by the digits gathered. Primitive: a
    /// single-digit menu is `max_digits = 1`; an N-digit PIN/extension is
    /// `max_digits = N` (optionally ended by `terminator`). `branches` matches
    /// the exact collected sequence; `default` covers no-match and timeout.
    /// (Play the prompt with a preceding `Play` node — collection allows
    /// barge-in over it.)
    Collect {
        max_digits: u8,
        timeout_ms: u64,
        terminator: Option<DtmfDigit>,
        branches: Vec<(Vec<DtmfDigit>, NodeId)>,
        default: NodeId,
    },
    /// Record the caller's audio until `max_duration_ms`, a `terminator` DTMF,
    /// or hangup, then advance to `next`. (Voicemail capture — the file location
    /// is chosen by the port.)
    Record {
        max_duration_ms: u64,
        terminator: Option<DtmfDigit>,
        next: NodeId,
    },
    /// Hand off to dialling `target`; advance to `on_answer` (typically a
    /// `Bridge`) or `on_fail`.
    Dial {
        target: TargetIdx,
        on_answer: NodeId,
        on_fail: NodeId,
    },
    /// Unconditional jump (used for re-prompt loops, shared sub-flows).
    Goto(NodeId),
    /// The call is connected — terminal for the control flow.
    Bridge,
    /// End the call.
    Hangup,
}

/// What moved the call: timers and DTMF and dial outcomes, never polling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphEvent {
    /// Begin (or re-enter) the node under the cursor.
    Enter,
    /// The current prompt finished playing.
    PromptFinished,
    /// A DTMF digit was collected.
    Dtmf(DtmfDigit),
    /// The dial under the cursor answered / failed.
    DialAnswered,
    DialFailed,
    /// A collect/menu timed out.
    Timeout,
}

/// A side effect for the executor to apply to the live switch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphEffect {
    /// Play this prompt on a player tap.
    Play(PromptId),
    /// Start collecting DTMF (up to `max_digits`, or until `terminator`, or the
    /// inter-digit `timeout_ms`).
    Collect {
        max_digits: u8,
        timeout_ms: u64,
        terminator: Option<DtmfDigit>,
    },
    /// Start recording the caller (until `max_duration_ms`, `terminator`, or
    /// hangup).
    Record {
        max_duration_ms: u64,
        terminator: Option<DtmfDigit>,
    },
    /// Dial this target.
    Dial(TargetIdx),
    /// Bridge the answered callee with the caller.
    Bridge,
    /// Hang the call up.
    Hangup,
    /// The graph has reached a terminal node.
    Done,
}

/// A serialized graph definition — the artifact a visual node designer produces
/// and `DialplanFlow::Application` carries: the nodes, the entry cursor, and the
/// prompt-id → audio-file table the nodes reference. Self-contained config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphDef {
    pub entry: NodeId,
    pub nodes: Vec<(NodeId, Node)>,
    /// Resolves the `PromptId`s used by `Play` nodes to audio files.
    #[serde(default)]
    pub prompts: Vec<(PromptId, String)>,
    /// Dial destinations (SIP URIs/AORs) indexed by a `Dial` node's `target`.
    /// Keeps the graph self-contained: "press 1 → dial targets\[0\]".
    #[serde(default)]
    pub targets: Vec<String>,
}

/// The live call graph + cursor.
pub struct CallGraph {
    nodes: HashMap<NodeId, Node>,
    cursor: NodeId,
    done: bool,
    /// DTMF accumulated at the current `Collect` node.
    input: Vec<DtmfDigit>,
}

impl CallGraph {
    /// Build a runnable graph from a serialized [`GraphDef`] (e.g. deserialized
    /// from a designer's JSON in `DialplanFlow::Application`).
    pub fn from_def(def: &GraphDef) -> Self {
        Self::new(def.nodes.iter().cloned().collect(), def.entry)
    }

    /// Build a graph from its nodes and the entry cursor.
    pub fn new(nodes: HashMap<NodeId, Node>, entry: NodeId) -> Self {
        Self {
            nodes,
            cursor: entry,
            done: false,
            input: Vec::new(),
        }
    }

    /// The call's current position.
    pub fn cursor(&self) -> NodeId {
        self.cursor
    }

    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Insert (or replace) a node — routing/expansion grows the graph live.
    pub fn insert(&mut self, id: NodeId, node: Node) {
        self.nodes.insert(id, node);
    }

    fn node(&self) -> Option<&Node> {
        self.nodes.get(&self.cursor)
    }

    /// Advance the call by one event, returning the effects to apply. Entering a
    /// node emits its action; `Goto`/terminal nodes are followed immediately so
    /// the cursor always rests on a node that's waiting for the next event.
    pub fn step(&mut self, event: GraphEvent) -> Vec<GraphEffect> {
        if self.done {
            return Vec::new();
        }
        let Some(node) = self.node().cloned() else {
            self.done = true;
            return vec![GraphEffect::Done];
        };

        match (node, event) {
            // -- Entering a node: emit its action ----------------------------
            (Node::Play { prompt, .. }, GraphEvent::Enter) => vec![GraphEffect::Play(prompt)],
            (
                Node::Collect {
                    max_digits,
                    timeout_ms,
                    terminator,
                    ..
                },
                GraphEvent::Enter,
            ) => {
                self.input.clear();
                vec![GraphEffect::Collect {
                    max_digits,
                    timeout_ms,
                    terminator,
                }]
            }
            (
                Node::Record {
                    max_duration_ms,
                    terminator,
                    ..
                },
                GraphEvent::Enter,
            ) => vec![GraphEffect::Record {
                max_duration_ms,
                terminator,
            }],
            (Node::Dial { target, .. }, GraphEvent::Enter) => vec![GraphEffect::Dial(target)],
            (Node::Goto(next), GraphEvent::Enter) => self.enter(next),
            (Node::Bridge, GraphEvent::Enter) => {
                self.done = true;
                vec![GraphEffect::Bridge, GraphEffect::Done]
            }
            (Node::Hangup, GraphEvent::Enter) => {
                self.done = true;
                vec![GraphEffect::Hangup, GraphEffect::Done]
            }

            // -- Transitions out of a node -----------------------------------
            (Node::Play { next, .. }, GraphEvent::PromptFinished) => self.enter(next),
            // Barge-in: a digit during a prompt interrupts it, advances to the
            // node's `next` (typically a `Collect`), and re-delivers the digit
            // there — so pressing a menu option mid-greeting routes at once.
            (Node::Play { next, .. }, GraphEvent::Dtmf(d)) => {
                let mut effects = self.enter(next);
                effects.extend(self.step(GraphEvent::Dtmf(d)));
                effects
            }
            (
                Node::Collect {
                    max_digits,
                    terminator,
                    branches,
                    default,
                    ..
                },
                GraphEvent::Dtmf(digit),
            ) => {
                if terminator == Some(digit) {
                    return self.finalize_collect(&branches, default);
                }
                self.input.push(digit);
                if self.input.len() as u8 >= max_digits {
                    self.finalize_collect(&branches, default)
                } else {
                    Vec::new() // keep collecting
                }
            }
            (
                Node::Collect {
                    branches, default, ..
                },
                GraphEvent::Timeout,
            ) => self.finalize_collect(&branches, default),
            (Node::Dial { on_answer, .. }, GraphEvent::DialAnswered) => self.enter(on_answer),
            (Node::Dial { on_fail, .. }, GraphEvent::DialFailed) => self.enter(on_fail),
            // Recording ends on the terminator digit or the max-duration timeout.
            (Node::Record { next, terminator, .. }, GraphEvent::Dtmf(d))
                if terminator == Some(d) =>
            {
                self.enter(next)
            }
            (Node::Record { next, .. }, GraphEvent::Timeout) => self.enter(next),

            // Any other event in the current node is ignored (e.g. a menu's own
            // prompt finishing while it waits for a digit — barge-in).
            _ => Vec::new(),
        }
    }

    /// Move the cursor to `id` and emit its entry action.
    fn enter(&mut self, id: NodeId) -> Vec<GraphEffect> {
        self.cursor = id;
        self.step(GraphEvent::Enter)
    }

    /// Route the collected digits to a branch (exact match) or `default`, clear
    /// the input register, and enter the chosen node.
    fn finalize_collect(
        &mut self,
        branches: &[(Vec<DtmfDigit>, NodeId)],
        default: NodeId,
    ) -> Vec<GraphEffect> {
        let next = branches
            .iter()
            .find(|(pat, _)| pat.as_slice() == self.input.as_slice())
            .map(|(_, n)| *n)
            .unwrap_or(default);
        self.input.clear();
        self.enter(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A small IVR, built only from primitives: Play (the menu prompt) → Collect
    // (one digit) → edges {1: dial, 2: voicemail, else: "invalid" then re-play}.
    const GREETING: NodeId = NodeId(1);
    const COLLECT: NodeId = NodeId(2);
    const DIAL: NodeId = NodeId(3);
    const BRIDGE: NodeId = NodeId(4);
    const VOICEMAIL: NodeId = NodeId(5);
    const INVALID: NodeId = NodeId(6);
    const VM_DONE: NodeId = NodeId(7);

    fn ivr() -> CallGraph {
        let mut n = HashMap::new();
        // The menu prompt is just a Play node feeding a Collect node.
        n.insert(
            GREETING,
            Node::Play {
                prompt: PromptId(100),
                next: COLLECT,
            },
        );
        n.insert(
            COLLECT,
            Node::Collect {
                max_digits: 1,
                timeout_ms: 5000,
                terminator: None,
                branches: vec![
                    (vec![DtmfDigit::D1], DIAL),
                    (vec![DtmfDigit::D2], VOICEMAIL),
                ],
                default: INVALID,
            },
        );
        n.insert(
            DIAL,
            Node::Dial {
                target: 0,
                on_answer: BRIDGE,
                on_fail: VOICEMAIL,
            },
        );
        n.insert(BRIDGE, Node::Bridge);
        n.insert(
            VOICEMAIL,
            Node::Play {
                prompt: PromptId(102),
                next: VM_DONE,
            },
        );
        n.insert(VM_DONE, Node::Hangup);
        // Invalid: play a prompt, then back to the greeting/collect (re-prompt).
        n.insert(
            INVALID,
            Node::Play {
                prompt: PromptId(103),
                next: GREETING,
            },
        );
        CallGraph::new(n, GREETING)
    }

    fn collect_one() -> Vec<GraphEffect> {
        vec![GraphEffect::Collect {
            max_digits: 1,
            timeout_ms: 5000,
            terminator: None,
        }]
    }

    #[test]
    fn press_one_dials_and_bridges() {
        let mut g = ivr();
        assert_eq!(g.step(GraphEvent::Enter), vec![GraphEffect::Play(PromptId(100))]);
        // Greeting finishes → start collecting a digit.
        assert_eq!(g.step(GraphEvent::PromptFinished), collect_one());
        assert_eq!(g.cursor(), COLLECT);
        // Caller presses 1 → dial.
        assert_eq!(g.step(GraphEvent::Dtmf(DtmfDigit::D1)), vec![GraphEffect::Dial(0)]);
        assert_eq!(g.cursor(), DIAL);
        // Agent answers → bridge, terminal.
        assert_eq!(
            g.step(GraphEvent::DialAnswered),
            vec![GraphEffect::Bridge, GraphEffect::Done]
        );
        assert!(g.is_done());
    }

    #[test]
    fn press_two_goes_to_voicemail() {
        let mut g = ivr();
        g.step(GraphEvent::Enter);
        g.step(GraphEvent::PromptFinished); // into collect
        assert_eq!(
            g.step(GraphEvent::Dtmf(DtmfDigit::D2)),
            vec![GraphEffect::Play(PromptId(102))]
        );
        assert_eq!(g.cursor(), VOICEMAIL);
        // Greeting done → hang up.
        assert_eq!(
            g.step(GraphEvent::PromptFinished),
            vec![GraphEffect::Hangup, GraphEffect::Done]
        );
    }

    #[test]
    fn invalid_digit_replays_the_menu() {
        let mut g = ivr();
        g.step(GraphEvent::Enter);
        g.step(GraphEvent::PromptFinished); // into collect
        // Press 9 (no branch) → invalid prompt.
        assert_eq!(
            g.step(GraphEvent::Dtmf(DtmfDigit::D9)),
            vec![GraphEffect::Play(PromptId(103))]
        );
        assert_eq!(g.cursor(), INVALID);
        // Invalid prompt finishes → replay greeting → collect again (re-prompt).
        assert_eq!(g.step(GraphEvent::PromptFinished), vec![GraphEffect::Play(PromptId(100))]);
        assert_eq!(g.step(GraphEvent::PromptFinished), collect_one());
        assert_eq!(g.cursor(), COLLECT);
    }

    #[test]
    fn multi_digit_pin_collected_then_routed() {
        // A 4-digit PIN ended by '#': only the correct sequence routes to OK.
        const PIN: NodeId = NodeId(1);
        const OK: NodeId = NodeId(2);
        const BAD: NodeId = NodeId(3);
        let mut n = HashMap::new();
        n.insert(
            PIN,
            Node::Collect {
                max_digits: 4,
                timeout_ms: 8000,
                terminator: Some(DtmfDigit::Pound),
                branches: vec![(
                    vec![DtmfDigit::D1, DtmfDigit::D2, DtmfDigit::D3, DtmfDigit::D4],
                    OK,
                )],
                default: BAD,
            },
        );
        n.insert(OK, Node::Bridge);
        n.insert(BAD, Node::Hangup);
        let mut g = CallGraph::new(n, PIN);

        assert_eq!(
            g.step(GraphEvent::Enter),
            vec![GraphEffect::Collect {
                max_digits: 4,
                timeout_ms: 8000,
                terminator: Some(DtmfDigit::Pound),
            }]
        );
        // First three digits keep collecting (no effects).
        assert!(g.step(GraphEvent::Dtmf(DtmfDigit::D1)).is_empty());
        assert!(g.step(GraphEvent::Dtmf(DtmfDigit::D2)).is_empty());
        assert!(g.step(GraphEvent::Dtmf(DtmfDigit::D3)).is_empty());
        // Fourth digit hits max_digits → matches → bridge.
        assert_eq!(
            g.step(GraphEvent::Dtmf(DtmfDigit::D4)),
            vec![GraphEffect::Bridge, GraphEffect::Done]
        );
    }

    #[test]
    fn wrong_pin_with_terminator_routes_to_default() {
        const PIN: NodeId = NodeId(1);
        const OK: NodeId = NodeId(2);
        const BAD: NodeId = NodeId(3);
        let mut n = HashMap::new();
        n.insert(
            PIN,
            Node::Collect {
                max_digits: 4,
                timeout_ms: 8000,
                terminator: Some(DtmfDigit::Pound),
                branches: vec![(
                    vec![DtmfDigit::D1, DtmfDigit::D2, DtmfDigit::D3, DtmfDigit::D4],
                    OK,
                )],
                default: BAD,
            },
        );
        n.insert(OK, Node::Bridge);
        n.insert(BAD, Node::Hangup);
        let mut g = CallGraph::new(n, PIN);
        g.step(GraphEvent::Enter);
        // "12#" → terminator ends collection early with [1,2] → no match → BAD.
        g.step(GraphEvent::Dtmf(DtmfDigit::D1));
        g.step(GraphEvent::Dtmf(DtmfDigit::D2));
        assert_eq!(
            g.step(GraphEvent::Dtmf(DtmfDigit::Pound)),
            vec![GraphEffect::Hangup, GraphEffect::Done]
        );
    }

    #[test]
    fn menu_timeout_takes_the_default_edge() {
        let mut g = ivr();
        g.step(GraphEvent::Enter);
        g.step(GraphEvent::PromptFinished); // into collect
        assert_eq!(
            g.step(GraphEvent::Timeout),
            vec![GraphEffect::Play(PromptId(103))]
        );
        assert_eq!(g.cursor(), INVALID);
    }

    #[test]
    fn barge_in_digit_during_greeting_routes_immediately() {
        let mut g = ivr();
        // Greeting starts playing…
        assert_eq!(g.step(GraphEvent::Enter), vec![GraphEffect::Play(PromptId(100))]);
        // …caller presses 1 *during* it (no PromptFinished): the prompt is
        // interrupted, collection starts, and the digit routes straight to dial.
        assert_eq!(
            g.step(GraphEvent::Dtmf(DtmfDigit::D1)),
            vec![
                GraphEffect::Collect {
                    max_digits: 1,
                    timeout_ms: 5000,
                    terminator: None,
                },
                GraphEffect::Dial(0),
            ]
        );
        assert_eq!(g.cursor(), DIAL);
    }

    #[test]
    fn voicemail_records_then_advances() {
        // greeting Play -> Record(#) -> Hangup
        let (greeting, record, done) = (NodeId(1), NodeId(2), NodeId(3));
        let mut n = HashMap::new();
        n.insert(greeting, Node::Play { prompt: PromptId(200), next: record });
        n.insert(
            record,
            Node::Record {
                max_duration_ms: 30000,
                terminator: Some(DtmfDigit::Pound),
                next: done,
            },
        );
        n.insert(done, Node::Hangup);
        let mut g = CallGraph::new(n, greeting);

        assert_eq!(g.step(GraphEvent::Enter), vec![GraphEffect::Play(PromptId(200))]);
        // Greeting finishes -> start recording.
        assert_eq!(
            g.step(GraphEvent::PromptFinished),
            vec![GraphEffect::Record {
                max_duration_ms: 30000,
                terminator: Some(DtmfDigit::Pound),
            }]
        );
        // Caller presses # -> recording ends -> hang up.
        assert_eq!(
            g.step(GraphEvent::Dtmf(DtmfDigit::Pound)),
            vec![GraphEffect::Hangup, GraphEffect::Done]
        );
    }

    #[test]
    fn recording_ends_on_max_duration() {
        let (record, done) = (NodeId(1), NodeId(2));
        let mut n = HashMap::new();
        n.insert(
            record,
            Node::Record {
                max_duration_ms: 30000,
                terminator: None,
                next: done,
            },
        );
        n.insert(done, Node::Hangup);
        let mut g = CallGraph::new(n, record);
        g.step(GraphEvent::Enter);
        assert_eq!(
            g.step(GraphEvent::Timeout),
            vec![GraphEffect::Hangup, GraphEffect::Done]
        );
    }

    #[test]
    fn graph_round_trips_through_json() {
        // A graph is plain config: it must serialize/deserialize losslessly so a
        // visual designer can read and write it.
        let node = Node::Collect {
            max_digits: 4,
            timeout_ms: 8000,
            terminator: Some(DtmfDigit::Pound),
            branches: vec![(vec![DtmfDigit::D1, DtmfDigit::D2], NodeId(7))],
            default: NodeId(9),
        };
        let json = serde_json::to_string(&node).unwrap();
        let back: Node = serde_json::from_str(&json).unwrap();
        assert_eq!(node, back);
    }

    #[test]
    fn graph_def_from_designer_json_builds_a_runnable_graph() {
        // A complete IVR a designer might emit: greeting → collect → {1: dial}.
        let def = GraphDef {
            entry: NodeId(1),
            nodes: vec![
                (
                    NodeId(1),
                    Node::Play {
                        prompt: PromptId(100),
                        next: NodeId(2),
                    },
                ),
                (
                    NodeId(2),
                    Node::Collect {
                        max_digits: 1,
                        timeout_ms: 5000,
                        terminator: None,
                        branches: vec![(vec![DtmfDigit::D1], NodeId(3))],
                        default: NodeId(3),
                    },
                ),
                (
                    NodeId(3),
                    Node::Dial {
                        target: 0,
                        on_answer: NodeId(4),
                        on_fail: NodeId(4),
                    },
                ),
                (NodeId(4), Node::Bridge),
            ],
            prompts: vec![(PromptId(100), "/sounds/welcome.wav".to_string())],
            targets: vec!["sip:agent@pbx".to_string()],
        };

        // Round-trips through JSON (the on-disk / API designer format).
        let json = serde_json::to_string(&def).unwrap();
        let back: GraphDef = serde_json::from_str(&json).unwrap();
        assert_eq!(def, back);

        // And builds a graph that runs: entering the entry node plays prompt 100.
        let mut g = CallGraph::from_def(&back);
        assert_eq!(g.cursor(), NodeId(1));
        assert_eq!(g.step(GraphEvent::Enter), vec![GraphEffect::Play(PromptId(100))]);
    }

    #[test]
    fn dial_failure_falls_through_to_voicemail() {
        let mut g = ivr();
        g.step(GraphEvent::Enter);
        g.step(GraphEvent::PromptFinished);
        g.step(GraphEvent::Dtmf(DtmfDigit::D1)); // dial
        assert_eq!(
            g.step(GraphEvent::DialFailed),
            vec![GraphEffect::Play(PromptId(102))]
        );
        assert_eq!(g.cursor(), VOICEMAIL);
    }
}
