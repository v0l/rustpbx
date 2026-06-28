//! Routing the external [`CallCommand`] API onto the new engine's control plane.
//!
//! `CallCommand` is the **stable public contract** that RWI / console / AMI
//! adapters speak — it does not change. What changes is the *implementation*:
//! instead of the god object's monolithic handler, the new engine routes each
//! command into one of a few structured surfaces:
//!
//! * **Lifecycle** — `Session::accept`/`reject`/`close` (answer/reject/ring/hangup)
//! * **Session** — the per-protocol mailbox `SessionCmd` (DTMF, hold, REFER/transfer, SIP messages, re-INVITE)
//! * **Graph** — a [`CallGraph`](super::graph) node / live mutation (play, collect, record, app, queue)
//! * **Switch** — a switch/mixer op (bridge, conference, supervisor route-gains, join/leave, mute)
//! * **Read** — a state query (conference info/list), not a mutation
//! * **Event** — inbound leg-lifecycle signals that are events, not commands
//!
//! [`route`] is a **pure, exhaustive** classifier: every variant is mapped, so
//! the match won't compile if a `CallCommand` is added without giving it a home.
//! That is the guarantee that the API surface stays complete as it widens.

use crate::call::domain::CallCommand;

/// Which control-plane surface a [`CallCommand`] is dispatched to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandRoute {
    /// Connection lifecycle: `Session::accept`/`reject`/`close`.
    Lifecycle,
    /// A per-protocol op issued via `SessionCmd` (returns `Unsupported` if the
    /// protocol can't do it).
    Session,
    /// A flow/media op expressed as a graph node or live graph mutation.
    Graph,
    /// A switch/mixer op (participants, conference, supervisor gains, mute).
    Switch,
    /// A read/query — answered from state, not a mutation.
    Read,
    /// An inbound leg-lifecycle signal: an event, not an external command.
    Event,
}

/// Classify a [`CallCommand`] onto the new engine's control plane. Exhaustive by
/// construction — adding a variant forces a routing decision here.
pub fn route(cmd: &CallCommand) -> CommandRoute {
    use CallCommand as C;
    use CommandRoute::*;
    match cmd {
        // Lifecycle.
        C::Answer { .. } | C::Reject { .. } | C::Ring { .. } | C::Hangup(_) => Lifecycle,

        // Per-protocol mailbox (SessionCmd). Transfer is SIP REFER; the
        // cross-session variants are inter-actor handoff (Switch), below.
        C::Hold { .. }
        | C::Unhold { .. }
        | C::SendDtmf { .. }
        | C::Transfer { .. }
        | C::TransferComplete { .. }
        | C::TransferCancel { .. }
        | C::HandleReInvite { .. }
        | C::RefreshSession
        | C::SendSipMessage { .. }
        | C::SendSipNotify { .. }
        | C::SendSipOptionsPing => Session,

        // Flow / media — a graph node or live graph mutation.
        C::Play { .. }
        | C::StopPlayback { .. }
        | C::DtmfCollect { .. }
        | C::StartRecording { .. }
        | C::PauseRecording
        | C::ResumeRecording
        | C::StopRecording
        | C::StartApp { .. }
        | C::StopApp { .. }
        | C::InjectAppEvent { .. }
        | C::QueueEnqueue { .. }
        | C::QueueDequeue { .. } => Graph,

        // Switch / mixer — participants, conference, supervisor gains, mute, and
        // cross-session handoff.
        C::Bridge { .. }
        | C::Unbridge { .. }
        | C::BridgeCrossSession { .. }
        | C::TransferCompleteCrossSession { .. }
        | C::SupervisorListen { .. }
        | C::SupervisorWhisper { .. }
        | C::SupervisorBarge { .. }
        | C::SupervisorTakeover { .. }
        | C::SupervisorStop { .. }
        | C::ConferenceCreate { .. }
        | C::ConferenceAdd { .. }
        | C::ConferenceRemove { .. }
        | C::ConferenceMute { .. }
        | C::ConferenceUnmute { .. }
        | C::ConferenceDestroy { .. }
        | C::ConferenceEnd { .. }
        | C::ConferenceKick { .. }
        | C::ConferenceMuteAll { .. }
        | C::MuteTrack { .. }
        | C::UnmuteTrack { .. }
        | C::JoinMixer { .. }
        | C::LeaveMixer => Switch,

        // Reads.
        C::ConferenceInfo { .. } | C::ConferenceList => Read,

        // Inbound leg-lifecycle signals — events, not commands.
        C::LegAdd { .. } | C::LegRemove { .. } | C::LegConnected { .. } | C::LegFailed { .. } => {
            Event
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::call::domain::{CallCommand as C, HangupCommand};

    #[test]
    fn lifecycle_commands_route_to_lifecycle() {
        assert_eq!(
            route(&C::Hangup(HangupCommand::all(None, None))),
            CommandRoute::Lifecycle
        );
    }

    #[test]
    fn media_flow_commands_route_to_graph() {
        assert_eq!(route(&C::PauseRecording), CommandRoute::Graph);
        assert_eq!(route(&C::ResumeRecording), CommandRoute::Graph);
        assert_eq!(route(&C::StopRecording), CommandRoute::Graph);
    }

    #[test]
    fn protocol_ops_route_to_session() {
        assert_eq!(route(&C::RefreshSession), CommandRoute::Session);
        assert_eq!(route(&C::SendSipOptionsPing), CommandRoute::Session);
    }

    #[test]
    fn conference_ops_route_to_switch_and_reads_to_read() {
        assert_eq!(route(&C::ConferenceList), CommandRoute::Read);
        assert_eq!(route(&C::LeaveMixer), CommandRoute::Switch);
    }

    // The router's value is exhaustiveness: this module won't compile if a new
    // `CallCommand` variant is added without a route — the API can't drift.
}
