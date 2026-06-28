//! The call switch — the *consumer* of the [`Session`] layer and the engine
//! that will replace the god object's `select!` loop.
//!
//! It owns a **dynamic set** of legs (as [`ConnectedPort`]s) plus a real
//! [`ConferenceAudioMixer`] (the audio bus), reacts to each leg's pushed
//! [`SessionEvent`]s, and routes external [`SwitchCommand`]s. It is
//! protocol-agnostic — it speaks only the generic `Session` surface, so it
//! drives `SipSession`, an SFU session, or the test fake identically.
//!
//! Multi-party is *intra-switch*: a 2-party call, a 3-way, and a conference are
//! all "N ports + mixer" on one switch (see `docs/call-port-design.md` §4.1).
//! The mixer does real PCM mixing — each participant gets an audio in/out
//! channel pair; the RTP layer (later) pumps a `SipSession`'s media between its
//! RTP track and these channels.

use super::{CloseCause, ConnectedPort, Session, SessionCmd, SessionEvent};
use crate::call::domain::LegId;
use crate::media::conference_mixer::{AudioFrame, ConferenceAudioMixer};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

/// Internal mix rate for a switch's audio bus (8 kHz telephony PCM).
const SWITCH_MIX_RATE: u32 = 8_000;

/// Per-participant audio: send incoming frames in, receive mixed frames out.
pub type AudioIo = (mpsc::Sender<AudioFrame>, mpsc::Receiver<AudioFrame>);

/// Switch-local port identity (distinct from the durable `SessionId`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PortId(pub u64);

/// The mixer participant id for a port.
fn port_leg_id(id: PortId) -> LegId {
    LegId::new(format!("port-{}", id.0))
}

/// External control input to the switch.
pub enum SwitchCommand {
    /// Add a session that was moved in (handoff / conference join).
    AddParticipant(Box<dyn Session>),
    /// Remove (and hang up) one participant.
    RemoveParticipant(PortId),
    /// Route a protocol-specific command to one participant.
    ToPort(PortId, SessionCmd),
    /// Tear the whole call down.
    Hangup,
}

/// Owns a call (any number of legs) and drives it to completion.
pub struct CallSwitch {
    ports: HashMap<PortId, ConnectedPort>,
    /// Real PCM mixer; mixing routes are full-mesh-minus-self, maintained by
    /// the mixer as participants are added/removed.
    mixer: Arc<ConferenceAudioMixer>,
    /// Per-port audio channels, until claimed by the RTP pump (or a test).
    audio: HashMap<PortId, AudioIo>,
    event_tx: mpsc::UnboundedSender<(PortId, SessionEvent)>,
    event_rx: Option<mpsc::UnboundedReceiver<(PortId, SessionEvent)>>,
    next_port: u64,
    /// When a participant leaves and the active count drops below this, the
    /// call ends (2 = a normal call needs both parties; a conference can set 1).
    min_active: usize,
    /// When true, the switch does NOT wire legs into its `ConferenceAudioMixer`
    /// via `connect_media`; the media plane is the unified `MixerBridge`, driven
    /// by the DialCall claiming each leg's `take_media_io`. The standalone `run`
    /// loop uses the conference path (false).
    unified: bool,
}

impl Default for CallSwitch {
    fn default() -> Self {
        Self::new()
    }
}

impl CallSwitch {
    pub fn new() -> Self {
        Self::build(false)
    }

    /// A switch whose media plane is the unified [`MixerBridge`] (no conference
    /// mixer wiring). Used by the [`DialCall`](super::dial_call::DialCall).
    pub fn new_unified() -> Self {
        Self::build(true)
    }

    fn build(unified: bool) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let mixer_id = format!("call-switch-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        let mixer = Arc::new(ConferenceAudioMixer::new(mixer_id, SWITCH_MIX_RATE));
        mixer.start();
        Self {
            ports: HashMap::new(),
            mixer,
            audio: HashMap::new(),
            event_tx,
            event_rx: Some(event_rx),
            next_port: 0,
            min_active: 2,
            unified,
        }
    }

    /// Set the minimum active participant count below which the call ends.
    pub fn with_min_active(mut self, min_active: usize) -> Self {
        self.min_active = min_active;
        self
    }

    /// Claim a port's audio channels (used by the RTP pump to wire a real leg's
    /// media, or by tests to drive audio directly).
    #[allow(dead_code)] // the RTP pump seam; currently exercised by tests only
    pub(crate) fn take_audio(&mut self, id: PortId) -> Option<AudioIo> {
        self.audio.remove(&id)
    }

    /// Mechanism: add a participant's port + mixer participant, WITHOUT event
    /// forwarding. The reducer-driven DialCall uses this and handles events
    /// itself; the standalone `run` loop uses [`add`](Self::add).
    pub(crate) async fn add_port(&mut self, session: Box<dyn Session>) -> PortId {
        let id = PortId(self.next_port);
        self.next_port += 1;

        let mut port = ConnectedPort::new(session);
        if !self.unified {
            // Negotiated audio codec for this leg (default PCMU).
            let codec = port
                .session_ref()
                .media()
                .streams()
                .iter()
                .find(|s| matches!(s.kind, super::MediaKind::Audio))
                .map(|s| s.codec.codec)
                .unwrap_or(audio_codec::CodecType::PCMU);
            match self.mixer.add_participant(port_leg_id(id), codec).await {
                Ok(channels) => {
                    // Offer the channels to the leg: a SIP session starts its RTP
                    // pump and consumes them; a fake returns them unconsumed and
                    // we keep them (take_audio / tests).
                    if let Some(io) = port.session_mut().connect_media(channels) {
                        self.audio.insert(id, io);
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "mixer add_participant failed");
                }
            }
        }

        self.ports.insert(id, port);
        id
    }

    /// Exclusive access to a participant's session (to accept/close/command it).
    pub(crate) fn session_mut(&mut self, id: PortId) -> Option<&mut dyn Session> {
        self.ports.get_mut(&id).map(|p| p.session_mut())
    }

    /// Add a participant and forward its events into the merged mailbox (for the
    /// standalone [`run`](Self::run) loop). Returns the switch-local `PortId`.
    pub async fn add(&mut self, session: Box<dyn Session>) -> PortId {
        let id = self.add_port(session).await;
        let mut events = self.ports[&id].session_ref().events();
        let tx = self.event_tx.clone();
        tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(event) => {
                        if tx.send((id, event)).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        id
    }

    /// Remove (and hang up) one participant.
    pub(crate) async fn remove(&mut self, id: PortId) -> bool {
        self.audio.remove(&id);
        let _ = self.mixer.remove_participant(&port_leg_id(id)).await;
        self.ports.remove(&id).is_some()
    }

    pub(crate) async fn close_all(&mut self) {
        for port in self.ports.values_mut() {
            port.session_mut().close(CloseCause::Normal).await;
        }
        self.ports.clear();
        self.audio.clear();
    }

    /// Run the call until it ends, reacting to participant events and external
    /// commands. Consumes the switch (it owns the legs).
    pub async fn run(mut self, mut commands: mpsc::Receiver<SwitchCommand>) {
        let mut events = self
            .event_rx
            .take()
            .expect("CallSwitch::run called more than once");

        loop {
            tokio::select! {
                command = commands.recv() => {
                    match command {
                        None | Some(SwitchCommand::Hangup) => {
                            self.close_all().await;
                            break;
                        }
                        Some(SwitchCommand::AddParticipant(session)) => {
                            self.add(session).await;
                        }
                        Some(SwitchCommand::RemoveParticipant(id)) => {
                            self.remove(id).await;
                            if self.ports.len() < self.min_active {
                                self.close_all().await;
                                break;
                            }
                        }
                        Some(SwitchCommand::ToPort(id, cmd)) => {
                            if let Some(port) = self.ports.get_mut(&id) {
                                let _ = port.session_mut().command(cmd).await;
                            }
                        }
                    }
                }
                Some((id, event)) = events.recv() => {
                    if matches!(event, SessionEvent::Terminated(_)) {
                        self.remove(id).await;
                        if self.ports.len() < self.min_active {
                            self.close_all().await;
                            break;
                        }
                    }
                }
            }
        }
        self.mixer.stop().await;
    }
}

#[cfg(test)]
mod tests {
    use super::super::fake::{FakeSession, SessionProbe};
    use super::super::{Direction, DtmfDigit, SessionEvent, SessionState, TerminationCause};
    use super::*;

    async fn add_party(switch: &mut CallSwitch) -> (PortId, SessionProbe) {
        let (s, p) = FakeSession::new(Direction::Inbound, SessionState::Active);
        let id = switch.add(Box::new(s)).await;
        (id, p)
    }

    fn terminated() -> SessionEvent {
        SessionEvent::Terminated(TerminationCause::Hangup)
    }

    #[tokio::test]
    async fn switch_mixes_real_audio_between_participants() {
        let mut switch = CallSwitch::new();
        let (sa, _pa) = FakeSession::new(Direction::Inbound, SessionState::Active);
        let (sb, _pb) = FakeSession::new(Direction::Outbound, SessionState::Active);
        let ia = switch.add_port(Box::new(sa)).await;
        let ib = switch.add_port(Box::new(sb)).await;

        let (a_in, _a_out) = switch.take_audio(ia).expect("a audio");
        let (_b_in, mut b_out) = switch.take_audio(ib).expect("b audio");

        // A speaks; the real mixer should deliver it to B (and not back to A).
        a_in.send(AudioFrame::new(vec![1200i16; 160], SWITCH_MIX_RATE))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        assert!(
            b_out.try_recv().is_ok(),
            "B should hear A through the switch's real audio mixer"
        );

        switch.mixer.stop().await;
    }

    #[tokio::test]
    async fn two_party_remote_bye_releases_the_other() {
        let mut switch = CallSwitch::new();
        let (_ida, a_probe) = add_party(&mut switch).await;
        let (_idb, b_probe) = add_party(&mut switch).await;

        let (_tx, rx) = mpsc::channel(8);
        let handle = tokio::spawn(switch.run(rx));

        a_probe.inject(terminated());

        handle.await.unwrap();
        assert!(b_probe.is_closed(), "the surviving leg must be released");
    }

    #[tokio::test]
    async fn hangup_command_tears_down_all_parties() {
        let mut switch = CallSwitch::new().with_min_active(1);
        let (_a, a_probe) = add_party(&mut switch).await;
        let (_b, b_probe) = add_party(&mut switch).await;
        let (_c, c_probe) = add_party(&mut switch).await;

        let (tx, rx) = mpsc::channel(8);
        let handle = tokio::spawn(switch.run(rx));
        tx.send(SwitchCommand::Hangup).await.unwrap();

        handle.await.unwrap();
        assert!(a_probe.is_closed() && b_probe.is_closed() && c_probe.is_closed());
    }

    #[tokio::test]
    async fn conference_continues_until_below_min_active() {
        let mut switch = CallSwitch::new();
        let (_a, a_probe) = add_party(&mut switch).await;
        let (_b, b_probe) = add_party(&mut switch).await;
        let (_c, c_probe) = add_party(&mut switch).await;

        let (_tx, rx) = mpsc::channel(8);
        let handle = tokio::spawn(switch.run(rx));

        a_probe.inject(terminated()); // 3 -> 2, still up
        b_probe.inject(terminated()); // 2 -> 1, ends

        handle.await.unwrap();
        assert!(c_probe.is_closed());
    }

    #[tokio::test]
    async fn participant_added_at_runtime_joins_the_call() {
        let mut switch = CallSwitch::new();
        let (_a, _pa) = add_party(&mut switch).await;
        let (_b, _pb) = add_party(&mut switch).await;

        let (tx, rx) = mpsc::channel(8);
        let handle = tokio::spawn(switch.run(rx));

        let (c, c_probe) = FakeSession::new(Direction::Outbound, SessionState::Active);
        tx.send(SwitchCommand::AddParticipant(Box::new(c))).await.unwrap();
        tx.send(SwitchCommand::Hangup).await.unwrap();

        handle.await.unwrap();
        assert!(c_probe.is_closed());
    }

    #[tokio::test]
    async fn unsupported_port_command_is_ignored_and_call_continues() {
        let mut switch = CallSwitch::new();
        let (id_a, _pa) = add_party(&mut switch).await;
        let (_b, b_probe) = add_party(&mut switch).await;

        let (tx, rx) = mpsc::channel(8);
        let handle = tokio::spawn(switch.run(rx));

        tx.send(SwitchCommand::ToPort(id_a, SessionCmd::SendDtmf(vec![DtmfDigit::D1])))
            .await
            .unwrap();
        tx.send(SwitchCommand::Hangup).await.unwrap();

        handle.await.unwrap();
        assert!(b_probe.is_closed());
    }
}
