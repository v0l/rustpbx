//! The real [`CallActions`](super::graph_runner::CallActions): executes a call
//! graph's effects on the live switch.
//!
//! - `play`  → a player tap on the caller's bridge, driven by a [`Player`];
//!   its completion is reported as [`GraphEvent::PromptFinished`].
//! - `collect` → arms an inter-digit timeout; DTMF arrives via the caller's
//!   [`SessionEvent::Dtmf`] (forwarded into the graph's event stream).
//! - `dial`  → the [`Dialer`]; its outcome is `DialAnswered`/`DialFailed`.
//! - `bridge`→ adds the answered callee into the [`MixerBridge`] (caller↔callee).
//! - `hangup`→ closes the caller.
//!
//! The caller is answered up front (IVR plays prompts), and a single forwarder
//! turns the caller's inbound DTMF into graph events. This is the switch-facing
//! half of the convergence: one engine (`GraphRunner` + this port) serves dial,
//! queue, and IVR, all as graphs.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use audio_codec::{CodecType, PcmBuf};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use super::dial_call::Dialer;
use super::graph::{GraphDef, GraphEvent, PromptId};
use super::graph_runner::CallActions;
use super::mixer_bridge::MixerBridge;
use super::mixer_bridge::MixerBridgeHandle;
use super::player::Player;
use super::reducer::TargetIdx;
use super::rtp_socket::RtpStreamIo;
use super::switch::{CallSwitch, PortId};
use super::{CloseCause, MediaKind, Session, SessionEvent};
use crate::media::unified_mixer::TapId;

/// Resolves a prompt id to its audio (mono PCM + sample rate).
pub type PromptResolver = Arc<dyn Fn(PromptId) -> Option<(PcmBuf, u32)> + Send + Sync>;

/// How a supervisor monitor leg participates in a call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorMode {
    /// Hear everything, be heard by no one.
    Listen,
    /// Be heard only by the target leg (and hear everything).
    Whisper,
    /// Be heard by everyone (full participant).
    Barge,
}

/// Load a WAV file as mono PCM + its sample rate.
fn load_wav_mono(file: &str) -> Option<(PcmBuf, u32)> {
    let mut reader = crate::media::wav_reader::WavReader::open(file).ok()?;
    let channels = reader.spec().channels.max(1) as usize;
    let rate = reader.spec().sample_rate;
    let all: PcmBuf = reader.samples().filter_map(|s| s.ok()).collect();
    let mono: PcmBuf = if channels <= 1 {
        all
    } else {
        all.iter().step_by(channels).copied().collect()
    };
    (!mono.is_empty()).then_some((mono, rate))
}

/// Build a prompt resolver from a designer [`GraphDef`]: eagerly loads each
/// referenced audio file so a self-contained graph runs straight on a
/// [`GraphCall`].
pub fn prompts_from_def(def: &GraphDef) -> PromptResolver {
    let mut map = std::collections::HashMap::new();
    for (id, file) in &def.prompts {
        if let Some(audio) = load_wav_mono(file) {
            map.insert(*id, audio);
        }
    }
    Arc::new(move |p| map.get(&p).cloned())
}

/// Claim a port's media seam (codec + RTP IO) from its session.
fn claim_leg(switch: &mut CallSwitch, port: PortId) -> Option<(CodecType, RtpStreamIo)> {
    let session = switch.session_mut(port)?;
    let codec = session
        .media()
        .streams()
        .iter()
        .find(|s| matches!(s.kind, MediaKind::Audio))
        .map(|s| s.codec.codec)
        .unwrap_or(CodecType::PCMU);
    let io = session.take_media_io()?;
    Some((codec, io))
}

/// Executes call-graph effects on a live [`CallSwitch`] + [`MixerBridge`].
pub struct GraphCall {
    switch: CallSwitch,
    caller_id: PortId,
    bridge: Option<MixerBridgeHandle>,
    codec: CodecType,
    dialer: Arc<dyn Dialer>,
    prompts: PromptResolver,
    events_tx: mpsc::UnboundedSender<GraphEvent>,
    /// The answered callee waiting to be bridged.
    pending: Arc<Mutex<Option<Box<dyn Session>>>>,
    /// The current prompt player + its tap.
    player: Option<(Player, TapId)>,
    /// Cancels the in-flight collect timeout.
    collect_cancel: Option<CancellationToken>,
    /// The active recorder tap + its max-duration timer cancel.
    recorder: Option<(TapId, CancellationToken)>,
    /// Where recordings are written.
    recordings_dir: std::path::PathBuf,
    /// Leg-identity map: external LegId -> (switch slot, mixer tap), so commands
    /// targeting a specific leg resolve to the right tap.
    legs: super::leg_map::LegMap,
}

impl GraphCall {
    /// Build the port: answer the caller, start its DTMF→graph forwarder, and
    /// open a caller-only bridge for player taps. `events_tx` is the runner's
    /// event sink.
    pub async fn new(
        caller: Box<dyn Session>,
        dialer: Arc<dyn Dialer>,
        prompts: PromptResolver,
        events_tx: mpsc::UnboundedSender<GraphEvent>,
    ) -> Self {
        let mut switch = CallSwitch::new_unified();
        let caller_id = switch.add_port(caller).await;

        // IVR answers the caller so prompts can be heard, and forwards its DTMF.
        if let Some(session) = switch.session_mut(caller_id) {
            let _ = session.accept().await;
            let mut events = session.events();
            let tx = events_tx.clone();
            tokio::spawn(async move {
                while let Ok(event) = events.recv().await {
                    match event {
                        SessionEvent::Dtmf(d) => {
                            if tx.send(GraphEvent::Dtmf(d)).is_err() {
                                break;
                            }
                        }
                        SessionEvent::Terminated(_) => break,
                        _ => {}
                    }
                }
            });
        }

        // Caller-only bridge: claim its media seam for player taps.
        let mut mb = MixerBridge::new(8000);
        let (mut bridge, mut codec, mut caller_tap) = (None, CodecType::PCMU, None);
        if let Some((c, io)) = claim_leg(&mut switch, caller_id) {
            codec = c;
            caller_tap = Some(mb.add_leg(c, io));
            bridge = Some(mb.spawn());
        }

        // The caller is the well-known leg "caller".
        let mut legs = super::leg_map::LegMap::new();
        legs.insert(
            crate::call::domain::LegId::new("caller"),
            caller_id,
            caller_tap,
            super::leg_map::LegRole::Caller,
        );

        Self {
            switch,
            caller_id,
            bridge,
            codec,
            legs,
            dialer,
            prompts,
            events_tx,
            pending: Arc::new(Mutex::new(None)),
            player: None,
            collect_cancel: None,
            recorder: None,
            recordings_dir: std::env::temp_dir(),
        }
    }

    /// Set the directory recordings are written to.
    pub fn with_recordings_dir(mut self, dir: std::path::PathBuf) -> Self {
        self.recordings_dir = dir;
        self
    }

    fn stop_player(&mut self) {
        if let Some((player, tap)) = self.player.take() {
            player.stop();
            if let Some(bridge) = &self.bridge {
                bridge.remove_leg(tap);
            }
        }
    }

    /// Add a supervisor monitor leg to the live call and set its mixer gains for
    /// the requested [`SupervisorMode`]: `Listen` (hears all, silent),
    /// `Whisper` (heard by `target` only), or `Barge` (heard by everyone).
    /// Returns false if the leg could not join the bridge. The supervisor's
    /// `session` is established by the caller (dialed or an inbound monitor).
    pub async fn add_supervisor(
        &mut self,
        session: Box<dyn Session>,
        leg_id: crate::call::domain::LegId,
        mode: SupervisorMode,
        target: Option<crate::call::domain::LegId>,
    ) -> bool {
        let port = self.switch.add_port(session).await;
        let Some((codec, io)) = claim_leg(&mut self.switch, port) else {
            return false;
        };
        let Some(bridge) = &self.bridge else {
            return false;
        };
        let Some(tap) = bridge.add_leg(codec, io).await else {
            return false;
        };
        self.legs
            .insert(leg_id, port, Some(tap), super::leg_map::LegRole::Supervisor);
        match mode {
            SupervisorMode::Barge => {} // audible to everyone (default gain)
            SupervisorMode::Listen => bridge.set_source_muted(tap, true),
            SupervisorMode::Whisper => {
                // Silent to all by default, audible only to the target leg.
                bridge.set_source_default(tap, 0.0);
                if let Some(target_tap) = target.and_then(|t| self.legs.tap(&t)) {
                    bridge.set_gain(tap, target_tap, 1.0);
                }
            }
        }
        true
    }

    /// Start a one-shot player tap streaming `pcm` (resampled from `rate`) to the
    /// room, firing [`GraphEvent::PromptFinished`] on completion. Shared by the
    /// graph `Play` node and the external `Play` command.
    async fn start_player(&mut self, pcm: PcmBuf, rate: u32) {
        let Some(bridge) = &self.bridge else {
            let _ = self.events_tx.send(GraphEvent::PromptFinished);
            return;
        };
        if let Some((tap, sink)) = bridge.add_player(self.codec).await {
            let (player, done) = Player::play_once(pcm, rate, self.codec, sink);
            self.player = Some((player, tap));
            let tx = self.events_tx.clone();
            tokio::spawn(async move {
                let _ = done.await;
                let _ = tx.send(GraphEvent::PromptFinished);
            });
        }
    }

    /// Stop the active recorder (cancels its timer and removes the tap, which
    /// closes the writer so the WAV is finalized).
    fn stop_recorder(&mut self) {
        if let Some((tap, cancel)) = self.recorder.take() {
            cancel.cancel();
            if let Some(bridge) = &self.bridge {
                bridge.remove_leg(tap);
            }
        }
    }

    /// Handle an external [`CallCommand`] (the stable RWI/console/AMI API) by
    /// routing it onto the new control plane (see
    /// [`command_route`](super::command_route)). The full enum is accepted;
    /// behaviours we have are executed, the rest return `not_supported` so the
    /// API surface stays complete while coverage widens.
    pub async fn dispatch_command(
        &mut self,
        cmd: crate::call::domain::CallCommand,
    ) -> crate::call::runtime::CommandResult {
        use super::command_route::{CommandRoute, route};
        use crate::call::runtime::CommandResult;
        use crate::call::domain::CallCommand as C;

        match route(&cmd) {
            CommandRoute::Lifecycle => match cmd {
                C::Hangup(_) => {
                    self.hangup().await;
                    CommandResult::success()
                }
                C::Answer { .. } => {
                    if let Some(s) = self.switch.session_mut(self.caller_id) {
                        let _ = s.accept().await;
                    }
                    CommandResult::success()
                }
                _ => CommandResult::not_supported("lifecycle command not yet on the graph engine"),
            },
            CommandRoute::Session => {
                // Map to the per-protocol mailbox on the caller leg.
                let session_cmd = match &cmd {
                    C::SendDtmf { digits, .. } => Some(super::SessionCmd::SendDtmf(
                        digits.chars().filter_map(super::DtmfDigit::from_char).collect(),
                    )),
                    C::Hold { .. } => Some(super::SessionCmd::Hold),
                    C::Unhold { .. } => Some(super::SessionCmd::Unhold),
                    C::Transfer { target, .. } => {
                        Some(super::SessionCmd::Refer { target: target.clone() })
                    }
                    _ => None,
                };
                match session_cmd {
                    Some(sc) => match self.switch.session_mut(self.caller_id) {
                        Some(session) => match session.command(sc).await {
                            Ok(()) => CommandResult::success(),
                            Err(_) => CommandResult::not_supported(
                                "the session does not support this command",
                            ),
                        },
                        None => CommandResult::failure("no caller session"),
                    },
                    None => CommandResult::not_supported(
                        "session command not yet routed (e.g. transfer/SIP message)",
                    ),
                }
            }
            CommandRoute::Graph => match cmd {
                C::Play { source, .. } => match source {
                    crate::call::domain::MediaSource::File { path } => match load_wav_mono(&path) {
                        Some((pcm, rate)) => {
                            self.stop_player();
                            self.stop_recorder();
                            self.start_player(pcm, rate).await;
                            CommandResult::success()
                        }
                        None => CommandResult::failure("could not load the audio file"),
                    },
                    _ => CommandResult::not_supported(
                        "only File audio sources are supported on the graph engine",
                    ),
                },
                C::StopPlayback { .. } => {
                    self.stop_player();
                    CommandResult::success()
                }
                C::StopRecording => {
                    self.stop_recorder();
                    CommandResult::success()
                }
                _ => CommandResult::not_supported(
                    "this media/app command maps to a graph node (collect/record/app/queue)",
                ),
            },
            CommandRoute::Switch => {
                // Resolve the target leg-id and the mute action, then mute/unmute
                // that leg's tap on the live bridge.
                let target = match &cmd {
                    C::ConferenceMute { leg_id, .. } => Some((leg_id.clone(), true)),
                    C::ConferenceUnmute { leg_id, .. } => Some((leg_id.clone(), false)),
                    C::MuteTrack { track_id } => {
                        Some((crate::call::domain::LegId::new(track_id.clone()), true))
                    }
                    C::UnmuteTrack { track_id } => {
                        Some((crate::call::domain::LegId::new(track_id.clone()), false))
                    }
                    _ => None,
                };
                match target {
                    Some((leg_id, muted)) => match (self.legs.tap(&leg_id), &self.bridge) {
                        (Some(tap), Some(bridge)) => {
                            bridge.set_source_muted(tap, muted);
                            CommandResult::success()
                        }
                        _ => CommandResult::failure("unknown leg, or no bridge, to mute"),
                    },
                    None => CommandResult::not_supported(
                        "this switch op needs a new leg (supervisor) or conf state",
                    ),
                }
            }
            CommandRoute::Read => {
                CommandResult::not_supported("state reads not yet on the graph engine")
            }
            CommandRoute::Event => {
                CommandResult::failure("leg-lifecycle signals are events, not commands")
            }
        }
    }
}

#[async_trait]
impl CallActions for GraphCall {
    async fn play(&mut self, prompt: PromptId) {
        self.stop_player();
        self.stop_recorder();
        let Some((pcm, rate)) = (self.prompts)(prompt) else {
            // Nothing to play — treat as instantly finished so the graph advances.
            let _ = self.events_tx.send(GraphEvent::PromptFinished);
            return;
        };
        self.start_player(pcm, rate).await;
    }

    async fn collect(&mut self, _max_digits: u8, timeout_ms: u64, _terminator: Option<super::DtmfDigit>) {
        // Cut off any still-playing prompt (barge-in), then arm the timeout.
        // DTMF itself is already forwarded into the event stream.
        self.stop_player();
        if let Some(c) = self.collect_cancel.take() {
            c.cancel();
        }
        let cancel = CancellationToken::new();
        self.collect_cancel = Some(cancel.clone());
        let tx = self.events_tx.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => {}
                _ = tokio::time::sleep(Duration::from_millis(timeout_ms)) => {
                    let _ = tx.send(GraphEvent::Timeout);
                }
            }
        });
    }

    async fn record(&mut self, max_duration_ms: u64, _terminator: Option<super::DtmfDigit>) {
        // Terminator handling lives in the graph (a DTMF advances the node); the
        // port records until the tap is removed or the max-duration elapses.
        self.stop_player();
        self.stop_recorder();
        let Some(bridge) = &self.bridge else { return };
        // A sink-only recorder tap; we drain its outbound (the room mix — here
        // just the caller) to a codec WAV file.
        let (io, _in_tx, mut out_rx) = super::rtp_socket::loopback();
        let Some(tap) = bridge.add_recorder(self.codec, io).await else {
            return;
        };
        let codec = self.codec;
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let path = self.recordings_dir.join(format!("rec-{stamp}.wav"));
        tokio::spawn(async move {
            let Ok(file) = std::fs::File::create(&path) else {
                return;
            };
            let mut writer = crate::media::wav_writer::CodecWavWriter::new(
                file,
                codec.samplerate(),
                1,
                Some(codec),
            );
            let _ = writer.write_header_internal();
            while let Some(payload) = out_rx.recv().await {
                let _ = writer.write_packet_internal(&payload);
            }
            let _ = writer.finalize_internal();
        });

        // Max-duration timeout → ends the Record node.
        let cancel = CancellationToken::new();
        let tx = self.events_tx.clone();
        let timer = cancel.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = timer.cancelled() => {}
                _ = tokio::time::sleep(Duration::from_millis(max_duration_ms)) => {
                    let _ = tx.send(GraphEvent::Timeout);
                }
            }
        });
        self.recorder = Some((tap, cancel));
    }

    async fn dial(&mut self, target: TargetIdx) {
        let dialer = self.dialer.clone();
        let tx = self.events_tx.clone();
        let pending = self.pending.clone();
        tokio::spawn(async move {
            let event = match dialer.dial(target).await {
                Ok(session) => {
                    *pending.lock().await = Some(session);
                    GraphEvent::DialAnswered
                }
                Err(_) => GraphEvent::DialFailed,
            };
            let _ = tx.send(event);
        });
    }

    async fn bridge(&mut self) {
        self.stop_player();
        self.stop_recorder();
        let callee = self.pending.lock().await.take();
        if let Some(callee) = callee {
            let port = self.switch.add_port(callee).await;
            if let Some((codec, io)) = claim_leg(&mut self.switch, port)
                && let Some(bridge) = &self.bridge
                && let Some(tap) = bridge.add_leg(codec, io).await
            {
                // Register the bridged callee as the leg "callee".
                self.legs.insert(
                    crate::call::domain::LegId::new("callee"),
                    port,
                    Some(tap),
                    super::leg_map::LegRole::Callee,
                );
            }
        }
    }

    async fn hangup(&mut self) {
        self.stop_player();
        self.stop_recorder();
        if let Some(session) = self.switch.session_mut(self.caller_id) {
            session.close(CloseCause::Normal).await;
        }
    }

    async fn dispatch(
        &mut self,
        cmd: crate::call::domain::CallCommand,
    ) -> crate::call::runtime::CommandResult {
        self.dispatch_command(cmd).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::dial_call::DialError;
    use super::super::fake::FakeSession;
    use super::super::graph::{CallGraph, Node, NodeId};
    use super::super::graph_runner::run;
    use super::super::rtp_socket::loopback;
    use super::super::{Direction, DtmfDigit, SessionState};
    use std::collections::HashMap;

    struct OneShotDialer {
        callee: Mutex<Option<Box<dyn Session>>>,
    }
    #[async_trait]
    impl Dialer for OneShotDialer {
        async fn dial(&self, _t: TargetIdx) -> Result<Box<dyn Session>, DialError> {
            self.callee.lock().await.take().ok_or(DialError)
        }
    }

    #[tokio::test]
    async fn ivr_plays_prompt_collects_digit_dials_and_bridges_on_real_switch() {
        // Caller leg with an observable media seam.
        let (caller_io, _c_in, mut caller_out_rx) = loopback();
        let (caller_s, caller_probe) =
            FakeSession::new(Direction::Inbound, SessionState::Establishing);
        let caller: Box<dyn Session> = Box::new(caller_s.with_media_io(caller_io));

        // The agent the IVR will dial on "press 1".
        let (callee_io, _ce_in, _ce_out) = loopback();
        let (callee_s, callee_probe) = FakeSession::new(Direction::Outbound, SessionState::Active);
        let callee: Box<dyn Session> = Box::new(callee_s.with_media_io(callee_io));
        let dialer = Arc::new(OneShotDialer {
            callee: Mutex::new(Some(callee)),
        });

        // Prompt 100 = a short tone.
        let prompts: PromptResolver = Arc::new(|p: PromptId| {
            (p == PromptId(100)).then(|| (vec![3000i16; 1600], 8000u32))
        });

        let (tx, rx) = mpsc::unbounded_channel();
        let port = GraphCall::new(caller, dialer, prompts, tx).await;

        // IVR: Play(100) -> Collect(1) -> press 1 -> Dial -> answer -> Bridge.
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
                default: bridge,
            },
        );
        n.insert(dial, Node::Dial { target: 0, on_answer: bridge, on_fail: bridge });
        n.insert(bridge, Node::Bridge);
        let graph = CallGraph::new(n, greeting);

        let (_cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let runner = tokio::spawn(run(graph, port, rx, cmd_rx));

        // The caller hears the prompt through the real bridge/player tap.
        let mut heard = false;
        for _ in 0..20 {
            if let Ok(Some(p)) =
                tokio::time::timeout(Duration::from_millis(100), caller_out_rx.recv()).await
            {
                let mut dec = audio_codec::create_decoder(CodecType::PCMU);
                let pcm = dec.decode(&p);
                let mean =
                    pcm.iter().map(|&s| (s as f64).abs()).sum::<f64>() / pcm.len().max(1) as f64;
                if mean > 2000.0 {
                    heard = true;
                    break;
                }
            }
        }
        assert!(heard, "caller should hear the IVR prompt via the real player tap");

        // Wait for the ~200 ms prompt to finish (PromptFinished → Collect armed),
        // then press 1 → the runner dials & bridges. (No barge-in: the digit
        // must arrive at the Collect node, not during the Play.)
        tokio::time::sleep(Duration::from_millis(500)).await;
        caller_probe.inject(SessionEvent::Dtmf(DtmfDigit::D1));

        // The graph reaches Bridge (traversal done) and the runner returns the
        // port — which keeps the now-connected call (caller + bridged agent)
        // alive.
        let (mut port, _cmds) = tokio::time::timeout(Duration::from_secs(3), runner)
            .await
            .expect("the IVR graph should run to completion")
            .expect("runner task");
        assert!(
            !callee_probe.is_closed(),
            "the dialed agent is bridged and alive while the port is held"
        );

        // The bridged agent is registered as the leg "callee" — mute it by id.
        let mute = port
            .dispatch(crate::call::domain::CallCommand::ConferenceMute {
                conf_id: "c".into(),
                leg_id: "callee".into(),
            })
            .await;
        assert!(mute.success, "the bridged callee leg resolves via the leg map and is muted");

        // A supervisor joins the live call to whisper to the agent.
        let (sup_io, _s_in, _s_out) = loopback();
        let (sup_s, _sup_probe) = FakeSession::new(Direction::Outbound, SessionState::Active);
        let sup: Box<dyn Session> = Box::new(sup_s.with_media_io(sup_io));
        let joined = port
            .add_supervisor(
                sup,
                crate::call::domain::LegId::new("sup"),
                super::SupervisorMode::Whisper,
                Some(crate::call::domain::LegId::new("callee")),
            )
            .await;
        assert!(joined, "the supervisor leg joins the live bridge and gets whisper gains");
        // Dropping `port` here tears the call down (hangup-on-drop).
    }

    #[tokio::test]
    async fn voicemail_records_caller_audio_to_a_wav_file() {
        use super::super::graph::{CallGraph, Node, NodeId, PromptId};
        let tmp = std::env::temp_dir().join(format!(
            "vmtest-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&tmp).unwrap();

        // Caller leg; the test pushes the caller's "message" audio in.
        let (caller_io, caller_in_tx, _caller_out) = loopback();
        let (caller_s, _p) = FakeSession::new(Direction::Inbound, SessionState::Establishing);
        let caller: Box<dyn Session> = Box::new(caller_s.with_media_io(caller_io));
        let dialer = Arc::new(OneShotDialer {
            callee: Mutex::new(None),
        });
        let prompts: PromptResolver = Arc::new(|p: PromptId| {
            (p == PromptId(200)).then(|| (vec![1000i16; 800], 8000u32))
        });

        let (tx, rx) = mpsc::unbounded_channel();
        let port = GraphCall::new(caller, dialer, prompts, tx)
            .await
            .with_recordings_dir(tmp.clone());

        // greeting Play(200) -> Record(1500ms) -> Hangup
        let (greeting, record, done) = (NodeId(1), NodeId(2), NodeId(3));
        let mut n = HashMap::new();
        n.insert(greeting, Node::Play { prompt: PromptId(200), next: record });
        n.insert(
            record,
            Node::Record {
                max_duration_ms: 1500,
                terminator: None,
                next: done,
            },
        );
        n.insert(done, Node::Hangup);
        let graph = CallGraph::from_def(&super::super::graph::GraphDef {
            entry: greeting,
            nodes: n.into_iter().collect(),
            prompts: vec![],
            targets: vec![],
        });
        let (_cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let runner = tokio::spawn(run(graph, port, rx, cmd_rx));

        // After the ~100ms greeting, the caller leaves a message.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let payload = audio_codec::create_encoder(CodecType::PCMU).encode(&vec![4000i16; 160]);
        for _ in 0..25 {
            let _ = caller_in_tx.send(payload.clone()).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Record(1500ms) elapses -> Hangup -> the runner returns.
        let _ = tokio::time::timeout(Duration::from_secs(3), runner).await;
        tokio::time::sleep(Duration::from_millis(100)).await; // let the writer finalize

        let wav = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.path().extension().map(|x| x == "wav").unwrap_or(false));
        let wav = wav.expect("a voicemail WAV file should be written");
        let len = wav.metadata().unwrap().len();
        assert!(len > 44, "WAV should contain recorded audio beyond the header ({len} bytes)");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn dispatch_routes_the_callcommand_api() {
        use crate::call::domain::{CallCommand as C, HangupCommand};

        let (caller_io, _in, _out) = loopback();
        let (caller_s, caller_probe) = FakeSession::new(Direction::Inbound, SessionState::Establishing);
        let caller: Box<dyn Session> = Box::new(caller_s.with_media_io(caller_io));
        let dialer = Arc::new(OneShotDialer {
            callee: Mutex::new(None),
        });
        let prompts: PromptResolver = Arc::new(|_| None);
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut port = GraphCall::new(caller, dialer, prompts, tx).await;

        // A media command we have: succeeds.
        assert!(
            port.dispatch(C::StopPlayback { leg_id: None }).await.success,
            "StopPlayback is handled"
        );
        // A command not yet on the engine: accepted, reported not-supported
        // (the API surface stays complete).
        assert!(
            !port.dispatch(C::ConferenceList).await.success,
            "ConferenceList is accepted but not yet supported"
        );
        // Switch route: muting the caller leg succeeds (caller tap is tracked).
        assert!(
            port.dispatch(C::ConferenceMute {
                conf_id: "c".into(),
                leg_id: "caller".into(),
            })
            .await
            .success,
            "ConferenceMute mutes the tracked caller tap"
        );
        // A supervisor op (needs a new leg) is accepted but not yet supported.
        assert!(
            !port
                .dispatch(C::SupervisorListen {
                    supervisor_leg: "sup".into(),
                    target_leg: "caller".into(),
                    supervisor_session_id: None,
                })
                .await
                .success
        );

        // Lifecycle: hangup actually tears the caller down.
        assert!(port.dispatch(C::Hangup(HangupCommand::all(None, None))).await.success);
        assert!(caller_probe.is_closed(), "Hangup closed the caller");
    }

    #[tokio::test]
    async fn dispatch_play_file_streams_audio_to_the_caller() {
        use crate::call::domain::{CallCommand as C, MediaSource};

        // Write a short, loud mono WAV the Play command will stream.
        let tmp = std::env::temp_dir().join(format!(
            "playcmd-{}.wav",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        {
            let file = std::fs::File::create(&tmp).unwrap();
            let mut w = crate::media::wav_writer::CodecWavWriter::new(file, 8000, 1, None);
            w.write_header_internal().unwrap();
            // 0.2s of a loud value as PCM16 samples.
            let samples: Vec<i16> = vec![6000i16; 1600];
            let bytes: Vec<u8> = samples.iter().flat_map(|s| s.to_le_bytes()).collect();
            w.write_packet_internal(&bytes).unwrap();
            w.finalize_internal().unwrap();
        }

        // Caller leg with an observable media seam + a real bridge.
        let (caller_io, _c_in, mut caller_out_rx) = loopback();
        let (caller_s, _p) = FakeSession::new(Direction::Inbound, SessionState::Establishing);
        let caller: Box<dyn Session> = Box::new(caller_s.with_media_io(caller_io));
        let dialer = Arc::new(OneShotDialer {
            callee: Mutex::new(None),
        });
        let prompts: PromptResolver = Arc::new(|_| None);
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut port = GraphCall::new(caller, dialer, prompts, tx).await;

        // Dispatch the external Play command.
        let result = port
            .dispatch(C::Play {
                leg_id: None,
                source: MediaSource::File {
                    path: tmp.to_string_lossy().to_string(),
                },
                options: None,
            })
            .await;
        assert!(result.success, "Play(File) was accepted");

        // The caller hears the streamed audio on its media seam.
        let mut heard = false;
        for _ in 0..30 {
            if let Ok(Some(p)) =
                tokio::time::timeout(Duration::from_millis(100), caller_out_rx.recv()).await
            {
                let mut dec = audio_codec::create_decoder(CodecType::PCMU);
                let pcm = dec.decode(&p);
                let mean =
                    pcm.iter().map(|&s| (s as f64).abs()).sum::<f64>() / pcm.len().max(1) as f64;
                if mean > 2000.0 {
                    heard = true;
                    break;
                }
            }
        }
        let _ = std::fs::remove_file(&tmp);
        assert!(heard, "the caller should hear the Play(File) audio via the player tap");
    }
}
