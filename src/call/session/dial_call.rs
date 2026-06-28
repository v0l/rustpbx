//! `DialCall` 2014 the glue that turns the pure [`reducer`](super::reducer) into
//! a running call. It feeds real events into `reduce()` and applies each
//! [`Effect`] onto the live `Session`/`CallSwitch` layer:
//!
//! ```text
//!   leg events / dial results ─▶ reduce(event) ─▶ [effect] ─▶ apply to switch/sessions
//! ```
//!
//! The only thing it abstracts is *how a target is dialled* (the [`Dialer`]
//! trait), so the whole flow is exercisable end-to-end with fakes.

use super::graph_runner::GraphCommand;
use super::mixer_bridge::{MixerBridge, MixerBridgeHandle};
use super::player::Player;
use super::reducer::{Effect, Event, FlowReducer, TargetIdx};
use super::switch::{CallSwitch, PortId};
use super::{CloseCause, DtmfDigit, Session, SessionCmd, SessionEvent};
use crate::call::domain::CallCommand;
use crate::call::runtime::CommandResult;
use crate::media::unified_mixer::TapId;
use async_trait::async_trait;
use audio_codec::{CodecType, PcmBuf};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// PCM mix rate for the unified media bridge (8 kHz telephony).
const MIX_RATE: u32 = 8_000;

/// Originates a callee leg. Resolves when the target answers (`Ok`) or fails
/// (`Err`). For SIP this wraps `SipSession::dial`.
#[async_trait]
pub trait Dialer: Send + Sync {
    async fn dial(&self, target: TargetIdx) -> Result<Box<dyn Session>, DialError>;
}

/// A dial attempt did not produce an answered session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DialError;

/// Outcome of one dial attempt, delivered back to the DialCall loop.
enum DialOutcome {
    Answered(Box<dyn Session>),
    Failed,
    /// The callee did not answer within the configured ring timeout.
    RingTimedOut,
}
type DialResult = (TargetIdx, DialOutcome);

/// Drives one call: a caller leg + a reducer + a switch + outstanding dials.
pub struct DialCall {
    reducer: FlowReducer,
    dialer: Arc<dyn Dialer>,
    switch: CallSwitch,
    /// The inbound caller, held until `run` adds it to the switch (the add is
    /// async because it allocates a mixer participant).
    caller: Option<Box<dyn Session>>,
    caller_id: PortId,
    caller_events: tokio::sync::broadcast::Receiver<SessionEvent>,
    caller_answered: bool,
    /// In-flight (ringing) dials, abortable so `CancelDial` can stop them.
    dialing: HashMap<TargetIdx, JoinHandle<()>>,
    /// Answered callees not yet bridged into the switch.
    pending: HashMap<TargetIdx, Box<dyn Session>>,
    /// Bridged callees, target → its port in the switch.
    bridged: HashMap<TargetIdx, PortId>,
    dial_tx: mpsc::UnboundedSender<DialResult>,
    dial_rx: mpsc::UnboundedReceiver<DialResult>,
    /// The unified media bridge, spawned on the first `Bridge` once both legs'
    /// media seams can be claimed. `None` until then (and for media-less fakes).
    bridge: Option<MixerBridgeHandle>,
    /// Each bridged callee's mixer tap, so a leaving callee can be removed live
    /// (the caller leaving tears the whole bridge down via `End`).
    callee_taps: HashMap<TargetIdx, TapId>,
    /// Optional hold/MoH audio (PCM + rate + codec) played to the caller while
    /// waiting for a callee. Set for queue flows; absent for direct calls (which
    /// answer only on callee-answer and pass real ringback through).
    hold: Option<(PcmBuf, u32, CodecType)>,
    /// The running MoH player and its tap, while hold audio is playing.
    moh: Option<(Player, TapId)>,
    /// Per-dial ring timeout (queue `ring_timeout`): a callee that does not
    /// answer within this is treated as a ring timeout and the flow fails over.
    ring_timeout: Option<Duration>,
    /// Audio to play to the (already-answered) caller before hanging up when the
    /// flow is exhausted — the queue's `PlayThenHangup`/failure prompt.
    failure_audio: Option<(PcmBuf, u32, CodecType)>,
    /// External command sink (RWI/console/AMI via the registry). `None` until
    /// `with_commands` wires it; serviced in `run`'s select loop.
    commands: Option<mpsc::UnboundedReceiver<GraphCommand>>,
    /// Leg-identity map: external LegId -> (port, tap), so leg-targeted commands
    /// (conference mute) resolve to the right mixer tap.
    legs: super::leg_map::LegMap,
}

/// Receive from an optional command channel: pending forever when absent, so it
/// can sit in a `select!` without busy-looping a closed/missing channel.
async fn recv_opt(
    commands: &mut Option<mpsc::UnboundedReceiver<GraphCommand>>,
) -> Option<GraphCommand> {
    match commands {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

impl DialCall {
    /// Build a DialCall for an inbound caller, a dialplan flow (as a
    /// [`FlowReducer`]), and a dialer.
    pub fn new(reducer: FlowReducer, caller: Box<dyn Session>, dialer: Arc<dyn Dialer>) -> Self {
        let caller_events = caller.events();
        let switch = CallSwitch::new_unified();
        let (dial_tx, dial_rx) = mpsc::unbounded_channel();
        Self {
            reducer,
            dialer,
            switch,
            caller: Some(caller),
            caller_id: PortId(0), // set in `run` once the caller is added
            caller_events,
            caller_answered: false,
            dialing: HashMap::new(),
            pending: HashMap::new(),
            bridged: HashMap::new(),
            dial_tx,
            dial_rx,
            bridge: None,
            callee_taps: HashMap::new(),
            hold: None,
            moh: None,
            ring_timeout: None,
            failure_audio: None,
            commands: None,
            legs: super::leg_map::LegMap::new(),
        }
    }

    /// Wire the live-control command channel (the dialer-engine equivalent of the
    /// graph runner's command seam).
    pub fn with_commands(mut self, rx: mpsc::UnboundedReceiver<GraphCommand>) -> Self {
        self.commands = Some(rx);
        self
    }

    /// Handle an external [`CallCommand`] on a dialer-driven (Targets/Queue)
    /// call. Routes via the shared [`command_route`](super::command_route); the
    /// cleanly-mappable subset is executed, the rest report `not_supported`.
    async fn dispatch(&mut self, cmd: CallCommand) -> CommandResult {
        use super::command_route::{CommandRoute, route};
        use CallCommand as C;
        match route(&cmd) {
            CommandRoute::Lifecycle => match cmd {
                // Drive a clean teardown through the reducer (same path as a
                // real caller hangup).
                C::Hangup(_) => {
                    let effects = self.reducer.reduce(Event::CallerHangup);
                    self.apply(effects).await;
                    CommandResult::success()
                }
                _ => CommandResult::not_supported("lifecycle command not yet on the dialer engine"),
            },
            CommandRoute::Session => {
                let session_cmd = match &cmd {
                    C::SendDtmf { digits, .. } => Some(SessionCmd::SendDtmf(
                        digits.chars().filter_map(DtmfDigit::from_char).collect(),
                    )),
                    C::Hold { .. } => Some(SessionCmd::Hold),
                    C::Unhold { .. } => Some(SessionCmd::Unhold),
                    C::Transfer { target, .. } => {
                        Some(SessionCmd::Refer { target: target.clone() })
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
                    None => CommandResult::not_supported("session command not yet routed"),
                }
            }
            CommandRoute::Switch => {
                // Mute/unmute a leg, resolved through the leg-identity map.
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
                    None => CommandResult::not_supported("this switch op needs a new leg"),
                }
            }
            _ => CommandResult::not_supported("not yet supported on the dialer engine"),
        }
    }

    /// Audio played to the answered caller before hangup on exhaustion (queue
    /// `PlayThenHangup`/failure prompt).
    pub fn with_failure_audio(mut self, pcm: PcmBuf, source_rate: u32, codec: CodecType) -> Self {
        self.failure_audio = Some((pcm, source_rate, codec));
        self
    }

    /// Set the per-dial ring timeout (queue escalation): a callee that does not
    /// answer within `d` fails over to the next target/stage.
    pub fn with_ring_timeout(mut self, d: Duration) -> Self {
        self.ring_timeout = Some(d);
        self
    }

    /// Configure hold/MoH audio (a queue flow): the caller is answered
    /// immediately and hears this looping audio until a callee bridges in.
    pub fn with_caller_hold(mut self, pcm: PcmBuf, source_rate: u32, codec: CodecType) -> Self {
        self.hold = Some((pcm, source_rate, codec));
        self
    }

    /// Run the call to completion.
    pub async fn run(mut self) {
        // Add the caller to the switch (allocates its mixer participant).
        let caller = self.caller.take().expect("caller present");
        self.caller_id = self.switch.add_port(caller).await;

        // Queue flow: answer the caller up front and start hold music while we
        // dial, so there is an actual waiting window to fill.
        if self.hold.is_some() {
            if let Some(session) = self.switch.session_mut(self.caller_id) {
                let _ = session.accept().await;
            }
            self.caller_answered = true;
            self.ensure_bridge();
            self.start_moh().await;
        }

        // Kick the flow off.
        let effects = self.reducer.reduce(Event::Start);
        self.apply(effects).await;

        while !self.reducer.is_ended() {
            tokio::select! {
                Some((target, outcome)) = self.dial_rx.recv() => {
                    self.dialing.remove(&target);
                    let event = match outcome {
                        DialOutcome::Answered(session) => {
                            self.pending.insert(target, session);
                            Event::CalleeAnswered { target }
                        }
                        DialOutcome::Failed => Event::CalleeFailed { target },
                        DialOutcome::RingTimedOut => Event::RingTimeout { target },
                    };
                    let effects = self.reducer.reduce(event);
                    self.apply(effects).await;
                }
                event = self.caller_events.recv() => {
                    if matches!(event, Ok(SessionEvent::Terminated(_))) {
                        let ev = if self.caller_answered {
                            Event::CallerHangup
                        } else {
                            Event::CallerCancel
                        };
                        let effects = self.reducer.reduce(ev);
                        self.apply(effects).await;
                    }
                }
                maybe_cmd = recv_opt(&mut self.commands) => {
                    match maybe_cmd {
                        Some((command, reply)) => {
                            let result = self.dispatch(command).await;
                            let _ = reply.send(result);
                        }
                        None => self.commands = None, // channel closed; stop polling it
                    }
                }
            }
        }

        // Final teardown: everything still owned is released.
        for (_, handle) in self.dialing.drain() {
            handle.abort();
        }
        self.switch.close_all().await;
    }

    async fn apply(&mut self, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::Dial { target } => self.spawn_dial(target),
                Effect::CancelDial { target } => {
                    if let Some(handle) = self.dialing.remove(&target) {
                        handle.abort();
                    }
                    self.pending.remove(&target);
                }
                Effect::AnswerCaller => {
                    // A queue flow already answered the caller up front.
                    if !self.caller_answered {
                        self.caller_answered = true;
                        if let Some(session) = self.switch.session_mut(self.caller_id) {
                            let _ = session.accept().await;
                        }
                    }
                }
                Effect::Bridge { target } => {
                    if let Some(session) = self.pending.remove(&target) {
                        // A real party is here — stop hold music before bridging.
                        self.stop_moh();
                        // Adding it to the switch meshes it with the caller.
                        let port = self.switch.add_port(session).await;
                        self.bridged.insert(target, port);
                        self.bridge_port(target, port).await;
                    }
                }
                Effect::HangupCaller { .. } => {
                    self.play_failure_then_close_caller().await;
                }
                Effect::HangupCallee { target } => {
                    if let Some(port) = self.bridged.remove(&target) {
                        self.switch.remove(port).await;
                    }
                    if let Some(tap) = self.callee_taps.remove(&target)
                        && let Some(handle) = &self.bridge
                    {
                        handle.remove_leg(tap);
                    }
                    self.pending.remove(&target);
                }
                Effect::End => {
                    self.switch.close_all().await;
                }
            }
        }
    }

    /// Claim a leg's media seam (codec + RTP IO) from its session, if any.
    fn claim_leg(&mut self, port: PortId) -> Option<(CodecType, super::rtp_socket::RtpStreamIo)> {
        let session = self.switch.session_mut(port)?;
        let codec = session
            .media()
            .streams()
            .iter()
            .find(|s| matches!(s.kind, super::MediaKind::Audio))
            .map(|s| s.codec.codec)
            .unwrap_or(CodecType::PCMU);
        let io = session.take_media_io()?;
        Some((codec, io))
    }

    /// On exhaustion: if the caller was answered and a failure prompt is
    /// configured, play it once (bounded) before sending BYE. Otherwise just
    /// close the caller.
    async fn play_failure_then_close_caller(&mut self) {
        if let Some((pcm, rate, codec)) = self.failure_audio.clone()
            && self.caller_answered
        {
            self.stop_moh();
            self.ensure_bridge();
            if let Some(handle) = &self.bridge
                && let Some((tap, sink)) = handle.add_player(codec).await
            {
                let (player, done) = Player::play_once(pcm, rate, codec, sink);
                let _ = tokio::time::timeout(Duration::from_secs(30), done).await;
                player.stop();
                handle.remove_leg(tap);
            }
        }
        if let Some(session) = self.switch.session_mut(self.caller_id) {
            session.close(CloseCause::Normal).await;
        }
    }

    /// Start hold music on the caller-only bridge (queue flow).
    async fn start_moh(&mut self) {
        let Some((pcm, rate, codec)) = self.hold.clone() else {
            return;
        };
        if self.moh.is_some() {
            return;
        }
        if let Some(handle) = &self.bridge
            && let Some((tap, sink)) = handle.add_player(codec).await
        {
            let player = Player::start(pcm, rate, codec, sink, true);
            self.moh = Some((player, tap));
        }
    }

    /// Stop hold music and remove its player tap.
    fn stop_moh(&mut self) {
        if let Some((player, tap)) = self.moh.take() {
            player.stop();
            if let Some(handle) = &self.bridge {
                handle.remove_leg(tap);
            }
        }
    }

    /// Bridge a freshly added callee into the media plane: build the bridge on
    /// the first one (caller + callee), or add the leg live for later ones (a
    /// conference growing).
    async fn bridge_port(&mut self, target: TargetIdx, port: PortId) {
        if self.bridge.is_none() {
            self.ensure_bridge();
            return;
        }
        if let Some((codec, io)) = self.claim_leg(port)
            && let Some(handle) = &self.bridge
            && let Some(tap) = handle.add_leg(codec, io).await
        {
            self.callee_taps.insert(target, tap);
            self.legs.insert(
                crate::call::domain::LegId::new(format!("callee-{target}")),
                port,
                Some(tap),
                super::leg_map::LegRole::Callee,
            );
        }
    }

    /// Build and spawn the unified media bridge once (caller + first callee).
    /// Legs without a media seam (fakes, or SIP legs with no RTP attached) are
    /// skipped, so signalling-only paths stay unaffected.
    fn ensure_bridge(&mut self) {
        if self.bridge.is_some() {
            return;
        }
        let mut mb = MixerBridge::new(MIX_RATE);
        let mut any = false;
        if let Some((codec, io)) = self.claim_leg(self.caller_id) {
            let tap = mb.add_leg(codec, io);
            self.legs.insert(
                crate::call::domain::LegId::new("caller"),
                self.caller_id,
                Some(tap),
                super::leg_map::LegRole::Caller,
            );
            any = true;
        }
        let entries: Vec<(TargetIdx, PortId)> =
            self.bridged.iter().map(|(t, p)| (*t, *p)).collect();
        for (target, port) in entries {
            if let Some((codec, io)) = self.claim_leg(port) {
                let tap = mb.add_leg(codec, io);
                self.callee_taps.insert(target, tap);
                self.legs.insert(
                    crate::call::domain::LegId::new(format!("callee-{target}")),
                    port,
                    Some(tap),
                    super::leg_map::LegRole::Callee,
                );
                any = true;
            }
        }
        if any {
            self.bridge = Some(mb.spawn());
        }
    }

    fn spawn_dial(&mut self, target: TargetIdx) {
        let dialer = self.dialer.clone();
        let tx = self.dial_tx.clone();
        let ring = self.ring_timeout;
        let handle = tokio::spawn(async move {
            let outcome = match ring {
                Some(d) => match tokio::time::timeout(d, dialer.dial(target)).await {
                    Ok(Ok(s)) => DialOutcome::Answered(s),
                    Ok(Err(_)) => DialOutcome::Failed,
                    Err(_) => DialOutcome::RingTimedOut,
                },
                None => match dialer.dial(target).await {
                    Ok(s) => DialOutcome::Answered(s),
                    Err(_) => DialOutcome::Failed,
                },
            };
            let _ = tx.send((target, outcome));
        });
        self.dialing.insert(target, handle);
    }
}

#[cfg(test)]
mod tests {
    use super::super::fake::{FakeSession, SessionProbe};
    use super::super::reducer::{Stage, Strategy};
    use super::super::{Direction, SessionState};
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;

    fn one_stage(strategy: Strategy, count: usize) -> FlowReducer {
        FlowReducer::new(vec![Stage { strategy, count }])
    }

    /// A test dialer pre-loaded with an outcome per target. `Answer` hands back a
    /// fake session (and the test keeps its probe); `Fail` errors. An optional
    /// delay lets parallel races be ordered deterministically.
    struct FakeDialer {
        outcomes: Mutex<HashMap<TargetIdx, Outcome>>,
        dialed: Mutex<Vec<TargetIdx>>,
    }
    enum Outcome {
        Answer(Box<dyn Session>, Duration),
        Fail(Duration),
    }

    impl FakeDialer {
        fn new() -> Self {
            Self {
                outcomes: Mutex::new(HashMap::new()),
                dialed: Mutex::new(Vec::new()),
            }
        }
        fn answers(&self, target: TargetIdx, delay_ms: u64) -> SessionProbe {
            let (s, p) = FakeSession::new(Direction::Outbound, SessionState::Active);
            self.outcomes.lock().unwrap().insert(
                target,
                Outcome::Answer(Box::new(s), Duration::from_millis(delay_ms)),
            );
            p
        }
        /// Answer with a caller-supplied session (e.g. one with a media seam).
        fn answer_with(&self, target: TargetIdx, session: Box<dyn Session>) {
            self.outcomes
                .lock()
                .unwrap()
                .insert(target, Outcome::Answer(session, Duration::ZERO));
        }
        fn fails(&self, target: TargetIdx, delay_ms: u64) {
            self.outcomes
                .lock()
                .unwrap()
                .insert(target, Outcome::Fail(Duration::from_millis(delay_ms)));
        }
        fn dialed_targets(&self) -> Vec<TargetIdx> {
            self.dialed.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Dialer for FakeDialer {
        async fn dial(&self, target: TargetIdx) -> Result<Box<dyn Session>, DialError> {
            self.dialed.lock().unwrap().push(target);
            let outcome = self.outcomes.lock().unwrap().remove(&target);
            match outcome {
                Some(Outcome::Answer(session, delay)) => {
                    tokio::time::sleep(delay).await;
                    Ok(session)
                }
                Some(Outcome::Fail(delay)) => {
                    tokio::time::sleep(delay).await;
                    Err(DialError)
                }
                None => Err(DialError),
            }
        }
    }

    fn caller() -> (Box<dyn Session>, SessionProbe) {
        let (s, p) = FakeSession::new(Direction::Inbound, SessionState::Establishing);
        (Box::new(s), p)
    }

    #[tokio::test]
    async fn sequential_success_bridges_first_target() {
        let dialer = Arc::new(FakeDialer::new());
        let callee0 = dialer.answers(0, 0);
        let (caller_box, caller_probe) = caller();

        let exec = DialCall::new(
            one_stage(Strategy::Sequential, 2),
            caller_box,
            dialer.clone(),
        );
        let handle = tokio::spawn(exec.run());

        // Give it a moment to dial + bridge, then the caller hangs up.
        tokio::time::sleep(Duration::from_millis(20)).await;
        caller_probe.inject(SessionEvent::Terminated(super::super::TerminationCause::Hangup));

        handle.await.unwrap();
        assert_eq!(dialer.dialed_targets(), vec![0], "only the first target is dialled");
        assert!(callee0.is_closed(), "the bridged callee is released when the caller leaves");
    }

    #[tokio::test]
    async fn external_mute_command_resolves_a_leg_via_the_map() {
        use crate::call::domain::CallCommand as C;

        let dialer = Arc::new(FakeDialer::new());
        // The fake callee has a real media seam so it joins the bridge (gets a tap).
        let (callee_s, _callee_probe) =
            FakeSession::new(Direction::Outbound, SessionState::Active);
        let (callee_io, _ci, _co) = super::super::rtp_socket::loopback();
        dialer.answer_with(0, Box::new(callee_s.with_media_io(callee_io)));
        // Caller also needs a media seam to form the bridge.
        let (caller_s, _cp) = FakeSession::new(Direction::Inbound, SessionState::Establishing);
        let (caller_io, _ai, _ao) = super::super::rtp_socket::loopback();
        let caller_box: Box<dyn Session> = Box::new(caller_s.with_media_io(caller_io));

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let exec = DialCall::new(one_stage(Strategy::Sequential, 1), caller_box, dialer.clone())
            .with_commands(cmd_rx);
        let handle = tokio::spawn(exec.run());

        // Let it dial + bridge (caller + callee-0 both have taps now).
        tokio::time::sleep(Duration::from_millis(40)).await;

        // Mute the bridged callee by its leg id.
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send((
                C::ConferenceMute {
                    conf_id: "c".into(),
                    leg_id: "callee-0".into(),
                },
                reply_tx,
            ))
            .unwrap();
        let result = reply_rx.await.unwrap();
        assert!(result.success, "the bridged callee leg resolves via the map and is muted");

        // An unknown leg fails (proves it's really resolving, not blanket-success).
        let (rt, rr) = tokio::sync::oneshot::channel();
        cmd_tx
            .send((
                C::ConferenceMute {
                    conf_id: "c".into(),
                    leg_id: "ghost".into(),
                },
                rt,
            ))
            .unwrap();
        assert!(!rr.await.unwrap().success, "an unknown leg id does not resolve");
    }

    #[tokio::test]
    async fn external_hangup_command_tears_the_call_down() {
        use crate::call::domain::HangupCommand;

        let dialer = Arc::new(FakeDialer::new());
        let callee0 = dialer.answers(0, 0);
        let (caller_box, _caller_probe) = caller();

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let exec = DialCall::new(one_stage(Strategy::Sequential, 2), caller_box, dialer.clone())
            .with_commands(cmd_rx);
        let handle = tokio::spawn(exec.run());

        // Let it dial + bridge, then deliver an external Hangup command.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send((CallCommand::Hangup(HangupCommand::all(None, None)), reply_tx))
            .unwrap();
        let result = reply_rx.await.unwrap();
        assert!(result.success, "the Hangup command was accepted");

        handle.await.unwrap();
        assert!(
            callee0.is_closed(),
            "the external Hangup command tore the bridged call down"
        );
    }

    #[tokio::test]
    async fn sequential_failover_to_second_target() {
        let dialer = Arc::new(FakeDialer::new());
        dialer.fails(0, 0);
        let callee1 = dialer.answers(1, 0);
        let (caller_box, _caller_probe) = caller();

        let exec = DialCall::new(
            one_stage(Strategy::Sequential, 2),
            caller_box,
            dialer.clone(),
        );
        let handle = tokio::spawn(exec.run());

        tokio::time::sleep(Duration::from_millis(30)).await;
        // Hang the call up so the DialCall finishes.
        // (caller probe path is exercised in the success test.)
        drop(handle);

        assert_eq!(dialer.dialed_targets(), vec![0, 1], "failover dials the next target");
        // The second target answered and was bridged (kept open until teardown).
        assert!(!callee1.is_closed());
    }

    #[tokio::test]
    async fn sequential_all_fail_hangs_up_caller() {
        let dialer = Arc::new(FakeDialer::new());
        dialer.fails(0, 0);
        dialer.fails(1, 0);
        let (caller_box, caller_probe) = caller();

        let exec = DialCall::new(
            one_stage(Strategy::Sequential, 2),
            caller_box,
            dialer.clone(),
        );
        tokio::spawn(exec.run()).await.unwrap();

        assert_eq!(dialer.dialed_targets(), vec![0, 1]);
        assert!(caller_probe.is_closed(), "caller is hung up when all targets fail");
    }

    #[tokio::test]
    async fn parallel_forks_all_and_first_answer_wins() {
        let dialer = Arc::new(FakeDialer::new());
        // target 1 answers immediately; 0 and 2 would answer much later.
        let callee1 = dialer.answers(1, 0);
        let _c0 = dialer.answers(0, 500);
        let _c2 = dialer.answers(2, 500);
        let (caller_box, caller_probe) = caller();

        let exec = DialCall::new(
            one_stage(Strategy::Parallel, 3),
            caller_box,
            dialer.clone(),
        );
        let handle = tokio::spawn(exec.run());

        tokio::time::sleep(Duration::from_millis(30)).await;
        caller_probe.inject(SessionEvent::Terminated(super::super::TerminationCause::Hangup));
        handle.await.unwrap();

        let mut dialed = dialer.dialed_targets();
        dialed.sort();
        assert_eq!(dialed, vec![0, 1, 2], "all targets are forked");
        assert!(callee1.is_closed(), "the winning callee was bridged then released");
    }

    #[tokio::test]
    async fn queue_falls_through_to_fallback_stage() {
        // Stage 0 (the "queue") has one agent that fails; stage 1 (the fallback)
        // has one target that answers. The DialCall must fall through and bridge
        // the fallback — a real queue-with-fallback flow.
        let dialer = Arc::new(FakeDialer::new());
        dialer.fails(0, 0);
        let fallback = dialer.answers(1, 0);
        let (caller_box, caller_probe) = caller();

        let reducer = FlowReducer::new(vec![
            Stage { strategy: Strategy::Sequential, count: 1 }, // queue agents
            Stage { strategy: Strategy::Sequential, count: 1 }, // fallback
        ]);
        let exec = DialCall::new(reducer, caller_box, dialer.clone());
        let handle = tokio::spawn(exec.run());

        tokio::time::sleep(Duration::from_millis(30)).await;
        caller_probe.inject(SessionEvent::Terminated(super::super::TerminationCause::Hangup));
        handle.await.unwrap();

        assert_eq!(dialer.dialed_targets(), vec![0, 1], "failed agent then fallback");
        assert!(fallback.is_closed(), "the fallback target was bridged then released");
    }

    #[tokio::test]
    async fn bridges_audio_through_the_unified_mixer() {
        use super::super::rtp_socket::loopback;

        // Caller leg: inbound, with a media seam the test drives.
        let (caller_io, caller_in_tx, _caller_out_rx) = loopback();
        let (caller_s, _cp) = FakeSession::new(Direction::Inbound, SessionState::Establishing);
        let caller_box: Box<dyn Session> = Box::new(caller_s.with_media_io(caller_io));

        // Callee leg: answered by the dialer, with a media seam we observe.
        let (callee_io, _callee_in_tx, mut callee_out_rx) = loopback();
        let (callee_s, _) = FakeSession::new(Direction::Outbound, SessionState::Active);
        let callee_box: Box<dyn Session> = Box::new(callee_s.with_media_io(callee_io));

        let dialer = Arc::new(FakeDialer::new());
        dialer.outcomes.lock().unwrap().insert(
            0,
            Outcome::Answer(callee_box, Duration::from_millis(0)),
        );

        let exec = DialCall::new(one_stage(Strategy::Sequential, 1), caller_box, dialer.clone());
        let _h = tokio::spawn(exec.run());

        // Let the DialCall dial, answer the caller, and bridge (spawning the
        // unified MixerBridge from both legs' claimed media IO).
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Caller speaks; two same-codec legs → passthrough to the callee.
        let payload: Vec<u8> = vec![7u8; 160];
        for _ in 0..5 {
            caller_in_tx.send(payload.clone()).await.unwrap();
        }

        let got = tokio::time::timeout(Duration::from_secs(1), callee_out_rx.recv())
            .await
            .expect("callee should receive audio")
            .expect("callee channel open");
        assert_eq!(
            got, payload,
            "callee hears the caller through the DialCall's unified mixer"
        );
    }

    #[tokio::test]
    async fn queue_plays_hold_music_to_the_waiting_caller() {
        use super::super::rtp_socket::loopback;

        // Caller leg with a media seam we observe.
        let (caller_io, _caller_in_tx, mut caller_out_rx) = loopback();
        let (caller_s, _cp) = FakeSession::new(Direction::Inbound, SessionState::Establishing);
        let caller_box: Box<dyn Session> = Box::new(caller_s.with_media_io(caller_io));

        // The agent answers only after a delay — the waiting window MoH fills.
        let (callee_io, _callee_in_tx, _callee_out_rx) = loopback();
        let (callee_s, _) = FakeSession::new(Direction::Outbound, SessionState::Active);
        let callee_box: Box<dyn Session> = Box::new(callee_s.with_media_io(callee_io));
        let dialer = Arc::new(FakeDialer::new());
        dialer.outcomes.lock().unwrap().insert(
            0,
            Outcome::Answer(callee_box, Duration::from_millis(400)),
        );

        // Hold audio: a constant tone, looped.
        let tone: PcmBuf = vec![3000i16; 8000];
        let exec = DialCall::new(one_stage(Strategy::Sequential, 1), caller_box, dialer.clone())
            .with_caller_hold(tone, 8000, CodecType::PCMU);
        let _h = tokio::spawn(exec.run());

        // While the agent is still ringing, the caller hears the hold tone.
        let mut best = 0.0f64;
        for _ in 0..15 {
            if let Ok(Some(p)) =
                tokio::time::timeout(Duration::from_millis(100), caller_out_rx.recv()).await
            {
                let mut dec = audio_codec::create_decoder(CodecType::PCMU);
                let pcm = dec.decode(&p);
                let mean =
                    pcm.iter().map(|&s| (s as f64).abs()).sum::<f64>() / pcm.len().max(1) as f64;
                if mean > best {
                    best = mean;
                }
                if best > 2000.0 {
                    break;
                }
            }
        }
        assert!(
            best > 2000.0,
            "the waiting caller should hear hold music, peak {best}"
        );
    }

    #[tokio::test]
    async fn ring_timeout_fails_over_to_the_next_target() {
        let dialer = Arc::new(FakeDialer::new());
        // Target 0 would answer, but only after 500 ms — longer than the ring
        // timeout, so it must time out and fail over.
        let _slow = dialer.answers(0, 500);
        let fast = dialer.answers(1, 0);
        let (caller_box, caller_probe) = caller();

        let exec = DialCall::new(one_stage(Strategy::Sequential, 2), caller_box, dialer.clone())
            .with_ring_timeout(Duration::from_millis(100));
        let handle = tokio::spawn(exec.run());

        tokio::time::sleep(Duration::from_millis(250)).await;
        caller_probe.inject(SessionEvent::Terminated(super::super::TerminationCause::Hangup));
        handle.await.unwrap();

        assert_eq!(
            dialer.dialed_targets(),
            vec![0, 1],
            "target 0 rings out, failover dials target 1"
        );
        assert!(fast.is_closed(), "the failover target was bridged then released");
    }

    #[tokio::test]
    async fn failure_audio_plays_then_hangs_up_on_exhaustion() {
        use super::super::rtp_socket::loopback;

        // Caller answered up front (queue), with a media seam we observe.
        let (caller_io, _caller_in_tx, mut caller_out_rx) = loopback();
        let (caller_s, caller_probe) = FakeSession::new(Direction::Inbound, SessionState::Establishing);
        let caller_box: Box<dyn Session> = Box::new(caller_s.with_media_io(caller_io));

        // The only agent fails → the flow is exhausted.
        let dialer = Arc::new(FakeDialer::new());
        dialer.fails(0, 0);

        let tone: PcmBuf = vec![2500i16; 8000];
        let exec = DialCall::new(one_stage(Strategy::Sequential, 1), caller_box, dialer.clone())
            // Hold so the caller is answered up front, and a failure prompt.
            .with_caller_hold(vec![10i16; 1600], 8000, CodecType::PCMU)
            .with_failure_audio(tone, 8000, CodecType::PCMU);
        let handle = tokio::spawn(exec.run());

        // The caller should hear the failure prompt (level ~2500) before BYE.
        let mut best = 0.0f64;
        for _ in 0..30 {
            if let Ok(Some(p)) =
                tokio::time::timeout(Duration::from_millis(100), caller_out_rx.recv()).await
            {
                let mut dec = audio_codec::create_decoder(CodecType::PCMU);
                let pcm = dec.decode(&p);
                let mean =
                    pcm.iter().map(|&s| (s as f64).abs()).sum::<f64>() / pcm.len().max(1) as f64;
                if mean > best {
                    best = mean;
                }
                if best > 2000.0 {
                    break;
                }
            }
        }
        assert!(best > 2000.0, "caller should hear the failure prompt, peak {best}");

        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(caller_probe.is_closed(), "caller is hung up after the prompt");
    }
}
