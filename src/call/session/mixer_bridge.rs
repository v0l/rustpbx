//! Drives a [`UnifiedMixer`] from the sessions' RTP media seams.
//!
//! Each leg contributes an [`RtpStreamIo`] (claimed from its `SipSession` via
//! [`crate::call::session::Session::take_media_io`]): a stream of inbound codec
//! payloads (RTP already stripped by the session's socket task) and a sink for
//! outbound payloads. The bridge adds one mixer **tap** per leg and runs a 20 ms
//! clock:
//!
//! 1. drain each leg's inbound payloads → `mixer.ingest(tap, …)`,
//! 2. `mixer.tick()` (the tap decides passthrough / transcode / mix),
//! 3. fan each tap's output payload back to that leg's outbound sink.
//!
//! This is the unified-mixer replacement for the bespoke `RtpPump` +
//! `ConferenceAudioMixer` path: a 2-party call is just two taps (each sees one
//! source → passthrough/transcode), a conference is N taps (mix), and a
//! recorder is a sink-only tap — all on this one loop.

use std::collections::HashMap;
use std::time::Duration;

use audio_codec::CodecType;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::rtp_socket::RtpStreamIo;
use crate::media::unified_mixer::{Packet, TapId, TapRole, UnifiedMixer};

/// A leg add/remove issued to a running bridge.
enum BridgeCommand {
    Add {
        codec: CodecType,
        role: TapRole,
        io: RtpStreamIo,
        reply: oneshot::Sender<TapId>,
    },
    /// Add a source-only player tap (MoH/IVR prompt/ringback). The reply carries
    /// the tap id and a sink the caller pushes codec payloads into; the bridge
    /// routes them to whoever should hear the prompt.
    AddPlayer {
        codec: CodecType,
        reply: oneshot::Sender<(TapId, mpsc::Sender<Vec<u8>>)>,
    },
    Remove(TapId),
    /// Set the linear gain of `src` into `dst` (0.0 = muted). The supervisor /
    /// conference-mute mechanism.
    SetGain { src: TapId, dst: TapId, gain: f32 },
    /// Mute/unmute a source into every sink (conference mute / listen-only).
    SetSourceMuted { src: TapId, muted: bool },
    /// Set a source's default gain into sinks without a per-pair override (the
    /// whisper base).
    SetSourceDefault { src: TapId, gain: f32 },
}

/// Media-clock period for the mixing path: 20 ms playout cadence. Only runs when
/// a sink has >1 source; plain 2-party calls are event-driven with no timer.
const MIX_FRAME: Duration = Duration::from_millis(20);

/// Builder/owner of a mixer-backed call bridge before it is spawned.
pub struct MixerBridge {
    mixer: UnifiedMixer,
    legs: HashMap<TapId, RtpStreamIo>,
}

impl MixerBridge {
    /// `room_rate` is the PCM mix rate used only for N-party mixing (8 kHz for
    /// telephony). Passthrough and single-source transcode are unaffected.
    pub fn new(room_rate: u32) -> Self {
        Self {
            mixer: UnifiedMixer::new(room_rate),
            legs: HashMap::new(),
        }
    }

    /// Add a participant leg with its negotiated codec and media seam.
    pub fn add_leg(&mut self, codec: CodecType, io: RtpStreamIo) -> TapId {
        let tap = self.mixer.add_tap(codec);
        self.legs.insert(tap, io);
        tap
    }

    /// Add a sink-only recorder tap (receives the room mix; never a source).
    /// The returned `(TapId, RtpStreamIo)` carries the recorder's outbound sink;
    /// drive a writer off it. The inbound half is unused.
    pub fn add_recorder(&mut self, codec: CodecType, io: RtpStreamIo) -> TapId {
        let tap = self.mixer.add_recorder(codec);
        self.legs.insert(tap, io);
        tap
    }

    pub fn leg_count(&self) -> usize {
        self.legs.len()
    }

    /// Spawn the bridge, returning a handle that stops it on drop and can add or
    /// remove legs live. The loop is event-driven for plain 2-party calls (no
    /// timer) and switches to the media clock whenever membership makes a sink
    /// have >1 source.
    pub fn spawn(self) -> MixerBridgeHandle {
        let cancel = CancellationToken::new();
        // Merged inbound packet stream; the loop keeps a sender clone so the
        // channel never closes across add/remove.
        let (merged_tx, merged_rx) = mpsc::channel::<(TapId, Vec<u8>)>(1024);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<BridgeCommand>();

        let mut outbounds: HashMap<TapId, mpsc::Sender<Vec<u8>>> = HashMap::new();
        let mut readers: HashMap<TapId, CancellationToken> = HashMap::new();
        for (tap, io) in self.legs {
            outbounds.insert(tap, io.outbound);
            let rc = cancel.child_token();
            spawn_reader(tap, io.inbound, merged_tx.clone(), rc.clone());
            readers.insert(tap, rc);
        }

        let task = tokio::spawn(run(Loop {
            mixer: self.mixer,
            outbounds,
            readers,
            merged_tx,
            merged_rx,
            cmd_rx,
            cancel: cancel.clone(),
        }));
        MixerBridgeHandle {
            cancel,
            cmd_tx,
            task: Some(task),
        }
    }
}

/// Forward one leg's inbound payloads into the merged stream until cancelled or
/// the socket task closes.
fn spawn_reader(
    tap: TapId,
    mut inbound: mpsc::Receiver<Vec<u8>>,
    merged_tx: mpsc::Sender<(TapId, Vec<u8>)>,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                msg = inbound.recv() => match msg {
                    Some(p) => {
                        if merged_tx.send((tap, p)).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                },
            }
        }
    });
}

/// Running bridge; cancels its loop when dropped, and the conduit for live
/// add/remove of legs.
pub struct MixerBridgeHandle {
    cancel: CancellationToken,
    cmd_tx: mpsc::UnboundedSender<BridgeCommand>,
    task: Option<JoinHandle<()>>,
}

impl MixerBridgeHandle {
    pub fn stop(&self) {
        self.cancel.cancel();
    }

    /// Add a participant leg live (e.g. a 3rd party joining). Returns its tap id,
    /// or `None` if the bridge has stopped.
    pub async fn add_leg(&self, codec: CodecType, io: RtpStreamIo) -> Option<TapId> {
        self.add(codec, TapRole::Participant, io).await
    }

    /// Add a sink-only recorder leg live.
    pub async fn add_recorder(&self, codec: CodecType, io: RtpStreamIo) -> Option<TapId> {
        self.add(codec, TapRole::Recorder, io).await
    }

    async fn add(&self, codec: CodecType, role: TapRole, io: RtpStreamIo) -> Option<TapId> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(BridgeCommand::Add {
                codec,
                role,
                io,
                reply,
            })
            .ok()?;
        rx.await.ok()
    }

    /// Add a source-only player tap (hold music, IVR prompt, ringback). Returns
    /// its tap id and a sink to push codec payloads into — each pushed frame is
    /// routed to whoever should hear it (mixed in for conferences, passed
    /// through for a single listener). Remove it with [`remove_leg`] when the
    /// prompt finishes.
    pub async fn add_player(
        &self,
        codec: CodecType,
    ) -> Option<(TapId, mpsc::Sender<Vec<u8>>)> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(BridgeCommand::AddPlayer { codec, reply })
            .ok()?;
        rx.await.ok()
    }

    /// Remove a leg or player live (a party leaving / a prompt ending).
    pub fn remove_leg(&self, tap: TapId) {
        let _ = self.cmd_tx.send(BridgeCommand::Remove(tap));
    }

    /// Set the gain of `src` into `dst` live (1.0 = unchanged, 0.0 = muted) —
    /// the supervisor (whisper/listen) and conference-mute control.
    pub fn set_gain(&self, src: TapId, dst: TapId, gain: f32) {
        let _ = self.cmd_tx.send(BridgeCommand::SetGain { src, dst, gain });
    }

    /// Mute (or unmute) a source into every sink — conference mute / listen-only.
    pub fn set_source_muted(&self, src: TapId, muted: bool) {
        let _ = self.cmd_tx.send(BridgeCommand::SetSourceMuted { src, muted });
    }

    /// Set a source's default gain (whisper base: 0 to all, then `set_gain` the
    /// one sink that should hear it).
    pub fn set_source_default(&self, src: TapId, gain: f32) {
        let _ = self.cmd_tx.send(BridgeCommand::SetSourceDefault { src, gain });
    }

    /// Await the loop's completion (after `stop` or all legs closing).
    pub async fn join(mut self) {
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for MixerBridgeHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// All state owned by the running loop (one task; no shared locks).
struct Loop {
    mixer: UnifiedMixer,
    outbounds: HashMap<TapId, mpsc::Sender<Vec<u8>>>,
    readers: HashMap<TapId, CancellationToken>,
    /// Held so the merged channel never closes across add/remove.
    merged_tx: mpsc::Sender<(TapId, Vec<u8>)>,
    merged_rx: mpsc::Receiver<(TapId, Vec<u8>)>,
    cmd_rx: mpsc::UnboundedReceiver<BridgeCommand>,
    cancel: CancellationToken,
}

fn fan_out(outbounds: &HashMap<TapId, mpsc::Sender<Vec<u8>>>, outs: Vec<(TapId, Packet)>) {
    for (tap, packet) in outs {
        if let Some(out) = outbounds.get(&tap) {
            let _ = out.try_send(packet.payload);
        }
    }
}

async fn run(mut s: Loop) {
    // `mixing` is recomputed whenever membership changes. A single loop handles
    // both modes: the clock branch is gated on `mixing` so a plain 2-party call
    // never wakes on a timer.
    let mut mixing = s.mixer.needs_clock();
    let mut cmd_open = true;
    let mut clock = tokio::time::interval(MIX_FRAME);
    clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = s.cancel.cancelled() => break,
            cmd = s.cmd_rx.recv(), if cmd_open => match cmd {
                Some(BridgeCommand::Add { codec, role, io, reply }) => {
                    let tap = s.mixer.add_tap_with_role(codec, role);
                    s.outbounds.insert(tap, io.outbound);
                    let rc = s.cancel.child_token();
                    spawn_reader(tap, io.inbound, s.merged_tx.clone(), rc.clone());
                    s.readers.insert(tap, rc);
                    mixing = s.mixer.needs_clock();
                    let _ = reply.send(tap);
                }
                Some(BridgeCommand::AddPlayer { codec, reply }) => {
                    let tap = s.mixer.add_tap_with_role(codec, TapRole::Player);
                    // Player is source-only: a sink feeds its inbound; it is
                    // never a destination, so no outbound entry.
                    let (in_tx, in_rx) = mpsc::channel::<Vec<u8>>(256);
                    let rc = s.cancel.child_token();
                    spawn_reader(tap, in_rx, s.merged_tx.clone(), rc.clone());
                    s.readers.insert(tap, rc);
                    mixing = s.mixer.needs_clock();
                    let _ = reply.send((tap, in_tx));
                }
                Some(BridgeCommand::Remove(tap)) => {
                    if let Some(rc) = s.readers.remove(&tap) {
                        rc.cancel();
                    }
                    s.outbounds.remove(&tap);
                    s.mixer.remove_tap(tap);
                    mixing = s.mixer.needs_clock();
                }
                Some(BridgeCommand::SetGain { src, dst, gain }) => {
                    s.mixer.set_gain(src, dst, gain);
                }
                Some(BridgeCommand::SetSourceMuted { src, muted }) => {
                    if muted {
                        s.mixer.mute_source(src);
                    } else {
                        s.mixer.unmute_source(src);
                    }
                }
                Some(BridgeCommand::SetSourceDefault { src, gain }) => {
                    s.mixer.set_source_default(src, gain);
                }
                None => cmd_open = false,
            },
            msg = s.merged_rx.recv() => {
                if let Some((tap, payload)) = msg {
                    if mixing {
                        s.mixer.feed(tap, payload);
                    } else {
                        fan_out(&s.outbounds, s.mixer.on_inbound(tap, payload));
                    }
                }
            }
            _ = clock.tick(), if mixing => {
                fan_out(&s.outbounds, s.mixer.mix_frame().into_iter().collect());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::call::session::rtp_socket::RtpSocketTask;
    use std::sync::Arc;
    use tokio::net::UdpSocket;

    /// Build a leg facing an external party: a bound socket whose outbound is
    /// sent to `party_addr`, returning the leg's media seam and socket task.
    async fn leg_facing(party_addr: std::net::SocketAddr) -> (RtpStreamIo, RtpSocketTask, u16) {
        let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let port = sock.local_addr().unwrap().port();
        let (task, io) = RtpSocketTask::start(sock, party_addr, 0, 160);
        (io, task, port)
    }

    fn pcmu_payload(value: i16) -> Vec<u8> {
        let mut enc = audio_codec::create_encoder(CodecType::PCMU);
        enc.encode(&vec![value; 160])
    }

    fn rtp(pt: u8, seq: u16, payload: &[u8]) -> Vec<u8> {
        let mut pkt = vec![0x80, pt & 0x7f];
        pkt.extend_from_slice(&seq.to_be_bytes());
        pkt.extend_from_slice(&0u32.to_be_bytes());
        pkt.extend_from_slice(&1u32.to_be_bytes());
        pkt.extend_from_slice(payload);
        pkt
    }

    #[tokio::test]
    async fn two_party_audio_crosses_the_mixer() {
        // External parties.
        let alice = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bob = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let alice_addr = alice.local_addr().unwrap();
        let bob_addr = bob.local_addr().unwrap();

        // Two legs: A faces Alice, B faces Bob.
        let (io_a, _task_a, port_a) = leg_facing(alice_addr).await;
        let (io_b, _task_b, _port_b) = leg_facing(bob_addr).await;

        let mut bridge = MixerBridge::new(8000);
        bridge.add_leg(CodecType::PCMU, io_a);
        bridge.add_leg(CodecType::PCMU, io_b);
        let _handle = bridge.spawn();

        // Alice speaks: send several RTP frames into leg A's socket.
        let payload = pcmu_payload(2000);
        for seq in 0..10u16 {
            alice
                .send_to(&rtp(0, seq, &payload), format!("127.0.0.1:{port_a}"))
                .await
                .unwrap();
        }

        // Bob should receive Alice's audio (same codec → passthrough payload).
        let mut buf = vec![0u8; 2048];
        let mut got = None;
        for _ in 0..20 {
            match tokio::time::timeout(Duration::from_millis(100), bob.recv_from(&mut buf)).await {
                Ok(Ok((n, _))) => {
                    // Strip the 12-byte RTP header the leg added.
                    if n > 12 {
                        got = Some(buf[12..n].to_vec());
                        break;
                    }
                }
                _ => continue,
            }
        }
        assert_eq!(got.as_deref(), Some(payload.as_slice()), "Bob should hear Alice");
    }

    #[tokio::test]
    async fn cross_codec_call_transcodes_on_the_callee_leg() {
        let alice = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bob = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let alice_addr = alice.local_addr().unwrap();
        let bob_addr = bob.local_addr().unwrap();

        let (io_a, _task_a, port_a) = leg_facing(alice_addr).await;
        let (io_b, _task_b, _port_b) = leg_facing(bob_addr).await;

        let mut bridge = MixerBridge::new(8000);
        bridge.add_leg(CodecType::PCMU, io_a); // Alice speaks PCMU
        bridge.add_leg(CodecType::PCMA, io_b); // Bob speaks PCMA
        let _handle = bridge.spawn();

        let payload = pcmu_payload(2000);
        for seq in 0..10u16 {
            alice
                .send_to(&rtp(0, seq, &payload), format!("127.0.0.1:{port_a}"))
                .await
                .unwrap();
        }

        let mut buf = vec![0u8; 2048];
        let mut got = None;
        for _ in 0..20 {
            match tokio::time::timeout(Duration::from_millis(100), bob.recv_from(&mut buf)).await {
                Ok(Ok((n, _))) if n > 12 => {
                    got = Some(buf[12..n].to_vec());
                    break;
                }
                _ => continue,
            }
        }
        let got = got.expect("Bob should receive transcoded audio");
        // Decodes as PCMA back to roughly Alice's level (transcoded, not raw).
        let mut dec = audio_codec::create_decoder(CodecType::PCMA);
        let pcm = dec.decode(&got);
        let mean = pcm.iter().map(|&s| s as f64).sum::<f64>() / pcm.len() as f64;
        assert!((mean - 2000.0).abs() < 400.0, "transcoded mean was {mean}");
        assert_ne!(got, payload, "payload must be re-encoded, not passed through");
    }

    #[tokio::test]
    async fn three_party_conference_mixes_over_the_clock() {
        // Three external parties; each leg faces one. With 3 participants every
        // dst has 2 sources, so the bridge runs the media-clock mixing path.
        let alice = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bob = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let carol = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (io_a, _ta, port_a) = leg_facing(alice.local_addr().unwrap()).await;
        let (io_b, _tb, port_b) = leg_facing(bob.local_addr().unwrap()).await;
        let (io_c, _tc, _port_c) = leg_facing(carol.local_addr().unwrap()).await;

        let mut bridge = MixerBridge::new(8000);
        bridge.add_leg(CodecType::PCMU, io_a);
        bridge.add_leg(CodecType::PCMU, io_b);
        bridge.add_leg(CodecType::PCMU, io_c);
        let _handle = bridge.spawn();

        // Alice and Bob both speak (level 1000 each); Carol stays silent.
        let a_payload = pcmu_payload(1000);
        let b_payload = pcmu_payload(1000);
        for seq in 0..25u16 {
            alice
                .send_to(&rtp(0, seq, &a_payload), format!("127.0.0.1:{port_a}"))
                .await
                .unwrap();
            bob.send_to(&rtp(0, seq, &b_payload), format!("127.0.0.1:{port_b}"))
                .await
                .unwrap();
        }

        // Carol should hear Alice+Bob mixed (~2000), proving the clock path.
        let mut buf = vec![0u8; 2048];
        let mut best = 0.0f64;
        for _ in 0..40 {
            match tokio::time::timeout(Duration::from_millis(100), carol.recv_from(&mut buf)).await {
                Ok(Ok((n, _))) if n > 12 => {
                    let mut dec = audio_codec::create_decoder(CodecType::PCMU);
                    let pcm = dec.decode(&buf[12..n]);
                    let mean = pcm.iter().map(|&s| s as f64).sum::<f64>() / pcm.len().max(1) as f64;
                    if mean > best {
                        best = mean;
                    }
                    if best > 1500.0 {
                        break;
                    }
                }
                _ => continue,
            }
        }
        assert!(best > 1500.0, "Carol should hear the A+B mix, peak mean {best}");
    }

    #[tokio::test]
    async fn supervisor_whisper_reaches_only_the_agent() {
        use crate::call::session::rtp_socket::loopback;

        // caller, agent, supervisor — 3 participants → clock mixing path.
        let (io_caller, _caller_in, mut caller_out) = loopback();
        let (io_agent, _agent_in, mut agent_out) = loopback();
        let (io_sup, sup_in, _sup_out) = loopback();

        let mut bridge = MixerBridge::new(8000);
        let caller_tap = bridge.add_leg(CodecType::PCMU, io_caller);
        let _agent_tap = bridge.add_leg(CodecType::PCMU, io_agent);
        let sup_tap = bridge.add_leg(CodecType::PCMU, io_sup);
        let handle = bridge.spawn();

        // Supervisor whispers to the agent only: mute supervisor into the caller.
        handle.set_gain(sup_tap, caller_tap, 0.0);
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Only the supervisor talks.
        let payload = pcmu_payload(2000);
        for _ in 0..25 {
            sup_in.send(payload.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let mean = |p: &[u8]| {
            let mut dec = audio_codec::create_decoder(CodecType::PCMU);
            let pcm = dec.decode(p);
            pcm.iter().map(|&s| (s as f64).abs()).sum::<f64>() / pcm.len().max(1) as f64
        };

        let mut agent_heard = 0.0f64;
        let mut caller_heard = 0.0f64;
        for _ in 0..40 {
            if let Ok(Some(p)) =
                tokio::time::timeout(Duration::from_millis(50), agent_out.recv()).await
            {
                agent_heard = agent_heard.max(mean(&p));
            }
            if let Ok(Some(p)) =
                tokio::time::timeout(Duration::from_millis(50), caller_out.recv()).await
            {
                caller_heard = caller_heard.max(mean(&p));
            }
            if agent_heard > 500.0 {
                break;
            }
        }
        assert!(agent_heard > 500.0, "agent hears the whisper (peak {agent_heard})");
        assert!(caller_heard < 100.0, "caller does NOT hear the whisper (peak {caller_heard})");
    }

    #[tokio::test]
    async fn leg_added_live_turns_call_into_a_conference() {
        use crate::call::session::rtp_socket::loopback;

        // Start as a plain 2-party call (event-driven, no clock).
        let (io_a, a_in, _a_out) = loopback();
        let (io_b, b_in, _b_out) = loopback();
        let mut bridge = MixerBridge::new(8000);
        bridge.add_leg(CodecType::PCMU, io_a);
        bridge.add_leg(CodecType::PCMU, io_b);
        let handle = bridge.spawn();

        // A third party joins live → the bridge switches to the mixing clock.
        let (io_c, _c_in, mut c_out) = loopback();
        let _c_tap = handle
            .add_leg(CodecType::PCMU, io_c)
            .await
            .expect("live add_leg");

        // Alice and Bob both speak (level 1000).
        let payload = pcmu_payload(1000);
        for _ in 0..20 {
            a_in.send(payload.clone()).await.unwrap();
            b_in.send(payload.clone()).await.unwrap();
        }

        // Carol, who joined live, should hear the A+B mix (~2000).
        let mut best = 0.0f64;
        for _ in 0..40 {
            if let Ok(Some(p)) =
                tokio::time::timeout(Duration::from_millis(100), c_out.recv()).await
            {
                let mut dec = audio_codec::create_decoder(CodecType::PCMU);
                let pcm = dec.decode(&p);
                let mean = pcm.iter().map(|&s| s as f64).sum::<f64>() / pcm.len().max(1) as f64;
                if mean > best {
                    best = mean;
                }
                if best > 1500.0 {
                    break;
                }
            }
        }
        assert!(
            best > 1500.0,
            "Carol should hear the A+B mix after joining live, peak {best}"
        );
    }

    #[tokio::test]
    async fn player_tap_feeds_a_waiting_listener() {
        use crate::call::session::rtp_socket::loopback;

        // A lone caller waiting (no other participant) hears nothing…
        let (io_caller, _c_in, mut c_out) = loopback();
        let mut bridge = MixerBridge::new(8000);
        bridge.add_leg(CodecType::PCMU, io_caller);
        let handle = bridge.spawn();

        // …until hold music / a prompt is added as a player tap.
        let (_ptap, player) = handle.add_player(CodecType::PCMU).await.expect("add player");
        let frame = pcmu_payload(1500);
        for _ in 0..5 {
            player.send(frame.clone()).await.unwrap();
        }

        // The caller hears the player's audio (single source → passthrough).
        let got = tokio::time::timeout(Duration::from_secs(1), c_out.recv())
            .await
            .expect("caller should receive prompt audio")
            .expect("caller channel open");
        assert_eq!(got, frame, "the waiting caller hears the player tap");
    }
}
