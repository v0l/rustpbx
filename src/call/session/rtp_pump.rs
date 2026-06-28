//! The RTP pump — bridges one leg's RTP/AVP media socket to the switch's PCM
//! [`AudioFrame`] channels.
//!
//! ```text
//!   remote RTP ─▶ socket ─▶ decode ─▶ to_mixer (Sender<AudioFrame>)
//!   from_mixer (Receiver<AudioFrame>) ─▶ encode ─▶ socket ─▶ remote RTP
//! ```
//!
//! This is the missing link that lets a real SIP leg carry audio through the
//! [`CallSwitch`]'s [`ConferenceAudioMixer`]: claim a port's audio channels via
//! `take_audio`, bind the negotiated RTP socket, and start a pump. Plain
//! RTP/AVP (PCMU/PCMA) only — no rustrtc/WebRTC — which is what telephony calls
//! use.

use crate::media::conference_mixer::AudioFrame;
use audio_codec::{CodecType, create_decoder, create_encoder};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const RTP_HEADER_LEN: usize = 12;

/// The static RTP payload type for a codec (RFC 3551). Dynamic PTs aren't
/// supported here; we default to PCMU.
fn payload_type(codec: CodecType) -> u8 {
    match codec {
        CodecType::PCMU => 0,
        CodecType::PCMA => 8,
        CodecType::G722 => 9,
        CodecType::G729 => 18,
        _ => 0,
    }
}

/// A running RTP↔PCM pump for one leg. Drop or [`stop`](Self::stop) to tear down.
pub struct RtpPump {
    cancel: CancellationToken,
}

impl RtpPump {
    /// Start pumping between `socket` (sending to `remote`) and the mixer
    /// channels: decoded incoming RTP goes to `to_mixer`; PCM from `from_mixer`
    /// is encoded and sent as RTP.
    pub fn start(
        socket: Arc<UdpSocket>,
        remote: SocketAddr,
        codec: CodecType,
        sample_rate: u32,
        to_mixer: mpsc::Sender<AudioFrame>,
        mut from_mixer: mpsc::Receiver<AudioFrame>,
    ) -> Self {
        let cancel = CancellationToken::new();

        // Inbound: socket -> decode -> mixer.
        let recv_cancel = cancel.clone();
        let recv_socket = socket.clone();
        tokio::spawn(async move {
            let mut decoder = create_decoder(codec);
            let mut buf = [0u8; 2048];
            loop {
                tokio::select! {
                    _ = recv_cancel.cancelled() => break,
                    res = recv_socket.recv_from(&mut buf) => {
                        let Ok((n, _src)) = res else { break; };
                        if n <= RTP_HEADER_LEN {
                            continue;
                        }
                        let pcm = decoder.decode(&buf[RTP_HEADER_LEN..n]);
                        if to_mixer.send(AudioFrame::new(pcm, sample_rate)).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });

        // Outbound: mixer -> encode -> socket.
        let send_cancel = cancel.clone();
        tokio::spawn(async move {
            let mut encoder = create_encoder(codec);
            let pt = payload_type(codec);
            let ssrc: u32 = rand::random();
            let mut seq: u16 = rand::random();
            let mut timestamp: u32 = 0;
            loop {
                tokio::select! {
                    _ = send_cancel.cancelled() => break,
                    frame = from_mixer.recv() => {
                        let Some(frame) = frame else { break; };
                        let payload = encoder.encode(&frame.samples);
                        let mut packet = Vec::with_capacity(RTP_HEADER_LEN + payload.len());
                        packet.push(0x80); // V=2, no padding/extension/CSRC
                        packet.push(pt); // marker=0 + payload type
                        packet.extend_from_slice(&seq.to_be_bytes());
                        packet.extend_from_slice(&timestamp.to_be_bytes());
                        packet.extend_from_slice(&ssrc.to_be_bytes());
                        packet.extend_from_slice(&payload);
                        if socket.send_to(&packet, remote).await.is_err() {
                            break;
                        }
                        seq = seq.wrapping_add(1);
                        timestamp = timestamp.wrapping_add(frame.samples.len() as u32);
                    }
                }
            }
        });

        Self { cancel }
    }

    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

impl Drop for RtpPump {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn rtp_pump_round_trips_pcm_over_real_udp() {
        // The pump's socket and a stand-in "remote" peer.
        let pump_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let remote_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let pump_addr = pump_sock.local_addr().unwrap();
        let remote_addr = remote_sock.local_addr().unwrap();

        let (to_mixer_tx, mut to_mixer_rx) = mpsc::channel(16);
        let (from_mixer_tx, from_mixer_rx) = mpsc::channel(16);

        let _pump = RtpPump::start(
            pump_sock,
            remote_addr,
            CodecType::PCMU,
            8000,
            to_mixer_tx,
            from_mixer_rx,
        );

        // ── Outbound: PCM into the mixer channel → RTP on the wire. ─────────
        let pcm: Vec<i16> = (0..160).map(|i| ((i * 100) % 8000) as i16).collect();
        from_mixer_tx
            .send(AudioFrame::new(pcm.clone(), 8000))
            .await
            .unwrap();

        let mut buf = [0u8; 2048];
        let (n, src) = timeout(Duration::from_secs(1), remote_sock.recv_from(&mut buf))
            .await
            .expect("rtp not received")
            .unwrap();
        assert_eq!(src, pump_addr, "RTP came from the pump's socket");
        assert!(n > RTP_HEADER_LEN, "has an RTP header + payload");
        assert_eq!(buf[0], 0x80, "RTP version 2");
        assert_eq!(buf[1] & 0x7f, 0, "PCMU payload type 0");
        let mut decoder = create_decoder(CodecType::PCMU);
        assert_eq!(decoder.decode(&buf[RTP_HEADER_LEN..n]).len(), 160);

        // ── Inbound: RTP on the wire → decoded PCM in the mixer channel. ────
        let mut encoder = create_encoder(CodecType::PCMU);
        let payload = encoder.encode(&pcm);
        let mut packet = vec![0x80, 0x00, 0, 1, 0, 0, 0, 0, 0, 0, 0, 7];
        packet.extend_from_slice(&payload);
        remote_sock.send_to(&packet, pump_addr).await.unwrap();

        let frame = timeout(Duration::from_secs(1), to_mixer_rx.recv())
            .await
            .expect("decoded frame not received")
            .expect("channel open");
        assert_eq!(frame.samples.len(), 160, "decoded a full 20ms PCMU frame");
    }

    /// The whole media path: A's RTP → pump → switch mixer → pump → B's RTP.
    #[tokio::test]
    async fn audio_flows_rtp_to_rtp_through_the_switch_mixer() {
        use super::super::fake::FakeSession;
        use super::super::switch::CallSwitch;
        use super::super::{Direction, SessionState};

        let mut switch = CallSwitch::new();
        let (sa, _pa) = FakeSession::new(Direction::Inbound, SessionState::Active);
        let (sb, _pb) = FakeSession::new(Direction::Outbound, SessionState::Active);
        let ia = switch.add_port(Box::new(sa)).await;
        let ib = switch.add_port(Box::new(sb)).await;
        let (a_in, a_out) = switch.take_audio(ia).unwrap();
        let (b_in, b_out) = switch.take_audio(ib).unwrap();

        // A pump per leg, each with a stand-in remote peer.
        let sock_a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let remote_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let _pump_a = RtpPump::start(
            sock_a.clone(),
            remote_a.local_addr().unwrap(),
            CodecType::PCMU,
            8000,
            a_in,
            a_out,
        );
        let sock_b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let remote_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let _pump_b = RtpPump::start(
            sock_b.clone(),
            remote_b.local_addr().unwrap(),
            CodecType::PCMU,
            8000,
            b_in,
            b_out,
        );

        // A's remote streams loud RTP audio into A's pump.
        let mut encoder = create_encoder(CodecType::PCMU);
        let payload = encoder.encode(&vec![6000i16; 160]);
        let mut packet = vec![0x80, 0x00, 0, 1, 0, 0, 0, 0, 0, 0, 0, 9];
        packet.extend_from_slice(&payload);
        let pump_a_addr = sock_a.local_addr().unwrap();
        let feeder = tokio::spawn(async move {
            for _ in 0..50 {
                let _ = remote_a.send_to(&packet, pump_a_addr).await;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });

        // B's remote should receive mixed RTP that decodes to non-silence.
        let mut decoder = create_decoder(CodecType::PCMU);
        let mut buf = [0u8; 2048];
        let mut heard_audio = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            match timeout(Duration::from_millis(500), remote_b.recv_from(&mut buf)).await {
                Ok(Ok((n, _))) if n > RTP_HEADER_LEN => {
                    let pcm = decoder.decode(&buf[RTP_HEADER_LEN..n]);
                    if pcm.iter().any(|&s| s.unsigned_abs() > 500) {
                        heard_audio = true;
                        break;
                    }
                }
                _ => {}
            }
        }

        feeder.abort();
        assert!(
            heard_audio,
            "B's remote should receive A's audio as RTP, mixed through the switch"
        );
    }
}
