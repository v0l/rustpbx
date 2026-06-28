//! WebRTC media adapter: bridges a `rustrtc` `PeerConnection`'s audio track onto
//! the **same** [`RtpStreamIo`] seam a `SipSession` exposes — proving WebRTC is
//! "just another protocol that plugs into the mixer/switch."
//!
//! The key fact: `rustrtc` hands us **codec payloads** (`AudioFrame.data`, with
//! its `payload_type`), having already done ICE / DTLS-SRTP / depacketisation.
//! So the adapter is a thin payload pump:
//!
//! ```text
//!   inbound:  track.recv()  → AudioFrame.data → RtpStreamIo.inbound  (→ mixer)
//!   outbound: RtpStreamIo.outbound → AudioFrame → source.send_audio  (→ peer)
//! ```
//!
//! The [`UnifiedMixer`](crate::media::unified_mixer) tap transcodes if the WebRTC
//! codec (e.g. Opus) differs from the other leg's — the mixer's existing job.
//! Nothing in the switch/mixer changes; a WebRTC leg yields a `take_media_io`
//! exactly like a SIP leg. ICE/DTLS/SRTP live in `rustrtc`, not here.

use std::sync::Arc;

use audio_codec::CodecType;
use bytes::Bytes;
use rustrtc::media::{AudioFrame, MediaSample, MediaStreamTrack, SampleStreamSource, SampleStreamTrack};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::rtp_socket::{RtpStreamIo, loopback};

/// One RTP frame's timestamp step at the codec clock (20 ms).
fn ts_step(codec: CodecType) -> u32 {
    codec.clock_rate() / 50
}

/// Pumps audio between a WebRTC peer's track/source and the mixer seam. Drops
/// (cancels both pumps) when dropped.
pub struct WebRtcMediaBridge {
    cancel: CancellationToken,
    _inbound: JoinHandle<()>,
    _outbound: JoinHandle<()>,
}

impl WebRtcMediaBridge {
    /// Start the bridge for a negotiated peer: `inbound` is the remote track we
    /// receive from, `outbound` is the source we send to. Returns the
    /// [`RtpStreamIo`] the switch consumes (identical to a SIP leg's).
    pub fn start(
        inbound: Arc<SampleStreamTrack>,
        outbound: SampleStreamSource,
        codec: CodecType,
    ) -> (Self, RtpStreamIo) {
        let (io, in_tx, mut out_rx) = loopback();
        let cancel = CancellationToken::new();

        // Inbound: peer → mixer. Strip the AudioFrame to its codec payload.
        let rc = cancel.clone();
        let inbound_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rc.cancelled() => break,
                    r = inbound.recv() => match r {
                        Ok(MediaSample::Audio(frame)) => {
                            if in_tx.send(frame.data.to_vec()).await.is_err() {
                                break;
                            }
                        }
                        Ok(_) => {}        // ignore video
                        Err(_) => break,   // track closed
                    },
                }
            }
        });

        // Outbound: mixer → peer. Wrap each payload as an AudioFrame.
        let pt = codec.payload_type();
        let clock = codec.clock_rate();
        let step = ts_step(codec);
        let rc = cancel.clone();
        let outbound_task = tokio::spawn(async move {
            let mut ts: u32 = 0;
            let mut seq: u16 = 0;
            loop {
                tokio::select! {
                    _ = rc.cancelled() => break,
                    msg = out_rx.recv() => match msg {
                        Some(payload) => {
                            let frame = AudioFrame {
                                data: Bytes::from(payload),
                                payload_type: Some(pt),
                                clock_rate: clock,
                                rtp_timestamp: ts,
                                sequence_number: Some(seq),
                                marker: false,
                                ..Default::default()
                            };
                            if outbound.send_audio(frame).await.is_err() {
                                break;
                            }
                            ts = ts.wrapping_add(step);
                            seq = seq.wrapping_add(1);
                        }
                        None => break,
                    },
                }
            }
        });

        (
            Self {
                cancel,
                _inbound: inbound_task,
                _outbound: outbound_task,
            },
            io,
        )
    }

    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

impl Drop for WebRtcMediaBridge {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustrtc::media::{MediaKind, track::sample_track};

    #[tokio::test]
    async fn webrtc_payloads_cross_to_and_from_the_mixer_seam() {
        // `in_*`: the peer→us track (peer writes to in_source, we recv on in_track).
        let (in_source, in_track, _) = sample_track(MediaKind::Audio, 16);
        // `out_*`: the us→peer source (we send to out_source, peer recvs on out_track).
        let (out_source, out_track, _) = sample_track(MediaKind::Audio, 16);

        let (_bridge, mut io) = WebRtcMediaBridge::start(in_track, out_source, CodecType::PCMU);

        // 1. A payload received from the WebRTC peer surfaces on the mixer seam.
        let payload: Vec<u8> = (0..160).map(|i| i as u8).collect();
        in_source
            .send_audio(AudioFrame {
                data: Bytes::from(payload.clone()),
                payload_type: Some(0),
                clock_rate: 8000,
                ..Default::default()
            })
            .await
            .unwrap();
        let got = tokio::time::timeout(std::time::Duration::from_secs(1), io.inbound.recv())
            .await
            .expect("inbound timed out")
            .expect("inbound closed");
        assert_eq!(got, payload, "WebRTC payload reaches the mixer seam unchanged");

        // 2. A payload from the mixer is sent to the WebRTC peer.
        io.outbound.send(payload.clone()).await.unwrap();
        let frame = tokio::time::timeout(std::time::Duration::from_secs(1), out_track.recv())
            .await
            .expect("outbound timed out")
            .expect("outbound closed");
        match frame {
            MediaSample::Audio(f) => {
                assert_eq!(f.data.as_ref(), payload.as_slice(), "mixer payload sent to peer");
                assert_eq!(f.payload_type, Some(0));
            }
            _ => panic!("expected audio"),
        }
    }
}
