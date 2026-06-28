//! The per-session primary RTP socket task.
//!
//! A session owns its connection, which includes its RTP socket. This task is
//! that socket's driver: it does **RTP framing only** — no codecs. Received
//! packets are stripped of their RTP header and the raw codec payload is pushed
//! into the session's inbound media channel; payloads written to the outbound
//! channel are RTP-framed (sequence/timestamp/SSRC) and sent to the remote.
//!
//! Codec work (decode / transcode / mix) lives in the mixer's tap, not here —
//! so this task is identical for every plain-RTP/AVP leg regardless of codec.
//! The inbound/outbound channels are the seam exposed through `media()`; the
//! switch's tap reads inbound payloads into the mixer and writes the mixer's
//! output back to outbound.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const RTP_VERSION: u8 = 2;
const RTP_HEADER_LEN: usize = 12;

/// The media-plane seam handed to the switch: write codec payloads to
/// `outbound`, read received codec payloads from `inbound`. RTP headers never
/// cross this boundary.
pub struct RtpStreamIo {
    pub outbound: mpsc::Sender<Vec<u8>>,
    pub inbound: mpsc::Receiver<Vec<u8>>,
}

/// Drives one session's RTP socket. Dropping it (or cancelling) stops both
/// directions.
pub struct RtpSocketTask {
    cancel: CancellationToken,
    _recv: JoinHandle<()>,
    _send: JoinHandle<()>,
}

impl RtpSocketTask {
    /// Start the socket task for a bound socket sending to `remote`.
    /// `payload_type` is the static RTP PT (e.g. 0 for PCMU); `samples_per_frame`
    /// is the RTP timestamp increment per outbound packet (e.g. 160 for 20 ms at
    /// 8 kHz).
    pub fn start(
        socket: Arc<UdpSocket>,
        remote: SocketAddr,
        payload_type: u8,
        samples_per_frame: u32,
    ) -> (Self, RtpStreamIo) {
        let (in_tx, in_rx) = mpsc::channel::<Vec<u8>>(256);
        let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(256);
        let cancel = CancellationToken::new();

        let recv_socket = socket.clone();
        let recv_cancel = cancel.clone();
        let recv = tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            loop {
                tokio::select! {
                    _ = recv_cancel.cancelled() => break,
                    r = recv_socket.recv_from(&mut buf) => {
                        let n = match r {
                            Ok((n, _from)) => n,
                            Err(_) => continue,
                        };
                        if let Some(payload) = strip_rtp(&buf[..n])
                            && in_tx.send(payload.to_vec()).await.is_err()
                        {
                            break;
                        }
                    }
                }
            }
        });

        let send_socket = socket;
        let send_cancel = cancel.clone();
        let send = tokio::spawn(async move {
            let ssrc: u32 = rand::random();
            let mut seq: u16 = rand::random();
            let mut ts: u32 = rand::random();
            loop {
                tokio::select! {
                    _ = send_cancel.cancelled() => break,
                    msg = out_rx.recv() => {
                        let Some(payload) = msg else { break };
                        let pkt = build_rtp(payload_type, seq, ts, ssrc, &payload);
                        let _ = send_socket.send_to(&pkt, remote).await;
                        seq = seq.wrapping_add(1);
                        ts = ts.wrapping_add(samples_per_frame);
                    }
                }
            }
        });

        (
            Self {
                cancel,
                _recv: recv,
                _send: send,
            },
            RtpStreamIo {
                outbound: out_tx,
                inbound: in_rx,
            },
        )
    }

    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

impl Drop for RtpSocketTask {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// An [`RtpStreamIo`] not backed by a socket, plus the opposite ends: push
/// payloads to the returned sender to feed the leg's inbound, read the returned
/// receiver to observe its outbound. Used for non-socket taps (a recorder sink
/// draining the room mix to a file) and for deterministic tests.
pub fn loopback() -> (RtpStreamIo, mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) {
    let (in_tx, in_rx) = mpsc::channel(256);
    let (out_tx, out_rx) = mpsc::channel(256);
    (
        RtpStreamIo {
            outbound: out_tx,
            inbound: in_rx,
        },
        in_tx,
        out_rx,
    )
}

/// Strip the RTP header, returning the payload slice. Honours the CSRC count and
/// the extension header. Returns `None` if the packet is malformed or not RTPv2.
fn strip_rtp(pkt: &[u8]) -> Option<&[u8]> {
    if pkt.len() < RTP_HEADER_LEN {
        return None;
    }
    let version = pkt[0] >> 6;
    if version != RTP_VERSION {
        return None;
    }
    let has_ext = (pkt[0] & 0x10) != 0;
    let csrc_count = (pkt[0] & 0x0f) as usize;
    let mut offset = RTP_HEADER_LEN + 4 * csrc_count;
    if has_ext {
        if pkt.len() < offset + 4 {
            return None;
        }
        let ext_words = u16::from_be_bytes([pkt[offset + 2], pkt[offset + 3]]) as usize;
        offset += 4 + 4 * ext_words;
    }
    if offset > pkt.len() {
        return None;
    }
    Some(&pkt[offset..])
}

/// Build a minimal RTPv2 packet (no CSRC, no extension, marker clear).
fn build_rtp(payload_type: u8, seq: u16, ts: u32, ssrc: u32, payload: &[u8]) -> Vec<u8> {
    let mut pkt = Vec::with_capacity(RTP_HEADER_LEN + payload.len());
    pkt.push(RTP_VERSION << 6); // V=2, P=0, X=0, CC=0
    pkt.push(payload_type & 0x7f); // M=0, PT
    pkt.extend_from_slice(&seq.to_be_bytes());
    pkt.extend_from_slice(&ts.to_be_bytes());
    pkt.extend_from_slice(&ssrc.to_be_bytes());
    pkt.extend_from_slice(payload);
    pkt
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_rtp_framing() {
        let payload = vec![9u8; 160];
        let pkt = build_rtp(0, 1234, 5678, 0xdead_beef, &payload);
        assert_eq!(pkt.len(), RTP_HEADER_LEN + 160);
        assert_eq!(pkt[0] >> 6, RTP_VERSION);
        assert_eq!(pkt[1] & 0x7f, 0);
        assert_eq!(strip_rtp(&pkt), Some(payload.as_slice()));
    }

    #[test]
    fn strip_rejects_non_rtp_and_short() {
        assert_eq!(strip_rtp(&[0u8; 4]), None); // too short
        assert_eq!(strip_rtp(&[0u8; 12]), None); // version 0
    }

    #[test]
    fn strip_skips_csrc_and_extension() {
        // CC=2 → 2 CSRC (8 bytes) after the 12-byte header.
        let mut pkt = vec![(RTP_VERSION << 6) | 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        pkt.extend_from_slice(&[0u8; 8]); // CSRCs
        pkt.extend_from_slice(&[7u8, 7, 7]); // payload
        assert_eq!(strip_rtp(&pkt), Some([7u8, 7, 7].as_slice()));
    }

    #[tokio::test]
    async fn outbound_payload_is_framed_and_sent() {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer.local_addr().unwrap();
        let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());

        let (_task, io) = RtpSocketTask::start(sock, peer_addr, 0, 160);
        let payload: Vec<u8> = (0..160).map(|i| i as u8).collect();
        io.outbound.send(payload.clone()).await.unwrap();

        let mut buf = vec![0u8; 2048];
        let (n, _) = peer.recv_from(&mut buf).await.unwrap();
        assert_eq!(strip_rtp(&buf[..n]), Some(payload.as_slice()));
    }

    #[tokio::test]
    async fn inbound_rtp_surfaces_payload() {
        let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let task_addr = sock.local_addr().unwrap();
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer.local_addr().unwrap();

        let (_task, mut io) = RtpSocketTask::start(sock, peer_addr, 0, 160);
        let payload: Vec<u8> = (0..160).map(|i| (255 - i) as u8).collect();
        let pkt = build_rtp(0, 1, 0, 1, &payload);
        peer.send_to(&pkt, task_addr).await.unwrap();

        let got = tokio::time::timeout(std::time::Duration::from_secs(1), io.inbound.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, payload);
    }
}
