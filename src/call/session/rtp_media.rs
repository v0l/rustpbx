//! SDP↔socket coordination for a plain RTP/AVP leg.
//!
//! Binds the local RTP socket, generates the SDP that advertises *that real
//! port*, and parses the remote's SDP for where to send. This is the piece that
//! lets a live SIP call actually carry audio: `SipSession::dial` offers
//! [`RtpMedia::local_sdp`], `accept` answers with it, and once the remote SDP is
//! known, [`RtpMedia::start_pump`] bridges the socket to the switch's mixer
//! channels.
//!
//! Plain RTP/AVP (PCMU/PCMA/…) only — no WebRTC/SRTP.

use super::rtp_pump::RtpPump;
use super::rtp_socket::{RtpSocketTask, RtpStreamIo};
use crate::media::conference_mixer::AudioFrame;
use anyhow::{Result, anyhow};
use audio_codec::CodecType;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

/// Wire name + static payload type for a codec (RFC 3551).
fn codec_wire(codec: CodecType) -> (&'static str, u8) {
    match codec {
        CodecType::PCMU => ("PCMU", 0),
        CodecType::PCMA => ("PCMA", 8),
        CodecType::G722 => ("G722", 9),
        CodecType::G729 => ("G729", 18),
        _ => ("PCMU", 0),
    }
}

/// A bound local RTP endpoint for one leg.
pub struct RtpMedia {
    socket: Arc<UdpSocket>,
    local_addr: SocketAddr,
    codec: CodecType,
    sample_rate: u32,
}

impl RtpMedia {
    /// Bind a local RTP socket (ephemeral port on loopback for now).
    pub async fn bind(codec: CodecType) -> Result<Self> {
        Self::bind_on("127.0.0.1", codec).await
    }

    pub async fn bind_on(ip: &str, codec: CodecType) -> Result<Self> {
        let socket = UdpSocket::bind(format!("{ip}:0")).await?;
        let local_addr = socket.local_addr()?;
        let sample_rate = 8000; // RTP clock for narrowband telephony codecs
        Ok(Self {
            socket: Arc::new(socket),
            local_addr,
            codec,
            sample_rate,
        })
    }

    pub fn local_port(&self) -> u16 {
        self.local_addr.port()
    }

    /// The SDP (offer or answer — same shape) advertising this leg's real RTP
    /// port and codec.
    pub fn local_sdp(&self) -> String {
        let (name, pt) = codec_wire(self.codec);
        let ip = self.local_addr.ip();
        let port = self.local_addr.port();
        format!(
            "v=0\r\n\
             o=- 0 0 IN IP4 {ip}\r\n\
             s=-\r\n\
             c=IN IP4 {ip}\r\n\
             t=0 0\r\n\
             m=audio {port} RTP/AVP {pt}\r\n\
             a=rtpmap:{pt} {name}/8000\r\n\
             a=sendrecv\r\n"
        )
    }

    /// Parse the remote RTP endpoint (connection IP + audio media port) from
    /// their SDP.
    pub fn parse_remote(sdp: &str) -> Result<SocketAddr> {
        let mut ip: Option<&str> = None;
        let mut port: Option<u16> = None;
        for line in sdp.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("c=IN IP4 ") {
                ip = Some(rest.trim());
            } else if let Some(rest) = line.strip_prefix("m=audio ") {
                port = rest.split_whitespace().next().and_then(|p| p.parse().ok());
            }
        }
        let ip = ip.ok_or_else(|| anyhow!("no c=IN IP4 line in SDP"))?;
        let port = port.ok_or_else(|| anyhow!("no m=audio port in SDP"))?;
        format!("{ip}:{port}")
            .parse()
            .map_err(|e| anyhow!("bad remote rtp addr: {e}"))
    }

    /// Start this leg's primary RTP socket task, sending to `remote`. Consumes
    /// the endpoint; the session keeps the returned [`RtpSocketTask`] alive and
    /// hands the [`RtpStreamIo`] seam to the switch. This is the unified-mixer
    /// path: the socket task does RTP framing only, leaving codec work to the
    /// mixer's tap.
    pub fn start_socket_task(self, remote: SocketAddr) -> (RtpSocketTask, RtpStreamIo) {
        let (_name, pt) = codec_wire(self.codec);
        // 20 ms frame at the codec's RTP clock (narrowband telephony: 8 kHz).
        let samples_per_frame = self.sample_rate / 50;
        RtpSocketTask::start(self.socket, remote, pt, samples_per_frame)
    }

    /// Bridge this socket to the switch's mixer channels, sending RTP to
    /// `remote`. Consumes the endpoint; the returned pump runs until dropped.
    pub fn start_pump(
        self,
        remote: SocketAddr,
        to_mixer: mpsc::Sender<AudioFrame>,
        from_mixer: mpsc::Receiver<AudioFrame>,
    ) -> RtpPump {
        RtpPump::start(
            self.socket,
            remote,
            self.codec,
            self.sample_rate,
            to_mixer,
            from_mixer,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_sdp_advertises_the_bound_port() {
        let media = RtpMedia::bind(CodecType::PCMU).await.unwrap();
        let sdp = media.local_sdp();
        assert!(sdp.contains(&format!("m=audio {} RTP/AVP 0", media.local_port())));
        assert!(sdp.contains("a=rtpmap:0 PCMU/8000"));
        assert!(sdp.contains("c=IN IP4 127.0.0.1"));
    }

    #[tokio::test]
    async fn local_sdp_round_trips_through_parse_remote() {
        let media = RtpMedia::bind(CodecType::PCMA).await.unwrap();
        let sdp = media.local_sdp();
        let parsed = RtpMedia::parse_remote(&sdp).unwrap();
        assert_eq!(parsed, media.local_addr);
        // PCMA advertises payload type 8.
        assert!(sdp.contains("m=audio") && sdp.contains("RTP/AVP 8"));
    }

    #[test]
    fn parse_remote_extracts_ip_and_port() {
        let sdp = "v=0\r\nc=IN IP4 192.0.2.7\r\nm=audio 49170 RTP/AVP 0\r\n";
        let addr = RtpMedia::parse_remote(sdp).unwrap();
        assert_eq!(addr, "192.0.2.7:49170".parse().unwrap());
    }

    #[test]
    fn parse_remote_errors_without_media() {
        assert!(RtpMedia::parse_remote("v=0\r\nc=IN IP4 192.0.2.7\r\n").is_err());
    }
}
