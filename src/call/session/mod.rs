//! Two-layer call model foundation — see `docs/call-port-design.md`.
//!
//! SIP-FIRST: `SipSession` is the real concrete type and will be built/proven
//! first against the existing SIP e2e suite. This module only lays down the
//! trait skeleton plus a test-only fake double, so the *move/handoff* and
//! *hangup-on-drop* semantics of the design can be proven with pure unit tests
//! before any SIP wiring exists. It intentionally touches no god-object code.
//!
//! The two layers:
//!   * [`Session`]  — durable, protocol-aware, owns ONE connection to one party.
//!   * [`ConnectedPort`] — ephemeral, switch-local; OWNS a `Session` by value.
//!     Dropping it without [`ConnectedPort::into_session`] == hangup.

use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Identity & generic vocabulary (no string identifiers — generated, monotonic)
// ---------------------------------------------------------------------------

/// Opaque session identity. Stable for the life of the connection, including
/// across a handoff between switches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(u64);

impl SessionId {
    /// Allocate a fresh, process-unique id.
    pub fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        SessionId(NEXT.fetch_add(1, Ordering::Relaxed))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

/// Which way the connection was established relative to the switch.
/// Inbound = *to* the switch (a call comes in); Outbound = *from* it (we dial /
/// join). "Waiting to be answered" is exactly `Inbound + Establishing`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Inbound,
    Outbound,
}

/// Generic connection lifecycle. Protocol-specific phases (SIP Ringing /
/// EarlyMedia, an SFU room-join handshake) surface as events, NOT as variants
/// here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Establishing,
    Active,
    Closing,
    Closed,
}

/// Why an established/active connection was torn down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseCause {
    /// Normal graceful teardown (SIP BYE).
    Normal,
    /// The owning [`ConnectedPort`] was dropped without a handoff — implicit
    /// hangup via the destructor (mirrors today's `SipSession` Drop teardown).
    Dropped,
    /// Caller/originator cancelled before answer.
    Cancelled,
    /// Teardown due to an error.
    Error,
}

/// Why an inbound, establishing connection was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectCause {
    Busy,
    Declined,
    NotFound,
}

/// The switch mixer's per-participant audio channels: send a leg's incoming
/// audio in, receive the mixed result out.
pub type MixerChannels = (
    tokio::sync::mpsc::Sender<crate::media::conference_mixer::AudioFrame>,
    tokio::sync::mpsc::Receiver<crate::media::conference_mixer::AudioFrame>,
);

/// A pushed, connection-level event (never polled). Generic shape; concrete
/// sessions translate their protocol's progress into these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEvent {
    /// Remote is ringing (SIP 180).
    Ringing,
    /// Remote is providing early media (SIP 183 with SDP).
    EarlyMedia,
    /// The connection became active (SIP 200/ACK).
    Answered,
    /// An inbound DTMF digit from the remote (SIP INFO / RFC 2833), surfaced so
    /// the call graph's `Collect` nodes can drive IVR.
    Dtmf(DtmfDigit),
    /// The connection ended; `cause` says why.
    Terminated(TerminationCause),
}

/// Why a connection terminated, classified from the protocol's teardown reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminationCause {
    /// Normal hangup (BYE).
    Hangup,
    /// Cancelled before answer (CANCEL).
    Cancelled,
    /// Declined/busy before answer.
    Rejected,
    /// Timeout or protocol/transport error.
    Failed,
}

/// Errors from generic [`Session`] operations. Protocol-specific operations are
/// issued as commands elsewhere and may return `Unsupported`; the generic
/// surface only fails on state/direction misuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionError {
    WrongState {
        expected: SessionState,
        actual: SessionState,
    },
    WrongDirection {
        expected: Direction,
        actual: Direction,
    },
    /// The underlying protocol/transport stack failed the operation. Detail is
    /// logged at the call site (kept off the enum to avoid carrying strings
    /// through the generic surface).
    Backend,
    /// The concrete session does not support this command (e.g. REFER on a
    /// non-SIP session). "Capability is an outcome, not a queryable property."
    Unsupported,
}

/// A single DTMF event. Modelled as an enum (not a char) so the digit alphabet
/// is closed; the wire `char` is produced only at the SIP INFO boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DtmfDigit {
    D0,
    D1,
    D2,
    D3,
    D4,
    D5,
    D6,
    D7,
    D8,
    D9,
    Star,
    Pound,
    A,
    B,
    C,
    D,
}

impl DtmfDigit {
    /// The wire character for this digit (RFC 2833 / `application/dtmf-relay`).
    pub fn as_char(self) -> char {
        match self {
            DtmfDigit::D0 => '0',
            DtmfDigit::D1 => '1',
            DtmfDigit::D2 => '2',
            DtmfDigit::D3 => '3',
            DtmfDigit::D4 => '4',
            DtmfDigit::D5 => '5',
            DtmfDigit::D6 => '6',
            DtmfDigit::D7 => '7',
            DtmfDigit::D8 => '8',
            DtmfDigit::D9 => '9',
            DtmfDigit::Star => '*',
            DtmfDigit::Pound => '#',
            DtmfDigit::A => 'A',
            DtmfDigit::B => 'B',
            DtmfDigit::C => 'C',
            DtmfDigit::D => 'D',
        }
    }

    /// Parse a wire DTMF character back into a digit (the SIP INFO / RFC 2833
    /// boundary). Returns `None` for anything outside the closed alphabet.
    pub fn from_char(c: char) -> Option<Self> {
        Some(match c {
            '0' => DtmfDigit::D0,
            '1' => DtmfDigit::D1,
            '2' => DtmfDigit::D2,
            '3' => DtmfDigit::D3,
            '4' => DtmfDigit::D4,
            '5' => DtmfDigit::D5,
            '6' => DtmfDigit::D6,
            '7' => DtmfDigit::D7,
            '8' => DtmfDigit::D8,
            '9' => DtmfDigit::D9,
            '*' => DtmfDigit::Star,
            '#' => DtmfDigit::Pound,
            'A' | 'a' => DtmfDigit::A,
            'B' | 'b' => DtmfDigit::B,
            'C' | 'c' => DtmfDigit::C,
            'D' | 'd' => DtmfDigit::D,
            _ => return None,
        })
    }
}

/// Protocol-specific operations issued as commands (not generic trait verbs).
/// The owning session honours what it can and returns
/// [`SessionError::Unsupported`] otherwise — capability is an outcome, not a
/// queryable property. `target` is a SIP URI string (a wire value).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionCmd {
    SendDtmf(Vec<DtmfDigit>),
    Hold,
    Unhold,
    Refer { target: String },
}

// ---------------------------------------------------------------------------
// Media plane (minimal first cut — see §2.1 of the design doc)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    Audio,
    Video,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NegotiationState {
    Negotiating,
    Negotiated,
    Renegotiating,
}

/// Per-stream media flow direction (SDP `a=sendrecv`/`sendonly`/`recvonly`/
/// `inactive`). Distinct from [`Direction`], which is the session's
/// inbound/outbound relationship to the switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaDirection {
    SendRecv,
    SendOnly,
    RecvOnly,
    Inactive,
}

/// One negotiated media stream (≈ one SDP m-line's selected codec). `media()`
/// exposes a *set* of these, not one pipe — so stereo Opus, audio+video, or an
/// SFU's many participant feeds are all expressible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaStream {
    pub kind: MediaKind,
    /// The selected codec + RTP params for this stream.
    pub codec: crate::media::negotiate::NegotiatedCodec,
    pub direction: MediaDirection,
    pub transport: rustrtc::TransportMode,
}

/// The stateful, multi-stream, renegotiable media plane of a connection. The
/// switch (mixer) reads `streams()` to decide routing/transcoding; the endpoint
/// owns whether/how it renegotiates.
pub trait MediaEndpoint: Send + Sync {
    fn streams(&self) -> &[MediaStream];
    fn negotiation(&self) -> NegotiationState;

    /// The underlying media-plane handle the switch's mixer consumes, when this
    /// endpoint is backed by a real RTP peer. `None` for endpoints without one
    /// (e.g. the test fake) — those simply aren't patched into the mixer.
    fn peer(&self) -> Option<std::sync::Arc<dyn crate::proxy::proxy_call::media_peer::MediaPeer>> {
        None
    }
}

// ---------------------------------------------------------------------------
// The durable connection
// ---------------------------------------------------------------------------

/// One connection to one party — primarily a SIP dialog (the same shape later
/// fits a WebRTC peer, an SFU participant feed, a media file). The generic
/// surface is intentionally NOT telephony-shaped: it carries only what every
/// connection has. Protocol-specific actions (REFER, DTMF, re-INVITE) are
/// issued as commands, not methods here. Outbound establishment is a concrete
/// factory (`SipSession::dial`, …), not a trait verb.
#[async_trait]
pub trait Session: Send + Sync {
    fn id(&self) -> SessionId;

    fn direction(&self) -> Direction;

    fn state(&self) -> SessionState;

    fn media(&self) -> &dyn MediaEndpoint;

    /// Subscribe to this connection's pushed lifecycle events. Each call returns
    /// an independent receiver (broadcast), so multiple observers — the switch,
    /// a recorder, the CDR reporter — can watch the same session.
    fn events(&self) -> tokio::sync::broadcast::Receiver<SessionEvent>;

    /// Wire this session's media to the switch mixer's audio channels. A SIP leg
    /// with a negotiated RTP endpoint starts its pump and consumes the channels
    /// (returns `None`); a session without media returns them unconsumed so the
    /// switch can keep them (e.g. for tests). Default: not consumed.
    fn connect_media(&mut self, channels: MixerChannels) -> Option<MixerChannels> {
        Some(channels)
    }

    /// Hand the switch this session's RTP media seam: a sink for outbound codec
    /// payloads and a stream of inbound codec payloads (RTP framing already
    /// stripped by the session's own socket task). The switch bridges this to
    /// the mixer's tap. Returns `None` for sessions without a plain-RTP leg
    /// (the test fake, or before media is attached) and after it has been taken
    /// once — the inbound receiver is single-owner.
    fn take_media_io(&mut self) -> Option<rtp_socket::RtpStreamIo> {
        None
    }

    /// Accept an INBOUND connection that is still `Establishing` (SIP 200 OK /
    /// WebRTC answer / SFU bot-invite accept). Generic — "answering" is just the
    /// response to an inbound, establishing connection. Errors if not
    /// `Inbound + Establishing`.
    async fn accept(&mut self) -> Result<(), SessionError>;

    /// Reject an INBOUND, `Establishing` connection (SIP 4xx/6xx / decline).
    async fn reject(&mut self, cause: RejectCause) -> Result<(), SessionError>;

    /// Universal teardown for an established/active connection (SIP BYE / leave
    /// room / close file). Idempotent; non-blocking in real impls.
    async fn close(&mut self, cause: CloseCause);

    /// Issue a protocol-specific command. The default is "unsupported" — a
    /// session opts in only to what its protocol can do. This is the message
    /// channel for REFER/DTMF/hold, kept off the universal surface.
    async fn command(&mut self, cmd: SessionCmd) -> Result<(), SessionError> {
        let _ = cmd;
        Err(SessionError::Unsupported)
    }
}

// ---------------------------------------------------------------------------
// The switch-local handle
// ---------------------------------------------------------------------------

/// A switch's view of a connection. It OWNS the [`Session`] by value
/// (move-based, exclusive ownership — never `Arc`). The whole point of the
/// split: the durable connection and the switch's handle to it are different
/// things with different lifetimes.
///
/// Ownership rules:
///   * Drop the port WITHOUT [`into_session`](Self::into_session) ⇒ the owned
///     `Session` is dropped ⇒ its destructor tears the connection down
///     (hangup-on-drop, mirroring today's `SipSession` Drop).
///   * [`into_session`](Self::into_session) moves the `Session` out, so the port
///     can be dropped *without* hanging up — this is how a connection is handed
///     off to another switch/port.
pub struct ConnectedPort {
    session: Option<Box<dyn Session>>,
}

impl ConnectedPort {
    /// Take ownership of a `Session` into a switch-local port.
    pub fn new(session: Box<dyn Session>) -> Self {
        Self {
            session: Some(session),
        }
    }

    /// The id of the owned session (stable across handoff).
    pub fn id(&self) -> SessionId {
        self.session_ref().id()
    }

    /// Shared access to the owned session.
    pub fn session_ref(&self) -> &dyn Session {
        self.session
            .as_deref()
            .expect("ConnectedPort session already handed off")
    }

    /// Exclusive access to the owned session (to drive accept/reject/close).
    pub fn session_mut(&mut self) -> &mut dyn Session {
        self.session
            .as_deref_mut()
            .expect("ConnectedPort session already handed off")
    }

    /// Hand the session off. Consumes the port and returns the owned `Session`
    /// WITHOUT triggering hangup-on-drop — the connection survives the move.
    pub fn into_session(mut self) -> Box<dyn Session> {
        self.session
            .take()
            .expect("ConnectedPort session already handed off")
        // `self` (now holding `None`) drops here without tearing anything down.
    }
}

impl Drop for ConnectedPort {
    fn drop(&mut self) {
        // If the session was NOT handed off, dropping the `Box<dyn Session>`
        // here runs the connection's own destructor — implicit hangup. (Real
        // SIP impls additionally emit a CDR / spawn graceful BYE from Drop, as
        // the current god object already does.)
        let _ = self.session.take();
    }
}

pub mod command_registry;
pub mod command_route;
pub mod dial_call;
pub mod graph;
pub mod graph_call;
pub mod graph_runner;
pub mod leg_map;
pub mod mixer_bridge;
pub mod player;
pub mod reducer;
pub mod rtp_media;
pub mod rtp_pump;
pub mod rtp_socket;
pub mod sip;
pub mod switch;
pub mod webrtc;

#[cfg(test)]
mod fake;
