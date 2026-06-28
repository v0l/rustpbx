//! Test-only in-memory [`Session`] double, used to prove the move/handoff and
//! hangup-on-drop semantics of [`ConnectedPort`] without any real connection.
//!
//! It is NOT a product type — there is no non-SIP `Session` in the codebase
//! yet, and per the SIP-first stance the real concrete type is `SipSession`.

use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Shared observation point so a test can inspect what happened to a session
/// after it has been moved into a port and/or dropped, and inject events as if
/// they came from the remote party.
struct FakeShared {
    closed: AtomicBool,
    close_cause: Mutex<Option<CloseCause>>,
    reject_cause: Mutex<Option<RejectCause>>,
    events_tx: broadcast::Sender<SessionEvent>,
}

/// Handle a test keeps to observe and drive a [`FakeSession`] from the outside.
#[derive(Clone)]
pub struct SessionProbe(Arc<FakeShared>);

impl SessionProbe {
    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::SeqCst)
    }
    pub fn close_cause(&self) -> Option<CloseCause> {
        *self.0.close_cause.lock().unwrap()
    }
    pub fn reject_cause(&self) -> Option<RejectCause> {
        *self.0.reject_cause.lock().unwrap()
    }
    /// Inject a lifecycle event as if the remote party drove it (e.g. a remote
    /// BYE arriving while the switch owns the session).
    pub fn inject(&self, event: SessionEvent) {
        let _ = self.0.events_tx.send(event);
    }
}

struct FakeMedia {
    streams: Vec<MediaStream>,
    negotiation: NegotiationState,
}

impl MediaEndpoint for FakeMedia {
    fn streams(&self) -> &[MediaStream] {
        &self.streams
    }
    fn negotiation(&self) -> NegotiationState {
        self.negotiation
    }
}

pub struct FakeSession {
    id: SessionId,
    direction: Direction,
    state: SessionState,
    media: FakeMedia,
    /// Optional media seam, so a test can route audio through the switch/mixer
    /// without a real RTP socket (see `rtp_socket::loopback`).
    media_io: Option<super::rtp_socket::RtpStreamIo>,
    shared: Arc<FakeShared>,
}

impl FakeSession {
    /// Build a fake in the given direction/state. Returns the session plus a
    /// [`SessionProbe`] the test holds to observe teardown.
    pub fn new(direction: Direction, state: SessionState) -> (Self, SessionProbe) {
        let shared = Arc::new(FakeShared {
            closed: AtomicBool::new(false),
            close_cause: Mutex::new(None),
            reject_cause: Mutex::new(None),
            events_tx: broadcast::channel(8).0,
        });
        let session = FakeSession {
            id: SessionId::next(),
            direction,
            state,
            media: FakeMedia {
                streams: vec![MediaStream {
                    kind: MediaKind::Audio,
                    codec: crate::media::negotiate::NegotiatedCodec {
                        codec: audio_codec::CodecType::PCMU,
                        payload_type: 0,
                        clock_rate: 8000,
                        channels: 1,
                    },
                    direction: MediaDirection::SendRecv,
                    transport: rustrtc::TransportMode::Rtp,
                }],
                negotiation: NegotiationState::Negotiated,
            },
            media_io: None,
            shared: shared.clone(),
        };
        (session, SessionProbe(shared))
    }

    /// Attach a media seam so this fake participates in the switch's mixer.
    pub fn with_media_io(mut self, io: super::rtp_socket::RtpStreamIo) -> Self {
        self.media_io = Some(io);
        self
    }

    fn mark_closed(&self, cause: CloseCause) {
        // First close wins — a graceful close() must not be overwritten by the
        // Dropped cause when the port is later dropped.
        if !self.shared.closed.swap(true, Ordering::SeqCst) {
            *self.shared.close_cause.lock().unwrap() = Some(cause);
        }
    }
}

#[async_trait]
impl Session for FakeSession {
    fn id(&self) -> SessionId {
        self.id
    }
    fn direction(&self) -> Direction {
        self.direction
    }
    fn state(&self) -> SessionState {
        self.state
    }
    fn media(&self) -> &dyn MediaEndpoint {
        &self.media
    }
    fn take_media_io(&mut self) -> Option<super::rtp_socket::RtpStreamIo> {
        self.media_io.take()
    }
    fn events(&self) -> broadcast::Receiver<SessionEvent> {
        self.shared.events_tx.subscribe()
    }

    async fn accept(&mut self) -> Result<(), SessionError> {
        if self.direction != Direction::Inbound {
            return Err(SessionError::WrongDirection {
                expected: Direction::Inbound,
                actual: self.direction,
            });
        }
        if self.state != SessionState::Establishing {
            return Err(SessionError::WrongState {
                expected: SessionState::Establishing,
                actual: self.state,
            });
        }
        self.state = SessionState::Active;
        Ok(())
    }

    async fn reject(&mut self, cause: RejectCause) -> Result<(), SessionError> {
        if self.direction != Direction::Inbound {
            return Err(SessionError::WrongDirection {
                expected: Direction::Inbound,
                actual: self.direction,
            });
        }
        if self.state != SessionState::Establishing {
            return Err(SessionError::WrongState {
                expected: SessionState::Establishing,
                actual: self.state,
            });
        }
        self.state = SessionState::Closed;
        *self.shared.reject_cause.lock().unwrap() = Some(cause);
        Ok(())
    }

    async fn close(&mut self, cause: CloseCause) {
        self.state = SessionState::Closed;
        self.mark_closed(cause);
        let _ = self
            .shared
            .events_tx
            .send(SessionEvent::Terminated(TerminationCause::Hangup));
    }
}

impl Drop for FakeSession {
    fn drop(&mut self) {
        // Hangup-on-drop: if the connection was never gracefully closed, the
        // destructor tears it down (mirrors today's SipSession Drop teardown).
        self.mark_closed(CloseCause::Dropped);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn into_session_moves_ownership_without_hangup() {
        let (sess, probe) = FakeSession::new(Direction::Inbound, SessionState::Active);
        let port = ConnectedPort::new(Box::new(sess));
        let id = port.id();

        // Hand the session off: the port is consumed but the connection lives.
        let moved = port.into_session();
        assert!(
            !probe.is_closed(),
            "handoff must NOT hang up the connection"
        );
        assert_eq!(moved.id(), id, "id is stable across handoff");

        // Dropping the moved session (nobody re-homed it) is the hangup.
        drop(moved);
        assert!(probe.is_closed());
        assert_eq!(probe.close_cause(), Some(CloseCause::Dropped));
    }

    #[tokio::test]
    async fn drop_port_without_handoff_hangs_up() {
        let (sess, probe) = FakeSession::new(Direction::Inbound, SessionState::Active);
        let port = ConnectedPort::new(Box::new(sess));
        drop(port);
        assert!(probe.is_closed(), "dropping a live port hangs up");
        assert_eq!(probe.close_cause(), Some(CloseCause::Dropped));
    }

    #[tokio::test]
    async fn graceful_close_wins_over_drop() {
        let (sess, probe) = FakeSession::new(Direction::Inbound, SessionState::Active);
        let mut port = ConnectedPort::new(Box::new(sess));

        port.session_mut().close(CloseCause::Normal).await;
        assert!(probe.is_closed());
        assert_eq!(probe.close_cause(), Some(CloseCause::Normal));

        // Later dropping the port must NOT downgrade the recorded cause.
        drop(port);
        assert_eq!(probe.close_cause(), Some(CloseCause::Normal));
    }

    #[tokio::test]
    async fn accept_inbound_establishing_activates() {
        let (mut sess, _p) = FakeSession::new(Direction::Inbound, SessionState::Establishing);
        assert!(sess.accept().await.is_ok());
        assert_eq!(sess.state(), SessionState::Active);
    }

    #[tokio::test]
    async fn accept_outbound_is_wrong_direction() {
        let (mut sess, _p) = FakeSession::new(Direction::Outbound, SessionState::Establishing);
        assert_eq!(
            sess.accept().await,
            Err(SessionError::WrongDirection {
                expected: Direction::Inbound,
                actual: Direction::Outbound,
            })
        );
    }

    #[tokio::test]
    async fn accept_active_is_wrong_state() {
        let (mut sess, _p) = FakeSession::new(Direction::Inbound, SessionState::Active);
        assert_eq!(
            sess.accept().await,
            Err(SessionError::WrongState {
                expected: SessionState::Establishing,
                actual: SessionState::Active,
            })
        );
    }

    #[tokio::test]
    async fn reject_inbound_establishing_closes() {
        let (mut sess, probe) = FakeSession::new(Direction::Inbound, SessionState::Establishing);
        assert!(sess.reject(RejectCause::Busy).await.is_ok());
        assert_eq!(sess.state(), SessionState::Closed);
        assert_eq!(probe.reject_cause(), Some(RejectCause::Busy));
    }

    #[tokio::test]
    async fn unsupported_command_is_an_outcome_not_a_capability() {
        // The fake doesn't override `command`, so it uses the trait default:
        // every protocol-specific command is reported Unsupported. This is the
        // "capability is an outcome" design — no capabilities() to query.
        let (mut sess, _p) = FakeSession::new(Direction::Inbound, SessionState::Active);
        assert_eq!(
            sess.command(SessionCmd::Refer {
                target: "sip:bob@example.com".to_string()
            })
            .await,
            Err(SessionError::Unsupported)
        );
        assert_eq!(
            sess.command(SessionCmd::SendDtmf(vec![DtmfDigit::D1])).await,
            Err(SessionError::Unsupported)
        );
    }

    #[tokio::test]
    async fn events_stream_delivers_terminated_on_close() {
        let (mut sess, _p) = FakeSession::new(Direction::Inbound, SessionState::Active);
        let mut rx = sess.events();
        sess.close(CloseCause::Normal).await;
        let event = rx.try_recv().expect("a terminated event should be queued");
        assert_eq!(event, SessionEvent::Terminated(TerminationCause::Hangup));
    }

    #[tokio::test]
    async fn media_exposes_stream_set() {
        let (sess, _p) = FakeSession::new(Direction::Inbound, SessionState::Active);
        let streams = sess.media().streams();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].kind, MediaKind::Audio);
        assert_eq!(streams[0].codec.codec, audio_codec::CodecType::PCMU);
        assert_eq!(streams[0].direction, MediaDirection::SendRecv);
        assert_eq!(sess.media().negotiation(), NegotiationState::Negotiated);
    }
}
