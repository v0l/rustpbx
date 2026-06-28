//! The first real [`Session`] implementation: one SIP INVITE leg.
//!
//! SIP-FIRST. This wraps a single rsipstack INVITE dialog behind the generic
//! [`Session`] trait:
//!   * inbound  (UAS) = [`ServerInviteDialog`] — can `accept`/`reject`/`bye`
//!   * outbound (UAC) = [`ClientInviteDialog`] — can `cancel`/`hangup`/`bye`
//!
//! Direction is intrinsic to which dialog kind we hold, so "answering" is just
//! `accept()` on an inbound, establishing leg — no telephony capability trait.
//!
//! Scope of THIS slice (surgical, reversible): the type exists and maps the
//! generic surface onto the real dialog API, with the decision logic
//! (state mapping, reject-code mapping) extracted as pure, unit-tested helpers.
//! It is NOT yet wired into the god object, so prod paths are unchanged. Two
//! follow-ups are called out with TODOs: real media-plane wiring and
//! teardown-on-drop integration.

use super::rtp_media::RtpMedia;
use super::rtp_pump::RtpPump;
use super::rtp_socket::{RtpSocketTask, RtpStreamIo};
use super::{
    CloseCause, Direction, DtmfDigit, MediaDirection, MediaEndpoint, MediaKind, MediaStream,
    MixerChannels, NegotiationState, RejectCause, Session, SessionCmd, SessionError, SessionEvent,
    SessionId, SessionState, TerminationCause,
};
use crate::media::negotiate::{MediaNegotiator, NegotiatedLegProfile};
use std::net::SocketAddr;
use crate::proxy::proxy_call::media_peer::MediaPeer;
use async_trait::async_trait;
use std::sync::Arc;
use rsipstack::dialog::client_dialog::ClientInviteDialog;
use rsipstack::dialog::dialog::{DialogState, DialogStateReceiver, TerminatedReason};
use rsipstack::dialog::dialog_layer::DialogLayer;
use rsipstack::dialog::invitation::InviteOption;
use rsipstack::dialog::server_dialog::ServerInviteDialog;
use rsipstack::sip::{Header, StatusCode, Uri};
use tokio::sync::broadcast;
use tracing::warn;

/// Capacity of a leg's event broadcast channel.
const EVENT_CHANNEL_CAPACITY: usize = 32;

// ---------------------------------------------------------------------------
// Pure decision helpers (no live SIP stack needed — unit-tested below)
// ---------------------------------------------------------------------------

/// Map an rsipstack dialog's coarse state to the generic [`SessionState`].
/// `WaitAck` (we sent 200, awaiting ACK) counts as `Active` — the call is up
/// from our side.
pub(crate) fn map_session_state(
    confirmed: bool,
    waiting_ack: bool,
    terminated: bool,
) -> SessionState {
    if terminated {
        SessionState::Closed
    } else if confirmed || waiting_ack {
        SessionState::Active
    } else {
        SessionState::Establishing
    }
}

/// Classify a SIP dialog teardown reason into a generic [`TerminationCause`].
pub(crate) fn termination_cause(reason: &TerminatedReason) -> TerminationCause {
    use TerminatedReason as T;
    match reason {
        T::UacBye | T::UasBye => TerminationCause::Hangup,
        T::UacCancel => TerminationCause::Cancelled,
        T::UacBusy | T::UasBusy | T::UasDecline => TerminationCause::Rejected,
        T::Timeout
        | T::ProxyError(_)
        | T::ProxyAuthRequired
        | T::UacOther(_)
        | T::UasOther(_) => TerminationCause::Failed,
    }
}

/// Translate a single dialog state into the lifecycle event it represents, if
/// any. Setup/mid-dialog states (Calling/Trying/Updated/Info/…) carry no
/// lifecycle event.
fn event_for_state(state: &DialogState) -> Option<SessionEvent> {
    match state {
        // TODO(early-media): distinguish 183-with-SDP as EarlyMedia.
        DialogState::Early(_, _) => Some(SessionEvent::Ringing),
        DialogState::WaitAck(_, _) | DialogState::Confirmed(_, _) => Some(SessionEvent::Answered),
        DialogState::Terminated(_, reason) => {
            Some(SessionEvent::Terminated(termination_cause(reason)))
        }
        _ => None,
    }
}

/// Parse a DTMF digit out of an incoming SIP INFO request body
/// (`application/dtmf-relay`: a `Signal=<digit>` line). Pure for testing.
pub(crate) fn dtmf_from_info_body(body: &[u8]) -> Option<DtmfDigit> {
    let text = String::from_utf8_lossy(body);
    for line in text.lines() {
        if let Some(value) = line.trim().strip_prefix("Signal=") {
            return value.trim().chars().next().and_then(DtmfDigit::from_char);
        }
    }
    None
}

/// Spawn a task that translates the dialog's raw state stream into deduplicated
/// lifecycle [`SessionEvent`]s on the broadcast channel, then exits on
/// termination. Incoming DTMF (SIP INFO) is surfaced as `SessionEvent::Dtmf`
/// (not deduplicated — repeated digits are distinct).
fn spawn_event_translator(
    mut state_rx: DialogStateReceiver,
    events_tx: broadcast::Sender<SessionEvent>,
) {
    tokio::spawn(async move {
        let mut last: Option<SessionEvent> = None;
        while let Some(state) = state_rx.recv().await {
            if let DialogState::Info(_, req, _) = &state {
                if let Some(d) = dtmf_from_info_body(req.body()) {
                    let _ = events_tx.send(SessionEvent::Dtmf(d));
                }
                continue;
            }
            let Some(event) = event_for_state(&state) else {
                continue;
            };
            if last != Some(event) {
                last = Some(event);
                let _ = events_tx.send(event);
            }
            if matches!(event, SessionEvent::Terminated(_)) {
                break;
            }
        }
    });
}

/// Map a generic [`RejectCause`] to the SIP status used to decline an inbound
/// INVITE.
pub(crate) fn reject_status(cause: RejectCause) -> StatusCode {
    match cause {
        RejectCause::Busy => StatusCode::BusyHere,
        RejectCause::Declined => StatusCode::Decline,
        RejectCause::NotFound => StatusCode::NotFound,
    }
}

/// Map an rsipstack/rustrtc SDP direction to the generic [`MediaDirection`].
pub(crate) fn map_direction(d: rustrtc::Direction) -> MediaDirection {
    match d {
        rustrtc::Direction::SendRecv => MediaDirection::SendRecv,
        rustrtc::Direction::SendOnly => MediaDirection::SendOnly,
        rustrtc::Direction::RecvOnly => MediaDirection::RecvOnly,
        rustrtc::Direction::Inactive => MediaDirection::Inactive,
    }
}

/// Project a per-leg negotiated profile into the generic stream set: one stream
/// per negotiated m-line (audio, then video). DTMF is an audio sub-format, not
/// its own stream, so it is not surfaced here.
///
/// TODO(per-stream-direction): the leg currently carries a single direction
/// (from the audio m-line); asymmetric per-m-line directions would need the
/// profile to track direction per stream.
pub(crate) fn streams_from_profile(profile: &NegotiatedLegProfile) -> Vec<MediaStream> {
    let direction = map_direction(profile.direction);
    let mut out = Vec::new();
    if let Some(audio) = &profile.audio {
        out.push(MediaStream {
            kind: MediaKind::Audio,
            codec: audio.clone(),
            direction,
            transport: profile.transport.clone(),
        });
    }
    if let Some(video) = &profile.video {
        out.push(MediaStream {
            kind: MediaKind::Video,
            codec: video.clone(),
            direction,
            transport: profile.transport.clone(),
        });
    }
    out
}

// ---------------------------------------------------------------------------
// Media plane (placeholder — real MediaPeer wiring is a later slice)
// ---------------------------------------------------------------------------

/// The media plane for a SIP leg: the streams negotiated from this leg's SDP,
/// plus the underlying RTP `MediaPeer` the mixer consumes. Empty +
/// `Negotiating` + no peer until set.
struct SipMedia {
    streams: Vec<MediaStream>,
    negotiation: NegotiationState,
    peer: Option<Arc<dyn MediaPeer>>,
}

impl Default for SipMedia {
    fn default() -> Self {
        Self {
            streams: Vec::new(),
            negotiation: NegotiationState::Negotiating,
            peer: None,
        }
    }
}

impl MediaEndpoint for SipMedia {
    fn streams(&self) -> &[MediaStream] {
        &self.streams
    }
    fn negotiation(&self) -> NegotiationState {
        self.negotiation
    }
    fn peer(&self) -> Option<Arc<dyn MediaPeer>> {
        self.peer.clone()
    }
}

// ---------------------------------------------------------------------------
// The leg
// ---------------------------------------------------------------------------

/// Which SIP role this leg plays. Direction is intrinsic to the variant.
/// `Clone` so `Drop` can hand a handle to a spawned teardown task.
#[derive(Clone)]
enum SipDialog {
    Inbound(ServerInviteDialog),
    Outbound(ClientInviteDialog),
}

/// Whether a dropped/closing session should still tear its connection down.
/// Pure so the hangup-on-drop decision is testable without a live dialog.
pub(crate) fn should_teardown_on_drop(already_closed: bool, state: SessionState) -> bool {
    !already_closed && state != SessionState::Closed
}

/// Best-effort SIP teardown for one leg. Shared by the graceful `close()` path
/// and the `Drop` safety-net (which runs it on a cloned handle in a spawned
/// task, since `Drop` cannot await).
async fn teardown_dialog(dialog: &SipDialog) {
    match dialog {
        SipDialog::Inbound(d) => {
            let s = d.state();
            if s.is_confirmed() || s.waiting_ack() {
                if let Err(e) = d.bye().await {
                    warn!(error = %e, "SipSession teardown inbound bye failed");
                }
            } else if !s.is_terminated()
                && let Err(e) = d.reject(Some(StatusCode::RequestTerminated), None)
            {
                warn!(error = %e, "SipSession teardown inbound reject failed");
            }
        }
        SipDialog::Outbound(d) => {
            // hangup() handles confirmed -> BYE / early -> CANCEL.
            if let Err(e) = d.hangup().await {
                warn!(error = %e, "SipSession teardown outbound hangup failed");
            }
        }
    }
}

/// One SIP INVITE leg, exposed as a generic [`Session`].
pub struct SipSession {
    id: SessionId,
    dialog: SipDialog,
    /// SDP answer + headers applied when an inbound leg is `accept()`ed. Set by
    /// [`SipSession::set_answer`] once media is negotiated.
    answer: Option<(Vec<Header>, Vec<u8>)>,
    media: SipMedia,
    /// This leg's local SDP, used as the base for hold/unhold re-INVITEs.
    local_sdp: Option<String>,
    /// Set once the leg has been gracefully closed, so the `Drop` safety-net
    /// does not fire a duplicate teardown.
    closed: bool,
    /// Broadcast of lifecycle events, fed by the dialog state translator task.
    events_tx: broadcast::Sender<SessionEvent>,
    /// Bound RTP endpoint for this leg, taken when the pump starts.
    rtp: Option<RtpMedia>,
    /// Remote RTP endpoint parsed from the negotiated SDP.
    rtp_remote: Option<SocketAddr>,
    /// The running RTP pump (kept alive for the leg's lifetime). Legacy
    /// conference-mixer path; superseded by the socket-task seam below.
    pump: Option<RtpPump>,
    /// This leg's primary RTP socket task (unified-mixer path), kept alive for
    /// the leg's lifetime. Started by [`SipSession::attach_rtp`].
    socket_task: Option<RtpSocketTask>,
    /// The RTP media seam handed to the switch via [`Session::take_media_io`].
    media_io: Option<RtpStreamIo>,
}

impl SipSession {
    /// Wrap an inbound (UAS) INVITE leg — a caller dialling into us — together
    /// with the dialog's state receiver, which drives the event stream.
    pub fn inbound(dialog: ServerInviteDialog, state_rx: DialogStateReceiver) -> Self {
        Self::new(SipDialog::Inbound(dialog), state_rx)
    }

    /// Wrap an outbound (UAC) INVITE leg — a callee we dialled — together with
    /// the dialog's state receiver.
    pub fn outbound(dialog: ClientInviteDialog, state_rx: DialogStateReceiver) -> Self {
        Self::new(SipDialog::Outbound(dialog), state_rx)
    }

    fn new(dialog: SipDialog, state_rx: DialogStateReceiver) -> Self {
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        spawn_event_translator(state_rx, events_tx.clone());
        Self {
            id: SessionId::next(),
            dialog,
            answer: None,
            media: SipMedia::default(),
            local_sdp: None,
            closed: false,
            events_tx,
            rtp: None,
            rtp_remote: None,
            pump: None,
            socket_task: None,
            media_io: None,
        }
    }

    /// Attach this leg's bound RTP endpoint and the negotiated remote address.
    /// The session owns the socket; its primary socket task is started lazily on
    /// the first [`Session::take_media_io`] (unified-mixer path), or the legacy
    /// pump starts on `connect_media` (conference path). The two are mutually
    /// exclusive per leg.
    pub fn attach_rtp(&mut self, rtp: RtpMedia, remote: SocketAddr) {
        self.rtp = Some(rtp);
        self.rtp_remote = Some(remote);
    }

    /// Originate an OUTBOUND call (the `dial` factory). Drives the INVITE to
    /// completion via the dialog layer and resolves to a connected outbound
    /// `SipSession` once the callee answers (or an error if it fails/rejects).
    /// Outbound establishment is protocol-specific, so it lives here — not on
    /// the generic trait.
    pub async fn dial(
        dialog_layer: &DialogLayer,
        opt: InviteOption,
    ) -> Result<(Self, Option<String>), SessionError> {
        let (state_tx, state_rx) = tokio::sync::mpsc::unbounded_channel();
        match dialog_layer.do_invite(opt, state_tx).await {
            Ok((dialog, final_response)) => {
                // The callee's answer SDP (for parsing its RTP endpoint).
                let answer_sdp = final_response
                    .filter(|r| !r.body().is_empty())
                    .map(|r| String::from_utf8_lossy(r.body()).to_string());
                Ok((Self::outbound(dialog, state_rx), answer_sdp))
            }
            Err(e) => {
                warn!(error = %e, "SipSession::dial failed");
                Err(SessionError::Backend)
            }
        }
    }

    /// Record this leg's local SDP (the offer/answer we sent), used as the base
    /// for hold/unhold re-INVITEs.
    pub fn set_local_sdp(&mut self, sdp: String) {
        self.local_sdp = Some(sdp);
    }

    /// Attach this leg's RTP media peer (the handle a switch's mixer consumes).
    pub fn set_media_peer(&mut self, peer: Arc<dyn MediaPeer>) {
        self.media.peer = Some(peer);
    }

    async fn send_dtmf(&self, digits: &[DtmfDigit]) -> Result<(), SessionError> {
        for digit in digits {
            let body = format!("Signal={}\r\nDuration=250\r\n", digit.as_char()).into_bytes();
            let headers = vec![Header::ContentType(
                rsipstack::sip::headers::ContentType::from("application/dtmf-relay"),
            )];
            let res = match &self.dialog {
                SipDialog::Inbound(d) => d.info(Some(headers), Some(body)).await,
                SipDialog::Outbound(d) => d.info(Some(headers), Some(body)).await,
            };
            res.map_err(|e| {
                warn!(error = %e, "SipSession DTMF INFO failed");
                SessionError::Backend
            })?;
        }
        Ok(())
    }

    async fn reoffer_direction(&self, direction: &str) -> Result<(), SessionError> {
        let base = self.local_sdp.as_deref().ok_or(SessionError::Backend)?;
        let sdp = rustrtc::modify_sdp_direction(base, direction);
        let headers = vec![Header::ContentType(
            rsipstack::sip::headers::ContentType::from("application/sdp"),
        )];
        let res = match &self.dialog {
            SipDialog::Inbound(d) => d.reinvite(Some(headers), Some(sdp.into_bytes())).await,
            SipDialog::Outbound(d) => d.reinvite(Some(headers), Some(sdp.into_bytes())).await,
        };
        res.map(|_| ()).map_err(|e| {
            warn!(error = %e, "SipSession hold/unhold re-INVITE failed");
            SessionError::Backend
        })
    }

    async fn send_refer(&self, target: &str) -> Result<(), SessionError> {
        let refer_to = Uri::try_from(target).map_err(|_| {
            warn!(target, "SipSession REFER target is not a valid URI");
            SessionError::Backend
        })?;
        let res = match &self.dialog {
            SipDialog::Inbound(d) => d.refer(refer_to, None, None).await,
            SipDialog::Outbound(d) => d.refer(refer_to, None, None).await,
        };
        res.map(|_| ()).map_err(|e| {
            warn!(error = %e, "SipSession REFER failed");
            SessionError::Backend
        })
    }

    /// Provide the SDP answer (and any extra headers) to send when an inbound
    /// leg is accepted.
    pub fn set_answer(&mut self, headers: Vec<Header>, sdp: Vec<u8>) {
        self.answer = Some((headers, sdp));
    }

    /// Record this leg's negotiated SDP (the inbound 200-OK answer, or the
    /// callee's answer for an outbound leg). Parses it at the wire boundary
    /// into typed streams and marks the media plane `Negotiated`.
    pub fn set_negotiated_sdp(&mut self, sdp: &str) {
        let profile = MediaNegotiator::extract_leg_profile(sdp);
        self.media.streams = streams_from_profile(&profile);
        self.media.negotiation = NegotiationState::Negotiated;
    }

    fn dialog_state(&self) -> DialogState {
        match &self.dialog {
            SipDialog::Inbound(d) => d.state(),
            SipDialog::Outbound(d) => d.state(),
        }
    }
}

#[async_trait]
impl Session for SipSession {
    fn id(&self) -> SessionId {
        self.id
    }

    fn direction(&self) -> Direction {
        match &self.dialog {
            SipDialog::Inbound(_) => Direction::Inbound,
            SipDialog::Outbound(_) => Direction::Outbound,
        }
    }

    fn state(&self) -> SessionState {
        let s = self.dialog_state();
        map_session_state(s.is_confirmed(), s.waiting_ack(), s.is_terminated())
    }

    fn media(&self) -> &dyn MediaEndpoint {
        &self.media
    }

    fn take_media_io(&mut self) -> Option<RtpStreamIo> {
        if self.media_io.is_none()
            && let (Some(rtp), Some(remote)) = (self.rtp.take(), self.rtp_remote)
        {
            let (task, io) = rtp.start_socket_task(remote);
            self.socket_task = Some(task);
            self.media_io = Some(io);
        }
        self.media_io.take()
    }

    fn events(&self) -> broadcast::Receiver<SessionEvent> {
        self.events_tx.subscribe()
    }

    fn connect_media(&mut self, channels: MixerChannels) -> Option<MixerChannels> {
        match (self.rtp.take(), self.rtp_remote) {
            (Some(rtp), Some(remote)) => {
                let (to_mixer, from_mixer) = channels;
                self.pump = Some(rtp.start_pump(remote, to_mixer, from_mixer));
                None
            }
            // No RTP attached: leave the channels for the switch to keep.
            (rtp, _) => {
                self.rtp = rtp;
                Some(channels)
            }
        }
    }

    async fn accept(&mut self) -> Result<(), SessionError> {
        let dialog = match &self.dialog {
            SipDialog::Inbound(d) => d,
            SipDialog::Outbound(_) => {
                return Err(SessionError::WrongDirection {
                    expected: Direction::Inbound,
                    actual: Direction::Outbound,
                });
            }
        };
        if self.state() != SessionState::Establishing {
            return Err(SessionError::WrongState {
                expected: SessionState::Establishing,
                actual: self.state(),
            });
        }
        let (headers, body) = match self.answer.clone() {
            Some((h, b)) => (Some(h), Some(b)),
            None => (None, None),
        };
        dialog.accept(headers, body).map_err(|e| {
            warn!(error = %e, "SipSession::accept failed");
            SessionError::Backend
        })
    }

    async fn reject(&mut self, cause: RejectCause) -> Result<(), SessionError> {
        let dialog = match &self.dialog {
            SipDialog::Inbound(d) => d,
            SipDialog::Outbound(_) => {
                return Err(SessionError::WrongDirection {
                    expected: Direction::Inbound,
                    actual: Direction::Outbound,
                });
            }
        };
        if self.state() != SessionState::Establishing {
            return Err(SessionError::WrongState {
                expected: SessionState::Establishing,
                actual: self.state(),
            });
        }
        dialog
            .reject(Some(reject_status(cause)), None)
            .map_err(|e| {
                warn!(error = %e, "SipSession::reject failed");
                SessionError::Backend
            })
    }

    async fn close(&mut self, _cause: CloseCause) {
        if !should_teardown_on_drop(self.closed, self.state()) {
            self.closed = true;
            return;
        }
        teardown_dialog(&self.dialog).await;
        self.closed = true;
    }

    async fn command(&mut self, cmd: SessionCmd) -> Result<(), SessionError> {
        // SIP supports all of these mid-dialog; an SFU/player session would
        // fall through to the trait default (`Unsupported`).
        match cmd {
            SessionCmd::SendDtmf(digits) => self.send_dtmf(&digits).await,
            SessionCmd::Hold => self.reoffer_direction("sendonly").await,
            SessionCmd::Unhold => self.reoffer_direction("sendrecv").await,
            SessionCmd::Refer { target } => self.send_refer(&target).await,
        }
    }
}

impl Drop for SipSession {
    fn drop(&mut self) {
        // Hangup-on-drop: if a live leg was never gracefully closed (e.g. its
        // owning ConnectedPort was dropped without a handoff), spawn a
        // best-effort teardown on a cloned handle. `Drop` cannot await.
        if !should_teardown_on_drop(self.closed, self.state()) {
            return;
        }
        let dialog = self.dialog.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move { teardown_dialog(&dialog).await });
            }
            Err(_) => {
                warn!(
                    session_id = self.id.get(),
                    "SipSession dropped outside a tokio runtime; teardown skipped"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests (pure helpers only — live dialogs need a running SIP stack)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dtmf_digit_wire_chars() {
        use super::super::DtmfDigit;
        assert_eq!(DtmfDigit::D0.as_char(), '0');
        assert_eq!(DtmfDigit::D9.as_char(), '9');
        assert_eq!(DtmfDigit::Star.as_char(), '*');
        assert_eq!(DtmfDigit::Pound.as_char(), '#');
        assert_eq!(DtmfDigit::A.as_char(), 'A');
        assert_eq!(DtmfDigit::D.as_char(), 'D');
    }

    #[test]
    fn parses_dtmf_from_sip_info_body() {
        use super::super::DtmfDigit;
        // application/dtmf-relay payload.
        let body = b"Signal=5\r\nDuration=250\r\n";
        assert_eq!(dtmf_from_info_body(body), Some(DtmfDigit::D5));
        assert_eq!(
            dtmf_from_info_body(b"Signal=#\r\n"),
            Some(DtmfDigit::Pound)
        );
        assert_eq!(dtmf_from_info_body(b"Duration=100\r\n"), None);
        assert_eq!(dtmf_from_info_body(b"Signal=Z\r\n"), None);
    }

    #[test]
    fn termination_cause_classification() {
        assert_eq!(
            termination_cause(&TerminatedReason::UacBye),
            TerminationCause::Hangup
        );
        assert_eq!(
            termination_cause(&TerminatedReason::UasBye),
            TerminationCause::Hangup
        );
        assert_eq!(
            termination_cause(&TerminatedReason::UacCancel),
            TerminationCause::Cancelled
        );
        assert_eq!(
            termination_cause(&TerminatedReason::UasDecline),
            TerminationCause::Rejected
        );
        assert_eq!(
            termination_cause(&TerminatedReason::UacBusy),
            TerminationCause::Rejected
        );
        assert_eq!(
            termination_cause(&TerminatedReason::Timeout),
            TerminationCause::Failed
        );
        assert_eq!(
            termination_cause(&TerminatedReason::ProxyAuthRequired),
            TerminationCause::Failed
        );
    }

    #[test]
    fn teardown_on_drop_decision() {
        // Live, never-closed leg => tear down.
        assert!(should_teardown_on_drop(false, SessionState::Active));
        assert!(should_teardown_on_drop(false, SessionState::Establishing));
        // Already gracefully closed => do not double-bye.
        assert!(!should_teardown_on_drop(true, SessionState::Active));
        // Already terminated => nothing to tear down.
        assert!(!should_teardown_on_drop(false, SessionState::Closed));
    }

    #[test]
    fn state_mapping_truth_table() {
        // terminated dominates everything.
        assert_eq!(map_session_state(true, true, true), SessionState::Closed);
        assert_eq!(map_session_state(false, false, true), SessionState::Closed);
        // confirmed or waiting-ack => Active.
        assert_eq!(map_session_state(true, false, false), SessionState::Active);
        assert_eq!(map_session_state(false, true, false), SessionState::Active);
        // neither => still establishing.
        assert_eq!(
            map_session_state(false, false, false),
            SessionState::Establishing
        );
    }

    #[test]
    fn reject_cause_maps_to_sip_status() {
        assert!(matches!(
            reject_status(RejectCause::Busy),
            StatusCode::BusyHere
        ));
        assert!(matches!(
            reject_status(RejectCause::Declined),
            StatusCode::Decline
        ));
        assert!(matches!(
            reject_status(RejectCause::NotFound),
            StatusCode::NotFound
        ));
    }

    use super::super::MediaKind;
    use audio_codec::CodecType;

    #[test]
    fn profile_with_no_media_yields_no_streams() {
        let streams = streams_from_profile(&NegotiatedLegProfile::default());
        assert!(streams.is_empty());
    }

    #[test]
    fn profile_audio_only_yields_one_audio_stream() {
        let profile = NegotiatedLegProfile {
            audio: Some(crate::media::negotiate::NegotiatedCodec {
                codec: CodecType::PCMA,
                payload_type: 8,
                clock_rate: 8000,
                channels: 1,
            }),
            ..NegotiatedLegProfile::default()
        };
        let streams = streams_from_profile(&profile);
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].kind, MediaKind::Audio);
        assert_eq!(streams[0].codec.payload_type, 8);
        assert_eq!(streams[0].codec.codec, CodecType::PCMA);
    }

    #[test]
    fn stereo_opus_is_expressible_as_a_stream() {
        // NOTE: `CodecType` is audio-only today, so a realistic video
        // NegotiatedCodec is not yet constructible. The stream *set* shape
        // (and stereo channels) is what we pin here; video lands when the
        // codec model grows video variants.
        let profile = NegotiatedLegProfile {
            audio: Some(crate::media::negotiate::NegotiatedCodec {
                codec: CodecType::Opus,
                payload_type: 111,
                clock_rate: 48000,
                channels: 2,
            }),
            ..NegotiatedLegProfile::default()
        };
        let streams = streams_from_profile(&profile);
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].kind, MediaKind::Audio);
        assert_eq!(streams[0].codec.channels, 2, "stereo Opus is expressible");
    }

    #[test]
    fn negotiated_sdp_parses_into_streams_at_wire_boundary() {
        // Minimal SDP answer with a single PCMU audio m-line (sendrecv default).
        let sdp = "v=0\r\n\
o=- 0 0 IN IP4 127.0.0.1\r\n\
s=-\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
m=audio 49170 RTP/AVP 0\r\n\
a=rtpmap:0 PCMU/8000\r\n";
        let profile = MediaNegotiator::extract_leg_profile(sdp);
        let streams = streams_from_profile(&profile);
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].kind, MediaKind::Audio);
        assert_eq!(streams[0].codec.codec, CodecType::PCMU);
        assert_eq!(streams[0].codec.payload_type, 0);
        assert_eq!(streams[0].direction, MediaDirection::SendRecv);
    }

    #[test]
    fn direction_mapping_covers_all_variants() {
        assert_eq!(
            map_direction(rustrtc::Direction::SendRecv),
            MediaDirection::SendRecv
        );
        assert_eq!(
            map_direction(rustrtc::Direction::SendOnly),
            MediaDirection::SendOnly
        );
        assert_eq!(
            map_direction(rustrtc::Direction::RecvOnly),
            MediaDirection::RecvOnly
        );
        assert_eq!(
            map_direction(rustrtc::Direction::Inactive),
            MediaDirection::Inactive
        );
    }

    #[test]
    fn hold_sdp_sendonly_is_parsed_into_stream_direction() {
        // A hold re-INVITE marks the audio m-line sendonly.
        let sdp = "v=0\r\n\
o=- 0 0 IN IP4 127.0.0.1\r\n\
s=-\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
m=audio 49170 RTP/AVP 0\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=sendonly\r\n";
        let profile = MediaNegotiator::extract_leg_profile(sdp);
        let streams = streams_from_profile(&profile);
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].direction, MediaDirection::SendOnly);
    }
}
