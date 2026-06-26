//! SipSession adapter for the stream-based queue controller.
//!
//! This is the **only** place where the pure/event-driven queue engine
//! (`crate::call::graph`) touches the SIP/media world. It provides:
//!
//! * [`SipSessionQueueBackend`] — implements [`QueueBackend`] by reusing the
//!   session's existing dial / answer / bridge / playback helpers.
//! * [`CalleeEventTranslator`] — implements [`EventSource`] by reading the
//!   caller and callee dialog-state channels and mapping `DialogState` →
//!   `GraphEvent`. It does **not** borrow the session.
//! * [`SipSession::run_queue_graph`] — the dispatch entry point invoked from
//!   `process()` behind a config flag.
//!
//! Everything decision-related lives in the pure reducer; this file is wiring.

use super::{CalleeError, SipSession, into_callee_err};
use crate::call::graph::executor::EventSource;
use crate::call::graph::{
    FallbackPlan, GraphConfig, GraphEvent, GraphPhase, NodeId, PlayerKind, QueueBackend,
    QueueController, QueueGraph, Strategy,
};
use anyhow::Result;
use async_trait::async_trait;
use rsipstack::dialog::DialogId;
use rsipstack::dialog::client_dialog::ClientInviteDialog;
use rsipstack::dialog::dialog::DialogState;
use rsipstack::sip::{Response, StatusCode, Uri};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Per-target dial bookkeeping shared between the backend (writer) and the
/// event translator (reader).
struct DialEntry {
    dialog: ClientInviteDialog,
    callee_uri: Uri,
    invite_option: rsipstack::dialog::invitation::InviteOption,
    response: Option<Response>,
    connected: bool,
}

#[derive(Default)]
struct SharedDials {
    /// Call-ID → node. Keyed on Call-ID (not the full `DialogId`) because
    /// `do_invite_async` registers an *early* dialog id with no remote tag,
    /// while `DialogState::Confirmed` carries the *confirmed* id (remote tag
    /// added). The Call-ID is stable across that transition.
    node_by_callid: HashMap<String, NodeId>,
    entries: HashMap<NodeId, DialEntry>,
}

impl SharedDials {
    fn node_for(&self, id: &DialogId) -> Option<NodeId> {
        self.node_by_callid.get(&id.call_id).cloned()
    }
}

type Shared = Arc<Mutex<SharedDials>>;

/// Effect executor over a live [`SipSession`].
struct SipSessionQueueBackend<'a> {
    session: &'a mut SipSession,
    targets: Vec<crate::call::Location>,
    fallback_targets: Vec<crate::call::Location>,
    dials: Shared,
    hold_audio: Option<String>,
    default_expires: u64,
}

impl SipSessionQueueBackend<'_> {
    /// Shared dial path used by both candidate and fallback targets.
    async fn do_dial(&mut self, node: &NodeId, target: &crate::call::Location) -> Result<()> {
        let (option, callee_uri, _call_id) = self
            .session
            .build_target_invite_option(target, None)
            .await
            .map_err(|(c, t, _)| anyhow::anyhow!("build invite failed: {c} {t}"))?;

        let state_tx = self
            .session
            .callee_event_tx
            .clone()
            .ok_or_else(|| anyhow::anyhow!("no callee event sender"))?;

        let (dialog, _join) = self
            .session
            .server
            .dialog_layer
            .do_invite_async(option.clone(), state_tx)?;

        let dialog_id = dialog.id();
        info!(%node, %callee_uri, %dialog_id, "queue-graph: INVITE sent");

        let mut dials = self.dials.lock().unwrap();
        dials
            .node_by_callid
            .insert(dialog_id.call_id.clone(), node.clone());
        dials.entries.insert(
            node.clone(),
            DialEntry {
                dialog,
                callee_uri,
                invite_option: option,
                response: None,
                connected: false,
            },
        );
        Ok(())
    }
}

#[async_trait]
impl QueueBackend for SipSessionQueueBackend<'_> {
    async fn dial_target(&mut self, node: &NodeId, idx: usize) -> Result<()> {
        let target = self
            .targets
            .get(idx)
            .ok_or_else(|| anyhow::anyhow!("target index {idx} out of range"))?
            .clone();
        self.do_dial(node, &target).await
    }

    async fn dial_fallback(&mut self, node: &NodeId) -> Result<()> {
        let target = self
            .fallback_targets
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no reachable fallback target"))?;
        info!(%node, "queue-graph: dialing fallback target");
        self.do_dial(node, &target).await
    }

    async fn relay_caller_ringing(&mut self) {
        if let Err(e) = self.session.server_dialog.ringing(None, None) {
            warn!(error = %e, "queue-graph: failed to relay 180 ringing to caller");
        }
    }

    async fn cancel_invite(&mut self, node: &NodeId) {
        let dialog = self
            .dials
            .lock()
            .unwrap()
            .entries
            .get(node)
            .map(|e| e.dialog.clone());
        if let Some(dialog) = dialog {
            info!(%node, "queue-graph: CANCEL ringing target");
            if let Err(e) = dialog.cancel().await {
                warn!(%node, error = %e, "queue-graph: CANCEL failed");
            }
        }
    }

    async fn hangup_callee(&mut self, node: &NodeId) {
        let dialog = self
            .dials
            .lock()
            .unwrap()
            .entries
            .get(node)
            .map(|e| e.dialog.clone());
        if let Some(dialog) = dialog {
            info!(%node, "queue-graph: BYE connected target");
            if let Err(e) = dialog.hangup().await {
                warn!(%node, error = %e, "queue-graph: callee BYE failed");
            }
        }
    }

    async fn answer_caller(&mut self) {
        if self.session.server_dialog.state().is_confirmed() {
            return;
        }
        let caller_answer = self.session.prepare_app_caller_media_bridge().await;
        if let Err(e) = self.session.accept_call(None, caller_answer, None).await {
            warn!(error = %e, "queue-graph: failed to answer caller");
        }
    }

    async fn hangup_caller(&mut self, code: u16) {
        if self.session.server_dialog.state().is_confirmed() {
            if let Err(e) = self.session.server_dialog.bye().await {
                warn!(error = %e, "queue-graph: caller BYE failed");
            }
        } else {
            let status = StatusCode::Other(code, "Busy Here".to_string());
            if let Err(e) = self.session.server_dialog.reject(Some(status), None) {
                warn!(error = %e, "queue-graph: caller reject failed");
            }
        }
    }

    async fn start_player(&mut self, kind: PlayerKind) {
        if !matches!(kind, PlayerKind::Hold) {
            return;
        }
        let Some(audio) = self.hold_audio.clone() else {
            return;
        };
        self.session.prepare_queue_playback_media().await;
        if let Err(e) = self
            .session
            .play_audio_file(&audio, false, SipSession::QUEUE_HOLD_TRACK_ID, true)
            .await
        {
            warn!(error = %e, "queue-graph: failed to start hold music");
        }
    }

    async fn stop_player(&mut self, _kind: PlayerKind) {
        self.session
            .stop_playback_track(SipSession::QUEUE_HOLD_TRACK_ID, false)
            .await;
    }

    async fn bridge(&mut self, _a: &NodeId, b: &NodeId) {
        // `b` is the winning callee node. Pull the stashed answer + INVITE
        // context and reuse the existing answer/bridge finalizer.
        let entry = {
            let mut dials = self.dials.lock().unwrap();
            if let Some(e) = dials.entries.get_mut(b) {
                e.connected = true;
            }
            dials.entries.get(b).map(|e| {
                (
                    e.dialog.id(),
                    e.response.clone(),
                    e.callee_uri.clone(),
                    e.invite_option.clone(),
                )
            })
        };
        let Some((dialog_id, response, callee_uri, invite_option)) = entry else {
            warn!(%b, "queue-graph: bridge requested for unknown node");
            return;
        };
        if let Err(e) = self
            .session
            .finalize_callee_connection(
                dialog_id,
                response,
                callee_uri,
                Some(SipSession::QUEUE_HOLD_TRACK_ID),
                &invite_option,
                self.default_expires,
            )
            .await
        {
            warn!(%b, error = ?e, "queue-graph: bridge (finalize) failed");
        }
    }

    async fn clear_routes(&mut self, _node: &NodeId) {
        // Bridge teardown is driven by the BYE on the callee dialog; nothing
        // extra to do here in the anchored-media model.
    }
}

/// Reads caller + callee dialog-state channels and maps them to `GraphEvent`s.
struct CalleeEventTranslator<'a> {
    caller_rx: &'a mut mpsc::UnboundedReceiver<DialogState>,
    callee_rx: &'a mut mpsc::UnboundedReceiver<DialogState>,
    dials: Shared,
    caller_gone: bool,
}

impl CalleeEventTranslator<'_> {
    fn map_callee(&mut self, st: DialogState) -> Option<GraphEvent> {
        match st {
            DialogState::Early(id, _resp) => {
                let node = self.dials.lock().unwrap().node_for(&id)?;
                Some(GraphEvent::CalleeRinging { node })
            }
            DialogState::Confirmed(id, resp) => {
                let mut dials = self.dials.lock().unwrap();
                let node = dials.node_for(&id)?;
                if let Some(entry) = dials.entries.get_mut(&node) {
                    entry.response = Some(resp);
                }
                Some(GraphEvent::CalleeAnswered { node })
            }
            DialogState::Terminated(id, _reason) => {
                let dials = self.dials.lock().unwrap();
                let node = dials.node_for(&id)?;
                let connected = dials
                    .entries
                    .get(&node)
                    .map(|e| e.connected)
                    .unwrap_or(false);
                Some(if connected {
                    GraphEvent::CalleeBye { node }
                } else {
                    GraphEvent::CalleeRejected { node, code: 480 }
                })
            }
            _ => None,
        }
    }
}

#[async_trait]
impl EventSource for CalleeEventTranslator<'_> {
    async fn next(&mut self) -> Option<GraphEvent> {
        loop {
            tokio::select! {
                st = self.caller_rx.recv() => {
                    match st {
                        Some(DialogState::Terminated(_, _)) if !self.caller_gone => {
                            self.caller_gone = true;
                            return Some(GraphEvent::CallerBye);
                        }
                        Some(_) => continue,
                        None => {
                            // Caller channel closed; nothing more to translate.
                            return None;
                        }
                    }
                }
                st = self.callee_rx.recv() => {
                    match st {
                        Some(st) => {
                            if let Some(ev) = self.map_callee(st) {
                                return Some(ev);
                            }
                            continue;
                        }
                        None => return None,
                    }
                }
            }
        }
    }
}

impl SipSession {
    /// Run an inbound queue through the stream-based call-graph controller.
    ///
    /// Returns `Ok(())` when a target answered and the caller is bridged (the
    /// `process()` main loop then takes over the live call). Returns `Err` when
    /// the hunt ended without a connection so the caller leg can be cleaned up.
    pub(crate) async fn run_queue_graph(
        &mut self,
        plan: &crate::call::QueuePlan,
        caller_state_rx: &mut mpsc::UnboundedReceiver<DialogState>,
        callee_state_rx: &mut mpsc::UnboundedReceiver<DialogState>,
    ) -> Result<(), CalleeError> {
        use crate::call::DialStrategy;

        self.meta.queue_name = Some(plan.queue_name.clone());
        info!("queue-graph: executing queue via call-graph controller");

        let (agents, strategy) = match &plan.dial_strategy {
            Some(DialStrategy::Sequential(locs)) => (locs.clone(), Strategy::Sequential),
            Some(DialStrategy::Parallel(locs)) => (locs.clone(), Strategy::Parallel),
            None => {
                warn!("queue-graph: no dial strategy");
                return Err(into_callee_err(
                    &StatusCode::TemporarilyUnavailable,
                    Some("No dial strategy".to_string()),
                ));
            }
        };

        let targets = self
            .resolve_custom_targets(agents, plan.acd_policy.as_deref())
            .await;

        let hold_audio = plan
            .hold
            .as_ref()
            .and_then(|h| h.audio_file.clone());

        // Map the queue's fallback action onto the graph's fallback plan. A
        // redirect / transfer-to-URI becomes a dial+bridge of one more callee
        // leg; an explicit failure becomes a clean hangup; everything else
        // (re-queue, skill-group) is a busy hangup in this version.
        let (fallback_plan, fallback_uri) = self.resolve_queue_fallback(plan);
        let fallback_targets = match fallback_uri {
            Some(uri_str) => match rsipstack::sip::Uri::try_from(uri_str.as_str()) {
                Ok(uri) => {
                    let loc = crate::call::Location {
                        aor: uri,
                        ..Default::default()
                    };
                    self.resolve_custom_targets(vec![loc], None).await
                }
                Err(e) => {
                    warn!(uri = %uri_str, error = %e, "queue-graph: bad fallback URI");
                    Vec::new()
                }
            },
            None => Vec::new(),
        };

        let config = GraphConfig {
            strategy,
            target_count: targets.len(),
            ring_timeout: plan.ring_timeout,
            accept_immediately: plan.accept_immediately,
            has_hold_music: hold_audio.is_some(),
            fallback: fallback_plan,
        };

        let default_expires = self
            .server
            .proxy_config
            .session_expires
            .unwrap_or(crate::proxy::proxy_call::session_timer::DEFAULT_SESSION_EXPIRES);

        let dials: Shared = Arc::new(Mutex::new(SharedDials::default()));

        let backend = SipSessionQueueBackend {
            session: self,
            targets,
            fallback_targets,
            dials: dials.clone(),
            hold_audio,
            default_expires,
        };
        let source = CalleeEventTranslator {
            caller_rx: caller_state_rx,
            callee_rx: callee_state_rx,
            dials,
            caller_gone: false,
        };

        let controller = QueueController::new(QueueGraph::new(config), backend);
        let phase = controller.run(source).await.map_err(|e| {
            into_callee_err(
                &StatusCode::ServerInternalError,
                Some(format!("queue graph error: {e}")),
            )
        })?;

        match phase {
            GraphPhase::Connected => {
                info!("queue-graph: target connected; handing off to main loop");
                Ok(())
            }
            other => {
                info!(?other, "queue-graph: ended without connection");
                Err(into_callee_err(
                    &StatusCode::BusyHere,
                    Some("Queue ended without connection".to_string()),
                ))
            }
        }
    }

    /// Derive the graph fallback plan + an optional dial target URI from the
    /// queue plan's configured fallback action.
    fn resolve_queue_fallback(
        &self,
        plan: &crate::call::QueuePlan,
    ) -> (FallbackPlan, Option<String>) {
        use crate::call::{FailureAction, QueueFallbackAction, TransferEndpoint};
        match &plan.fallback {
            Some(QueueFallbackAction::Redirect { target }) => {
                (FallbackPlan::DialBridge, Some(target.to_string()))
            }
            Some(QueueFallbackAction::Failure(FailureAction::Transfer(
                TransferEndpoint::Uri(uri),
            ))) => (FallbackPlan::DialBridge, Some(uri.clone())),
            Some(QueueFallbackAction::Failure(FailureAction::Hangup { code, .. })) => {
                let c = code.as_ref().map(|s| s.code()).unwrap_or(486);
                (FallbackPlan::Hangup(c), None)
            }
            Some(QueueFallbackAction::Failure(FailureAction::PlayThenHangup {
                status_code,
                ..
            })) => (FallbackPlan::Hangup(status_code.code()), None),
            // None, re-queue, skill-group, IVR transfer: clean busy in v1.
            _ => (FallbackPlan::Hangup(486), None),
        }
    }
}
