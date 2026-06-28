//! New-engine call path (behind the `session_engine` flag).
//!
//! For a simple direct (`Targets`) call this routes the whole call through the
//! clean `call::session` stack instead of the legacy `SipSession` god object:
//! the caller leg becomes a `SipSession::inbound`, a SIP+RTP-backed `Dialer`
//! originates the callee(s), and the `DialCall`/`FlowReducer` drive
//! dial→answer→bridge. Each leg owns its RTP socket task; the `DialCall` claims
//! their media seams (`take_media_io`) into a `MixerBridge`, so audio crosses
//! the `UnifiedMixer` (passthrough/transcode for 2-party, mix for conferences).
//!
//! Plain RTP/AVP only — the dispatch in `build_and_serve` restricts this path to
//! `DialplanFlow::Targets`; everything else still uses the god object.

use crate::call::session::dial_call::{DialError, Dialer, DialCall};
use crate::call::session::graph::{CallGraph, GraphDef};
use crate::call::session::graph_call::{GraphCall, prompts_from_def};
use crate::call::session::graph_runner::{self, CallActions};
use crate::call::session::reducer::FlowReducer;
use crate::call::session::rtp_media::RtpMedia;
use crate::call::session::sip::SipSession;
use crate::call::session::Session;
use crate::call::{DialStrategy, DialplanFlow, Location};
use crate::media::wav_reader::WavReader;
use crate::proxy::proxy_call::CallContext;
use crate::proxy::server::SipServerRef;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use audio_codec::{CodecType, PcmBuf};
use std::time::Duration;

/// Whether the new engine can serve `flow` without regressing behaviour: plain
/// `Targets`, or a `Queue` chain whose features are all covered (hold + dial +
/// ring timeout + dial-stage fallback). Queues using announcements, failure
/// audio, rich fallback actions, ACD policy, retry codes, no-trying timeout, or
/// an `Application` tail still go to the god object.
pub fn supports_flow(flow: &DialplanFlow) -> bool {
    use crate::call::{FailureAction, QueueFallbackAction};
    let mut cur = flow;
    loop {
        match cur {
            DialplanFlow::Targets(_) => return true,
            DialplanFlow::Queue { plan, next } => {
                // Fallback action: only plain hangup / play-then-hangup are
                // covered (we answer the caller, optionally play a prompt, BYE).
                // Redirect / overflow-to-queue are not yet supported.
                let fallback_ok = match &plan.fallback {
                    None => true,
                    Some(QueueFallbackAction::Failure(
                        FailureAction::Hangup { .. } | FailureAction::PlayThenHangup { .. },
                    )) => true,
                    Some(_) => false,
                };
                let covered = plan.voice_prompts.is_none()
                    && plan.acd_policy.is_none()
                    && plan.retry_codes.is_none()
                    && plan.no_trying_timeout.is_none()
                    && fallback_ok;
                if !covered {
                    return false;
                }
                cur = next;
            }
            // An IVR/voicemail graph (a designer-built node graph carried in
            // `app_params`) is fully covered by the graph engine.
            DialplanFlow::Application { .. } => return graph_def(cur).is_some(),
        }
    }
}

/// Extract a designer graph from an `Application` flow (`app_name == "graph"`,
/// `app_params` = a serialized [`GraphDef`]).
fn graph_def(flow: &DialplanFlow) -> Option<GraphDef> {
    if let DialplanFlow::Application {
        app_name,
        app_params,
        ..
    } = flow
        && app_name == "graph"
    {
        return app_params
            .clone()
            .and_then(|p| serde_json::from_value(p).ok());
    }
    None
}

/// Turn a graph's self-contained dial targets (SIP URIs) into dialer locations.
fn graph_targets_to_locations(targets: &[String]) -> Vec<Location> {
    targets
        .iter()
        .filter_map(|t| {
            Some(Location {
                aor: t.parse().ok()?,
                destination: None,
                ..Default::default()
            })
        })
        .collect()
}

/// The failure-prompt audio file for a queue flow: an explicit `failure_audio`,
/// or the `PlayThenHangup` fallback's audio file.
fn failure_audio_file(flow: &DialplanFlow) -> Option<String> {
    use crate::call::{FailureAction, QueueFallbackAction};
    let DialplanFlow::Queue { plan, .. } = flow else {
        return None;
    };
    if let Some(f) = &plan.failure_audio {
        return Some(f.clone());
    }
    match &plan.fallback {
        Some(QueueFallbackAction::Failure(FailureAction::PlayThenHangup { audio_file, .. })) => {
            Some(audio_file.clone())
        }
        _ => None,
    }
}

/// Load a WAV file as mono PCM + its sample rate.
fn load_wav_mono(file: &str) -> Option<(PcmBuf, u32)> {
    let mut reader = WavReader::open(file)
        .map_err(|e| warn!(file, error = %e, "failed to open audio file"))
        .ok()?;
    let channels = reader.spec().channels.max(1) as usize;
    let rate = reader.spec().sample_rate;
    let all: PcmBuf = reader.samples().filter_map(|s| s.ok()).collect();
    let mono: PcmBuf = if channels <= 1 {
        all
    } else {
        all.iter().step_by(channels).copied().collect()
    };
    if mono.is_empty() { None } else { Some((mono, rate)) }
}

/// All dial targets across a flow's stages, in the reducer's global index order
/// (queue agents, then each fallback stage).
fn flatten_targets(flow: &DialplanFlow) -> Vec<Location> {
    let mut out = Vec::new();
    let mut cur = flow;
    loop {
        match cur {
            DialplanFlow::Targets(DialStrategy::Sequential(t))
            | DialplanFlow::Targets(DialStrategy::Parallel(t)) => {
                out.extend(t.clone());
                break;
            }
            DialplanFlow::Queue { plan, next } => {
                if let Some(DialStrategy::Sequential(t)) | Some(DialStrategy::Parallel(t)) =
                    &plan.dial_strategy
                {
                    out.extend(t.clone());
                }
                cur = next;
            }
            DialplanFlow::Application { .. } => break,
        }
    }
    out
}

/// The hold/MoH audio for a queue flow (mono PCM + its sample rate), loaded from
/// the queue's configured hold file. `None` for non-queue flows or if unreadable.
fn hold_audio(flow: &DialplanFlow) -> Option<(PcmBuf, u32)> {
    let DialplanFlow::Queue { plan, .. } = flow else {
        return None;
    };
    let file = plan.hold.as_ref()?.audio_file.as_ref()?;
    load_wav_mono(file)
}

/// The queue's per-dial ring timeout, if any.
fn ring_timeout(flow: &DialplanFlow) -> Option<Duration> {
    match flow {
        DialplanFlow::Queue { plan, .. } => plan.ring_timeout,
        _ => None,
    }
}
use rsipstack::dialog::dialog_layer::DialogLayer;
use rsipstack::dialog::invitation::InviteOption;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// A SIP+RTP dialer over a dialplan's resolved targets.
struct SipDialer {
    dialog_layer: Arc<DialogLayer>,
    targets: Vec<Location>,
    contact: rsipstack::sip::Uri,
    bind_ip: String,
}

#[async_trait]
impl Dialer for SipDialer {
    async fn dial(&self, target: usize) -> Result<Box<dyn Session>, DialError> {
        let loc = self.targets.get(target).ok_or(DialError)?;

        // Bind this callee leg's RTP endpoint and offer its real port.
        let callee_rtp = RtpMedia::bind_on(&self.bind_ip, CodecType::PCMU)
            .await
            .map_err(|_| DialError)?;

        let opt = InviteOption {
            callee: loc.aor.clone(),
            caller: self.contact.clone(),
            contact: self.contact.clone(),
            content_type: Some("application/sdp".to_string()),
            offer: Some(callee_rtp.local_sdp().into_bytes()),
            destination: loc.destination.clone(),
            ..Default::default()
        };

        let (mut session, answer_sdp) = SipSession::dial(self.dialog_layer.as_ref(), opt)
            .await
            .map_err(|_| DialError)?;

        // Wire the callee's RTP once we know where to send.
        if let Some(sdp) = answer_sdp
            && let Ok(remote) = RtpMedia::parse_remote(&sdp)
        {
            session.attach_rtp(callee_rtp, remote);
        }
        Ok(Box::new(session))
    }
}

/// Serve a direct (`Targets`) call through the new engine.
pub async fn serve(
    server: SipServerRef,
    context: CallContext,
    tx: &mut rsipstack::transaction::transaction::Transaction,
    cancel_token: tokio_util::sync::CancellationToken,
) -> Result<()> {
    // IVR/voicemail/Application graphs run on the graph engine, not the dialer.
    if let Some(def) = graph_def(&context.dialplan.flow) {
        return serve_graph(server, context, tx, cancel_token, def).await;
    }

    let session_id = context.session_id.clone();
    info!(session_id = %session_id, "Serving call via new session engine");

    let local_contact = server.default_contact_uri();
    let (state_tx, state_rx) = tokio::sync::mpsc::unbounded_channel();
    let server_dialog = server
        .dialog_layer
        .get_or_create_server_invite(tx, state_tx, None, local_contact.clone())
        .map_err(|e| anyhow!("failed to create server dialog: {e}"))?;

    // The caller's offered RTP endpoint.
    let caller_offer = String::from_utf8_lossy(server_dialog.initial_request().body()).to_string();
    let caller_remote = RtpMedia::parse_remote(&caller_offer)
        .map_err(|e| anyhow!("caller offer has no RTP endpoint: {e}"))?;

    let bind_ip = server
        .rtp_config
        .external_ip
        .clone()
        .or_else(|| server.rtp_config.bind_ip.clone())
        .unwrap_or_else(|| "127.0.0.1".to_string());

    // Bind our caller-facing RTP and prepare the answer SDP.
    let caller_rtp = RtpMedia::bind_on(&bind_ip, CodecType::PCMU)
        .await
        .map_err(|e| anyhow!("failed to bind caller RTP: {e}"))?;
    let answer_sdp = caller_rtp.local_sdp();

    let contact = local_contact
        .clone()
        .ok_or_else(|| anyhow!("no local contact for outbound INVITEs"))?;

    let mut caller = SipSession::inbound(server_dialog.clone(), state_rx);
    let ct = rsipstack::sip::Header::ContentType(rsipstack::sip::headers::ContentType::from(
        "application/sdp",
    ));
    caller.set_answer(vec![ct], answer_sdp.into_bytes());
    caller.attach_rtp(caller_rtp, caller_remote);

    // Targets for the dialer, flattened across all (queue + fallback) stages.
    let targets = flatten_targets(&context.dialplan.flow);
    let dialer = Arc::new(SipDialer {
        dialog_layer: server.dialog_layer.clone(),
        targets,
        contact,
        bind_ip,
    });

    let reducer = FlowReducer::from_flow(&context.dialplan.flow);
    let mut dial = DialCall::new(reducer, Box::new(caller), dialer);
    // Queue flows: hold music while waiting, and per-dial ring timeout.
    if let Some((pcm, rate)) = hold_audio(&context.dialplan.flow) {
        dial = dial.with_caller_hold(pcm, rate, CodecType::PCMU);
    }
    if let Some(d) = ring_timeout(&context.dialplan.flow) {
        dial = dial.with_ring_timeout(d);
    }
    if let Some((pcm, rate)) =
        failure_audio_file(&context.dialplan.flow).and_then(|f| load_wav_mono(&f))
    {
        dial = dial.with_failure_audio(pcm, rate, CodecType::PCMU);
    }
    // Live-control seam: register this call's command sink so RWI/console/AMI can
    // reach it by session id. The guard unregisters when the call ends.
    let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
    let _command_guard =
        crate::call::session::command_registry::global().register(session_id.clone(), command_tx);
    dial = dial.with_commands(command_rx);
    crate::utils::spawn(dial.run());

    // Pump the caller's INVITE transaction (sends our 200 once the dial
    // accepts) until the dialog terminates or the call is cancelled.
    let mut pump_dialog = server_dialog.clone();
    tokio::select! {
        r = pump_dialog.handle(tx) => {
            if let Err(e) = r {
                warn!(session_id = %session_id, error = %e, "server dialog handle error");
            }
        }
        _ = cancel_token.cancelled() => {
            debug!(session_id = %session_id, "session-engine call cancelled");
        }
    }

    Ok(())
}

/// Serve an IVR/voicemail/Application call by walking a designer [`GraphDef`] on
/// the graph engine (`GraphRunner` + `GraphCall`): the caller is answered up
/// front, prompts play on player taps, DTMF drives `Collect` nodes, and `Dial`
/// nodes bridge an agent — all over the unified mixer.
async fn serve_graph(
    server: SipServerRef,
    context: CallContext,
    tx: &mut rsipstack::transaction::transaction::Transaction,
    cancel_token: tokio_util::sync::CancellationToken,
    def: GraphDef,
) -> Result<()> {
    let session_id = context.session_id.clone();
    info!(session_id = %session_id, "Serving IVR/graph call via session engine");

    let local_contact = server.default_contact_uri();
    let (state_tx, state_rx) = tokio::sync::mpsc::unbounded_channel();
    let server_dialog = server
        .dialog_layer
        .get_or_create_server_invite(tx, state_tx, None, local_contact.clone())
        .map_err(|e| anyhow!("failed to create server dialog: {e}"))?;

    let caller_offer = String::from_utf8_lossy(server_dialog.initial_request().body()).to_string();
    let caller_remote = RtpMedia::parse_remote(&caller_offer)
        .map_err(|e| anyhow!("caller offer has no RTP endpoint: {e}"))?;

    let bind_ip = server
        .rtp_config
        .external_ip
        .clone()
        .or_else(|| server.rtp_config.bind_ip.clone())
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let caller_rtp = RtpMedia::bind_on(&bind_ip, CodecType::PCMU)
        .await
        .map_err(|e| anyhow!("failed to bind caller RTP: {e}"))?;
    let answer_sdp = caller_rtp.local_sdp();
    let contact = local_contact
        .clone()
        .ok_or_else(|| anyhow!("no local contact for outbound INVITEs"))?;

    let mut caller = SipSession::inbound(server_dialog.clone(), state_rx);
    let ct = rsipstack::sip::Header::ContentType(rsipstack::sip::headers::ContentType::from(
        "application/sdp",
    ));
    caller.set_answer(vec![ct], answer_sdp.into_bytes());
    caller.attach_rtp(caller_rtp, caller_remote);

    let dialer = Arc::new(SipDialer {
        dialog_layer: server.dialog_layer.clone(),
        targets: graph_targets_to_locations(&def.targets),
        contact,
        bind_ip,
    });
    let prompts = prompts_from_def(&def);
    let graph = CallGraph::from_def(&def);
    let (events_tx, events_rx) = tokio::sync::mpsc::unbounded_channel();
    // The live-control seam: external CallCommands (RWI/console/AMI) are looked
    // up by session id in the global registry and serviced by the running call.
    // The guard unregisters when this call ends.
    let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
    let _command_guard =
        crate::call::session::command_registry::global().register(session_id.clone(), command_tx);

    // Run the graph; once it bridges, hold the connected call until teardown.
    let call_done = cancel_token.child_token();
    let runner_done = call_done.clone();
    crate::utils::spawn(async move {
        let port = GraphCall::new(Box::new(caller), dialer, prompts, events_tx).await;
        // Traversal: walk the graph (servicing commands), then hold the connected
        // call alive and keep servicing external commands until teardown.
        let (mut port, mut command_rx) =
            graph_runner::run(graph, port, events_rx, command_rx).await;
        loop {
            tokio::select! {
                _ = runner_done.cancelled() => break,
                cmd = command_rx.recv() => match cmd {
                    Some((command, reply)) => {
                        let _ = reply.send(port.dispatch(command).await);
                    }
                    None => {
                        runner_done.cancelled().await;
                        break;
                    }
                },
            }
        }
    });

    let mut pump_dialog = server_dialog.clone();
    tokio::select! {
        r = pump_dialog.handle(tx) => {
            if let Err(e) = r {
                warn!(session_id = %session_id, error = %e, "server dialog handle error");
            }
        }
        _ = cancel_token.cancelled() => {
            debug!(session_id = %session_id, "session-engine graph call cancelled");
        }
    }
    call_done.cancel();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::call::{QueueFallbackAction, QueuePlan};

    fn loc(aor: &str) -> Location {
        Location {
            aor: format!("sip:{aor}@example.com").try_into().unwrap(),
            destination: None,
            ..Default::default()
        }
    }

    fn targets(aors: &[&str]) -> DialplanFlow {
        DialplanFlow::Targets(DialStrategy::Sequential(aors.iter().map(|a| loc(a)).collect()))
    }

    fn simple_queue(agents: &[&str], next: DialplanFlow) -> DialplanFlow {
        DialplanFlow::Queue {
            plan: QueuePlan {
                hold: None,
                fallback: None,
                dial_strategy: Some(DialStrategy::Sequential(
                    agents.iter().map(|a| loc(a)).collect(),
                )),
                voice_prompts: None,
                failure_audio: None,
                acd_policy: None,
                retry_codes: None,
                no_trying_timeout: None,
                ..Default::default()
            },
            next: Box::new(next),
        }
    }

    #[test]
    fn supports_targets_and_simple_queue() {
        assert!(supports_flow(&targets(&["a"])));
        assert!(supports_flow(&simple_queue(&["agent1"], targets(&["fallback"]))));
    }

    #[test]
    fn application_graph_is_supported_and_extracted() {
        use crate::call::session::graph::{GraphDef, Node, NodeId, PromptId};
        let def = GraphDef {
            entry: NodeId(1),
            nodes: vec![(NodeId(1), Node::Hangup)],
            prompts: vec![(PromptId(1), "/s/bye.wav".to_string())],
            targets: vec![],
        };
        let flow = DialplanFlow::Application {
            app_name: "graph".to_string(),
            app_params: Some(serde_json::to_value(&def).unwrap()),
            auto_answer: true,
        };
        assert!(supports_flow(&flow), "a graph application is covered");
        assert_eq!(graph_def(&flow), Some(def));

        // A non-graph application is not.
        let vm = DialplanFlow::Application {
            app_name: "voicemail".to_string(),
            app_params: None,
            auto_answer: true,
        };
        assert!(!supports_flow(&vm));
        assert!(graph_def(&vm).is_none());
    }

    #[test]
    fn graph_targets_parse_to_dial_locations() {
        let locs = graph_targets_to_locations(&[
            "sip:agent1@pbx.example".to_string(),
            "sip:agent2@pbx.example".to_string(),
        ]);
        assert_eq!(locs.len(), 2);
        assert!(locs[0].aor.to_string().contains("agent1"));
        assert!(locs[1].aor.to_string().contains("agent2"));
    }

    #[test]
    fn supports_queue_with_play_then_hangup_fallback() {
        use crate::call::FailureAction;
        use rsipstack::sip::StatusCode;
        let q = DialplanFlow::Queue {
            plan: QueuePlan {
                fallback: Some(QueueFallbackAction::Failure(FailureAction::PlayThenHangup {
                    audio_file: "/sounds/sorry.wav".to_string(),
                    use_early_media: false,
                    status_code: StatusCode::TemporarilyUnavailable,
                    reason: None,
                })),
                dial_strategy: Some(DialStrategy::Sequential(vec![loc("agent1")])),
                ..Default::default()
            },
            next: Box::new(targets(&["x"])),
        };
        assert!(supports_flow(&q), "play-then-hangup fallback is covered");
        assert_eq!(
            failure_audio_file(&q).as_deref(),
            Some("/sounds/sorry.wav"),
            "failure prompt is extracted from the fallback"
        );
    }

    #[test]
    fn rejects_queue_with_unsupported_features() {
        // A fallback action isn't covered yet → stays on the god object.
        let q = DialplanFlow::Queue {
            plan: QueuePlan {
                fallback: Some(QueueFallbackAction::Queue {
                    name: "overflow".to_string(),
                }),
                dial_strategy: Some(DialStrategy::Sequential(vec![loc("agent1")])),
                ..Default::default()
            },
            next: Box::new(targets(&["x"])),
        };
        assert!(!supports_flow(&q));
    }

    #[test]
    fn rejects_application_tail() {
        let app = || DialplanFlow::Application {
            app_name: "voicemail".to_string(),
            app_params: None,
            auto_answer: true,
        };
        assert!(!supports_flow(&simple_queue(&["agent1"], app())));
        assert!(!supports_flow(&app()));
    }

    #[test]
    fn flatten_orders_queue_then_fallback_targets() {
        let flow = simple_queue(&["agent1", "agent2"], targets(&["fallback"]));
        let flat = flatten_targets(&flow);
        let aors: Vec<String> = flat.iter().map(|l| l.aor.to_string()).collect();
        assert_eq!(aors.len(), 3);
        assert!(aors[0].contains("agent1"));
        assert!(aors[1].contains("agent2"));
        assert!(aors[2].contains("fallback"));
    }

    #[test]
    fn no_hold_or_ring_for_plain_targets() {
        let flow = targets(&["a"]);
        assert!(hold_audio(&flow).is_none());
        assert!(ring_timeout(&flow).is_none());
    }
}
