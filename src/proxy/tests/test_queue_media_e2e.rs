//! Queue Media E2E Test
//!
//! Verifies that a call routed into a **queue** and answered by an agent has
//! working **bidirectional** RTP through the proxy in anchored
//! (`media_proxy = all`) mode.
//!
//! This exercises the app-runtime queue path (inbound route → `queue` action →
//! `app_runtime.start_app("queue")`), which dials agents as *dynamically-added
//! legs* (`LegAdd` / `initiate_sip_leg`) rather than the fixed callee leg. That
//! path is where a whole class of media bugs lived undetected, because the
//! existing queue tests only asserted **signaling** (an agent received the
//! INVITE) and never that **audio actually flows both ways** once the agent
//! answers. Concretely, this test guards against:
//!
//! - early-media (183) consuming the leg's `HaveLocalOffer` so the final 200 OK
//!   answer fails to apply, leaving the agent leg on the wrong codec/endpoint
//!   and producing **no agent→caller audio**;
//! - dynamic legs never being wired into media forwarding at all.
//!
//! The single assertion that matters — and that no prior queue test made — is
//! that **both** the caller and the agent receive RTP packets.

use super::e2e_test_server::E2eTestServer;
use super::rtp_utils::{RtpReceiver, RtpSender, RtpStats, extract_media_endpoint};
use super::test_helpers::{build_sdp, pcma_sdp, pcmu_sdp};
use super::test_ua::{TestUa, TestUaEvent};
use crate::config::{MediaProxyMode, ProxyConfig};
use crate::proxy::routing::{
    DestConfig, MatchConditions, RouteAction, RouteQueueConfig, RouteQueueFallbackConfig,
    RouteQueueStrategyConfig, RouteQueueTargetConfig, RouteRule, TrunkConfig, TrunkDirection,
};
use super::test_ua::TestUaConfig;
use anyhow::{Result, anyhow};
use rsipstack::dialog::DialogId;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

/// Build a ProxyConfig with a single queue "support" whose only agent is the
/// registered user `bob`, plus a route that sends calls to `support` into it.
fn queue_proxy_config() -> ProxyConfig {
    queue_proxy_config_with(Vec::new(), false)
}

fn queue_proxy_config_with(audio_codecs: Vec<String>, accept_immediately: bool) -> ProxyConfig {
    let mut config = ProxyConfig {
        media_proxy: MediaProxyMode::All,
        // Match the E2E media test harness: disable RTP latching so the proxy
        // sends to the SDP-advertised receiver ports (the test's RtpReceivers)
        // rather than latching onto the RtpSenders' source ports.
        enable_latching: false,
        audio_codecs: if audio_codecs.is_empty() {
            None
        } else {
            Some(audio_codecs)
        },
        ..Default::default()
    };

    // Queue "support" → dial agent bob (resolved via the locator at call time).
    let queue_config = RouteQueueConfig {
        name: Some("support".to_string()),
        strategy: RouteQueueStrategyConfig {
            targets: vec![RouteQueueTargetConfig {
                uri: "sip:bob@127.0.0.1".to_string(),
                label: Some("Support Agent".to_string()),
            }],
            wait_timeout_secs: Some(10),
            ..Default::default()
        },
        accept_immediately,
        ..Default::default()
    };
    config.queues.insert("support".to_string(), queue_config);

    // Route: dial "support" → queue "support".
    let queue_route = RouteRule {
        name: "route_to_support".to_string(),
        priority: 10,
        match_conditions: MatchConditions {
            to_user: Some("support".to_string()),
            ..Default::default()
        },
        action: RouteAction {
            queue: Some("support".to_string()),
            auto_answer: true,
            ..Default::default()
        },
        ..Default::default()
    };
    config.routes = Some(vec![queue_route]);
    config
}

/// Same as [`queue_proxy_config_with`], but routes the queue through the
/// event-driven call-graph controller (`queue_graph_engine = true`). Used by the
/// `test_queue_graph_*` tests to A/B the parallel engine against the imperative
/// `execute_queue` path the other tests exercise.
fn queue_graph_config_with(audio_codecs: Vec<String>, accept_immediately: bool) -> ProxyConfig {
    let mut config = queue_proxy_config_with(audio_codecs, accept_immediately);
    config.queue_graph_engine = true;
    config
}

struct QueueMediaTestCtx {
    server: Arc<E2eTestServer>,
    caller_ua: TestUa,
    agent_ua: TestUa,
    caller_sender: RtpSender,
    caller_receiver: RtpReceiver,
    agent_sender: RtpSender,
    agent_receiver: RtpReceiver,
}

impl QueueMediaTestCtx {
    async fn setup() -> Result<Self> {
        Self::setup_with_config(queue_proxy_config()).await
    }

    async fn setup_with_config(config: ProxyConfig) -> Result<Self> {
        let server = Arc::new(E2eTestServer::start_with_config(config).await?);

        // alice = caller, bob = the queue's agent. Both register so the queue
        // can resolve bob's contact via the locator.
        let caller_ua = server.create_ua("alice").await?;
        let agent_ua = server.create_ua("bob").await?;

        sleep(Duration::from_millis(100)).await;

        let caller_sender = RtpSender::bind().await?;
        let caller_receiver = RtpReceiver::bind(0).await?;
        let agent_sender = RtpSender::bind().await?;
        let agent_receiver = RtpReceiver::bind(0).await?;

        Ok(Self {
            server,
            caller_ua,
            agent_ua,
            caller_sender,
            caller_receiver,
            agent_sender,
            agent_receiver,
        })
    }

    /// Caller dials the queue ("support"); the queue hunts the agent (bob), who
    /// answers. Returns (caller_dialog_id, agent_dialog_id, agent_offer_sdp).
    async fn establish_queue_call(
        &self,
        caller_sdp: String,
        agent_sdp: String,
    ) -> Result<(DialogId, DialogId, String)> {
        let caller = Arc::new(self.caller_ua.clone());
        let caller_handle =
            crate::utils::spawn(async move { caller.make_call("support", Some(caller_sdp)).await });

        // The agent leg is dialed by the queue; wait for bob's INVITE and answer.
        let mut agent: Option<(DialogId, String)> = None;
        for _ in 0..100 {
            let events = self.agent_ua.process_dialog_events().await?;
            for event in events {
                if let TestUaEvent::IncomingCall(id, offer) = event {
                    self.agent_ua.answer_call(&id, Some(agent_sdp.clone())).await?;
                    info!("Agent answered queue call");
                    agent = Some((id, offer.unwrap_or_default()));
                    break;
                }
            }
            if agent.is_some() {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        let (agent_id, agent_offer) =
            agent.ok_or_else(|| anyhow!("Agent never received INVITE from queue"))?;

        let caller_id = tokio::time::timeout(Duration::from_secs(8), caller_handle)
            .await
            .map_err(|_| anyhow!("Caller timed out waiting for queue connect"))?
            .map_err(|e| anyhow!("Caller task join error: {}", e))?
            .map_err(|e| anyhow!("Queue call failed: {}", e))?;

        Ok((caller_id, agent_id, agent_offer))
    }

    /// Poll the caller's dialog events until its call is terminated, or fail.
    async fn wait_for_caller_terminated(&self, caller_id: &DialogId, secs: u64) -> Result<()> {
        for _ in 0..(secs * 10) {
            let events = self.caller_ua.process_dialog_events().await?;
            for event in events {
                if let TestUaEvent::CallTerminated(id) = event {
                    if &id == caller_id {
                        return Ok(());
                    }
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        Err(anyhow!(
            "Caller call did not terminate after agent hung up (hangup cascade broken)"
        ))
    }

    async fn exchange_rtp(
        &self,
        caller_target: SocketAddr,
        agent_target: SocketAddr,
        payload_type: u8,
        duration_ms: u64,
    ) -> Result<(RtpStats, RtpStats)> {
        use super::rtp_utils::RtpPacket;

        let packet_interval_ms: u64 = 20;
        let packet_count = (duration_ms / packet_interval_ms) as usize;

        let caller_packets = RtpPacket::create_sequence(
            packet_count, 1000, 50000, 0xA1A1A1A1, payload_type, 160, 160,
        );
        let agent_packets = RtpPacket::create_sequence(
            packet_count, 2000, 60000, 0xB2B2B2B2, payload_type, 160, 160,
        );

        self.caller_receiver.start_receiving();
        self.agent_receiver.start_receiving();

        self.caller_sender
            .start_sending(agent_target, caller_packets, packet_interval_ms);
        self.agent_sender
            .start_sending(caller_target, agent_packets, packet_interval_ms);

        sleep(Duration::from_millis(duration_ms + 500)).await;

        self.caller_sender.stop();
        self.agent_sender.stop();
        sleep(Duration::from_millis(200)).await;

        Ok((
            self.caller_receiver.get_stats().await,
            self.agent_receiver.get_stats().await,
        ))
    }
}

/// A call routed through a queue and answered by an agent must have working
/// bidirectional RTP through the proxy in anchored mode.
#[tokio::test]
async fn test_queue_call_bidirectional_rtp() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = QueueMediaTestCtx::setup().await?;

    let caller_port = ctx.caller_receiver.port().unwrap();
    let agent_port = ctx.agent_receiver.port().unwrap();

    let caller_sdp = pcmu_sdp("127.0.0.1", caller_port);
    let agent_sdp = pcmu_sdp("127.0.0.1", agent_port);

    // 1. Caller dials the queue; agent answers.
    let (caller_id, _agent_id, agent_offer) =
        ctx.establish_queue_call(caller_sdp, agent_sdp).await?;
    info!(%caller_id, "Queue call connected to agent");

    // 2. Resolve the proxy-anchored RTP endpoints for both legs.
    let caller_answer_sdp = ctx
        .caller_ua
        .get_negotiated_answer_sdp(&caller_id)
        .await
        .ok_or_else(|| anyhow!("No answer SDP on caller side"))?;

    let agent_target = extract_media_endpoint(&agent_offer)
        .ok_or_else(|| anyhow!("Failed to parse agent-side proxy media endpoint"))?;
    let caller_target = extract_media_endpoint(&caller_answer_sdp)
        .ok_or_else(|| anyhow!("Failed to parse caller-side proxy media endpoint"))?;

    info!(%caller_target, %agent_target, "Anchored RTP endpoints resolved");

    // 3. Exchange RTP for ~2s (PCMU).
    let (caller_stats, agent_stats) = ctx
        .exchange_rtp(caller_target, agent_target, 0, 2000)
        .await?;

    info!(
        caller_received = caller_stats.packets_received,
        agent_received = agent_stats.packets_received,
        "Queue RTP exchange complete"
    );

    // 4. The assertions that matter: audio flows BOTH ways.
    assert!(
        agent_stats.packets_received > 0,
        "Agent should receive RTP from caller through the queue bridge (got 0)"
    );
    assert!(
        caller_stats.packets_received > 0,
        "Caller should receive RTP from the agent through the queue bridge (got 0) \
         — this is the regression that produced one-way (caller-only) audio"
    );

    assert!(
        agent_stats.payload_types.contains(&0),
        "Agent should receive PCMU (PT 0), got {:?}",
        agent_stats.payload_types
    );
    assert!(
        caller_stats.payload_types.contains(&0),
        "Caller should receive PCMU (PT 0) from agent, got {:?}",
        caller_stats.payload_types
    );

    ctx.caller_ua.hangup(&caller_id).await?;
    ctx.server.stop();
    Ok(())
}

/// Symmetric to the agent-hangup case: when the CALLER hangs up, the agent's
/// call must be torn down too. (NOTE: this exercises the UDP path; the
/// TLS-specific variant — where a TLS callee advertises a Contact without
/// `transport=TLS` and the cascade BYE wrongly goes out over UDP — cannot be
/// reproduced here because TestUa only does UDP signaling. That fix needs SIP/TLS
/// test infra; this guards the cascade direction itself.)
#[tokio::test]
async fn test_queue_caller_hangup_ends_agent_call() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = QueueMediaTestCtx::setup().await?;
    let caller_sdp = pcmu_sdp("127.0.0.1", ctx.caller_receiver.port().unwrap());
    let agent_sdp = pcmu_sdp("127.0.0.1", ctx.agent_receiver.port().unwrap());

    let (caller_id, agent_id, _agent_offer) =
        ctx.establish_queue_call(caller_sdp, agent_sdp).await?;
    info!(%caller_id, %agent_id, "Queue call connected; caller will hang up");

    ctx.caller_ua.hangup(&caller_id).await?;

    for _ in 0..50 {
        let events = ctx.agent_ua.process_dialog_events().await?;
        if events
            .iter()
            .any(|e| matches!(e, TestUaEvent::CallTerminated(id) if id == &agent_id))
        {
            info!("Agent call terminated after caller hangup (cascade OK)");
            ctx.server.stop();
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err(anyhow!(
        "Agent call did not terminate after caller hung up (caller->agent hangup cascade broken)"
    ))
}

/// When the caller offers multiple codecs but the agent negotiates a single one
/// (PCMA), the proxy's answer to the caller MUST end up consistent with that
/// codec. If it leaves PCMU in the caller answer, the caller may send PCMU which
/// is then relayed to a PCMA-only agent without transcoding — producing garbled
/// / choppy audio (observed in production with a Twilio caller offering PCMU/PCMA
/// and a PCMA-only Yealink agent).
///
/// KNOWN-FAILING / documented repro: when the caller is answered EARLY (hold
/// music, or accept_immediately) using the full allowed codec set, the answer is
/// never narrowed/renegotiated to the codec the agent later picks. The proper
/// fix is to either narrow the early caller answer to a single codec or
/// renegotiate (re-INVITE) the caller once the agent's codec is known.
/// Operational workaround in the meantime: pin `audio_codecs` to a single codec.
/// Remove `#[ignore]` when the proxy narrows/renegotiates correctly.
#[ignore = "documents the early-answer codec-mismatch bug; fix = narrow/renegotiate caller codec"]
#[tokio::test]
async fn test_queue_caller_answer_narrows_to_agent_codec() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    // Reproduce the production config: multiple allowed codecs. The bug is that
    // the caller answer is built from the allowed set instead of being narrowed
    // to the codec the agent actually negotiated.
    // accept_immediately=true answers the caller early (like hold music does in
    // production) using the full allowed codec set, before the agent picks a
    // single codec — the condition under which the caller answer fails to narrow.
    let ctx = QueueMediaTestCtx::setup_with_config(queue_proxy_config_with(
        vec!["pcma".to_string(), "pcmu".to_string(), "g722".to_string()],
        true,
    ))
    .await?;

    // Caller offers PCMU + PCMA; agent answers PCMA only.
    let caller_sdp = build_sdp(
        "127.0.0.1",
        ctx.caller_receiver.port().unwrap(),
        &[(0, "PCMU/8000"), (8, "PCMA/8000"), (101, "telephone-event/8000")],
    );
    let agent_sdp = pcma_sdp("127.0.0.1", ctx.agent_receiver.port().unwrap());

    let (caller_id, _agent_id, _agent_offer) =
        ctx.establish_queue_call(caller_sdp, agent_sdp).await?;

    let caller_answer = ctx
        .caller_ua
        .get_negotiated_answer_sdp(&caller_id)
        .await
        .ok_or_else(|| anyhow!("No answer SDP on caller side"))?;
    info!(%caller_answer, "Caller answer SDP");

    assert!(
        caller_answer.to_uppercase().contains("PCMA"),
        "caller answer should offer PCMA (the agent's codec): {caller_answer}"
    );
    assert!(
        !caller_answer.to_uppercase().contains("PCMU"),
        "caller answer must NOT include PCMU when the agent negotiated PCMA-only — \
         the caller could send PCMU and be relayed unconverted to a PCMA-only agent \
         (garbled audio): {caller_answer}"
    );

    ctx.caller_ua.hangup(&caller_id).await?;
    ctx.server.stop();
    Ok(())
}

/// Bidirectional audio must also work when the caller is answered EARLY (the
/// `accept_immediately` path, which routes the caller through the app media
/// bridge). This guards the early-answer/app-bridge path itself.
///
/// NOTE: production showed ~87% caller->agent loss specifically with HOLD MUSIC
/// (file playback through the bridge, then the stop/transition when the agent
/// connects). This test does NOT reproduce that — the early-answer app bridge
/// alone is clean here — so the hold-music transition is the suspect. A faithful
/// repro needs hold-music file playback in the harness (test-infra gap).
#[tokio::test]
async fn test_queue_early_answer_bidirectional_rtp() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = QueueMediaTestCtx::setup_with_config(queue_proxy_config_with(Vec::new(), true)).await?;

    let caller_sdp = pcmu_sdp("127.0.0.1", ctx.caller_receiver.port().unwrap());
    let agent_sdp = pcmu_sdp("127.0.0.1", ctx.agent_receiver.port().unwrap());

    let (caller_id, _agent_id, agent_offer) =
        ctx.establish_queue_call(caller_sdp, agent_sdp).await?;

    let caller_answer_sdp = ctx
        .caller_ua
        .get_negotiated_answer_sdp(&caller_id)
        .await
        .ok_or_else(|| anyhow!("No answer SDP on caller side"))?;
    let agent_target = extract_media_endpoint(&agent_offer)
        .ok_or_else(|| anyhow!("Failed to parse agent-side endpoint"))?;
    let caller_target = extract_media_endpoint(&caller_answer_sdp)
        .ok_or_else(|| anyhow!("Failed to parse caller-side endpoint"))?;

    let (caller_stats, agent_stats) = ctx
        .exchange_rtp(caller_target, agent_target, 0, 2000)
        .await?;
    info!(
        caller_received = caller_stats.packets_received,
        agent_received = agent_stats.packets_received,
        "Early-answer queue RTP exchange complete"
    );

    assert!(
        agent_stats.packets_received > 0,
        "Agent should receive RTP from caller even with early-answer/app-bridge (got 0) \
         — this is the ~87%-loss garbled-agent-audio regression"
    );
    assert!(
        caller_stats.packets_received > 0,
        "Caller should receive RTP from agent with early-answer/app-bridge (got 0)"
    );

    ctx.caller_ua.hangup(&caller_id).await?;
    ctx.server.stop();
    Ok(())
}

/// When no agent answers within the queue's ring timeout, the queue MUST give
/// up and run its fallback — it must not ring the agent forever. `execute_queue`
/// originally ignored the ring timeout (`dial_queue_sequential` took an
/// `_ring_timeout` it never used), so a no-answer call dialed indefinitely.
#[tokio::test]
async fn test_queue_no_answer_times_out_to_fallback() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    // Short 2s ring timeout + a hangup (486) fallback.
    let mut config = queue_proxy_config_with(Vec::new(), false);
    if let Some(q) = config.queues.get_mut("support") {
        q.strategy.wait_timeout_secs = Some(2);
        q.fallback = Some(RouteQueueFallbackConfig {
            redirect: None,
            failure_code: Some(486),
            failure_reason: None,
            failure_prompt: None,
            queue_ref: None,
            skill_group_ref: None,
        });
    }

    let ctx = QueueMediaTestCtx::setup_with_config(config).await?;
    let caller_sdp = pcmu_sdp("127.0.0.1", ctx.caller_receiver.port().unwrap());

    // Caller dials the queue; the agent receives the INVITE but never answers.
    let caller = Arc::new(ctx.caller_ua.clone());
    let handle =
        crate::utils::spawn(async move { caller.make_call("support", Some(caller_sdp)).await });

    let mut agent_rang = false;
    for _ in 0..50 {
        let events = ctx.agent_ua.process_dialog_events().await?;
        if events
            .iter()
            .any(|e| matches!(e, TestUaEvent::IncomingCall(..)))
        {
            agent_rang = true;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(agent_rang, "agent never received the INVITE from the queue");

    // With a 2s ring timeout the queue must give up; the caller call must
    // resolve (fail with the fallback code) — NOT ring forever.
    let res = tokio::time::timeout(Duration::from_secs(8), handle).await;
    assert!(
        res.is_ok(),
        "caller call did not complete within 8s after a 2s ring timeout — \
         the queue is dialing forever (ring_timeout not enforced)"
    );

    ctx.server.stop();
    Ok(())
}

// =============================================================================
// Parallel engine (`queue_graph_engine = true`) — the call-graph controller.
// These mirror the key imperative-path tests above to prove parity.
// =============================================================================

/// Call-graph engine: a queue call answered by an agent must have working
/// bidirectional RTP through the anchored proxy bridge.
#[tokio::test]
async fn test_queue_graph_bidirectional_rtp() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx =
        QueueMediaTestCtx::setup_with_config(queue_graph_config_with(Vec::new(), false)).await?;

    let caller_sdp = pcmu_sdp("127.0.0.1", ctx.caller_receiver.port().unwrap());
    let agent_sdp = pcmu_sdp("127.0.0.1", ctx.agent_receiver.port().unwrap());

    let (caller_id, _agent_id, agent_offer) =
        ctx.establish_queue_call(caller_sdp, agent_sdp).await?;
    info!(%caller_id, "call-graph queue connected to agent");

    let caller_answer_sdp = ctx
        .caller_ua
        .get_negotiated_answer_sdp(&caller_id)
        .await
        .ok_or_else(|| anyhow!("No answer SDP on caller side"))?;
    let agent_target = extract_media_endpoint(&agent_offer)
        .ok_or_else(|| anyhow!("Failed to parse agent-side endpoint"))?;
    let caller_target = extract_media_endpoint(&caller_answer_sdp)
        .ok_or_else(|| anyhow!("Failed to parse caller-side endpoint"))?;

    let (caller_stats, agent_stats) =
        ctx.exchange_rtp(caller_target, agent_target, 0, 2000).await?;
    info!(
        caller_received = caller_stats.packets_received,
        agent_received = agent_stats.packets_received,
        "call-graph queue RTP exchange complete"
    );

    assert!(
        agent_stats.packets_received > 0,
        "agent should receive RTP from caller through the call-graph bridge (got 0)"
    );
    assert!(
        caller_stats.packets_received > 0,
        "caller should receive RTP from agent through the call-graph bridge (got 0)"
    );

    ctx.caller_ua.hangup(&caller_id).await?;
    ctx.server.stop();
    Ok(())
}

/// Call-graph engine: PSTN-style fallback via an OUTBOUND TRUNK. The primary
/// target never answers; the fallback is a number that the locator can't
/// resolve, so it is routed through a configured outbound trunk (here a gateway
/// TestUa standing in for the PSTN carrier) and bridged. Proves the
/// dial+bridge fallback reuses the proxy's outbound routing (match_invite).
#[tokio::test]
async fn test_queue_graph_fallback_via_trunk() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    // The gateway listens on a fixed port so the trunk `dest` can point at it.
    let gw_port = portpicker::pick_unused_port().expect("a free port for the gateway");

    let mut config = queue_graph_config_with(Vec::new(), false);
    if let Some(q) = config.queues.get_mut("support") {
        q.strategy.wait_timeout_secs = Some(2);
        q.fallback = Some(RouteQueueFallbackConfig {
            redirect: Some("sip:5559999@127.0.0.1".to_string()),
            failure_code: None,
            failure_reason: None,
            failure_prompt: None,
            queue_ref: None,
            skill_group_ref: None,
        });
    }
    // Outbound trunk pointing at the gateway UA + a route that sends the
    // fallback number to it.
    config.trunks.insert(
        "testgw".to_string(),
        TrunkConfig {
            dest: format!("sip:127.0.0.1:{gw_port}"),
            direction: Some(TrunkDirection::Outbound),
            ..Default::default()
        },
    );
    if let Some(routes) = config.routes.as_mut() {
        routes.push(RouteRule {
            name: "out_to_gw".to_string(),
            priority: 20,
            match_conditions: MatchConditions {
                to_user: Some("5559999".to_string()),
                ..Default::default()
            },
            action: RouteAction {
                dest: Some(DestConfig::Single("testgw".to_string())),
                ..Default::default()
            },
            ..Default::default()
        });
    }

    let ctx = QueueMediaTestCtx::setup_with_config(config).await?;

    // Bring up the gateway UA on the fixed port (no registration needed — the
    // trunk dials it by address).
    let mut gw_ua = TestUa::new(TestUaConfig {
        username: "gw".to_string(),
        password: "x".to_string(),
        realm: "127.0.0.1".to_string(),
        local_port: gw_port,
        proxy_addr: ctx.server.proxy_addr,
    });
    gw_ua.start().await?;
    let gw_sender = RtpSender::bind().await?;
    let gw_receiver = RtpReceiver::bind(0).await?;
    sleep(Duration::from_millis(100)).await;

    let caller_sdp = pcmu_sdp("127.0.0.1", ctx.caller_receiver.port().unwrap());
    let gw_sdp = pcmu_sdp("127.0.0.1", gw_receiver.port().unwrap());

    let caller = Arc::new(ctx.caller_ua.clone());
    let caller_sdp_cl = caller_sdp.clone();
    let caller_handle = crate::utils::spawn(async move {
        caller.make_call("support", Some(caller_sdp_cl)).await
    });

    // bob rings but never answers; the gateway (via trunk) answers the fallback.
    let mut gw_call: Option<(DialogId, String)> = None;
    for _ in 0..120 {
        let _ = ctx.agent_ua.process_dialog_events().await?;
        for event in gw_ua.process_dialog_events().await? {
            if let TestUaEvent::IncomingCall(id, offer) = event {
                gw_ua.answer_call(&id, Some(gw_sdp.clone())).await?;
                info!("trunk gateway answered fallback");
                gw_call = Some((id, offer.unwrap_or_default()));
                break;
            }
        }
        if gw_call.is_some() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    let (_gw_id, gw_offer) =
        gw_call.ok_or_else(|| anyhow!("trunk gateway never received the fallback INVITE"))?;

    let caller_id = tokio::time::timeout(Duration::from_secs(8), caller_handle)
        .await
        .map_err(|_| anyhow!("caller timed out waiting for trunk fallback connect"))?
        .map_err(|e| anyhow!("caller task join error: {e}"))?
        .map_err(|e| anyhow!("trunk fallback call failed: {e}"))?;
    info!(%caller_id, "caller connected to trunk gateway via fallback");

    let caller_answer_sdp = ctx
        .caller_ua
        .get_negotiated_answer_sdp(&caller_id)
        .await
        .ok_or_else(|| anyhow!("No answer SDP on caller side"))?;
    let gw_target = extract_media_endpoint(&gw_offer)
        .ok_or_else(|| anyhow!("Failed to parse gateway endpoint"))?;
    let caller_target = extract_media_endpoint(&caller_answer_sdp)
        .ok_or_else(|| anyhow!("Failed to parse caller endpoint"))?;

    use super::rtp_utils::RtpPacket;
    ctx.caller_receiver.start_receiving();
    gw_receiver.start_receiving();
    let caller_pkts = RtpPacket::create_sequence(100, 1000, 50000, 0xA1A1A1A1, 0, 160, 160);
    let gw_pkts = RtpPacket::create_sequence(100, 2000, 60000, 0xD4D4D4D4, 0, 160, 160);
    ctx.caller_sender.start_sending(gw_target, caller_pkts, 20);
    gw_sender.start_sending(caller_target, gw_pkts, 20);
    sleep(Duration::from_millis(2500)).await;
    ctx.caller_sender.stop();
    gw_sender.stop();
    sleep(Duration::from_millis(200)).await;

    let caller_stats = ctx.caller_receiver.get_stats().await;
    let gw_stats = gw_receiver.get_stats().await;
    info!(
        caller_received = caller_stats.packets_received,
        gw_received = gw_stats.packets_received,
        "trunk fallback RTP exchange complete"
    );

    assert!(
        gw_stats.packets_received > 0,
        "trunk gateway should receive RTP from caller (got 0)"
    );
    assert!(
        caller_stats.packets_received > 0,
        "caller should receive RTP from the trunk gateway (got 0)"
    );

    ctx.caller_ua.hangup(&caller_id).await?;
    ctx.server.stop();
    Ok(())
}

/// Minimal AgentRegistry that resolves any `skill-group:*` target to one fixed
/// agent URI — enough to exercise skill-group fallback resolution.
struct SkillRegistry {
    agent_uri: String,
}

#[async_trait::async_trait]
impl crate::call::app::agent_registry::AgentRegistry for SkillRegistry {
    async fn register(
        &self,
        _: String,
        _: String,
        _: String,
        _: Vec<String>,
        _: u32,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    async fn unregister(&self, _: &str) -> anyhow::Result<()> {
        Ok(())
    }
    async fn get_agent(
        &self,
        _: &str,
    ) -> Option<crate::call::app::agent_registry::AgentRecord> {
        None
    }
    async fn list_agents(&self) -> Vec<crate::call::app::agent_registry::AgentRecord> {
        vec![]
    }
    async fn update_presence(
        &self,
        _: &str,
        _: crate::call::app::agent_registry::PresenceState,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    async fn start_call(&self, _: &str) -> anyhow::Result<()> {
        Ok(())
    }
    async fn end_call(&self, _: &str, _: u64) -> anyhow::Result<()> {
        Ok(())
    }
    async fn find_available_agents(
        &self,
        _: &[String],
    ) -> Vec<crate::call::app::agent_registry::AgentRecord> {
        vec![]
    }
    async fn select_agent(
        &self,
        _: &[String],
        _: crate::call::app::agent_registry::RoutingStrategy,
    ) -> Option<crate::call::app::agent_registry::AgentRecord> {
        None
    }
    async fn resolve_target(&self, target_uri: &str) -> Vec<String> {
        if target_uri.starts_with("skill-group:") {
            vec![self.agent_uri.clone()]
        } else {
            vec![]
        }
    }
}

/// Call-graph engine: **skill-group fallback via dial+bridge**. The primary
/// target never answers; the fallback is a skill group that the AgentRegistry
/// resolves to a registered agent, which is then dialed+bridged (not REFER'd).
/// This is the robust contact-center fallback path.
#[tokio::test]
async fn test_queue_graph_skillgroup_fallback_dial_bridge() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let mut config = queue_graph_config_with(Vec::new(), false);
    if let Some(q) = config.queues.get_mut("support") {
        q.strategy.wait_timeout_secs = Some(2);
        q.fallback = Some(RouteQueueFallbackConfig {
            redirect: None,
            failure_code: None,
            failure_reason: None,
            failure_prompt: None,
            queue_ref: None,
            skill_group_ref: Some("agents".to_string()),
        });
    }

    // The skill group resolves to charlie (a registered standard test user).
    let registry = Arc::new(SkillRegistry {
        agent_uri: "sip:charlie@127.0.0.1".to_string(),
    });
    let server = Arc::new(
        E2eTestServer::start_with_config_and_registry(config, Some(registry)).await?,
    );
    let caller_ua = server.create_ua("alice").await?;
    let _bob = server.create_ua("bob").await?; // primary target, never answers
    let charlie_ua = server.create_ua("charlie").await?; // skill-group agent
    sleep(Duration::from_millis(150)).await;

    let caller_receiver = RtpReceiver::bind(0).await?;
    let caller_sender = RtpSender::bind().await?;
    let charlie_receiver = RtpReceiver::bind(0).await?;
    let charlie_sender = RtpSender::bind().await?;

    let caller_sdp = pcmu_sdp("127.0.0.1", caller_receiver.port().unwrap());
    let charlie_sdp = pcmu_sdp("127.0.0.1", charlie_receiver.port().unwrap());

    let caller = Arc::new(caller_ua.clone());
    let caller_sdp_cl = caller_sdp.clone();
    let caller_handle = crate::utils::spawn(async move {
        caller.make_call("support", Some(caller_sdp_cl)).await
    });

    // bob rings but never answers; charlie (skill-group fallback) answers.
    let mut charlie_call: Option<(DialogId, String)> = None;
    for _ in 0..120 {
        for event in charlie_ua.process_dialog_events().await? {
            if let TestUaEvent::IncomingCall(id, offer) = event {
                charlie_ua.answer_call(&id, Some(charlie_sdp.clone())).await?;
                info!("skill-group agent (charlie) answered fallback");
                charlie_call = Some((id, offer.unwrap_or_default()));
                break;
            }
        }
        if charlie_call.is_some() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    let (_charlie_id, charlie_offer) = charlie_call
        .ok_or_else(|| anyhow!("skill-group agent never received the fallback INVITE"))?;

    let caller_id = tokio::time::timeout(Duration::from_secs(8), caller_handle)
        .await
        .map_err(|_| anyhow!("caller timed out waiting for skill-group fallback"))?
        .map_err(|e| anyhow!("caller task join error: {e}"))?
        .map_err(|e| anyhow!("skill-group fallback call failed: {e}"))?;
    info!(%caller_id, "caller connected to skill-group agent via fallback");

    let caller_answer_sdp = caller_ua
        .get_negotiated_answer_sdp(&caller_id)
        .await
        .ok_or_else(|| anyhow!("No answer SDP on caller side"))?;
    let charlie_target = extract_media_endpoint(&charlie_offer)
        .ok_or_else(|| anyhow!("Failed to parse skill-group agent endpoint"))?;
    let caller_target = extract_media_endpoint(&caller_answer_sdp)
        .ok_or_else(|| anyhow!("Failed to parse caller endpoint"))?;

    use super::rtp_utils::RtpPacket;
    caller_receiver.start_receiving();
    charlie_receiver.start_receiving();
    let caller_pkts = RtpPacket::create_sequence(100, 1000, 50000, 0xA1A1A1A1, 0, 160, 160);
    let charlie_pkts = RtpPacket::create_sequence(100, 2000, 60000, 0xE5E5E5E5, 0, 160, 160);
    caller_sender.start_sending(charlie_target, caller_pkts, 20);
    charlie_sender.start_sending(caller_target, charlie_pkts, 20);
    sleep(Duration::from_millis(2500)).await;
    caller_sender.stop();
    charlie_sender.stop();
    sleep(Duration::from_millis(200)).await;

    let caller_stats = caller_receiver.get_stats().await;
    let charlie_stats = charlie_receiver.get_stats().await;
    info!(
        caller_received = caller_stats.packets_received,
        charlie_received = charlie_stats.packets_received,
        "skill-group fallback RTP exchange complete"
    );

    assert!(
        charlie_stats.packets_received > 0,
        "skill-group agent should receive RTP from caller (got 0)"
    );
    assert!(
        caller_stats.packets_received > 0,
        "caller should receive RTP from the skill-group agent (got 0)"
    );

    caller_ua.hangup(&caller_id).await?;
    server.stop();
    Ok(())
}

/// Call-graph engine: caller hangup must cascade and tear down the agent leg.
/// This is THE prod bug ("caller hangs up but agent keeps ringing/connected")
/// the event-driven controller is designed to fix structurally.
#[tokio::test]
async fn test_queue_graph_caller_hangup_ends_agent_call() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx =
        QueueMediaTestCtx::setup_with_config(queue_graph_config_with(Vec::new(), false)).await?;
    let caller_sdp = pcmu_sdp("127.0.0.1", ctx.caller_receiver.port().unwrap());
    let agent_sdp = pcmu_sdp("127.0.0.1", ctx.agent_receiver.port().unwrap());

    let (caller_id, agent_id, _agent_offer) =
        ctx.establish_queue_call(caller_sdp, agent_sdp).await?;
    info!(%caller_id, %agent_id, "call-graph queue connected; caller will hang up");

    ctx.caller_ua.hangup(&caller_id).await?;

    for _ in 0..50 {
        let events = ctx.agent_ua.process_dialog_events().await?;
        if events
            .iter()
            .any(|e| matches!(e, TestUaEvent::CallTerminated(id) if id == &agent_id))
        {
            info!("agent call terminated after caller hangup (call-graph cascade OK)");
            ctx.server.stop();
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err(anyhow!(
        "call-graph: agent call did not terminate after caller hung up"
    ))
}

/// Call-graph engine: agent hangup must cascade to the caller.
#[tokio::test]
async fn test_queue_graph_agent_hangup_ends_caller_call() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx =
        QueueMediaTestCtx::setup_with_config(queue_graph_config_with(Vec::new(), false)).await?;
    let caller_sdp = pcmu_sdp("127.0.0.1", ctx.caller_receiver.port().unwrap());
    let agent_sdp = pcmu_sdp("127.0.0.1", ctx.agent_receiver.port().unwrap());

    let (caller_id, agent_id, _agent_offer) =
        ctx.establish_queue_call(caller_sdp, agent_sdp).await?;
    info!(%caller_id, %agent_id, "call-graph queue connected; agent will hang up");

    ctx.agent_ua.hangup(&agent_id).await?;
    ctx.wait_for_caller_terminated(&caller_id, 5).await?;
    info!("caller terminated after agent hangup (call-graph cascade OK)");

    ctx.server.stop();
    Ok(())
}

/// Call-graph engine: an unanswered agent must time out to fallback rather than
/// ring forever (ring timeout enforced by the controller's own timer).
#[tokio::test]
async fn test_queue_graph_no_answer_times_out() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let mut config = queue_graph_config_with(Vec::new(), false);
    if let Some(q) = config.queues.get_mut("support") {
        q.strategy.wait_timeout_secs = Some(2);
        q.fallback = Some(RouteQueueFallbackConfig {
            redirect: None,
            failure_code: Some(486),
            failure_reason: None,
            failure_prompt: None,
            queue_ref: None,
            skill_group_ref: None,
        });
    }

    let ctx = QueueMediaTestCtx::setup_with_config(config).await?;
    let caller_sdp = pcmu_sdp("127.0.0.1", ctx.caller_receiver.port().unwrap());

    let caller = Arc::new(ctx.caller_ua.clone());
    let handle =
        crate::utils::spawn(async move { caller.make_call("support", Some(caller_sdp)).await });

    let mut agent_rang = false;
    for _ in 0..50 {
        let events = ctx.agent_ua.process_dialog_events().await?;
        if events
            .iter()
            .any(|e| matches!(e, TestUaEvent::IncomingCall(..)))
        {
            agent_rang = true;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(agent_rang, "agent never received the INVITE from the call-graph queue");

    let res = tokio::time::timeout(Duration::from_secs(8), handle).await;
    assert!(
        res.is_ok(),
        "call-graph: caller call did not complete within 8s after a 2s ring timeout"
    );

    ctx.server.stop();
    Ok(())
}

/// Call-graph engine: dial+bridge fallback. The primary target (bob) never
/// answers; after the ring timeout the queue dials the configured fallback
/// target (charlie), who answers, and the caller gets two-way audio with charlie.
/// This proves the fallback is a real dial+bridge — not a REFER and not a busy.
#[tokio::test]
async fn test_queue_graph_fallback_dial_bridge() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    // support → dial bob (won't answer), 2s ring timeout, fallback = redirect
    // to charlie (a second registered agent).
    let mut config = queue_graph_config_with(Vec::new(), false);
    if let Some(q) = config.queues.get_mut("support") {
        q.strategy.wait_timeout_secs = Some(2);
        q.fallback = Some(RouteQueueFallbackConfig {
            redirect: Some("sip:charlie@127.0.0.1".to_string()),
            failure_code: None,
            failure_reason: None,
            failure_prompt: None,
            queue_ref: None,
            skill_group_ref: None,
        });
    }

    let ctx = QueueMediaTestCtx::setup_with_config(config).await?;
    // Register the fallback agent + its RTP endpoints.
    let charlie_ua = ctx.server.create_ua("charlie").await?;
    let charlie_sender = RtpSender::bind().await?;
    let charlie_receiver = RtpReceiver::bind(0).await?;
    sleep(Duration::from_millis(100)).await;

    let caller_sdp = pcmu_sdp("127.0.0.1", ctx.caller_receiver.port().unwrap());
    let charlie_sdp = pcmu_sdp("127.0.0.1", charlie_receiver.port().unwrap());

    let caller = Arc::new(ctx.caller_ua.clone());
    let caller_sdp_cl = caller_sdp.clone();
    let caller_handle = crate::utils::spawn(async move {
        caller.make_call("support", Some(caller_sdp_cl)).await
    });

    // bob rings but never answers; charlie (fallback) answers when dialed.
    let mut charlie_call: Option<(DialogId, String)> = None;
    for _ in 0..120 {
        // Drain bob's events without answering so it just rings.
        let _ = ctx.agent_ua.process_dialog_events().await?;
        for event in charlie_ua.process_dialog_events().await? {
            if let TestUaEvent::IncomingCall(id, offer) = event {
                charlie_ua.answer_call(&id, Some(charlie_sdp.clone())).await?;
                info!("fallback agent (charlie) answered");
                charlie_call = Some((id, offer.unwrap_or_default()));
                break;
            }
        }
        if charlie_call.is_some() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    let (_charlie_id, charlie_offer) =
        charlie_call.ok_or_else(|| anyhow!("fallback agent (charlie) never received INVITE"))?;

    let caller_id = tokio::time::timeout(Duration::from_secs(8), caller_handle)
        .await
        .map_err(|_| anyhow!("caller timed out waiting for fallback connect"))?
        .map_err(|e| anyhow!("caller task join error: {e}"))?
        .map_err(|e| anyhow!("fallback queue call failed: {e}"))?;
    info!(%caller_id, "caller connected to fallback agent");

    let caller_answer_sdp = ctx
        .caller_ua
        .get_negotiated_answer_sdp(&caller_id)
        .await
        .ok_or_else(|| anyhow!("No answer SDP on caller side"))?;
    let charlie_target = extract_media_endpoint(&charlie_offer)
        .ok_or_else(|| anyhow!("Failed to parse fallback agent endpoint"))?;
    let caller_target = extract_media_endpoint(&caller_answer_sdp)
        .ok_or_else(|| anyhow!("Failed to parse caller endpoint"))?;

    // Exchange RTP between caller and the fallback agent (charlie).
    use super::rtp_utils::RtpPacket;
    ctx.caller_receiver.start_receiving();
    charlie_receiver.start_receiving();
    let caller_pkts = RtpPacket::create_sequence(100, 1000, 50000, 0xA1A1A1A1, 0, 160, 160);
    let charlie_pkts = RtpPacket::create_sequence(100, 2000, 60000, 0xC3C3C3C3, 0, 160, 160);
    ctx.caller_sender.start_sending(charlie_target, caller_pkts, 20);
    charlie_sender.start_sending(caller_target, charlie_pkts, 20);
    sleep(Duration::from_millis(2500)).await;
    ctx.caller_sender.stop();
    charlie_sender.stop();
    sleep(Duration::from_millis(200)).await;

    let caller_stats = ctx.caller_receiver.get_stats().await;
    let charlie_stats = charlie_receiver.get_stats().await;
    info!(
        caller_received = caller_stats.packets_received,
        charlie_received = charlie_stats.packets_received,
        "fallback dial+bridge RTP exchange complete"
    );

    assert!(
        charlie_stats.packets_received > 0,
        "fallback agent should receive RTP from caller (got 0)"
    );
    assert!(
        caller_stats.packets_received > 0,
        "caller should receive RTP from the fallback agent (got 0)"
    );

    ctx.caller_ua.hangup(&caller_id).await?;
    ctx.server.stop();
    Ok(())
}

/// When the agent hangs up, the caller's call must be torn down too (the
/// hangup must cascade through the queue bridge). The dynamic-leg path failed
/// to propagate the agent BYE, leaving the caller stuck in a live call.
#[tokio::test]
async fn test_queue_agent_hangup_ends_caller_call() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = QueueMediaTestCtx::setup().await?;

    let caller_sdp = pcmu_sdp("127.0.0.1", ctx.caller_receiver.port().unwrap());
    let agent_sdp = pcmu_sdp("127.0.0.1", ctx.agent_receiver.port().unwrap());

    let (caller_id, agent_id, _agent_offer) =
        ctx.establish_queue_call(caller_sdp, agent_sdp).await?;
    info!(%caller_id, %agent_id, "Queue call connected; agent will hang up");

    // Agent hangs up.
    ctx.agent_ua.hangup(&agent_id).await?;

    // The caller's call must terminate as a result.
    ctx.wait_for_caller_terminated(&caller_id, 5).await?;
    info!("Caller call terminated after agent hangup (cascade OK)");

    ctx.server.stop();
    Ok(())
}
