//! Cutover validation: a real call through the proxy with the `session_engine`
//! flag ON, so a direct `Targets` call is served by the new `call::session`
//! engine (caller `SipSession::inbound`, SIP+RTP `Dialer`, `DialCall` +
//! `CallSwitch`) instead of the god object — proving the flagged production
//! path establishes a real SIP call end to end.

use super::e2e_test_server::E2eTestServer;
use super::rtp_utils::{RtpPacket, RtpReceiver, RtpSender, extract_media_endpoint};
use super::test_helpers;
use super::test_ua::TestUaEvent;
use crate::config::MediaProxyMode;
use anyhow::{Result, anyhow};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// SDP advertising a specific RTP port (PCMU).
fn sdp_with_port(port: u16) -> String {
    format!(
        "v=0\r\n\
         o=- 1 1 IN IP4 127.0.0.1\r\n\
         s=-\r\n\
         c=IN IP4 127.0.0.1\r\n\
         t=0 0\r\n\
         m=audio {port} RTP/AVP 0\r\n\
         a=rtpmap:0 PCMU/8000\r\n\
         a=sendrecv\r\n"
    )
}

const PCMU_SDP: &str = "v=0\r\n\
o=- 1 1 IN IP4 127.0.0.1\r\n\
s=-\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
m=audio 12346 RTP/AVP 0\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=sendrecv\r\n";

#[tokio::test]
async fn direct_call_via_session_engine_establishes() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    // Proxy with the new engine ON for direct (Targets) calls.
    let mut config = test_helpers::test_proxy_config(0);
    config.session_engine = true;
    config.media_proxy = MediaProxyMode::None;
    let server = Arc::new(E2eTestServer::start_with_config(config).await?);

    let alice = Arc::new(server.create_ua("alice").await?);
    let bob = server.create_ua("bob").await?;
    sleep(Duration::from_millis(100)).await;

    // Alice calls Bob.
    let caller = crate::utils::spawn({
        let a = alice.clone();
        async move { a.make_call("bob", Some(PCMU_SDP.to_string())).await }
    });

    // Bob answers the INVITE the new engine sent him.
    let mut answered = false;
    for _ in 0..50 {
        for event in bob.process_dialog_events().await? {
            if let TestUaEvent::IncomingCall(id, _) = event {
                bob.answer_call(&id, Some(PCMU_SDP.to_string())).await?;
                answered = true;
                break;
            }
        }
        if answered {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(answered, "Bob should receive the INVITE from the session engine");

    // Alice's call must establish (the engine accepted her with a 200 OK once
    // Bob answered) — the full reducer→executor→switch→SipSession path on a
    // real proxy call.
    let established = tokio::time::timeout(Duration::from_secs(5), caller).await;
    assert!(
        matches!(established, Ok(Ok(Ok(_)))),
        "direct call via session engine should establish: {established:?}"
    );

    Ok(())
}

#[tokio::test]
async fn session_engine_carries_audio_alice_to_bob() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    // Proxy with the new engine ON, no media proxy (the engine binds its own RTP).
    let mut config = test_helpers::test_proxy_config(0);
    config.session_engine = true;
    config.media_proxy = MediaProxyMode::None;
    let server = Arc::new(E2eTestServer::start_with_config(config).await?);

    let alice = Arc::new(server.create_ua("alice").await?);
    let bob = server.create_ua("bob").await?;
    sleep(Duration::from_millis(100)).await;

    // Real RTP endpoints for both parties.
    let alice_rx = RtpReceiver::bind(0).await?;
    let bob_rx = RtpReceiver::bind(0).await?;
    alice_rx.start_receiving();
    bob_rx.start_receiving();
    let alice_sender = RtpSender::bind().await?;
    let alice_sdp = sdp_with_port(alice_rx.port()?);
    let bob_sdp = sdp_with_port(bob_rx.port()?);

    // Alice calls Bob, advertising her real RTP port.
    let caller = crate::utils::spawn({
        let a = alice.clone();
        let sdp = alice_sdp.clone();
        async move { a.make_call("bob", Some(sdp)).await }
    });

    // Bob answers, advertising his real RTP port.
    let mut bob_offer_sdp = None;
    for _ in 0..50 {
        for event in bob.process_dialog_events().await? {
            if let TestUaEvent::IncomingCall(id, offer) = event {
                bob_offer_sdp = offer;
                bob.answer_call(&id, Some(bob_sdp.clone())).await?;
                break;
            }
        }
        if bob_offer_sdp.is_some() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(bob_offer_sdp.is_some(), "Bob should receive the engine's INVITE");

    // Alice's negotiated answer SDP tells her where the proxy's caller-leg RTP
    // socket is — that's where she sends.
    let dialog_id = tokio::time::timeout(Duration::from_secs(5), caller)
        .await
        .map_err(|_| anyhow!("caller timed out"))???;
    let answer_sdp = alice
        .get_negotiated_answer_sdp(&dialog_id)
        .await
        .ok_or_else(|| anyhow!("no negotiated answer SDP"))?;
    let alice_to_proxy =
        extract_media_endpoint(&answer_sdp).ok_or_else(|| anyhow!("no proxy media endpoint"))?;

    // Alice speaks: a stream of PCMU RTP into the proxy's caller leg.
    let packets = RtpPacket::create_sequence(60, 1000, 50000, 0xA1A1_A1A1, 0, 160, 160);
    alice_sender.start_sending(alice_to_proxy, packets, 20);
    sleep(Duration::from_secs(2)).await;
    alice_sender.stop();
    sleep(Duration::from_millis(200)).await;

    // Bob must have received Alice's audio, forwarded through the unified mixer.
    let bob_stats = bob_rx.get_stats().await;
    assert!(
        bob_stats.packets_received > 0,
        "Bob should receive Alice's RTP through the session engine's unified mixer (got {})",
        bob_stats.packets_received
    );

    Ok(())
}
