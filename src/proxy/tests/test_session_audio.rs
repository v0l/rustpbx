//! Real-SIP + real-RTP audio test for the unified media plane.
//!
//! Two *real* dialed `SipSession`s (over rsipstack UDP endpoints) are each given
//! a real `RtpMedia` socket whose remote is a raw UDP "phone". Their media seams
//! are claimed via `take_media_io` and bridged by `MixerBridge`. Audio sent from
//! one phone must cross the unified mixer and arrive at the other — proving the
//! production type (`SipSession`) carries audio end-to-end through the new media
//! plane over the wire, not just in unit tests.

use super::test_ua::{TestUa, TestUaConfig, TestUaEvent};
use crate::call::session::Session;
use crate::call::session::mixer_bridge::MixerBridge;
use crate::call::session::rtp_media::RtpMedia;
use crate::call::session::sip::SipSession;
use audio_codec::{CodecType, create_encoder};
use rsipstack::dialog::invitation::InviteOption;
use rsipstack::transport::SipAddr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::{sleep, timeout};

const OFFER_SDP: &str = "v=0\r\n\
o=us 0 0 IN IP4 127.0.0.1\r\n\
s=-\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
m=audio 40000 RTP/AVP 0\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=sendrecv\r\n";

const ANSWER_SDP: &str = "v=0\r\n\
o=them 0 0 IN IP4 127.0.0.1\r\n\
s=-\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
m=audio 40002 RTP/AVP 0\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=sendrecv\r\n";

fn ua_config(username: &str, local_port: u16, proxy_addr: SocketAddr) -> TestUaConfig {
    TestUaConfig {
        username: username.to_string(),
        password: String::new(),
        realm: "test".to_string(),
        local_port,
        proxy_addr,
    }
}

/// Start a UA that auto-answers the first incoming call.
async fn spawn_answerer(port: u16) -> TestUa {
    let mut ua = TestUa::new(ua_config("callee", port, "127.0.0.1:0".parse().unwrap()));
    ua.start().await.expect("answerer start");
    let answer = ua.clone();
    tokio::spawn(async move {
        for _ in 0..400 {
            if let Ok(events) = answer.process_dialog_events().await {
                for event in events {
                    if let TestUaEvent::IncomingCall(id, _sdp) = event {
                        let _ = answer.answer_call(&id, Some(ANSWER_SDP.to_string())).await;
                        return;
                    }
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    });
    ua
}

/// Dial `callee_port` from `alice`, returning the answered outbound SipSession.
async fn dial(alice: &TestUa, callee_port: u16) -> SipSession {
    let contact = alice.contact_uri().expect("contact");
    let uri: rsipstack::sip::Uri = format!("sip:callee@127.0.0.1:{callee_port}")
        .try_into()
        .expect("uri");
    let destination = SipAddr {
        r#type: Some(rsipstack::sip::Transport::Udp),
        addr: format!("127.0.0.1:{callee_port}")
            .parse::<SocketAddr>()
            .unwrap()
            .into(),
    };
    let opt = InviteOption {
        callee: uri,
        caller: contact.clone(),
        contact,
        content_type: Some("application/sdp".to_string()),
        offer: Some(OFFER_SDP.as_bytes().to_vec()),
        destination: Some(destination),
        ..Default::default()
    };
    let dialog_layer = alice.dialog_layer().expect("dialog layer");
    let (session, _) = timeout(
        Duration::from_secs(5),
        SipSession::dial(dialog_layer.as_ref(), opt),
    )
    .await
    .expect("dial timed out")
    .expect("dial should succeed");
    session
}

fn pcmu_payload(value: i16) -> Vec<u8> {
    create_encoder(CodecType::PCMU).encode(&vec![value; 160])
}

fn rtp(seq: u16, payload: &[u8]) -> Vec<u8> {
    let mut pkt = vec![0x80, 0x00];
    pkt.extend_from_slice(&seq.to_be_bytes());
    pkt.extend_from_slice(&0u32.to_be_bytes());
    pkt.extend_from_slice(&1u32.to_be_bytes());
    pkt.extend_from_slice(payload);
    pkt
}

#[tokio::test]
async fn real_sip_legs_bridge_audio_through_unified_mixer() {
    let _ = tracing_subscriber::fmt::try_init();

    let bob_port = portpicker::pick_unused_port().expect("bob port");
    let carol_port = portpicker::pick_unused_port().expect("carol port");
    let alice_port = portpicker::pick_unused_port().expect("alice port");

    // Two real UAS answerers.
    let _bob = spawn_answerer(bob_port).await;
    let _carol = spawn_answerer(carol_port).await;
    sleep(Duration::from_millis(100)).await;

    // The endpoint we dial from.
    let mut alice = TestUa::new(ua_config(
        "alice",
        alice_port,
        "127.0.0.1:0".parse().unwrap(),
    ));
    alice.start().await.expect("alice start");

    // Two real answered SIP legs.
    let mut leg_a = dial(&alice, bob_port).await;
    let mut leg_b = dial(&alice, carol_port).await;

    // Raw UDP "phones" — the actual RTP endpoints for each leg.
    let phone_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let phone_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let phone_a_addr = phone_a.local_addr().unwrap();
    let phone_b_addr = phone_b.local_addr().unwrap();

    // Bind real RTP sockets on each leg, remote = its phone. Capture the leg's
    // bound port before `attach_rtp` consumes the endpoint.
    let rtp_a = RtpMedia::bind(CodecType::PCMU).await.unwrap();
    let leg_a_rtp: SocketAddr = format!("127.0.0.1:{}", rtp_a.local_port()).parse().unwrap();
    leg_a.attach_rtp(rtp_a, phone_a_addr);

    let rtp_b = RtpMedia::bind(CodecType::PCMU).await.unwrap();
    let _leg_b_rtp: SocketAddr = format!("127.0.0.1:{}", rtp_b.local_port()).parse().unwrap();
    leg_b.attach_rtp(rtp_b, phone_b_addr);

    // Claim each leg's media seam and bridge them through the unified mixer.
    let io_a = Session::take_media_io(&mut leg_a).expect("leg A media io");
    let io_b = Session::take_media_io(&mut leg_b).expect("leg B media io");
    let mut bridge = MixerBridge::new(8000);
    bridge.add_leg(CodecType::PCMU, io_a);
    bridge.add_leg(CodecType::PCMU, io_b);
    let _handle = bridge.spawn();

    // Phone A speaks: RTP into leg A's socket.
    let payload = pcmu_payload(2000);
    for seq in 0..15u16 {
        phone_a.send_to(&rtp(seq, &payload), leg_a_rtp).await.unwrap();
    }

    // Phone B should receive Phone A's audio (same codec → passthrough),
    // proving audio crossed two real SIP legs via the unified mixer.
    let mut buf = vec![0u8; 2048];
    let mut got = None;
    for _ in 0..30 {
        match timeout(Duration::from_millis(100), phone_b.recv_from(&mut buf)).await {
            Ok(Ok((n, _))) if n > 12 => {
                got = Some(buf[12..n].to_vec());
                break;
            }
            _ => continue,
        }
    }
    assert_eq!(
        got.as_deref(),
        Some(payload.as_slice()),
        "Phone B should hear Phone A across two real SIP legs and the unified mixer"
    );

    // Keep legs alive until the assertion (drop tears down the SIP dialogs).
    let _ = Arc::new((leg_a, leg_b));
}
