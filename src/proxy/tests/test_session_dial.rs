//! Real-stack integration test for the new `call::session::sip::SipSession`.
//!
//! Stands up two real rsipstack UDP endpoints (via [`TestUa`]); one answers,
//! and we drive `SipSession::dial` from the other's dialog layer. This is the
//! first time the new `Session` type places an *actual* INVITE over the wire
//! and maps a *live* answered dialog — validating dial + state mapping end to
//! end, not against a fake.

use super::test_ua::{TestUa, TestUaConfig, TestUaEvent};
use crate::call::session::sip::SipSession;
use crate::call::session::{Direction, Session, SessionState};
use rsipstack::dialog::invitation::InviteOption;
use rsipstack::transport::SipAddr;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::{sleep, timeout};

const OFFER_SDP: &str = "v=0\r\n\
o=alice 0 0 IN IP4 127.0.0.1\r\n\
s=-\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
m=audio 40000 RTP/AVP 0\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=sendrecv\r\n";

const ANSWER_SDP: &str = "v=0\r\n\
o=bob 0 0 IN IP4 127.0.0.1\r\n\
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

#[tokio::test]
async fn sip_session_dial_against_real_uas_yields_active_outbound() {
    let _ = tracing_subscriber::fmt::try_init();

    let bob_port = portpicker::pick_unused_port().expect("bob port");
    let alice_port = portpicker::pick_unused_port().expect("alice port");
    let bob_addr: SocketAddr = format!("127.0.0.1:{bob_port}").parse().unwrap();

    // ── Bob: a real UAS that answers the first incoming call. ───────────────
    let mut bob = TestUa::new(ua_config("bob", bob_port, "127.0.0.1:0".parse().unwrap()));
    bob.start().await.expect("bob start");
    let bob_answer = bob.clone();
    let answer_task = tokio::spawn(async move {
        for _ in 0..250 {
            if let Ok(events) = bob_answer.process_dialog_events().await {
                for event in events {
                    if let TestUaEvent::IncomingCall(id, _sdp) = event {
                        let _ = bob_answer
                            .answer_call(&id, Some(ANSWER_SDP.to_string()))
                            .await;
                        return;
                    }
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    });

    // ── Alice: just hosts the endpoint/dialog layer we dial from. ───────────
    let mut alice = TestUa::new(ua_config("alice", alice_port, bob_addr));
    alice.start().await.expect("alice start");

    let contact = alice.contact_uri().expect("alice contact");
    let bob_uri: rsipstack::sip::Uri = format!("sip:bob@127.0.0.1:{bob_port}")
        .try_into()
        .expect("bob uri");
    let destination = SipAddr {
        r#type: Some(rsipstack::sip::Transport::Udp),
        addr: bob_addr.into(),
    };

    let opt = InviteOption {
        callee: bob_uri,
        caller: contact.clone(),
        contact,
        content_type: Some("application/sdp".to_string()),
        offer: Some(OFFER_SDP.as_bytes().to_vec()),
        destination: Some(destination),
        ..Default::default()
    };

    // ── The code under test: place a real INVITE via the new SipSession. ────
    let dialog_layer = alice.dialog_layer().expect("alice dialog layer");
    let (session, _answer_sdp) = timeout(
        Duration::from_secs(5),
        SipSession::dial(dialog_layer.as_ref(), opt),
    )
    .await
    .expect("dial did not time out")
    .expect("dial should succeed against an answering UAS");

    // A real, answered outbound leg.
    assert_eq!(session.direction(), Direction::Outbound);
    assert_eq!(
        session.state(),
        SessionState::Active,
        "an answered dialog maps to Active"
    );

    let _ = answer_task.await;
}
