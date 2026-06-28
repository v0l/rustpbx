//! Full real-stack B2BUA test driving the entire new `call::session` stack.
//!
//! Three real rsipstack endpoints:
//!   Alice (UAC)  ──INVITE──▶  Switch (our DialCall)  ──INVITE──▶  Bob (UAS)
//!
//! The Switch turns Alice's incoming INVITE into a `SipSession::inbound`, builds
//! a SIP-backed `Dialer`, and runs the `DialCall`. The reducer dials Bob; when
//! Bob answers, it accepts Alice and bridges. Alice's call succeeding proves the
//! whole spine — reducer → executor → switch → SipSession — works over the wire.

use super::test_ua::{TestUa, TestUaConfig, TestUaEvent};
use crate::call::session::dial_call::{DialError, Dialer, DialCall};
use crate::call::session::reducer::{FlowReducer, Stage, Strategy};
use crate::call::session::sip::SipSession;
use crate::call::session::Session;
use async_trait::async_trait;
use rsipstack::dialog::dialog_layer::DialogLayer;
use rsipstack::dialog::invitation::InviteOption;
use rsipstack::sip::prelude::HeadersExt;
use rsipstack::transaction::EndpointBuilder;
use rsipstack::transport::udp::UdpConnection;
use rsipstack::transport::{SipAddr, TransportLayer};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

const OFFER_SDP: &str = "v=0\r\n\
o=- 0 0 IN IP4 127.0.0.1\r\n\
s=-\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
m=audio 40000 RTP/AVP 0\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=sendrecv\r\n";

const ANSWER_SDP: &str = "v=0\r\n\
o=- 0 0 IN IP4 127.0.0.1\r\n\
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

fn udp(addr: SocketAddr) -> SipAddr {
    SipAddr {
        r#type: Some(rsipstack::sip::Transport::Udp),
        addr: addr.into(),
    }
}

/// A SIP-backed dialer: every target dials the one configured callee (Bob).
struct SipDialer {
    dialog_layer: Arc<DialogLayer>,
    contact: rsipstack::sip::Uri,
    callee_uri: rsipstack::sip::Uri,
    destination: SipAddr,
}

#[async_trait]
impl Dialer for SipDialer {
    async fn dial(&self, _target: usize) -> Result<Box<dyn Session>, DialError> {
        let opt = InviteOption {
            callee: self.callee_uri.clone(),
            caller: self.contact.clone(),
            contact: self.contact.clone(),
            content_type: Some("application/sdp".to_string()),
            offer: Some(OFFER_SDP.as_bytes().to_vec()),
            destination: Some(self.destination.clone()),
            ..Default::default()
        };
        match SipSession::dial(self.dialog_layer.as_ref(), opt).await {
            Ok((session, _answer_sdp)) => Ok(Box::new(session)),
            Err(_) => Err(DialError),
        }
    }
}

/// Stand up the Switch endpoint: on each incoming INVITE, wrap the caller as a
/// `SipSession::inbound` and run an `DialCall` that dials Bob and bridges.
async fn spawn_switch(
    switch_port: u16,
    callee_uri: rsipstack::sip::Uri,
    callee_dest: SipAddr,
) -> CancellationToken {
    let cancel = CancellationToken::new();
    let switch_addr: SocketAddr = format!("127.0.0.1:{switch_port}").parse().unwrap();

    let transport_layer = TransportLayer::new(cancel.clone());
    let connection = UdpConnection::create_connection(switch_addr, None, None)
        .await
        .expect("switch udp bind");
    transport_layer.add_transport(connection.into());
    let endpoint = EndpointBuilder::new()
        .with_cancel_token(cancel.clone())
        .with_transport_layer(transport_layer)
        .build();
    let mut incoming = endpoint.incoming_transactions().expect("incoming");
    let dialog_layer = Arc::new(DialogLayer::new(endpoint.inner.clone()));
    let contact: rsipstack::sip::Uri = format!("sip:switch@127.0.0.1:{switch_port}")
        .try_into()
        .unwrap();

    // Endpoint service loop.
    let serve_cancel = cancel.clone();
    crate::utils::spawn(async move {
        tokio::select! {
            _ = endpoint.serve() => {}
            _ = serve_cancel.cancelled() => {}
        }
    });

    // Incoming-request loop: wire INVITEs into the executor.
    crate::utils::spawn(async move {
        loop {
            let Some(mut tx) = incoming.recv().await else {
                break;
            };

            // In-dialog requests (ACK/BYE with a To-tag) go to the dialog.
            let has_to_tag = tx
                .original
                .to_header()
                .ok()
                .and_then(|to| to.tag().ok().flatten())
                .is_some();
            if has_to_tag {
                if let Some(mut dialog) = dialog_layer.match_dialog(&tx) {
                    crate::utils::spawn(async move {
                        dialog.handle(&mut tx).await.ok();
                    });
                    continue;
                }
            }

            match tx.original.method {
                rsipstack::sip::Method::Invite => {
                    let (state_tx, state_rx) = unbounded_channel();
                    if let Ok(server_dialog) = dialog_layer.get_or_create_server_invite(
                        &tx,
                        state_tx,
                        None,
                        Some(contact.clone()),
                    ) {
                        // Pump the INVITE transaction (sends our 200 once accepted).
                        let mut pump = server_dialog.clone();
                        crate::utils::spawn(async move {
                            pump.handle(&mut tx).await.ok();
                        });

                        // The caller leg, with the SDP answer we'll send on accept.
                        let mut caller = SipSession::inbound(server_dialog, state_rx);
                        let ct = rsipstack::sip::Header::ContentType(
                            rsipstack::sip::headers::ContentType::from("application/sdp"),
                        );
                        caller.set_answer(vec![ct], ANSWER_SDP.as_bytes().to_vec());

                        let dialer = Arc::new(SipDialer {
                            dialog_layer: dialog_layer.clone(),
                            contact: contact.clone(),
                            callee_uri: callee_uri.clone(),
                            destination: callee_dest.clone(),
                        });
                        let executor = DialCall::new(
                            FlowReducer::new(vec![Stage {
                                strategy: Strategy::Sequential,
                                count: 1,
                            }]),
                            Box::new(caller),
                            dialer,
                        );
                        crate::utils::spawn(executor.run());
                    }
                }
                rsipstack::sip::Method::Ack => {
                    let (state_tx, _rx) =
                        unbounded_channel::<rsipstack::dialog::dialog::DialogState>();
                    if let Ok(mut dialog) = dialog_layer.get_or_create_server_invite(
                        &tx,
                        state_tx,
                        None,
                        Some(contact.clone()),
                    ) {
                        crate::utils::spawn(async move {
                            dialog.handle(&mut tx).await.ok();
                        });
                    }
                }
                _ => {
                    tx.reply(rsipstack::sip::StatusCode::OK).await.ok();
                }
            }
        }
    });

    cancel
}

#[tokio::test]
async fn full_b2bua_call_alice_to_bob_via_executor() {
    let _ = tracing_subscriber::fmt::try_init();

    let bob_port = portpicker::pick_unused_port().expect("bob port");
    let switch_port = portpicker::pick_unused_port().expect("switch port");
    let alice_port = portpicker::pick_unused_port().expect("alice port");
    let bob_addr: SocketAddr = format!("127.0.0.1:{bob_port}").parse().unwrap();
    let switch_addr: SocketAddr = format!("127.0.0.1:{switch_port}").parse().unwrap();

    // ── Bob: real UAS that answers the first incoming call. ─────────────────
    let mut bob = TestUa::new(ua_config("bob", bob_port, "127.0.0.1:0".parse().unwrap()));
    bob.start().await.expect("bob start");
    let bob_answer = bob.clone();
    let answer_task = tokio::spawn(async move {
        for _ in 0..400 {
            if let Ok(events) = bob_answer.process_dialog_events().await {
                for event in events {
                    if let TestUaEvent::IncomingCall(id, _) = event {
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

    // ── Switch: our B2BUA running the DialCall. ─────────────────────────────
    let bob_uri: rsipstack::sip::Uri = format!("sip:bob@127.0.0.1:{bob_port}")
        .try_into()
        .unwrap();
    let switch_cancel = spawn_switch(switch_port, bob_uri, udp(bob_addr)).await;
    sleep(Duration::from_millis(150)).await; // let the switch bind/serve

    // ── Alice: real UAC, calls the Switch. ──────────────────────────────────
    let mut alice = TestUa::new(ua_config("alice", alice_port, switch_addr));
    alice.start().await.expect("alice start");

    let result = timeout(
        Duration::from_secs(10),
        alice.make_call_with_sdp("switch", Some(OFFER_SDP.to_string())),
    )
    .await;

    switch_cancel.cancel();
    let _ = answer_task.await;

    let call = result.expect("alice call should not time out");
    assert!(
        call.is_ok(),
        "Alice's call through the B2BUA executor should establish (got {call:?})"
    );
}
