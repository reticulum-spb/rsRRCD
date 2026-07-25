use std::path::PathBuf;

use rs_rrc::*;
use rsrrcd::{Action, HubConfig, Router};
use serde_cbor::Value;

fn config(room_registry_path: PathBuf) -> HubConfig {
    HubConfig {
        config_path: room_registry_path.with_extension("conf"),
        identity_path: room_registry_path.with_extension("identity"),
        room_registry_path,
        rns_config_dir: None,
        hub_name: "integration".into(),
        greeting: None,
        announce_on_start: false,
        announce_period_s: 0.0,
        include_joined_member_list: true,
        room_invite_timeout_s: 900.0,
        room_registry_prune_after_s: 30.0 * 24.0 * 3600.0,
        room_registry_prune_interval_s: 3600.0,
        max_nick_bytes: 32,
        max_rooms_per_session: 32,
        max_room_name_bytes: 64,
        max_msg_body_bytes: 350,
        rate_limit_msgs_per_minute: 240,
        ping_interval_s: 0.0,
        ping_timeout_s: 0.0,
        enable_resource_transfer: true,
        max_resource_bytes: 262_144,
        max_pending_resource_expectations: 8,
        resource_expectation_ttl_s: 30.0,
        trusted_identities: Vec::new(),
        banned_identities: Vec::new(),
        log_level: "INFO".into(),
        log_console: false,
        log_file: None,
    }
}

fn send(router: &mut Router, link: [u8; 16], envelope: Envelope) -> Vec<Action> {
    router.packet(link, &envelope.encode().unwrap())
}

fn envelopes(actions: &[Action]) -> Vec<([u8; 16], Envelope)> {
    actions
        .iter()
        .filter_map(|action| {
            let Action::Send(link, payload) = action else {
                return None;
            };
            Some((*link, Envelope::decode(payload).unwrap()))
        })
        .collect()
}

fn connect(router: &mut Router, link: [u8; 16], peer: [u8; 16], nick: &str) {
    router.established(link);
    router.identified(link, peer);
    let actions = send(router, link, Envelope::hello(&peer, Some(nick)));
    let welcome = envelopes(&actions)
        .into_iter()
        .find_map(|(_, envelope)| (envelope.message_type() == Some(T_WELCOME)).then_some(envelope))
        .unwrap();
    let welcome = welcome.welcome().unwrap();
    assert_eq!(welcome.hub_name.as_deref(), Some("integration"));
    assert!(welcome.capabilities.room_state);
    assert!(welcome.capabilities.user_list);
}

fn command(source: [u8; 16], room: &str, text: &str) -> Envelope {
    Envelope::command(&source, Some(room), text).unwrap()
}

#[test]
fn two_clients_complete_room_and_directory_flow() {
    let directory = tempfile::tempdir().unwrap();
    let mut router = Router::new(config(directory.path().join("rooms")), [9; 16]);
    let alice_link = [1; 16];
    let alice = [11; 16];
    let bob_link = [2; 16];
    let bob = [22; 16];
    connect(&mut router, alice_link, alice, "alice");
    connect(&mut router, bob_link, bob, "bob");

    for (link, peer) in [(alice_link, alice), (bob_link, bob)] {
        let actions = send(
            &mut router,
            link,
            Envelope::join(&peer, "lobby", None).unwrap(),
        );
        let joined = envelopes(&actions)
            .into_iter()
            .find_map(|(target, envelope)| {
                (target == link && envelope.message_type() == Some(T_JOINED)).then_some(envelope)
            })
            .unwrap();
        assert_eq!(joined.room(), Some("lobby"));
        assert!(joined.room_state().is_some());
    }

    let actions = send(
        &mut router,
        alice_link,
        Envelope::message(&alice, "lobby", "hello", false).unwrap(),
    );
    let delivered = envelopes(&actions)
        .into_iter()
        .filter(|(_, envelope)| {
            envelope.message_type() == Some(T_MSG) && envelope.body_text() == Some("hello")
        })
        .collect::<Vec<_>>();
    assert_eq!(delivered.len(), 2);
    assert!(delivered.iter().all(|(_, envelope)| {
        envelope.source() == Some(alice) && envelope.nick() == Some("alice")
    }));

    let actions = send(
        &mut router,
        alice_link,
        command(alice, "lobby", "/register lobby"),
    );
    assert!(envelopes(&actions).iter().any(|(_, envelope)| {
        envelope
            .room_state()
            .is_some_and(|state| state.registered && state.modes.contains('r'))
    }));

    let actions = send(&mut router, bob_link, command(bob, "lobby", "/list"));
    assert!(envelopes(&actions).iter().any(|(_, envelope)| {
        envelope
            .body_text()
            .is_some_and(|text| text.contains("lobby"))
    }));

    let actions = send(&mut router, bob_link, command(bob, "lobby", "/who lobby"));
    let users = envelopes(&actions)
        .into_iter()
        .find_map(|(_, envelope)| envelope.user_list())
        .unwrap();
    assert_eq!(users.len(), 2);
    assert!(
        users
            .iter()
            .any(|user| user.nick.as_deref() == Some("alice"))
    );
    assert!(users.iter().any(|user| user.nick.as_deref() == Some("bob")));

    let actions = send(
        &mut router,
        alice_link,
        command(alice, "lobby", "/mode lobby +m"),
    );
    assert!(envelopes(&actions).iter().any(|(_, envelope)| {
        envelope
            .room_state()
            .is_some_and(|state| state.modes.contains('m'))
    }));
}

#[test]
fn malformed_packet_is_rejected_and_claimed_source_is_replaced() {
    let directory = tempfile::tempdir().unwrap();
    let mut router = Router::new(config(directory.path().join("rooms")), [9; 16]);
    let link = [1; 16];
    router.established(link);
    router.identified(link, [11; 16]);

    let actions = router.packet(link, &[0xff, 0x00]);
    assert!(envelopes(&actions).iter().any(|(_, envelope)| {
        envelope.message_type() == Some(T_ERROR) && envelope.body_text().is_some()
    }));

    let mut wrong_source = Envelope::hello(&[22; 16], None);
    wrong_source.set(K_BODY, Value::Null);
    let actions = send(&mut router, link, wrong_source);
    assert!(
        envelopes(&actions)
            .iter()
            .any(|(_, envelope)| envelope.message_type() == Some(T_WELCOME))
    );

    send(
        &mut router,
        link,
        Envelope::join(&[22; 16], "lobby", None).unwrap(),
    );
    let actions = send(
        &mut router,
        link,
        Envelope::message(&[22; 16], "lobby", "spoof attempt", false).unwrap(),
    );
    let forwarded = envelopes(&actions)
        .into_iter()
        .find_map(|(_, envelope)| (envelope.message_type() == Some(T_MSG)).then_some(envelope))
        .unwrap();
    assert_eq!(forwarded.source(), Some([11; 16]));
}

#[test]
fn registered_room_state_survives_public_api_restart() {
    let directory = tempfile::tempdir().unwrap();
    let registry = directory.path().join("rooms");
    let alice_link = [1; 16];
    let alice = [11; 16];
    let bob_link = [2; 16];
    let bob = [22; 16];
    let mut router = Router::load(config(registry.clone()), [9; 16]).unwrap();
    connect(&mut router, alice_link, alice, "alice");
    connect(&mut router, bob_link, bob, "bob");
    for (link, peer) in [(alice_link, alice), (bob_link, bob)] {
        send(
            &mut router,
            link,
            Envelope::join(&peer, "lobby", None).unwrap(),
        );
    }

    for text in [
        "/register lobby",
        "/topic lobby Persistent topic",
        "/mode lobby +m",
        "/mode lobby +i",
        "/mode lobby +k secret",
        "/op lobby bob",
        "/voice lobby bob",
        "/ban lobby add bob",
    ] {
        let actions = send(&mut router, alice_link, command(alice, "lobby", text));
        assert!(
            !actions.is_empty(),
            "administrative command produced no response: {text}"
        );
    }
    drop(router);

    let restarted = Router::load(config(registry), [9; 16]).unwrap();
    let room = &restarted.state.rooms["lobby"];
    assert!(room.registered);
    assert_eq!(room.founder, Some(alice));
    assert_eq!(room.topic.as_deref(), Some("Persistent topic"));
    assert_eq!(room.key.as_deref(), Some("secret"));
    assert!(room.moderated);
    assert!(room.invite_only);
    assert!(room.operators.contains(&alice));
    assert!(room.operators.contains(&bob));
    assert!(room.voiced.contains(&bob));
    assert!(room.banned.contains(&bob));
    assert!(room.members.is_empty());
}
