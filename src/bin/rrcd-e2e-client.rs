use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rns_identity::identity::Identity;
use rns_runtime::application::await_path;
use rns_runtime::lifecycle::ShutdownSignal;
use rns_runtime::link_client::LinkSession;
use rs_rrc::*;
use serde_cbor::Value;
use sha2::{Digest, Sha256};

#[tokio::main]
async fn main() -> Result<()> {
    let mut arguments = std::env::args().skip(1);
    let destination = arguments
        .next()
        .context("usage: rrcd-e2e-client <destination hash> [rns config dir]")?;
    let config = arguments
        .next()
        .unwrap_or_else(|| format!("{}/.rsReticulum", std::env::var("HOME").unwrap_or_default()));
    let mode = arguments.next().unwrap_or_else(|| "setup".into());
    let bytes = hex::decode(destination)?;
    let destination = <[u8; 16]>::try_from(bytes.as_slice())
        .map_err(|_| anyhow::anyhow!("destination hash must be 32 hexadecimal characters"))?;

    let shutdown = ShutdownSignal::new();
    let runtime = rns_runtime::reticulum::init(
        Some(&config),
        None,
        shutdown.clone(),
        Arc::new(AtomicBool::new(true)),
    )
    .await?;
    await_path(&runtime, destination, Duration::from_secs(20)).await?;

    let identity = Identity::new();
    let (mut link, welcome) =
        connect_and_join(&runtime, destination, identity.clone(), "rrcd-rs-e2e").await?;
    let hub = welcome
        .map(K_BODY)
        .and_then(|body| map_get(body, B_WELCOME_HUB))
        .and_then(|value| match value {
            Value::Text(value) => Some(value.as_str()),
            _ => None,
        })
        .unwrap_or("unknown");

    if mode == "verify" {
        let state = receive_type(&mut link, T_NOTICE).await?;
        let body = state.text(K_BODY).unwrap_or_default();
        if !body.contains("registered") || !body.contains("+m") || !body.contains("Persistent E2E")
        {
            bail!("persisted room state was not restored: {body}");
        }
        link.close().await?;
        shutdown.trigger();
        println!("E2E VERIFY OK: persisted room modes and topic restored");
        return Ok(());
    }
    if mode != "setup" {
        bail!("unknown E2E mode {mode:?}; expected setup or verify");
    }

    send_command(&mut link, &identity, "/register e2e", "room registered").await?;
    send_command(&mut link, &identity, "/mode e2e +m", "+m").await?;
    send_command(
        &mut link,
        &identity,
        "/topic e2e Persistent E2E",
        "Persistent E2E",
    )
    .await?;

    let mut message = Envelope::new(T_MSG, &identity.hash);
    message.set(K_ROOM, Value::Text("e2e".into()));
    message.set(K_BODY, Value::Text("transport round trip".into()));
    link.send(&message.encode()?).await?;
    let echoed = receive_type(&mut link, T_MSG).await?;
    if echoed.text(K_BODY) != Some("transport round trip") {
        bail!("hub returned an unexpected message body");
    }

    let receiver_identity = Identity::new();
    let (mut receiver, _) = connect_and_join(
        &runtime,
        destination,
        receiver_identity,
        "rrcd-rs-e2e-receiver",
    )
    .await?;
    let resource_payload = vec![b'R'; 4096];
    let mut body = Map::new();
    body.insert(Value::Integer(B_RES_ID), Value::Bytes(vec![0x42; 8]));
    body.insert(Value::Integer(B_RES_KIND), Value::Text("notice".into()));
    body.insert(
        Value::Integer(B_RES_SIZE),
        Value::Integer(resource_payload.len() as i128),
    );
    body.insert(
        Value::Integer(B_RES_SHA256),
        Value::Bytes(Sha256::digest(&resource_payload).to_vec()),
    );
    body.insert(Value::Integer(B_RES_ENCODING), Value::Text("utf-8".into()));
    let mut resource_envelope = Envelope::new(T_RESOURCE_ENVELOPE, &identity.hash);
    resource_envelope.set(K_ROOM, Value::Text("e2e".into()));
    resource_envelope.set(K_BODY, Value::Map(body));
    link.send(&resource_envelope.encode()?).await?;
    link.send_resource(resource_payload.clone(), false, Duration::from_secs(30))
        .await?;

    let forwarded_envelope = receive_type(&mut receiver, T_RESOURCE_ENVELOPE).await?;
    if forwarded_envelope.bytes(K_SRC) != Some(identity.hash.as_slice()) {
        bail!("forwarded resource envelope did not preserve sender identity");
    }
    let received = receiver.recv_resource(Duration::from_secs(30)).await?;
    if received.data != resource_payload {
        bail!("forwarded Resource payload mismatch");
    }

    receiver.close().await?;
    link.close().await?;
    shutdown.trigger();
    println!("E2E SETUP OK: hub={hub} room=e2e commands+packet+resource");
    Ok(())
}

async fn send_command(
    link: &mut LinkSession,
    identity: &Identity,
    command: &str,
    expected: &str,
) -> Result<()> {
    let mut message = Envelope::new(T_MSG, &identity.hash);
    message.set(K_ROOM, Value::Text("e2e".into()));
    message.set(K_BODY, Value::Text(command.into()));
    link.send(&message.encode()?).await?;
    let receive = async {
        loop {
            let payload = link.recv().await?;
            let envelope = Envelope::decode(&payload)?;
            if envelope.integer(K_T) == Some(T_ERROR) {
                bail!(
                    "command {command:?} failed: {}",
                    envelope.text(K_BODY).unwrap_or("unknown error")
                );
            }
            if envelope.integer(K_T) == Some(T_NOTICE)
                && envelope
                    .text(K_BODY)
                    .is_some_and(|body| body.contains(expected))
            {
                return Ok::<_, anyhow::Error>(());
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(20), receive)
        .await
        .with_context(|| format!("timed out waiting for response to {command:?}"))?
}

async fn connect_and_join(
    runtime: &rns_runtime::reticulum::ReticulumHandle,
    destination: [u8; 16],
    identity: Identity,
    nickname: &str,
) -> Result<(LinkSession, Envelope)> {
    let mut link = LinkSession::open(
        runtime,
        identity.clone(),
        destination,
        1,
        Duration::from_secs(20),
    )
    .await?;
    link.identify().await?;
    let mut hello = Envelope::new(T_HELLO, &identity.hash);
    hello.set(K_NICK, Value::Text(nickname.into()));
    link.send(&hello.encode()?).await?;
    let welcome = receive_type(&mut link, T_WELCOME).await?;
    let mut join = Envelope::new(T_JOIN, &identity.hash);
    join.set(K_ROOM, Value::Text("e2e".into()));
    link.send(&join.encode()?).await?;
    receive_type(&mut link, T_JOINED).await?;
    Ok((link, welcome))
}

async fn receive_type(link: &mut LinkSession, expected: u64) -> Result<Envelope> {
    let receive = async {
        loop {
            let payload = link.recv().await?;
            let envelope = Envelope::decode(&payload)?;
            if envelope.integer(K_T) == Some(expected) {
                return Ok::<_, anyhow::Error>(envelope);
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(20), receive)
        .await
        .context("timed out waiting for hub response")?
}
