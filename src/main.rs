mod config;
mod constants;
mod protocol;
mod registry;
mod router;
mod state;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use rns_identity::identity::Identity;
use rns_runtime::lifecycle::{ShutdownSignal, install_signal_handlers};
use rns_runtime::link_session::{LinkListener, LinkListenerEvent};
use router::{Action, Router};
use serde_cbor::Value;
use tracing_subscriber::fmt::writer::{BoxMakeWriter, MakeWriterExt};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::config::HubConfig;
use crate::constants::DEST_NAME;
use crate::registry::RoomRegistry;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// rsRRCD ConfigObj/INI configuration file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Reticulum configuration directory override.
    #[arg(long)]
    rns_config: Option<PathBuf>,
    /// Hub identity file override.
    #[arg(long)]
    identity: Option<PathBuf>,
    /// Registered-room store override.
    #[arg(long)]
    room_registry: Option<PathBuf>,
    /// Disable the startup announce.
    #[arg(long)]
    no_announce: bool,
    /// Periodic announce interval in seconds (zero disables).
    #[arg(long)]
    announce_period: Option<f64>,
    /// Hub name advertised in WELCOME.
    #[arg(long)]
    hub_name: Option<String>,
    /// Greeting delivered after WELCOME.
    #[arg(long)]
    greeting: Option<String>,
    /// Include best-effort identity lists in JOINED/PARTED.
    #[arg(long)]
    include_joined_member_list: bool,
    /// Maximum rooms per connected session.
    #[arg(long)]
    max_rooms: Option<usize>,
    /// Maximum nickname length in UTF-8 bytes.
    #[arg(long)]
    max_nick_bytes: Option<usize>,
    /// Maximum room-name length in UTF-8 bytes.
    #[arg(long)]
    max_room_name_bytes: Option<usize>,
    /// Maximum text-message body length in UTF-8 bytes.
    #[arg(long)]
    max_msg_body_bytes: Option<usize>,
    /// Per-link message rate.
    #[arg(long)]
    rate_limit_msgs_per_minute: Option<u32>,
    /// Hub PING interval in seconds.
    #[arg(long)]
    ping_interval: Option<f64>,
    /// PONG timeout in seconds.
    #[arg(long)]
    ping_timeout: Option<f64>,
    /// Logging level override (trace, debug, info, warn, error).
    #[arg(long)]
    log_level: Option<String>,
    /// Log file override (an empty value disables file logging).
    #[arg(long)]
    log_file: Option<PathBuf>,
    /// Print the hub destination hash and exit.
    #[arg(long)]
    print_destination: bool,
    /// Increase logging verbosity.
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

fn default_home() -> PathBuf {
    std::env::var_os("RSRRCD_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|p| PathBuf::from(p).join(".rsRRC")))
        .unwrap_or_else(|| PathBuf::from(".rsRRC"))
}

fn default_rns_config_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".rsReticulum")
}

fn load_or_create_identity(path: &Path) -> Result<Identity> {
    if path.exists() {
        return Identity::from_file(path)
            .with_context(|| format!("failed to load identity {}", path.display()));
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let identity = Identity::new();
    identity
        .to_file(path)
        .with_context(|| format!("failed to save identity {}", path.display()))?;
    Ok(identity)
}

async fn apply(listener: &LinkListener, actions: Vec<Action>) {
    for action in actions {
        match action {
            Action::Send(link, payload) => {
                if let Err(error) = listener.send(link, payload).await {
                    tracing::debug!(link = %hex::encode(link), %error, "link send failed");
                }
            }
            Action::SendResource(link, payload) => match listener.send(link, payload).await {
                Ok(rns_runtime::link_manager::LinkPayloadSendReceipt::Resource(_)) => {}
                Ok(_) => tracing::warn!(
                    link = %hex::encode(link),
                    "resource payload unexpectedly fit in a link packet"
                ),
                Err(error) => tracing::warn!(
                    link = %hex::encode(link),
                    %error,
                    "resource send failed"
                ),
            },
            Action::Close(link) => {
                if let Err(error) = listener.close(link).await {
                    tracing::debug!(link = %hex::encode(link), %error, "link close failed");
                }
            }
        }
    }
}

fn periodic_announce_timer(period_s: f64) -> Option<tokio::time::Interval> {
    if period_s <= 0.0 {
        return None;
    }
    let period = Duration::from_secs_f64(period_s);
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    Some(interval)
}

async fn announce_hub(listener: &LinkListener, config: &HubConfig) -> Result<()> {
    let app_data = serde_cbor::to_vec(&Value::Map(BTreeMap::from([
        (Value::Text("proto".into()), Value::Text("rrc".into())),
        (Value::Text("v".into()), Value::Integer(1)),
        (
            Value::Text("hub".into()),
            Value::Text(config.hub_name.clone()),
        ),
    ])))?;
    listener
        .announce_with_app_data(Some(&app_data))
        .await
        .context("hub announce failed")
}

fn init_logging(
    config: &HubConfig,
    verbose: u8,
) -> Result<
    tracing_subscriber::reload::Handle<tracing_subscriber::EnvFilter, tracing_subscriber::Registry>,
> {
    let fallback_level = match verbose {
        0 => config.log_level.to_lowercase(),
        1 => "debug".into(),
        _ => "trace".into(),
    };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new(&fallback_level))
        .with_context(|| format!("invalid log level {:?}", config.log_level))?;
    let writer = match (config.log_console, config.log_file.as_ref()) {
        (true, Some(path)) => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .with_context(|| format!("failed to open log file {}", path.display()))?;
            BoxMakeWriter::new(std::io::stderr.and(file))
        }
        (true, None) => BoxMakeWriter::new(std::io::stderr),
        (false, Some(path)) => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .with_context(|| format!("failed to open log file {}", path.display()))?;
            BoxMakeWriter::new(file)
        }
        (false, None) => BoxMakeWriter::new(std::io::sink),
    };
    let (filter, handle) = tracing_subscriber::reload::Layer::new(filter);
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_writer(writer))
        .try_init()
        .map_err(|error| anyhow::anyhow!("failed to initialise logging: {error}"))?;
    Ok(handle)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let home = default_home();
    let config_path = args.config.unwrap_or_else(|| home.join("config"));
    if !config_path.exists() {
        if let Some(parent) = config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        HubConfig::write_default(&config_path)?;
        let bootstrap = HubConfig::load(&config_path)?;
        load_or_create_identity(&bootstrap.identity_path)?;
        RoomRegistry::save(
            &bootstrap.room_registry_path,
            &std::collections::HashMap::new(),
        )?;
        println!(
            "Created default rrcd-rs files. Edit the configuration and start again:\n\
             - Config:   {}\n\
             - Identity: {}\n\
             - Rooms:    {}",
            config_path.display(),
            bootstrap.identity_path.display(),
            bootstrap.room_registry_path.display(),
        );
        return Ok(());
    }
    let mut config = HubConfig::load(&config_path)?;
    if let Some(path) = args.rns_config {
        config.rns_config_dir = Some(path);
    }
    if config.rns_config_dir.is_none() {
        config.rns_config_dir = Some(default_rns_config_dir());
    }
    if let Some(path) = args.identity {
        config.identity_path = path;
    }
    if let Some(path) = args.room_registry {
        config.room_registry_path = path;
    }
    if args.no_announce {
        config.announce_on_start = false;
    }
    if let Some(value) = args.announce_period {
        anyhow::ensure!(value.is_finite() && value >= 0.0, "invalid announce period");
        config.announce_period_s = value;
    }
    if let Some(value) = args.hub_name {
        config.hub_name = value;
    }
    if let Some(value) = args.greeting {
        config.greeting = (!value.is_empty()).then_some(value);
    }
    if args.include_joined_member_list {
        config.include_joined_member_list = true;
    }
    if let Some(value) = args.max_rooms {
        anyhow::ensure!(value > 0, "max rooms must be positive");
        config.max_rooms_per_session = value;
    }
    if let Some(value) = args.max_nick_bytes {
        anyhow::ensure!(value > 0, "max nick bytes must be positive");
        config.max_nick_bytes = value;
    }
    if let Some(value) = args.max_room_name_bytes {
        anyhow::ensure!(value > 0, "max room name bytes must be positive");
        config.max_room_name_bytes = value;
    }
    if let Some(value) = args.max_msg_body_bytes {
        anyhow::ensure!(value > 0, "max message body bytes must be positive");
        config.max_msg_body_bytes = value;
    }
    if let Some(value) = args.rate_limit_msgs_per_minute {
        anyhow::ensure!(value > 0, "rate limit must be positive");
        config.rate_limit_msgs_per_minute = value;
    }
    if let Some(value) = args.ping_interval {
        anyhow::ensure!(value.is_finite() && value >= 0.0, "invalid ping interval");
        config.ping_interval_s = value;
    }
    if let Some(value) = args.ping_timeout {
        anyhow::ensure!(value.is_finite() && value >= 0.0, "invalid ping timeout");
        config.ping_timeout_s = value;
    }
    if let Some(value) = args.log_level {
        config.log_level = value;
    }
    if let Some(value) = args.log_file {
        config.log_file = (!value.as_os_str().is_empty()).then_some(value);
    }
    let log_filter = init_logging(&config, args.verbose)?;
    let identity = load_or_create_identity(&config.identity_path)?;

    let shutdown = ShutdownSignal::new();
    let _signal_rx = install_signal_handlers(shutdown.clone());
    let rns_path = config
        .rns_config_dir
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned());
    let runtime = rns_runtime::reticulum::init(
        rns_path.as_deref(),
        None,
        shutdown.clone(),
        Arc::new(AtomicBool::new(true)),
    )
    .await
    .context("Reticulum initialisation failed")?;

    let mut listener = LinkListener::listen(&runtime, &identity, DEST_NAME).await?;
    println!(
        "rrcd-rs destination <{}>",
        hex::encode(listener.destination_hash())
    );
    if args.print_destination {
        shutdown.trigger();
        return Ok(());
    }
    let mut router = Router::new(config, identity.hash);
    router.state.rooms = RoomRegistry::load(&router.config.room_registry_path)?;
    if router.config.announce_on_start {
        announce_hub(&listener, &router.config).await?;
        router.state.counters.announces += 1;
    }

    let mut configured_announce_period = router.config.announce_period_s;
    let mut configured_log_level = router.config.log_level.clone();
    let mut announce_period = periodic_announce_timer(configured_announce_period);
    let mut maintenance = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = shutdown.wait() => break,
            _ = maintenance.tick() => {
                let actions = router.liveness_tick();
                apply(&listener, actions).await;
            }
            _ = async {
                match announce_period.as_mut() {
                    Some(interval) => interval.tick().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Err(error) = announce_hub(&listener, &router.config).await {
                    tracing::warn!(%error, "periodic announce failed");
                } else {
                    router.state.counters.announces += 1;
                }
            }
            event = listener.next() => {
                let Some(event) = event else { break };
                let actions = match event {
                    LinkListenerEvent::Established { link_id } => {
                        tracing::info!(link = %hex::encode(link_id), "link established");
                        router.established(link_id);
                        vec![]
                    }
                    LinkListenerEvent::Identified { link_id, identity_hash } => {
                        tracing::info!(link = %hex::encode(link_id), peer = %hex::encode(identity_hash), "link identified");
                        router.identified(link_id, identity_hash)
                    }
                    LinkListenerEvent::Packet { link_id, data } => router.packet(link_id, &data),
                    LinkListenerEvent::Resource(resource) => {
                        tracing::debug!(
                            link = %hex::encode(resource.link_id),
                            hash = %hex::encode(resource.resource_hash),
                            bytes = resource.data.len(),
                            "resource received"
                        );
                        router.resource_received(resource.link_id, resource.data)
                    }
                    LinkListenerEvent::Closed { link_id } => {
                        tracing::info!(link = %hex::encode(link_id), "link closed");
                        router.closed(link_id)
                    }
                    LinkListenerEvent::Channel(_) => vec![],
                };
                apply(&listener, actions).await;
            }
        }
        if router.config.announce_period_s != configured_announce_period {
            configured_announce_period = router.config.announce_period_s;
            announce_period = periodic_announce_timer(configured_announce_period);
            tracing::info!(
                period_s = configured_announce_period,
                "periodic announce schedule reconfigured"
            );
        }
        if router.config.log_level != configured_log_level {
            configured_log_level = router.config.log_level.clone();
            match tracing_subscriber::EnvFilter::try_new(configured_log_level.to_lowercase()) {
                Ok(filter) => {
                    if let Err(error) = log_filter.reload(filter) {
                        tracing::warn!(%error, "failed to apply reloaded log level");
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, level = configured_log_level, "invalid reloaded log level");
                }
            }
        }
    }
    let active_links = router.state.sessions.keys().copied().collect::<Vec<_>>();
    for link in active_links {
        let actions = router.closed(link);
        apply(&listener, actions).await;
        if let Err(error) = listener.close(link).await {
            tracing::debug!(link = %hex::encode(link), %error, "link close during shutdown failed");
        }
    }
    shutdown.trigger();
    Ok(())
}
