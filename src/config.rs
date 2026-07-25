use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rns_runtime::config::Config;

#[derive(Clone, Debug)]
#[allow(dead_code)] // Parsed now; consumed by the persistence/liveness stages.
pub struct HubConfig {
    pub config_path: PathBuf,
    pub identity_path: PathBuf,
    pub room_registry_path: PathBuf,
    pub rns_config_dir: Option<PathBuf>,
    pub hub_name: String,
    pub greeting: Option<String>,
    pub announce_on_start: bool,
    pub announce_period_s: f64,
    pub include_joined_member_list: bool,
    pub room_invite_timeout_s: f64,
    pub room_registry_prune_after_s: f64,
    pub room_registry_prune_interval_s: f64,
    pub max_nick_bytes: usize,
    pub max_rooms_per_session: usize,
    pub max_room_name_bytes: usize,
    pub max_msg_body_bytes: usize,
    pub rate_limit_msgs_per_minute: u32,
    pub ping_interval_s: f64,
    pub ping_timeout_s: f64,
    pub enable_resource_transfer: bool,
    pub max_resource_bytes: usize,
    pub max_pending_resource_expectations: usize,
    pub resource_expectation_ttl_s: f64,
    pub trusted_identities: Vec<[u8; 16]>,
    pub banned_identities: Vec<[u8; 16]>,
    pub log_level: String,
    pub log_console: bool,
    pub log_file: Option<PathBuf>,
}

impl HubConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let parsed = Config::from_file(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let section = parsed
            .section("hub")
            .context("configuration has no [hub] section")?;
        let logging = parsed.section("logging");
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        let relative_path = |name: &str, default: &str| {
            let value = PathBuf::from(section.get(name).unwrap_or(default));
            if value.is_absolute() {
                value
            } else {
                base.join(value)
            }
        };
        let hashes = |name: &str| -> Result<Vec<[u8; 16]>> {
            section
                .get_list(name)
                .unwrap_or_default()
                .iter()
                .map(|value| {
                    parse_hash(value).with_context(|| format!("invalid {name} entry {value:?}"))
                })
                .collect()
        };
        Ok(Self {
            config_path: path.to_path_buf(),
            identity_path: relative_path("identity_path", "hub_identity"),
            room_registry_path: relative_path("room_registry_path", "rooms"),
            rns_config_dir: section
                .get("configdir")
                .filter(|v| !v.trim().is_empty())
                .map(PathBuf::from),
            hub_name: section.get("hub_name").unwrap_or("rrc").to_string(),
            greeting: section
                .get("greeting")
                .filter(|v| !v.is_empty())
                .map(str::to_string),
            announce_on_start: section.get_bool_or("announce_on_start", true),
            announce_period_s: positive_float(section.get_float("announce_period_s"), 0.0),
            include_joined_member_list: section.get_bool_or("include_joined_member_list", false),
            room_invite_timeout_s: positive_float(
                section.get_float("room_invite_timeout_s"),
                900.0,
            ),
            room_registry_prune_after_s: positive_float(
                section.get_float("room_registry_prune_after_s"),
                30.0 * 24.0 * 3600.0,
            ),
            room_registry_prune_interval_s: positive_float(
                section.get_float("room_registry_prune_interval_s"),
                3600.0,
            ),
            max_nick_bytes: positive_usize(section.get_uint("max_nick_bytes"), 32),
            max_rooms_per_session: positive_usize(section.get_uint("max_rooms_per_session"), 32),
            max_room_name_bytes: positive_usize(section.get_uint("max_room_name_bytes"), 64),
            max_msg_body_bytes: positive_usize(section.get_uint("max_msg_body_bytes"), 350),
            rate_limit_msgs_per_minute: section
                .get_uint("rate_limit_msgs_per_minute")
                .and_then(|v| u32::try_from(v).ok())
                .filter(|v| *v > 0)
                .unwrap_or(240),
            ping_interval_s: positive_float(section.get_float("ping_interval_s"), 0.0),
            ping_timeout_s: positive_float(section.get_float("ping_timeout_s"), 0.0),
            enable_resource_transfer: section.get_bool_or("enable_resource_transfer", true),
            max_resource_bytes: positive_usize(section.get_uint("max_resource_bytes"), 262_144),
            max_pending_resource_expectations: positive_usize(
                section.get_uint("max_pending_resource_expectations"),
                8,
            ),
            resource_expectation_ttl_s: positive_float(
                section.get_float("resource_expectation_ttl_s"),
                30.0,
            ),
            trusted_identities: hashes("trusted_identities")?,
            banned_identities: hashes("banned_identities")?,
            log_level: logging
                .and_then(|section| section.get("level"))
                .unwrap_or("INFO")
                .to_string(),
            log_console: logging
                .map(|section| section.get_bool_or("console", true))
                .unwrap_or(true),
            log_file: logging
                .and_then(|section| section.get("file"))
                .filter(|value| !value.trim().is_empty())
                .map(|value| {
                    let path = PathBuf::from(value);
                    if path.is_absolute() {
                        path
                    } else {
                        base.join(path)
                    }
                }),
        })
    }

    pub fn save_banned_identities(&self) -> Result<()> {
        let mut parsed = Config::from_file(&self.config_path)
            .with_context(|| format!("failed to read {}", self.config_path.display()))?;
        parsed.ensure_section("hub").set_list(
            "banned_identities",
            self.banned_identities.iter().map(hex::encode).collect(),
        );
        parsed
            .save_to(&self.config_path)
            .with_context(|| format!("failed to save {}", self.config_path.display()))
    }

    pub fn write_default(path: &Path) -> Result<()> {
        let mut config = Config::new();
        let hub = config.ensure_section("hub");
        hub.set("configdir", "");
        hub.set("identity_path", "hub_identity");
        hub.set("room_registry_path", "rooms");
        hub.set("announce_on_start", "yes");
        hub.set("announce_period_s", "0");
        hub.set("hub_name", "rrc");
        hub.set("greeting", "");
        hub.set_list("trusted_identities", vec![]);
        hub.set_list("banned_identities", vec![]);
        hub.set("include_joined_member_list", "no");
        hub.set("room_invite_timeout_s", "900");
        hub.set("room_registry_prune_after_s", "2592000");
        hub.set("room_registry_prune_interval_s", "3600");
        hub.set("max_nick_bytes", "32");
        hub.set("max_room_name_bytes", "64");
        hub.set("max_msg_body_bytes", "350");
        hub.set("max_rooms_per_session", "32");
        hub.set("rate_limit_msgs_per_minute", "240");
        hub.set("ping_interval_s", "0");
        hub.set("ping_timeout_s", "0");
        hub.set("enable_resource_transfer", "yes");
        hub.set("max_resource_bytes", "262144");
        hub.set("max_pending_resource_expectations", "8");
        hub.set("resource_expectation_ttl_s", "30");
        let logging = config.ensure_section("logging");
        logging.set("level", "INFO");
        logging.set("console", "yes");
        logging.set("file", "");
        config
            .save_to(path)
            .context("failed to write default config")
    }
}

fn positive_usize(value: Option<u64>, default: usize) -> usize {
    value
        .and_then(|v| usize::try_from(v).ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}
fn positive_float(value: Option<f64>, default: f64) -> f64 {
    value
        .filter(|v| v.is_finite() && *v >= 0.0)
        .unwrap_or(default)
}
fn parse_hash(value: &str) -> Result<[u8; 16]> {
    let value = value.trim().strip_prefix("0x").unwrap_or(value.trim());
    let bytes = hex::decode(value).context("identity hash is not hexadecimal")?;
    <[u8; 16]>::try_from(bytes.as_slice()).context("identity hash must be exactly 16 bytes")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_uses_rsreticulum_config_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config");
        HubConfig::write_default(&path).unwrap();
        let config = HubConfig::load(&path).unwrap();
        assert_eq!(config.config_path, path);
        assert_eq!(config.hub_name, "rrc");
        assert_eq!(config.max_msg_body_bytes, 350);
    }

    #[test]
    fn invalid_identity_list_entry_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config");
        HubConfig::write_default(&path).unwrap();
        let mut parsed = Config::from_file(&path).unwrap();
        parsed
            .ensure_section("hub")
            .set_list("trusted_identities", vec!["not-a-hash".into()]);
        parsed.save_to(&path).unwrap();
        let error = HubConfig::load(&path).unwrap_err().to_string();
        assert!(error.contains("invalid trusted_identities entry"));
    }
}
