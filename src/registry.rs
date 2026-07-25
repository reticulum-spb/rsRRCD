use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use rns_runtime::config::Config;

use crate::state::{IdentityHash, Room};

pub struct RoomRegistry;

impl RoomRegistry {
    pub fn load(path: &Path) -> Result<HashMap<String, Room>> {
        if !path.exists() {
            Self::save(path, &HashMap::new())?;
        }
        let config = Config::from_file(path)
            .with_context(|| format!("failed to load room registry {}", path.display()))?;
        let mut rooms = HashMap::new();
        for (name, section) in config.subsections("rooms") {
            let founder = section.get("founder").and_then(parse_hash);
            let hashes = |key: &str| -> HashSet<IdentityHash> {
                section
                    .get_list(key)
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|value| parse_hash(value))
                    .collect()
            };
            rooms.insert(
                name.to_string(),
                Room {
                    founder,
                    operators: hashes("operators"),
                    voiced: hashes("voiced"),
                    banned: hashes("bans"),
                    invited: section
                        .get_list("invited")
                        .unwrap_or_default()
                        .iter()
                        .filter_map(|entry| {
                            let (hash, expires) = entry.split_once(':')?;
                            Some((parse_hash(hash)?, expires.parse().ok()?))
                        })
                        .filter(|(_, expires)| *expires > now())
                        .collect(),
                    topic: section
                        .get("topic")
                        .filter(|v| !v.is_empty())
                        .map(str::to_string),
                    key: section
                        .get("key")
                        .filter(|v| !v.is_empty())
                        .map(str::to_string),
                    registered: true,
                    moderated: section.get_bool_or("moderated", false),
                    invite_only: section.get_bool_or("invite_only", false),
                    private: section.get_bool_or("private", false),
                    topic_ops_only: section.get_bool_or("topic_ops_only", false),
                    no_outside_messages: section.get_bool_or("no_outside_msgs", false),
                    last_used_ts: section.get_float("last_used_ts").unwrap_or_else(now),
                    ..Room::default()
                },
            );
        }
        Ok(rooms)
    }

    pub fn save(path: &Path, rooms: &HashMap<String, Room>) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut config = Config::new();
        let root = config.ensure_section("rooms");
        let mut names: Vec<_> = rooms
            .iter()
            .filter(|(_, room)| room.registered)
            .map(|(name, _)| name.clone())
            .collect();
        names.sort();
        for name in names {
            let room = &rooms[&name];
            let section = root.add_subsection(name);
            if let Some(founder) = room.founder {
                section.set("founder", &hex::encode(founder));
            }
            if let Some(topic) = room.topic.as_ref() {
                section.set("topic", topic);
            }
            if let Some(key) = room.key.as_ref() {
                section.set("key", key);
            }
            section.set("moderated", yes_no(room.moderated));
            section.set("invite_only", yes_no(room.invite_only));
            section.set("private", yes_no(room.private));
            section.set("topic_ops_only", yes_no(room.topic_ops_only));
            section.set("no_outside_msgs", yes_no(room.no_outside_messages));
            section.set_list("operators", sorted_hashes(&room.operators));
            section.set_list("voiced", sorted_hashes(&room.voiced));
            section.set_list("bans", sorted_hashes(&room.banned));
            let mut invited: Vec<_> = room
                .invited
                .iter()
                .filter(|(_, expires)| **expires > now())
                .map(|(hash, expires)| format!("{}:{expires}", hex::encode(hash)))
                .collect();
            invited.sort();
            section.set_list("invited", invited);
            section.set("last_used_ts", &room.last_used_ts.to_string());
        }
        config
            .save_to(path)
            .with_context(|| format!("failed to save room registry {}", path.display()))
    }
}

fn parse_hash(value: &str) -> Option<IdentityHash> {
    let value = value.trim().strip_prefix("0x").unwrap_or(value.trim());
    let bytes = hex::decode(value).ok()?;
    <IdentityHash>::try_from(bytes.as_slice()).ok()
}

fn sorted_hashes(values: &HashSet<IdentityHash>) -> Vec<String> {
    let mut values: Vec<_> = values.iter().map(hex::encode).collect();
    values.sort();
    values
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

pub fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_roundtrip_uses_rsreticulum_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rooms");
        let mut rooms = HashMap::new();
        let mut room = Room {
            founder: Some([1; 16]),
            registered: true,
            topic: Some("Welcome, everyone".into()),
            moderated: true,
            private: true,
            last_used_ts: 42.0,
            ..Room::default()
        };
        room.operators.insert([2; 16]);
        room.invited.insert([3; 16], now() + 300.0);
        room.invited.insert([4; 16], now() - 1.0);
        rooms.insert("lobby".into(), room);
        RoomRegistry::save(&path, &rooms).unwrap();
        let loaded = RoomRegistry::load(&path).unwrap();
        assert_eq!(loaded["lobby"].founder, Some([1; 16]));
        assert_eq!(loaded["lobby"].topic.as_deref(), Some("Welcome, everyone"));
        assert!(loaded["lobby"].moderated);
        assert!(loaded["lobby"].operators.contains(&[2; 16]));
        assert!(loaded["lobby"].invited.contains_key(&[3; 16]));
        assert!(!loaded["lobby"].invited.contains_key(&[4; 16]));
    }
}
