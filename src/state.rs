use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::config::HubConfig;

pub type LinkId = [u8; 16];
pub type IdentityHash = [u8; 16];
pub type PartNotification = (String, Vec<LinkId>, Option<IdentityHash>, Option<String>);

#[derive(Debug)]
pub struct RateState {
    tokens: f64,
    updated: Instant,
}

#[derive(Debug)]
pub struct Session {
    pub welcomed: bool,
    pub peer: Option<IdentityHash>,
    pub nick: Option<String>,
    pub rooms: HashSet<String>,
    pub peer_caps: HashMap<i128, bool>,
    pub awaiting_pong: Option<Instant>,
    pub last_ping: Instant,
    rate: RateState,
}

impl Session {
    fn new(rate: u32) -> Self {
        Self {
            welcomed: false,
            peer: None,
            nick: None,
            rooms: HashSet::new(),
            peer_caps: HashMap::new(),
            awaiting_pong: None,
            last_ping: Instant::now(),
            rate: RateState {
                tokens: rate as f64,
                updated: Instant::now(),
            },
        }
    }

    pub fn take_rate_token(&mut self, per_minute: u32) -> bool {
        let now = Instant::now();
        let capacity = per_minute.max(1) as f64;
        self.rate.tokens = (self.rate.tokens
            + now.duration_since(self.rate.updated).as_secs_f64() * capacity / 60.0)
            .min(capacity);
        self.rate.updated = now;
        if self.rate.tokens < 1.0 {
            false
        } else {
            self.rate.tokens -= 1.0;
            true
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Room {
    pub members: HashSet<LinkId>,
    pub founder: Option<IdentityHash>,
    pub operators: HashSet<IdentityHash>,
    pub voiced: HashSet<IdentityHash>,
    pub banned: HashSet<IdentityHash>,
    pub invited: HashMap<IdentityHash, f64>,
    pub topic: Option<String>,
    pub key: Option<String>,
    pub registered: bool,
    pub moderated: bool,
    pub invite_only: bool,
    pub private: bool,
    pub topic_ops_only: bool,
    pub no_outside_messages: bool,
    pub last_used_ts: f64,
}

impl Room {
    pub fn is_operator(&self, peer: &IdentityHash) -> bool {
        self.founder.as_ref() == Some(peer) || self.operators.contains(peer)
    }
    pub fn may_speak(&self, peer: &IdentityHash) -> bool {
        !self.moderated || self.is_operator(peer) || self.voiced.contains(peer)
    }
    pub fn is_invited(&self, peer: &IdentityHash, now: f64) -> bool {
        self.invited.get(peer).is_some_and(|expires| *expires > now)
    }
}

#[derive(Debug, Default)]
pub struct Counters {
    pub packets_in: u64,
    pub packets_bad: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub joins: u64,
    pub parts: u64,
    pub forwarded: u64,
    pub messages_forwarded: u64,
    pub notices_forwarded: u64,
    pub actions_forwarded: u64,
    pub errors: u64,
    pub rate_limited: u64,
    pub pings_in: u64,
    pub pings_out: u64,
    pub pongs_in: u64,
    pub pongs_out: u64,
    pub announces: u64,
    pub resources_received: u64,
    pub resources_sent: u64,
    pub resources_rejected: u64,
    pub resource_bytes_received: u64,
    pub resource_bytes_sent: u64,
}

pub struct HubState {
    pub sessions: HashMap<LinkId, Session>,
    pub rooms: HashMap<String, Room>,
    pub links_by_identity: HashMap<IdentityHash, LinkId>,
    pub links_by_nick: HashMap<String, HashSet<LinkId>>,
    pub counters: Counters,
    pub started: Instant,
}

impl Default for HubState {
    fn default() -> Self {
        Self {
            sessions: HashMap::new(),
            rooms: HashMap::new(),
            links_by_identity: HashMap::new(),
            links_by_nick: HashMap::new(),
            counters: Counters::default(),
            started: Instant::now(),
        }
    }
}

impl HubState {
    pub fn establish(&mut self, link: LinkId, config: &HubConfig) {
        self.sessions
            .entry(link)
            .or_insert_with(|| Session::new(config.rate_limit_msgs_per_minute));
    }

    pub fn identify(&mut self, link: LinkId, peer: IdentityHash) {
        if let Some(old_peer) = self.sessions.get(&link).and_then(|session| session.peer)
            && old_peer != peer
        {
            self.remove_identity_link(old_peer, link);
        }
        if let Some(session) = self.sessions.get_mut(&link) {
            session.peer = Some(peer);
            self.links_by_identity.insert(peer, link);
        }
    }

    fn remove_identity_link(&mut self, peer: IdentityHash, link: LinkId) {
        if self.links_by_identity.get(&peer) != Some(&link) {
            return;
        }
        let replacement = self.sessions.iter().find_map(|(candidate, session)| {
            (*candidate != link && session.peer == Some(peer)).then_some(*candidate)
        });
        match replacement {
            Some(replacement) => {
                self.links_by_identity.insert(peer, replacement);
            }
            None => {
                self.links_by_identity.remove(&peer);
            }
        }
    }

    pub fn set_nick(&mut self, link: LinkId, nick: Option<String>) {
        let old = self.sessions.get(&link).and_then(|s| s.nick.clone());
        if let Some(old) = old {
            let key = old.to_lowercase();
            if let Some(links) = self.links_by_nick.get_mut(&key) {
                links.remove(&link);
                if links.is_empty() {
                    self.links_by_nick.remove(&key);
                }
            }
        }
        if let Some(nick) = nick.as_ref() {
            self.links_by_nick
                .entry(nick.to_lowercase())
                .or_default()
                .insert(link);
        }
        if let Some(session) = self.sessions.get_mut(&link) {
            session.nick = nick;
        }
    }

    pub fn close(&mut self, link: LinkId) -> Vec<PartNotification> {
        let Some(session) = self.sessions.remove(&link) else {
            return vec![];
        };
        if let Some(peer) = session.peer {
            self.remove_identity_link(peer, link);
        }
        if let Some(nick) = session.nick.as_ref() {
            let key = nick.to_lowercase();
            if let Some(links) = self.links_by_nick.get_mut(&key) {
                links.remove(&link);
                if links.is_empty() {
                    self.links_by_nick.remove(&key);
                }
            }
        }
        let mut notifications = Vec::new();
        for room_name in session.rooms {
            if let Some(room) = self.rooms.get_mut(&room_name) {
                room.members.remove(&link);
                let recipients = room.members.iter().copied().collect::<Vec<_>>();
                let peer_still_in_room = session.peer.is_some_and(|peer| {
                    recipients.iter().any(|member| {
                        self.sessions
                            .get(member)
                            .is_some_and(|other| other.peer == Some(peer))
                    })
                });
                if !recipients.is_empty() && !peer_still_in_room {
                    notifications.push((
                        room_name.clone(),
                        recipients,
                        session.peer,
                        session.nick.clone(),
                    ));
                }
            }
            self.remove_ephemeral_room_if_empty(&room_name);
        }
        notifications
    }

    pub fn normalize_room(&self, value: &str, config: &HubConfig) -> anyhow::Result<String> {
        let room = value.trim();
        anyhow::ensure!(!room.is_empty(), "room name is empty");
        anyhow::ensure!(
            room.len() <= config.max_room_name_bytes,
            "room name too long"
        );
        anyhow::ensure!(!room.contains(['\n', '\r', '\0']), "invalid room name");
        Ok(room.to_lowercase())
    }

    pub fn remove_ephemeral_room_if_empty(&mut self, room_name: &str) {
        if self
            .rooms
            .get(room_name)
            .is_some_and(|r| r.members.is_empty() && !r.registered)
        {
            self.rooms.remove(room_name);
        }
    }
}
