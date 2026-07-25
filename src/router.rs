use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use serde_cbor::Value;
use sha2::{Digest, Sha256};

use crate::config::HubConfig;
use crate::registry::{RoomRegistry, now};
use crate::state::{HubState, IdentityHash, LinkId};
use rs_rrc::*;

const COMMAND_HELP: &str = "\
RRC commands:
 /help
 /list
 /who [room] (/names)
 /topic <room> [topic]
 /register <room> | /unregister <room> (founder)
Room operator:
 /mode <room> <+m|-m|+i|-i|+t|-t|+n|-n|+p|-p>
 /mode <room> +k <key> | -k
 /op|/deop|/voice|/devoice <room> <nick|hash>
 /invite|/ban <room> add|del|list [nick|hash]
 /unban <room> <nick|hash>
 /kick <room> <nick|hash>
Server operator:
 /stats
 /reload
 /kline add <nick|hash> | del <nick|hash> | list";

#[derive(Debug)]
pub enum Action {
    Send(LinkId, Vec<u8>),
    SendResource(LinkId, Vec<u8>),
    Close(LinkId),
}

pub struct Router {
    pub config: HubConfig,
    pub hub_identity: IdentityHash,
    pub state: HubState,
    pending_resources: HashMap<LinkId, Vec<ResourceExpectation>>,
    last_registry_prune: Instant,
}

#[derive(Debug)]
struct ResourceExpectation {
    id: Vec<u8>,
    kind: String,
    size: usize,
    sha256: Option<Vec<u8>>,
    encoding: Option<String>,
    room: Option<String>,
    expires: Instant,
}

impl Router {
    pub fn new(config: HubConfig, hub_identity: IdentityHash) -> Self {
        Self {
            config,
            hub_identity,
            state: HubState::default(),
            pending_resources: HashMap::new(),
            last_registry_prune: Instant::now(),
        }
    }

    pub fn established(&mut self, link: LinkId) {
        self.state.establish(link, &self.config);
    }

    pub fn identified(&mut self, link: LinkId, peer: IdentityHash) -> Vec<Action> {
        if self.config.banned_identities.contains(&peer) {
            return vec![Action::Close(link)];
        }
        self.state.identify(link, peer);
        vec![]
    }

    pub fn closed(&mut self, link: LinkId) -> Vec<Action> {
        self.pending_resources.remove(&link);
        let mut touched_registered = false;
        if let Some(session) = self.state.sessions.get(&link) {
            let touched = now();
            for room_name in &session.rooms {
                if let Some(room) = self.state.rooms.get_mut(room_name) {
                    room.last_used_ts = touched;
                    touched_registered |= room.registered;
                }
            }
        }
        let mut actions = Vec::new();
        for (room, recipients, peer, nick) in self.state.close(link) {
            let Some(peer) = peer else { continue };
            let mut env = Envelope::new(T_PARTED, &self.hub_identity);
            env.set(K_ROOM, Value::Text(room));
            if let Some(nick) = nick {
                env.set(K_NICK, Value::Text(nick));
            }
            if self.config.include_joined_member_list {
                env.set(K_BODY, Value::Array(vec![Value::Bytes(peer.to_vec())]));
            }
            self.broadcast(&mut actions, &recipients, env);
        }
        if touched_registered && let Err(error) = self.persist_rooms() {
            tracing::warn!(%error, "failed to persist room state after link close");
        }
        actions
    }

    pub fn packet(&mut self, link: LinkId, data: &[u8]) -> Vec<Action> {
        self.state.counters.packets_in += 1;
        self.state.counters.bytes_in += data.len() as u64;
        let Some(session) = self.state.sessions.get_mut(&link) else {
            return vec![];
        };
        let Some(peer) = session.peer else {
            return vec![];
        };
        if !session.take_rate_token(self.config.rate_limit_msgs_per_minute) {
            self.state.counters.rate_limited += 1;
            return self.error(link, None, "rate limited");
        }
        let envelope = match Envelope::decode(data) {
            Ok(value) => value,
            Err(error) => {
                self.state.counters.packets_bad += 1;
                return self.error(link, None, &format!("bad message: {error}"));
            }
        };
        match envelope.integer(K_T) {
            Some(T_PONG) => {
                self.state.counters.pongs_in += 1;
                if let Some(session) = self.state.sessions.get_mut(&link) {
                    session.awaiting_pong = None;
                }
                vec![]
            }
            Some(T_RESOURCE_ENVELOPE) => self.resource_envelope(link, &envelope),
            _ if !self.state.sessions[&link].welcomed => self.hello(link, peer, &envelope),
            Some(T_HELLO) => self.hello(link, peer, &envelope),
            Some(T_JOIN) => self.join(link, peer, &envelope),
            Some(T_PART) => self.part(link, peer, &envelope),
            Some(T_MSG | T_NOTICE | T_ACTION) => self.message(link, peer, envelope),
            Some(T_PING) => {
                self.state.counters.pings_in += 1;
                let mut pong = Envelope::new(T_PONG, &self.hub_identity);
                if let Some(body) = envelope.get(K_BODY) {
                    pong.set(K_BODY, body.clone());
                }
                self.state.counters.pongs_out += 1;
                self.send(link, pong)
            }
            _ => self.error(link, None, "unsupported message type"),
        }
    }

    pub fn liveness_tick(&mut self) -> Vec<Action> {
        self.prune_persistent_state();
        if self.config.ping_interval_s <= 0.0 {
            return vec![];
        }
        let now = Instant::now();
        let interval = Duration::from_secs_f64(self.config.ping_interval_s);
        let timeout = (self.config.ping_timeout_s > 0.0)
            .then(|| Duration::from_secs_f64(self.config.ping_timeout_s));
        let mut close = Vec::new();
        let mut ping = Vec::new();
        for (link, session) in &self.state.sessions {
            if session
                .awaiting_pong
                .zip(timeout)
                .is_some_and(|(sent, timeout)| now.duration_since(sent) >= timeout)
            {
                close.push(*link);
            } else if session.awaiting_pong.is_none()
                && now.duration_since(session.last_ping) >= interval
            {
                ping.push(*link);
            }
        }
        let mut actions: Vec<_> = close.into_iter().map(Action::Close).collect();
        for link in ping {
            if let Some(session) = self.state.sessions.get_mut(&link) {
                session.awaiting_pong = Some(now);
                session.last_ping = now;
            }
            let envelope = Envelope::new(T_PING, &self.hub_identity);
            self.state.counters.pings_out += 1;
            actions.extend(self.send(link, envelope));
        }
        actions
    }

    fn prune_persistent_state(&mut self) {
        let wall_now = now();
        let mut changed = false;
        for room in self.state.rooms.values_mut() {
            let before = room.invited.len();
            room.invited.retain(|_, expires| *expires > wall_now);
            changed |= room.invited.len() != before;
        }
        let prune_interval =
            Duration::from_secs_f64(self.config.room_registry_prune_interval_s.max(0.001));
        if self.last_registry_prune.elapsed() >= prune_interval {
            let before = self.state.rooms.len();
            let prune_after = self.config.room_registry_prune_after_s;
            self.state.rooms.retain(|_, room| {
                !room.registered
                    || !room.members.is_empty()
                    || wall_now - room.last_used_ts < prune_after
            });
            let pruned = before - self.state.rooms.len();
            if pruned > 0 {
                tracing::info!(rooms = pruned, "pruned stale registered rooms");
                changed = true;
            }
            self.last_registry_prune = Instant::now();
        }
        if changed && let Err(error) = self.persist_rooms() {
            tracing::warn!(%error, "failed to persist pruned room state");
        }
    }

    fn hello(&mut self, link: LinkId, peer: IdentityHash, envelope: &Envelope) -> Vec<Action> {
        if envelope.integer(K_T) != Some(T_HELLO) {
            return self.error(link, None, "send HELLO first");
        }
        let nick = envelope
            .text(K_NICK)
            .or_else(|| {
                envelope
                    .map(K_BODY)
                    .and_then(|m| match map_get(m, B_HELLO_NICK_LEGACY) {
                        Some(Value::Text(v)) => Some(v.as_str()),
                        _ => None,
                    })
            })
            .and_then(|v| normalize_nick(v, self.config.max_nick_bytes));
        if nick.is_some() {
            self.state.set_nick(link, nick);
        }
        if let Some(session) = self.state.sessions.get_mut(&link) {
            session.welcomed = true;
            if let Some(body) = envelope.map(K_BODY)
                && let Some(Value::Map(caps)) = map_get(body, B_HELLO_CAPS)
            {
                session.peer_caps = caps
                    .iter()
                    .filter_map(|(k, v)| match (k, v) {
                        (Value::Integer(k), Value::Bool(v)) => Some((*k, *v)),
                        _ => None,
                    })
                    .collect();
            }
        }
        let mut limits = Map::new();
        for (key, value) in [
            (0, self.config.max_nick_bytes),
            (1, self.config.max_room_name_bytes),
            (2, self.config.max_msg_body_bytes),
            (3, self.config.max_rooms_per_session),
            (4, self.config.rate_limit_msgs_per_minute as usize),
        ] {
            limits.insert(Value::Integer(key), Value::Integer(value as i128));
        }
        let mut caps = Map::new();
        caps.insert(Value::Integer(CAP_ACTION), Value::Bool(true));
        caps.insert(Value::Integer(CAP_DIRECT_NOTICE), Value::Bool(true));
        caps.insert(Value::Integer(CAP_ROOM_STATE), Value::Bool(true));
        caps.insert(Value::Integer(CAP_USER_LIST), Value::Bool(true));
        if self.config.enable_resource_transfer {
            caps.insert(Value::Integer(CAP_RESOURCE_ENVELOPE), Value::Bool(true));
        }
        let mut body = Map::new();
        body.insert(
            Value::Integer(B_WELCOME_HUB),
            Value::Text(self.config.hub_name.clone()),
        );
        body.insert(
            Value::Integer(B_WELCOME_VER),
            Value::Text(env!("CARGO_PKG_VERSION").into()),
        );
        body.insert(Value::Integer(B_WELCOME_CAPS), Value::Map(caps));
        body.insert(Value::Integer(B_WELCOME_LIMITS), Value::Map(limits));
        let mut welcome = Envelope::new(T_WELCOME, &self.hub_identity);
        welcome.set(K_BODY, Value::Map(body));
        let mut actions = self.send(link, welcome);
        if let Some(greeting) = self.config.greeting.clone() {
            actions.extend(self.smart_notice(link, None, &greeting, "motd"));
        }
        tracing::info!(peer = %hex::encode(peer), "client welcomed");
        actions
    }

    fn join(&mut self, link: LinkId, peer: IdentityHash, envelope: &Envelope) -> Vec<Action> {
        self.state.counters.joins += 1;
        let Some(name) = envelope.text(K_ROOM) else {
            return self.error(link, None, "JOIN requires room name");
        };
        let room_name = match self.state.normalize_room(name, &self.config) {
            Ok(v) => v,
            Err(e) => return self.error(link, None, &e.to_string()),
        };
        if let Some(nick) = envelope
            .text(K_NICK)
            .and_then(|value| normalize_nick(value, self.config.max_nick_bytes))
        {
            self.state.set_nick(link, Some(nick));
        }
        let session = &self.state.sessions[&link];
        if session.rooms.len() >= self.config.max_rooms_per_session
            && !session.rooms.contains(&room_name)
        {
            return self.error(link, Some(&room_name), "too many rooms");
        }
        let room = self.state.rooms.entry(room_name.clone()).or_default();
        if room.banned.contains(&peer) {
            return self.error(link, Some(&room_name), "banned from room");
        }
        let invited = room.is_invited(&peer, now());
        if room.invite_only && !room.is_operator(&peer) && !invited {
            return self.error(link, Some(&room_name), "invite-only (+i)");
        }
        if let Some(key) = room.key.as_ref()
            && !room.is_operator(&peer)
            && !invited
            && envelope.text(K_BODY) != Some(key)
        {
            return self.error(link, Some(&room_name), "bad key (+k)");
        }
        if room.members.is_empty() && room.founder.is_none() {
            room.founder = Some(peer);
        }
        room.last_used_ts = now();
        let existing: Vec<_> = room
            .members
            .iter()
            .copied()
            .filter(|member| *member != link)
            .collect();
        room.members.insert(link);
        room.invited.remove(&peer);
        let persist_join = room.registered;
        self.state
            .sessions
            .get_mut(&link)
            .unwrap()
            .rooms
            .insert(room_name.clone());
        let nick = self.state.sessions[&link].nick.clone();
        let mut actions = Vec::new();
        if !existing.is_empty() {
            for recipient in existing {
                let mut joined = Envelope::new(T_JOINED, &self.hub_identity);
                joined.set(K_ROOM, Value::Text(room_name.clone()));
                if self.peer_supports(recipient, CAP_ROOM_STATE) {
                    joined.set_room_state(&self.room_protocol_state(&room_name));
                }
                if let Some(nick) = nick.as_ref() {
                    joined.set(K_NICK, Value::Text(nick.clone()));
                }
                if self.config.include_joined_member_list {
                    joined.set(K_BODY, Value::Array(vec![Value::Bytes(peer.to_vec())]));
                }
                actions.extend(self.send(recipient, joined));
            }
        }
        let mut joined = Envelope::new(T_JOINED, &self.hub_identity);
        joined.set(K_ROOM, Value::Text(room_name.clone()));
        if self.peer_supports(link, CAP_ROOM_STATE) {
            joined.set_room_state(&self.room_protocol_state(&room_name));
        }
        if self.config.include_joined_member_list {
            let members = self.state.rooms[&room_name]
                .members
                .iter()
                .filter_map(|id| self.state.sessions.get(id)?.peer)
                .map(|id| Value::Bytes(id.to_vec()))
                .collect();
            joined.set(K_BODY, Value::Array(members));
        }
        actions.extend(self.send(link, joined));
        let room = &self.state.rooms[&room_name];
        let topic = room.topic.as_deref().unwrap_or("(none)");
        let registration = if room.registered {
            "registered"
        } else {
            "unregistered"
        };
        let modes = self.room_mode_string(&room_name);
        actions.extend(self.notice(
            link,
            Some(&room_name),
            &format!("room {room_name}: {registration}; mode={modes}; topic={topic}"),
        ));
        if persist_join && let Err(error) = self.persist_rooms() {
            actions.extend(self.error(
                link,
                Some(&room_name),
                &format!("room persist failed: {error}"),
            ));
        }
        actions
    }

    fn part(&mut self, link: LinkId, peer: IdentityHash, envelope: &Envelope) -> Vec<Action> {
        self.state.counters.parts += 1;
        let Some(name) = envelope.text(K_ROOM) else {
            return self.error(link, None, "PART requires room name");
        };
        let room_name = match self.state.normalize_room(name, &self.config) {
            Ok(v) => v,
            Err(e) => return self.error(link, None, &e.to_string()),
        };
        self.state
            .sessions
            .get_mut(&link)
            .unwrap()
            .rooms
            .remove(&room_name);
        let nick = self.state.sessions[&link].nick.clone();
        let recipients = if let Some(room) = self.state.rooms.get_mut(&room_name) {
            room.members.remove(&link);
            room.last_used_ts = now();
            room.members.iter().copied().collect::<Vec<_>>()
        } else {
            vec![]
        };
        let peer_still_in_room = recipients.iter().any(|recipient| {
            self.state
                .sessions
                .get(recipient)
                .is_some_and(|session| session.peer == Some(peer))
        });
        self.state.remove_ephemeral_room_if_empty(&room_name);
        let mut parted = Envelope::new(T_PARTED, &self.hub_identity);
        parted.set(K_ROOM, Value::Text(room_name.clone()));
        if self.config.include_joined_member_list {
            parted.set(K_BODY, Value::Array(vec![Value::Bytes(peer.to_vec())]));
        }
        let mut actions = self.send(link, parted.clone());
        if !peer_still_in_room {
            if let Some(nick) = nick {
                parted.set(K_NICK, Value::Text(nick));
            }
            self.broadcast(&mut actions, &recipients, parted);
        }
        if self
            .state
            .rooms
            .get(&room_name)
            .is_some_and(|room| room.registered)
            && let Err(error) = self.persist_rooms()
        {
            actions.extend(self.error(
                link,
                Some(&room_name),
                &format!("room persist failed: {error}"),
            ));
        }
        actions
    }

    fn message(&mut self, link: LinkId, peer: IdentityHash, mut envelope: Envelope) -> Vec<Action> {
        let message_type = envelope.integer(K_T).unwrap();
        if matches!(message_type, T_MSG | T_NOTICE)
            && envelope
                .text(K_BODY)
                .is_some_and(|text| text.trim_start().starts_with('/'))
        {
            let previous_nick = self.state.sessions[&link].nick.clone();
            if let Some(nick) = envelope
                .text(K_NICK)
                .and_then(|value| normalize_nick(value, self.config.max_nick_bytes))
            {
                self.state.set_nick(link, Some(nick));
            }
            let mut actions = self.command(link, peer, &envelope);
            actions.extend(self.nick_change_notices(link, peer, previous_nick));
            return actions;
        }
        if message_type == T_NOTICE && envelope.get(K_DST).is_some() {
            if envelope.get(K_ROOM).is_some() {
                return self.error(link, None, "direct notice must not include room");
            }
            let Some(dst) = envelope
                .bytes(K_DST)
                .and_then(|v| <[u8; 16]>::try_from(v).ok())
            else {
                return self.error(link, None, "direct notice requires destination identity");
            };
            let Some(target) = self.state.links_by_identity.get(&dst).copied() else {
                return self.error(link, None, "destination not connected");
            };
            self.authorize_envelope(link, peer, &mut envelope, None);
            self.count_forwarded(message_type);
            return self.send(target, envelope);
        }
        let Some(room) = envelope.text(K_ROOM) else {
            return self.error(link, None, "message requires room name");
        };
        let room_name = match self.state.normalize_room(room, &self.config) {
            Ok(v) => v,
            Err(e) => return self.error(link, None, &e.to_string()),
        };
        if envelope
            .text(K_BODY)
            .is_some_and(|v| v.len() > self.config.max_msg_body_bytes)
        {
            return self.error(link, Some(&room_name), "message too large");
        }
        let Some(room) = self.state.rooms.get(&room_name) else {
            return self.error(link, Some(&room_name), "no such room");
        };
        let in_room = self.state.sessions[&link].rooms.contains(&room_name);
        if !in_room && room.no_outside_messages {
            return self.error(link, Some(&room_name), "no outside messages (+n)");
        }
        if room.banned.contains(&peer) {
            return self.error(link, Some(&room_name), "banned from room");
        }
        if !room.may_speak(&peer) {
            return self.error(link, Some(&room_name), "room is moderated (+m)");
        }
        let recipients: Vec<_> = room.members.iter().copied().collect();
        let previous_nick = self.state.sessions[&link].nick.clone();
        self.authorize_envelope(link, peer, &mut envelope, Some(&room_name));
        let mut actions = Vec::new();
        self.broadcast(&mut actions, &recipients, envelope);
        actions.extend(self.nick_change_notices(link, peer, previous_nick));
        self.count_forwarded(message_type);
        actions
    }

    fn count_forwarded(&mut self, message_type: u64) {
        self.state.counters.forwarded += 1;
        match message_type {
            T_MSG => self.state.counters.messages_forwarded += 1,
            T_NOTICE => self.state.counters.notices_forwarded += 1,
            T_ACTION => self.state.counters.actions_forwarded += 1,
            _ => {}
        }
    }

    fn authorize_envelope(
        &mut self,
        link: LinkId,
        peer: IdentityHash,
        envelope: &mut Envelope,
        room: Option<&str>,
    ) {
        envelope.set(K_SRC, Value::Bytes(peer.to_vec()));
        if let Some(room) = room {
            envelope.set(K_ROOM, Value::Text(room.to_string()));
        }
        let nick = envelope
            .text(K_NICK)
            .and_then(|v| normalize_nick(v, self.config.max_nick_bytes))
            .or_else(|| self.state.sessions[&link].nick.clone());
        if nick != self.state.sessions[&link].nick {
            self.state.set_nick(link, nick.clone());
        }
        match nick {
            Some(nick) => envelope.set(K_NICK, Value::Text(nick)),
            None => envelope.remove(K_NICK),
        }
    }

    fn command(&mut self, link: LinkId, peer: IdentityHash, envelope: &Envelope) -> Vec<Action> {
        let text = envelope.text(K_BODY).unwrap().trim();
        let room = envelope.text(K_ROOM);
        let parts: Vec<_> = text.split_whitespace().collect();
        let command = parts
            .first()
            .map(|value| value.to_ascii_lowercase())
            .unwrap_or_default();
        match command.as_str() {
            "/help" => self.notice(link, None, COMMAND_HELP),
            "/stats" => {
                if !self.config.trusted_identities.contains(&peer) {
                    return self.error(link, None, "not authorized");
                }
                let report = self.format_stats();
                self.notice(link, None, &report)
            }
            "/reload" => {
                if !self.config.trusted_identities.contains(&peer) {
                    return self.error(link, None, "not authorized");
                }
                match self.reload() {
                    Ok(summary) => self.notice(link, None, &summary),
                    Err(error) => self.error(link, None, &format!("reload failed: {error}")),
                }
            }
            "/kline" => self.kline_command(link, peer, &parts),
            "/list" => {
                let mut rooms = self
                    .state
                    .rooms
                    .iter()
                    .filter(|(_, r)| r.registered && !r.private)
                    .map(|(name, r)| match r.topic.as_ref() {
                        Some(topic) => format!("  {name} - {topic}"),
                        None => format!("  {name}"),
                    })
                    .collect::<Vec<_>>();
                rooms.sort();
                if rooms.is_empty() {
                    self.notice(link, None, "No public rooms registered")
                } else {
                    rooms.insert(0, "Registered public rooms:".into());
                    self.notice(link, None, &rooms.join("\n"))
                }
            }
            "/who" | "/names" => {
                let Some(requested) = parts.get(1).copied().or(room) else {
                    return self.notice(link, None, "usage: /who [room]");
                };
                let room_name = match self.state.normalize_room(requested, &self.config) {
                    Ok(value) => value,
                    Err(error) => {
                        return self.notice(link, None, &format!("bad room: {error}"));
                    }
                };
                if self
                    .state
                    .rooms
                    .get(&room_name)
                    .is_some_and(|found| found.private)
                    && !self.config.trusted_identities.contains(&peer)
                {
                    return self.notice(link, None, &format!("room {room_name} is private"));
                }
                let mut users = self
                    .state
                    .rooms
                    .get(&room_name)
                    .into_iter()
                    .flat_map(|found| found.members.iter())
                    .filter_map(|id| self.state.sessions.get(id))
                    .filter_map(|s| {
                        let peer = s.peer?;
                        let identity = hex::encode(peer);
                        let room = &self.state.rooms[&room_name];
                        Some(UserInfo {
                            nick: s.nick.clone(),
                            identity,
                            operator: room.operators.contains(&peer),
                            voiced: room.voiced.contains(&peer),
                        })
                    })
                    .collect::<Vec<_>>();
                users.sort_by(|left, right| {
                    left.nick
                        .as_deref()
                        .unwrap_or(&left.identity)
                        .cmp(right.nick.as_deref().unwrap_or(&right.identity))
                });
                let members = users
                    .iter()
                    .map(|user| {
                        user.nick.as_ref().map_or(user.identity.clone(), |nick| {
                            format!("{nick} ({})", &user.identity[..12])
                        })
                    })
                    .collect::<Vec<_>>();
                let mut response = Envelope::new(T_NOTICE, &self.hub_identity);
                response.set(
                    K_BODY,
                    Value::Text(format!(
                        "members in {room_name}: {}",
                        if members.is_empty() {
                            "(none)".into()
                        } else {
                            members.join(", ")
                        }
                    )),
                );
                if self.peer_supports(link, CAP_USER_LIST) {
                    response.set_user_list(&users);
                }
                self.send(link, response)
            }
            "/register" | "/unregister" => {
                let register = command == "/register";
                let Some(requested) = parts.get(1) else {
                    return self.notice(link, room, "usage: /register|unregister <room>");
                };
                let room_name = match self.state.normalize_room(requested, &self.config) {
                    Ok(value) => value,
                    Err(error) => return self.error(link, room, &error.to_string()),
                };
                if !self.state.sessions[&link].rooms.contains(&room_name) {
                    return self.error(link, room, "must be present in the room");
                }
                let Some(state) = self.state.rooms.get_mut(&room_name) else {
                    return self.error(link, room, "no such room");
                };
                if state.founder != Some(peer) {
                    return self.error(link, Some(&room_name), "only the room founder can do that");
                }
                state.registered = register;
                state.last_used_ts = now();
                if register {
                    state.no_outside_messages = true;
                    state.topic_ops_only = true;
                    state.operators.insert(peer);
                }
                match self.persist_rooms() {
                    Ok(()) => self.broadcast_room_state_notice(
                        &room_name,
                        if register {
                            "room registered"
                        } else {
                            "room unregistered"
                        },
                    ),
                    Err(error) => self.error(
                        link,
                        Some(&room_name),
                        &format!("room persist failed: {error}"),
                    ),
                }
            }
            "/topic" => {
                let Some(requested) = parts.get(1) else {
                    return self.notice(link, room, "usage: /topic <room> [topic]");
                };
                let room_name = match self.state.normalize_room(requested, &self.config) {
                    Ok(value) => value,
                    Err(error) => return self.error(link, room, &error.to_string()),
                };
                let Some(state) = self.state.rooms.get(&room_name) else {
                    return self.error(link, room, "no such room");
                };
                if parts.len() == 2 {
                    let topic = state.topic.clone().unwrap_or_else(|| "(none)".into());
                    return self.notice(link, room, &format!("topic for {room_name}: {topic}"));
                }
                if state.topic_ops_only && !state.is_operator(&peer) {
                    return self.error(link, Some(&room_name), "not authorized (+t)");
                }
                let recipients: Vec<_> = state.members.iter().copied().collect();
                let topic = parts[2..].join(" ");
                let state = self.state.rooms.get_mut(&room_name).unwrap();
                state.topic = (!topic.is_empty()).then_some(topic.clone());
                state.last_used_ts = now();
                if let Err(error) = self.persist_rooms() {
                    return self.error(
                        link,
                        Some(&room_name),
                        &format!("room persist failed: {error}"),
                    );
                }
                self.room_state_notice_to(
                    &room_name,
                    &recipients,
                    &format!("topic for {room_name} is now: {topic}"),
                )
            }
            "/mode" => self.mode_command(link, peer, room, &parts),
            "/op" | "/deop" | "/voice" | "/devoice" => {
                self.member_mode_command(link, peer, room, &parts)
            }
            "/invite" | "/ban" | "/unban" => self.access_command(link, peer, room, &parts),
            "/kick" => self.kick_command(link, peer, room, &parts),
            _ => self.error(link, room, "unrecognized command"),
        }
    }

    fn mode_command(
        &mut self,
        link: LinkId,
        peer: IdentityHash,
        context_room: Option<&str>,
        parts: &[&str],
    ) -> Vec<Action> {
        if parts.len() < 3 {
            return self.notice(link, context_room, "usage: /mode <room> <mode> [key]");
        }
        let room_name = match self.state.normalize_room(parts[1], &self.config) {
            Ok(value) => value,
            Err(error) => return self.error(link, context_room, &error.to_string()),
        };
        let Some(state) = self.state.rooms.get(&room_name) else {
            return self.error(link, context_room, "no such room");
        };
        if !state.is_operator(&peer) {
            return self.error(link, Some(&room_name), "not authorized");
        }
        let flag = parts[2].to_ascii_lowercase();
        if matches!(flag.as_str(), "+o" | "-o" | "+v" | "-v") {
            let command = match flag.as_str() {
                "+o" => "/op",
                "-o" => "/deop",
                "+v" => "/voice",
                _ => "/devoice",
            };
            let translated = [command, parts[1], parts.get(3).copied().unwrap_or("")];
            return self.member_mode_command(link, peer, context_room, &translated);
        }
        let state = self.state.rooms.get_mut(&room_name).unwrap();
        match flag.as_str() {
            "+m" => state.moderated = true,
            "-m" => state.moderated = false,
            "+i" => state.invite_only = true,
            "-i" => state.invite_only = false,
            "+t" => state.topic_ops_only = true,
            "-t" => state.topic_ops_only = false,
            "+n" => state.no_outside_messages = true,
            "-n" => state.no_outside_messages = false,
            "+p" => state.private = true,
            "-p" => state.private = false,
            "+k" if parts.len() >= 4 => state.key = Some(parts[3..].join(" ")),
            "-k" => state.key = None,
            "+r" | "-r" => {
                return self.notice(
                    link,
                    context_room,
                    "use /register or /unregister to change +r",
                );
            }
            _ => return self.error(link, context_room, "unknown or incomplete room mode"),
        }
        state.last_used_ts = now();
        match self.persist_rooms() {
            Ok(()) => self.broadcast_room_state_notice(
                &room_name,
                &format!(
                    "mode for {room_name} is now: {}",
                    self.room_mode_string(&room_name)
                ),
            ),
            Err(error) => self.error(
                link,
                Some(&room_name),
                &format!("room persist failed: {error}"),
            ),
        }
    }

    fn kline_command(&mut self, link: LinkId, peer: IdentityHash, parts: &[&str]) -> Vec<Action> {
        if !self.config.trusted_identities.contains(&peer) {
            return self.error(link, None, "not authorized");
        }
        let operation = parts.get(1).map(|value| value.to_ascii_lowercase());
        match operation.as_deref() {
            Some("list") => {
                let value = if self.config.banned_identities.is_empty() {
                    "no klines".to_string()
                } else {
                    self.config
                        .banned_identities
                        .iter()
                        .map(hex::encode)
                        .collect::<Vec<_>>()
                        .join(" ")
                };
                self.notice(link, None, &value)
            }
            Some("add") | Some("del") if parts.len() >= 3 => {
                let operation = operation.as_deref().unwrap();
                let matches = self.resolve_identities(parts[2], None);
                let target = (matches.len() == 1).then_some(matches[0].0).or_else(|| {
                    let value = parts[2].strip_prefix("0x").unwrap_or(parts[2]);
                    hex::decode(value)
                        .ok()
                        .and_then(|bytes| IdentityHash::try_from(bytes.as_slice()).ok())
                });
                let Some(target) = target else {
                    return self.notice(
                        link,
                        None,
                        &self.format_target_matches(parts[2], &matches),
                    );
                };
                if operation == "add" {
                    if !self.config.banned_identities.contains(&target) {
                        self.config.banned_identities.push(target);
                    }
                } else {
                    self.config
                        .banned_identities
                        .retain(|identity| *identity != target);
                }
                if let Err(error) = self.config.save_banned_identities() {
                    return self.error(link, None, &format!("kline persist failed: {error}"));
                }
                let mut actions = self.notice(
                    link,
                    None,
                    &format!(
                        "kline {} for {}",
                        if operation == "add" {
                            "added"
                        } else {
                            "removed"
                        },
                        hex::encode(target)
                    ),
                );
                if operation == "add" {
                    actions.extend(self.state.sessions.iter().filter_map(
                        |(target_link, session)| {
                            (session.peer == Some(target)).then_some(Action::Close(*target_link))
                        },
                    ));
                }
                actions
            }
            _ => self.notice(
                link,
                None,
                "usage: /kline add <nick|hash> | del <hash> | list",
            ),
        }
    }

    fn reload(&mut self) -> anyhow::Result<String> {
        let old_config = self.config.clone();
        let new_config = HubConfig::load(&self.config.config_path)?;
        let mut disk_rooms = RoomRegistry::load(&new_config.room_registry_path)?;
        let old_registered = self
            .state
            .rooms
            .iter()
            .filter(|(_, room)| room.registered)
            .map(|(name, _)| name.clone())
            .collect::<HashSet<_>>();

        for (name, live) in &self.state.rooms {
            if let Some(disk) = disk_rooms.get_mut(name) {
                disk.members = live.members.clone();
                if disk.founder.is_none() {
                    disk.founder = live.founder;
                }
            } else if !live.members.is_empty() {
                let mut ephemeral = live.clone();
                ephemeral.registered = false;
                disk_rooms.insert(name.clone(), ephemeral);
            }
        }

        let new_registered = disk_rooms
            .iter()
            .filter(|(_, room)| room.registered)
            .map(|(name, _)| name.clone())
            .collect::<HashSet<_>>();
        let mut room_changes = new_registered
            .difference(&old_registered)
            .map(|name| format!("+ {name}"))
            .chain(
                old_registered
                    .difference(&new_registered)
                    .map(|name| format!("- {name}")),
            )
            .collect::<Vec<_>>();
        room_changes.sort();
        let config_changes = config_diff(&old_config, &new_config);
        self.config = new_config;
        self.state.rooms = disk_rooms;
        let mut lines = vec![
            format!(
                "reloaded: trusted={}->{} banned={}->{} registered_rooms={}->{}",
                old_config.trusted_identities.len(),
                self.config.trusted_identities.len(),
                old_config.banned_identities.len(),
                self.config.banned_identities.len(),
                old_registered.len(),
                new_registered.len()
            ),
            format!("policy: max_nick_bytes={}", self.config.max_nick_bytes),
        ];
        if config_changes.is_empty() {
            lines.push("config_changes: (none)".into());
        } else {
            lines.push("config_changes:".into());
            lines.extend(
                config_changes
                    .into_iter()
                    .take(12)
                    .map(|change| format!("- {change}")),
            );
        }
        lines.push("rooms_changes:".into());
        if room_changes.is_empty() {
            lines.push("- (none)".into());
        } else {
            lines.extend(room_changes.into_iter().map(|change| format!("- {change}")));
        }
        Ok(lines.join("\n"))
    }

    fn member_mode_command(
        &mut self,
        link: LinkId,
        peer: IdentityHash,
        context_room: Option<&str>,
        parts: &[&str],
    ) -> Vec<Action> {
        if parts.len() < 3 {
            return self.notice(
                link,
                context_room,
                "usage: /op|deop|voice|devoice <room> <nick|hash>",
            );
        }
        let room_name = match self.state.normalize_room(parts[1], &self.config) {
            Ok(value) => value,
            Err(error) => return self.error(link, context_room, &error.to_string()),
        };
        if !self
            .state
            .rooms
            .get(&room_name)
            .is_some_and(|room| room.is_operator(&peer))
        {
            return self.error(link, Some(&room_name), "not authorized");
        }
        let matches = self.resolve_identities(parts[2], Some(&room_name));
        let Some(target) = (matches.len() == 1).then_some(matches[0].0) else {
            return self.notice(
                link,
                context_room,
                &self.format_target_matches(parts[2], &matches),
            );
        };
        let command = parts[0].to_ascii_lowercase();
        let state = self.state.rooms.get_mut(&room_name).unwrap();
        match command.as_str() {
            "/op" => {
                state.operators.insert(target);
            }
            "/deop" if state.founder == Some(target) => {
                return self.error(link, context_room, "cannot deop founder");
            }
            "/deop" => {
                state.operators.remove(&target);
            }
            "/voice" => {
                state.voiced.insert(target);
            }
            "/devoice" => {
                state.voiced.remove(&target);
            }
            _ => unreachable!(),
        }
        state.last_used_ts = now();
        match self.persist_rooms() {
            Ok(()) => {
                let flag = match command.as_str() {
                    "/op" => "+o",
                    "/deop" => "-o",
                    "/voice" => "+v",
                    "/devoice" => "-v",
                    _ => unreachable!(),
                };
                self.broadcast_room_notice(
                    &room_name,
                    &format!(
                        "mode for {room_name} is now: {flag} {}",
                        &hex::encode(target)[..12]
                    ),
                )
            }
            Err(error) => self.error(
                link,
                Some(&room_name),
                &format!("room persist failed: {error}"),
            ),
        }
    }

    fn access_command(
        &mut self,
        link: LinkId,
        peer: IdentityHash,
        context_room: Option<&str>,
        parts: &[&str],
    ) -> Vec<Action> {
        if parts.len() < 3 {
            return self.notice(
                link,
                context_room,
                "usage: /invite|ban <room> add|del|list [nick|hash]",
            );
        }
        let room_name = match self.state.normalize_room(parts[1], &self.config) {
            Ok(value) => value,
            Err(error) => return self.error(link, context_room, &error.to_string()),
        };
        let command = parts[0].to_ascii_lowercase();
        let operation = if command == "/unban" {
            "del".to_string()
        } else {
            parts[2].to_ascii_lowercase()
        };
        let is_operator = self
            .state
            .rooms
            .get(&room_name)
            .is_some_and(|room| room.is_operator(&peer));
        if command == "/invite" && !is_operator {
            return self.error(link, Some(&room_name), "not authorized");
        }
        if operation == "list" {
            let state = &self.state.rooms[&room_name];
            let values = if command == "/invite" {
                state
                    .invited
                    .iter()
                    .filter(|(_, expires)| **expires > now())
                    .map(|(hash, expires)| {
                        format!(
                            "{} expires_in={}s",
                            hex::encode(hash),
                            (*expires - now()).max(0.0) as u64
                        )
                    })
                    .collect::<Vec<_>>()
            } else {
                state.banned.iter().map(hex::encode).collect::<Vec<_>>()
            };
            let message = if values.is_empty() {
                "(none)".to_string()
            } else {
                values.join(", ")
            };
            return self.notice(link, context_room, &message);
        }
        if !matches!(operation.as_str(), "add" | "del") {
            return self.notice(
                link,
                context_room,
                "usage: /invite|ban <room> add|del|list [nick|hash]",
            );
        }
        if !is_operator {
            return self.error(link, Some(&room_name), "not authorized");
        }
        let target_token = if command == "/unban" {
            parts.get(2)
        } else {
            parts.get(3)
        };
        let Some(target_token) = target_token else {
            return self.error(link, context_room, "target is required");
        };
        let target_room = (command != "/invite").then_some(room_name.as_str());
        let matches = self.resolve_identities(target_token, target_room);
        let target = (matches.len() == 1).then_some(matches[0].0).or_else(|| {
            hex::decode(target_token)
                .ok()
                .and_then(|bytes| IdentityHash::try_from(bytes.as_slice()).ok())
        });
        let Some(target) = target else {
            return self.notice(
                link,
                context_room,
                &self.format_target_matches(target_token, &matches),
            );
        };
        let state = self.state.rooms.get_mut(&room_name).unwrap();
        match (command.as_str(), operation.as_str()) {
            ("/invite", "add") => {
                if state.key.is_some() || state.invite_only {
                    state
                        .invited
                        .insert(target, now() + self.config.room_invite_timeout_s);
                }
            }
            ("/invite", "del") => {
                state.invited.remove(&target);
            }
            ("/ban", "add") => {
                state.banned.insert(target);
            }
            ("/ban", "del") | ("/unban", "del") => {
                state.banned.remove(&target);
            }
            _ => unreachable!(),
        }
        state.last_used_ts = now();
        let evicted = if command == "/ban" && operation == "add" {
            self.state.links_by_identity.get(&target).copied()
        } else {
            None
        };
        if let Some(target_link) = evicted {
            self.state
                .rooms
                .get_mut(&room_name)
                .unwrap()
                .members
                .remove(&target_link);
            if let Some(session) = self.state.sessions.get_mut(&target_link) {
                session.rooms.remove(&room_name);
            }
        }
        match self.persist_rooms() {
            Ok(()) => {
                let status = match (command.as_str(), operation.as_str()) {
                    ("/invite", "add")
                        if self.state.rooms[&room_name].key.is_some()
                            || self.state.rooms[&room_name].invite_only =>
                    {
                        format!(
                            "invite added in {room_name} (expires in {}s)",
                            self.config.room_invite_timeout_s as u64
                        )
                    }
                    ("/invite", "add") => {
                        format!("invite sent to {target_token} for {room_name}")
                    }
                    ("/invite", "del") => format!("invite removed in {room_name}"),
                    ("/ban", "add") => format!("ban added in {room_name}"),
                    _ => format!("ban removed in {room_name}"),
                };
                let mut actions = self.notice(link, context_room, &status);
                if command == "/invite"
                    && operation == "add"
                    && let Some(target_link) = self.state.links_by_identity.get(&target).copied()
                {
                    let text = if self.state.rooms[&room_name].key.is_some() {
                        format!(
                            "You have been invited to join {room_name}. This invite allows joining without the key (+k)."
                        )
                    } else {
                        format!("You have been invited to join {room_name}.")
                    };
                    actions.extend(self.notice(target_link, Some(&room_name), &text));
                }
                if let Some(target_link) = evicted {
                    actions.extend(self.error(
                        target_link,
                        Some(&room_name),
                        &format!("banned from {room_name}"),
                    ));
                }
                actions
            }
            Err(error) => self.error(
                link,
                Some(&room_name),
                &format!("room persist failed: {error}"),
            ),
        }
    }

    fn kick_command(
        &mut self,
        link: LinkId,
        peer: IdentityHash,
        context_room: Option<&str>,
        parts: &[&str],
    ) -> Vec<Action> {
        if parts.len() < 3 {
            return self.notice(link, context_room, "usage: /kick <room> <nick|hash>");
        }
        let room_name = match self.state.normalize_room(parts[1], &self.config) {
            Ok(value) => value,
            Err(error) => return self.error(link, context_room, &error.to_string()),
        };
        if !self
            .state
            .rooms
            .get(&room_name)
            .is_some_and(|room| room.is_operator(&peer))
        {
            return self.error(link, Some(&room_name), "not authorized");
        }
        let matches = self.resolve_identities(parts[2], Some(&room_name));
        let Some(target) = (matches.len() == 1).then_some(matches[0].0) else {
            return self.notice(
                link,
                context_room,
                &self.format_target_matches(parts[2], &matches),
            );
        };
        let Some(target_link) = self.state.links_by_identity.get(&target).copied() else {
            return self.error(link, context_room, "target is not connected");
        };
        self.state
            .rooms
            .get_mut(&room_name)
            .unwrap()
            .members
            .remove(&target_link);
        if let Some(session) = self.state.sessions.get_mut(&target_link) {
            session.rooms.remove(&room_name);
        }
        let mut actions = self.error(
            target_link,
            Some(&room_name),
            &format!("kicked from {room_name}"),
        );
        actions.extend(self.notice(
            link,
            context_room,
            &format!("kicked {} from {room_name}", parts[2]),
        ));
        actions
    }

    fn resolve_identities(
        &self,
        token: &str,
        room: Option<&str>,
    ) -> Vec<(IdentityHash, Option<String>)> {
        let token = token.trim().to_lowercase();
        if token.is_empty() {
            return vec![];
        }
        let hex_token = token.strip_prefix("0x").unwrap_or(&token);
        let is_hash_prefix = hex_token.len() >= 6
            && hex_token.len() % 2 == 0
            && hex_token
                .chars()
                .all(|character| character.is_ascii_hexdigit());
        let mut matches: Vec<(IdentityHash, Option<String>)> = Vec::new();
        for session in self.state.sessions.values() {
            if room.is_some_and(|room| !session.rooms.contains(room)) {
                continue;
            }
            let Some(peer) = session.peer else { continue };
            let nick_matches = session
                .nick
                .as_ref()
                .is_some_and(|nick| nick.to_lowercase() == token);
            if (nick_matches || (is_hash_prefix && hex::encode(peer).starts_with(hex_token)))
                && !matches.iter().any(|(identity, _)| *identity == peer)
            {
                matches.push((peer, session.nick.clone()));
            }
        }
        matches.sort_by_key(|(identity, _)| *identity);
        matches
    }

    fn format_target_matches(
        &self,
        token: &str,
        matches: &[(IdentityHash, Option<String>)],
    ) -> String {
        if matches.is_empty() {
            return format!("target '{token}' not found");
        }
        let items = matches
            .iter()
            .map(|(identity, nick)| {
                let nick = nick
                    .as_ref()
                    .map(|nick| format!("nick={nick:?}"))
                    .unwrap_or_else(|| "(no nick)".into());
                format!("  - {} {nick}", &hex::encode(identity)[..16])
            })
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            "ambiguous: '{token}' matches {} identities:\n{items}\nUse full or longer identity hash to disambiguate.",
            matches.len()
        )
    }

    fn room_mode_string(&self, room_name: &str) -> String {
        let Some(room) = self.state.rooms.get(room_name) else {
            return "(none)".into();
        };
        let mut flags = String::new();
        for (enabled, flag) in [
            (room.invite_only, 'i'),
            (room.key.is_some(), 'k'),
            (room.moderated, 'm'),
            (room.no_outside_messages, 'n'),
            (room.private, 'p'),
            (room.registered, 'r'),
            (room.topic_ops_only, 't'),
        ] {
            if enabled {
                flags.push(flag);
            }
        }
        if flags.is_empty() {
            "(none)".into()
        } else {
            format!("+{flags}")
        }
    }

    fn room_protocol_state(&self, room_name: &str) -> RoomState {
        let room = &self.state.rooms[room_name];
        RoomState {
            registered: room.registered,
            modes: self.room_mode_string(room_name),
            topic: room.topic.clone(),
        }
    }

    fn room_state_notice_to(
        &mut self,
        room_name: &str,
        recipients: &[LinkId],
        text: &str,
    ) -> Vec<Action> {
        let state = self.room_protocol_state(room_name);
        let mut actions = Vec::new();
        for recipient in recipients {
            let mut envelope = Envelope::new(T_NOTICE, &self.hub_identity);
            envelope.set(K_ROOM, Value::Text(room_name.to_string()));
            envelope.set(K_BODY, Value::Text(text.to_string()));
            if self.peer_supports(*recipient, CAP_ROOM_STATE) {
                envelope.set_room_state(&state);
            }
            actions.extend(self.send(*recipient, envelope));
        }
        actions
    }

    fn peer_supports(&self, link: LinkId, capability: i128) -> bool {
        self.state
            .sessions
            .get(&link)
            .and_then(|session| session.peer_caps.get(&capability))
            .copied()
            .unwrap_or(false)
    }

    fn broadcast_room_state_notice(&mut self, room_name: &str, text: &str) -> Vec<Action> {
        let recipients: Vec<_> = self
            .state
            .rooms
            .get(room_name)
            .map(|room| room.members.iter().copied().collect())
            .unwrap_or_default();
        self.room_state_notice_to(room_name, &recipients, text)
    }

    fn broadcast_room_notice(&mut self, room_name: &str, text: &str) -> Vec<Action> {
        let recipients: Vec<_> = self
            .state
            .rooms
            .get(room_name)
            .map(|room| room.members.iter().copied().collect())
            .unwrap_or_default();
        let mut actions = Vec::new();
        for recipient in recipients {
            actions.extend(self.notice(recipient, Some(room_name), text));
        }
        actions
    }

    fn nick_change_notices(
        &mut self,
        link: LinkId,
        peer: IdentityHash,
        previous: Option<String>,
    ) -> Vec<Action> {
        let Some(session) = self.state.sessions.get(&link) else {
            return Vec::new();
        };
        let current = session.nick.clone();
        if current == previous {
            return Vec::new();
        }
        let rooms = session.rooms.iter().cloned().collect::<Vec<_>>();
        let previous = previous.unwrap_or_else(|| hex::encode(peer)[..12].to_string());
        let current = current.unwrap_or_else(|| hex::encode(peer)[..12].to_string());
        let mut actions = Vec::new();
        for room in rooms {
            actions.extend(
                self.broadcast_room_notice(
                    &room,
                    &format!("nick changed: {previous} -> {current}"),
                ),
            );
        }
        actions
    }

    fn persist_rooms(&self) -> anyhow::Result<()> {
        RoomRegistry::save(&self.config.room_registry_path, &self.state.rooms)
    }

    fn resource_envelope(&mut self, link: LinkId, envelope: &Envelope) -> Vec<Action> {
        if !self.config.enable_resource_transfer {
            return self.error(link, envelope.text(K_ROOM), "resource transfer disabled");
        }
        let Some(body) = envelope.map(K_BODY) else {
            return self.error(
                link,
                envelope.text(K_ROOM),
                "invalid resource envelope body",
            );
        };
        let id = match map_get(body, B_RES_ID) {
            Some(Value::Bytes(value)) => value.clone(),
            _ => return self.error(link, envelope.text(K_ROOM), "resource envelope missing id"),
        };
        let kind = match map_get(body, B_RES_KIND) {
            Some(Value::Text(value)) if !value.is_empty() => value.clone(),
            _ => {
                return self.error(
                    link,
                    envelope.text(K_ROOM),
                    "resource envelope missing kind",
                );
            }
        };
        let valid_sha = match map_get(body, B_RES_SHA256) {
            None => true,
            Some(Value::Bytes(value)) => value.len() == 32,
            _ => false,
        };
        let valid_encoding = matches!(map_get(body, B_RES_ENCODING), None | Some(Value::Text(_)));
        let size = match map_get(body, B_RES_SIZE) {
            Some(Value::Integer(v)) if *v >= 0 => usize::try_from(*v).ok(),
            _ => None,
        };
        if !valid_sha || !valid_encoding || size.is_none() {
            return self.error(link, envelope.text(K_ROOM), "invalid resource envelope");
        }
        if size.unwrap() > self.config.max_resource_bytes {
            return self.error(link, envelope.text(K_ROOM), "resource too large");
        }
        let now = Instant::now();
        let expectations = self.pending_resources.entry(link).or_default();
        expectations.retain(|expectation| expectation.expires > now);
        if expectations.len() >= self.config.max_pending_resource_expectations {
            return self.error(
                link,
                envelope.text(K_ROOM),
                "too many pending resource expectations",
            );
        }
        expectations.push(ResourceExpectation {
            id,
            kind,
            size: size.unwrap(),
            sha256: match map_get(body, B_RES_SHA256) {
                Some(Value::Bytes(value)) => Some(value.clone()),
                _ => None,
            },
            encoding: match map_get(body, B_RES_ENCODING) {
                Some(Value::Text(value)) => Some(value.clone()),
                _ => None,
            },
            room: envelope.text(K_ROOM).map(str::to_string),
            expires: now + Duration::from_secs_f64(self.config.resource_expectation_ttl_s),
        });
        vec![]
    }

    pub fn resource_received(&mut self, link: LinkId, payload: Vec<u8>) -> Vec<Action> {
        let now = Instant::now();
        let actual_sha = Sha256::digest(&payload);
        let Some(expectations) = self.pending_resources.get_mut(&link) else {
            self.state.counters.resources_rejected += 1;
            return self.error(link, None, "resource received without expectation");
        };
        expectations.retain(|expectation| expectation.expires > now);
        let Some(index) = expectations.iter().position(|expectation| {
            expectation.size == payload.len()
                && expectation
                    .sha256
                    .as_ref()
                    .is_none_or(|expected| expected.as_slice() == actual_sha.as_slice())
        }) else {
            self.state.counters.resources_rejected += 1;
            return self.error(link, None, "resource does not match expectation");
        };
        let expectation = expectations.remove(index);
        self.state.counters.resources_received += 1;
        self.state.counters.resource_bytes_received += payload.len() as u64;
        tracing::debug!(
            id = %hex::encode(&expectation.id),
            kind = expectation.kind,
            bytes = payload.len(),
            "accepted expected resource"
        );
        if expectation.kind == "blob" {
            tracing::info!(bytes = payload.len(), "received expected blob resource");
            return vec![];
        }
        let encoding = expectation.encoding.as_deref().unwrap_or("utf-8");
        if !encoding.eq_ignore_ascii_case("utf-8") {
            self.state.counters.resources_rejected += 1;
            return self.error(
                link,
                expectation.room.as_deref(),
                "unsupported resource encoding",
            );
        }
        let Ok(text) = String::from_utf8(payload) else {
            self.state.counters.resources_rejected += 1;
            return self.error(
                link,
                expectation.room.as_deref(),
                "resource is not valid UTF-8",
            );
        };
        if expectation.kind == "motd" {
            tracing::info!(
                chars = text.chars().count(),
                "received expected MOTD resource"
            );
            return vec![];
        }
        let (resource_kind, message_type) = match expectation.kind.as_str() {
            "message" | "msg" => ("message", T_MSG),
            "notice" => ("notice", T_NOTICE),
            "action" => ("action", T_ACTION),
            _ => {
                self.state.counters.resources_rejected += 1;
                return self.error(link, expectation.room.as_deref(), "unknown resource kind");
            }
        };
        if text.len() > self.config.max_resource_bytes {
            self.state.counters.resources_rejected += 1;
            return self.error(link, expectation.room.as_deref(), "resource text too large");
        }
        let Some(room_name) = expectation.room else {
            self.state.counters.resources_rejected += 1;
            return vec![];
        };
        let Some(peer) = self
            .state
            .sessions
            .get(&link)
            .and_then(|session| session.peer)
        else {
            self.state.counters.resources_rejected += 1;
            return vec![];
        };
        let Some(room) = self.state.rooms.get(&room_name) else {
            self.state.counters.resources_rejected += 1;
            return self.error(link, Some(&room_name), "no such room");
        };
        if !room.members.contains(&link) || !room.may_speak(&peer) || room.banned.contains(&peer) {
            self.state.counters.resources_rejected += 1;
            return self.error(link, Some(&room_name), "resource message is not allowed");
        }
        let recipients: Vec<_> = room
            .members
            .iter()
            .copied()
            .filter(|recipient| message_type != T_NOTICE || *recipient != link)
            .collect();
        let nick = self.state.sessions[&link].nick.clone();
        let mut actions = Vec::new();
        for recipient in recipients {
            actions.extend(self.resource_text_actions(
                recipient,
                peer,
                nick.as_deref(),
                Some(&room_name),
                resource_kind,
                &text,
            ));
        }
        self.count_forwarded(message_type);
        actions
    }

    fn format_stats(&self) -> String {
        let c = &self.state.counters;
        let identified = self
            .state
            .sessions
            .values()
            .filter(|session| session.peer.is_some())
            .count();
        let welcomed = self
            .state
            .sessions
            .values()
            .filter(|session| session.welcomed)
            .count();
        let memberships: usize = self
            .state
            .rooms
            .values()
            .map(|room| room.members.len())
            .sum();
        let mut top_rooms = self
            .state
            .rooms
            .iter()
            .filter(|(_, room)| !room.members.is_empty())
            .map(|(name, room)| (name, room.members.len()))
            .collect::<Vec<_>>();
        top_rooms.sort_by(|(name_a, count_a), (name_b, count_b)| {
            count_b.cmp(count_a).then_with(|| name_a.cmp(name_b))
        });
        let top_rooms = top_rooms
            .into_iter()
            .take(5)
            .map(|(name, count)| format!("{name}:{count}"))
            .collect::<Vec<_>>();
        let mut lines = vec![
            format!("rrcd-rs {} stats", env!("CARGO_PKG_VERSION")),
            format!("uptime_s={:.1}", self.state.started.elapsed().as_secs_f64()),
            format!(
                "clients_total={} clients_identified={identified} clients_welcomed={welcomed}",
                self.state.sessions.len()
            ),
            format!("rooms={} memberships={memberships}", self.state.rooms.len()),
        ];
        if !top_rooms.is_empty() {
            lines.push(format!("top_rooms={}", top_rooms.join(", ")));
        }
        lines.extend([
            format!(
                "trust: trusted={} banned={}",
                self.config.trusted_identities.len(),
                self.config.banned_identities.len()
            ),
            format!(
                "limits: rate_limit_msgs_per_minute={} max_rooms_per_session={} max_room_name_bytes={} max_nick_bytes={}",
                self.config.rate_limit_msgs_per_minute,
                self.config.max_rooms_per_session,
                self.config.max_room_name_bytes,
                self.config.max_nick_bytes
            ),
            format!(
                "features: ping_interval_s={} ping_timeout_s={} announce_on_start={} announce_period_s={}",
                self.config.ping_interval_s,
                self.config.ping_timeout_s,
                self.config.announce_on_start,
                self.config.announce_period_s
            ),
            format!(
                "io: pkts_in={} pkts_bad={} bytes_in={} bytes_out={}",
                c.packets_in, c.packets_bad, c.bytes_in, c.bytes_out
            ),
            format!(
                "events: joins={} parts={} msgs_fwd={} notices_fwd={} actions_fwd={} errors_sent={} rate_limited={}",
                c.joins,
                c.parts,
                c.messages_forwarded,
                c.notices_forwarded,
                c.actions_forwarded,
                c.errors,
                c.rate_limited
            ),
            format!(
                "pings: in={} out={} pongs: in={} out={}",
                c.pings_in, c.pings_out, c.pongs_in, c.pongs_out
            ),
            format!(
                "announces={}",
                c.announces
            ),
            format!(
                "resources: sent={} received={} rejected={} bytes_sent={} bytes_received={}",
                c.resources_sent,
                c.resources_received,
                c.resources_rejected,
                c.resource_bytes_sent,
                c.resource_bytes_received
            ),
        ]);
        lines.join("\n")
    }

    fn notice(&mut self, link: LinkId, room: Option<&str>, text: &str) -> Vec<Action> {
        self.smart_notice(link, room, text, "notice")
    }

    fn packet_text_from(
        &mut self,
        link: LinkId,
        source: IdentityHash,
        nick: Option<&str>,
        room: Option<&str>,
        kind: &str,
        text: &str,
    ) -> Vec<Action> {
        let message_type = match kind {
            "message" | "msg" => T_MSG,
            "action" => T_ACTION,
            _ => T_NOTICE,
        };
        let mut env = Envelope::new(message_type, &source);
        if let Some(room) = room {
            env.set(K_ROOM, Value::Text(room.into()));
        }
        if let Some(nick) = nick {
            env.set(K_NICK, Value::Text(nick.into()));
        }
        env.set(K_BODY, Value::Text(text.into()));
        self.send(link, env)
    }

    fn smart_notice(
        &mut self,
        link: LinkId,
        room: Option<&str>,
        text: &str,
        kind: &str,
    ) -> Vec<Action> {
        self.resource_text_actions(link, self.hub_identity, None, room, kind, text)
    }

    fn resource_text_actions(
        &mut self,
        link: LinkId,
        source: IdentityHash,
        nick: Option<&str>,
        room: Option<&str>,
        kind: &str,
        text: &str,
    ) -> Vec<Action> {
        let payload = text.as_bytes().to_vec();
        if payload.len() <= 512 {
            return self.packet_text_from(link, source, nick, room, kind, text);
        }
        if !self.config.enable_resource_transfer || payload.len() > self.config.max_resource_bytes {
            let mut actions = Vec::new();
            for chunk in utf8_chunks(text, 300) {
                actions.extend(self.packet_text_from(link, source, nick, room, kind, chunk));
            }
            return actions;
        }
        let mut id = [0u8; 8];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut id);
        let mut body = Map::new();
        body.insert(Value::Integer(B_RES_ID), Value::Bytes(id.to_vec()));
        body.insert(Value::Integer(B_RES_KIND), Value::Text(kind.to_string()));
        body.insert(
            Value::Integer(B_RES_SIZE),
            Value::Integer(payload.len() as i128),
        );
        body.insert(
            Value::Integer(B_RES_SHA256),
            Value::Bytes(Sha256::digest(&payload).to_vec()),
        );
        body.insert(Value::Integer(B_RES_ENCODING), Value::Text("utf-8".into()));
        let mut envelope = Envelope::new(T_RESOURCE_ENVELOPE, &source);
        if let Some(room) = room {
            envelope.set(K_ROOM, Value::Text(room.into()));
        }
        if let Some(nick) = nick {
            envelope.set(K_NICK, Value::Text(nick.into()));
        }
        envelope.set(K_BODY, Value::Map(body));
        let mut actions = self.send(link, envelope);
        actions.push(Action::SendResource(link, payload));
        self.state.counters.resources_sent += 1;
        self.state.counters.resource_bytes_sent += text.len() as u64;
        actions
    }
    fn error(&mut self, link: LinkId, room: Option<&str>, text: &str) -> Vec<Action> {
        self.state.counters.errors += 1;
        let mut env = Envelope::new(T_ERROR, &self.hub_identity);
        if let Some(room) = room {
            env.set(K_ROOM, Value::Text(room.into()));
        }
        env.set(K_BODY, Value::Text(text.into()));
        self.send(link, env)
    }
    fn send(&mut self, link: LinkId, envelope: Envelope) -> Vec<Action> {
        match envelope.encode() {
            Ok(payload) => {
                self.state.counters.bytes_out += payload.len() as u64;
                vec![Action::Send(link, payload)]
            }
            Err(error) => {
                tracing::warn!(%error, "failed to encode outgoing envelope");
                vec![]
            }
        }
    }
    fn broadcast(&mut self, actions: &mut Vec<Action>, recipients: &[LinkId], envelope: Envelope) {
        let Ok(payload) = envelope.encode() else {
            return;
        };
        self.state.counters.bytes_out += (payload.len() * recipients.len()) as u64;
        actions.extend(
            recipients
                .iter()
                .map(|link| Action::Send(*link, payload.clone())),
        );
    }
}

fn utf8_chunks(value: &str, max_bytes: usize) -> Vec<&str> {
    if value.is_empty() || max_bytes == 0 {
        return vec![];
    }
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < value.len() {
        let mut end = (start + max_bytes).min(value.len());
        while end > start && !value.is_char_boundary(end) {
            end -= 1;
        }
        if end == start {
            end = value[start..]
                .char_indices()
                .nth(1)
                .map(|(offset, _)| start + offset)
                .unwrap_or(value.len());
        }
        chunks.push(&value[start..end]);
        start = end;
    }
    chunks
}

fn config_diff(old: &HubConfig, new: &HubConfig) -> Vec<String> {
    let mut changes = Vec::new();
    macro_rules! compare {
        ($field:ident) => {
            if old.$field != new.$field {
                changes.push(format!(
                    "{}: {:?} -> {:?}",
                    stringify!($field),
                    old.$field,
                    new.$field
                ));
            }
        };
    }
    compare!(room_registry_path);
    compare!(hub_name);
    compare!(greeting);
    compare!(announce_on_start);
    compare!(announce_period_s);
    compare!(include_joined_member_list);
    compare!(room_invite_timeout_s);
    compare!(room_registry_prune_after_s);
    compare!(room_registry_prune_interval_s);
    compare!(max_nick_bytes);
    compare!(max_rooms_per_session);
    compare!(max_room_name_bytes);
    compare!(max_msg_body_bytes);
    compare!(rate_limit_msgs_per_minute);
    compare!(ping_interval_s);
    compare!(ping_timeout_s);
    compare!(enable_resource_transfer);
    compare!(max_resource_bytes);
    compare!(max_pending_resource_expectations);
    compare!(resource_expectation_ttl_s);
    compare!(log_level);
    compare!(log_console);
    compare!(log_file);
    if old.trusted_identities != new.trusted_identities {
        changes.push(format!(
            "trusted_identities: len={} -> len={}",
            old.trusted_identities.len(),
            new.trusted_identities.len()
        ));
    }
    if old.banned_identities != new.banned_identities {
        changes.push(format!(
            "banned_identities: len={} -> len={}",
            old.banned_identities.len(),
            new.banned_identities.len()
        ));
    }
    changes
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::state::Room;

    fn config() -> HubConfig {
        HubConfig {
            config_path: PathBuf::from("config"),
            identity_path: PathBuf::from("identity"),
            room_registry_path: PathBuf::from("rooms"),
            rns_config_dir: None,
            hub_name: "test".into(),
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
            trusted_identities: vec![],
            banned_identities: vec![],
            log_level: "INFO".into(),
            log_console: true,
            log_file: None,
        }
    }

    fn client(message_type: u64, peer: IdentityHash) -> Envelope {
        Envelope::new(message_type, &peer)
    }

    fn connect(router: &mut Router, link: LinkId, peer: IdentityHash, nick: &str) {
        router.established(link);
        router.identified(link, peer);
        let hello = Envelope::hello(&peer, Some(nick));
        assert!(!router.packet(link, &hello.encode().unwrap()).is_empty());
    }

    fn connect_legacy(router: &mut Router, link: LinkId, peer: IdentityHash, nick: &str) {
        router.established(link);
        router.identified(link, peer);
        let mut hello = client(T_HELLO, peer);
        hello.set(K_NICK, Value::Text(nick.into()));
        assert!(!router.packet(link, &hello.encode().unwrap()).is_empty());
    }

    fn join(router: &mut Router, link: LinkId, peer: IdentityHash, room: &str) {
        let mut envelope = client(T_JOIN, peer);
        envelope.set(K_ROOM, Value::Text(room.into()));
        router.packet(link, &envelope.encode().unwrap());
    }

    fn slash(
        router: &mut Router,
        link: LinkId,
        peer: IdentityHash,
        room: &str,
        command: &str,
    ) -> Vec<Action> {
        let mut envelope = client(T_MSG, peer);
        envelope.set(K_ROOM, Value::Text(room.into()));
        envelope.set(K_BODY, Value::Text(command.into()));
        router.packet(link, &envelope.encode().unwrap())
    }

    fn action_envelope(action: &Action) -> Envelope {
        let Action::Send(_, payload) = action else {
            panic!("expected packet action");
        };
        Envelope::decode(payload).unwrap()
    }

    #[test]
    fn join_and_message_are_broadcast_with_authoritative_source() {
        let mut router = Router::new(config(), [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "alice");
        connect(&mut router, [2; 16], [22; 16], "bob");
        for (link, peer) in [([1; 16], [11; 16]), ([2; 16], [22; 16])] {
            let mut join = client(T_JOIN, peer);
            join.set(K_ROOM, Value::Text("Lobby".into()));
            router.packet(link, &join.encode().unwrap());
        }

        let mut message = client(T_MSG, [0; 16]);
        message.set(K_ROOM, Value::Text("LOBBY".into()));
        message.set(K_BODY, Value::Text("hello".into()));
        let actions = router.packet([1; 16], &message.encode().unwrap());
        assert_eq!(actions.len(), 2);
        for action in actions {
            let Action::Send(_, payload) = action else {
                panic!("unexpected action");
            };
            let forwarded = Envelope::decode(&payload).unwrap();
            assert_eq!(forwarded.bytes(K_SRC), Some(&[11; 16][..]));
            assert_eq!(forwarded.text(K_ROOM), Some("lobby"));
            assert_eq!(forwarded.text(K_NICK), Some("alice"));
        }
    }

    #[test]
    fn repeated_join_does_not_fan_out_to_the_joining_link_twice() {
        let mut router = Router::new(config(), [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "alice");
        connect(&mut router, [2; 16], [22; 16], "bob");
        join(&mut router, [1; 16], [11; 16], "lobby");
        join(&mut router, [2; 16], [22; 16], "lobby");

        let mut envelope = client(T_JOIN, [22; 16]);
        envelope.set(K_ROOM, Value::Text("lobby".into()));
        let actions = router.packet([2; 16], &envelope.encode().unwrap());
        let joined_recipients = actions
            .iter()
            .filter_map(|action| {
                let Action::Send(recipient, payload) = action else {
                    return None;
                };
                (Envelope::decode(payload).unwrap().integer(K_T) == Some(T_JOINED))
                    .then_some(*recipient)
            })
            .collect::<Vec<_>>();
        assert_eq!(joined_recipients, vec![[1; 16], [2; 16]]);
    }

    #[test]
    fn commands_and_mode_flags_are_case_insensitive_and_join_reports_room_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config();
        cfg.room_registry_path = dir.path().join("rooms");
        let mut router = Router::new(cfg, [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "alice");

        let mut envelope = client(T_JOIN, [11; 16]);
        envelope.set(K_ROOM, Value::Text("Lobby".into()));
        let actions = router.packet([1; 16], &envelope.encode().unwrap());
        let room_state = actions
            .iter()
            .map(action_envelope)
            .find(|envelope| envelope.integer(K_T) == Some(T_NOTICE))
            .unwrap();
        assert_eq!(
            room_state.text(K_BODY),
            Some("room lobby: unregistered; mode=(none); topic=(none)")
        );
        let joined = actions
            .iter()
            .map(action_envelope)
            .find(|envelope| envelope.integer(K_T) == Some(T_JOINED))
            .unwrap();
        assert_eq!(
            joined.room_state(),
            Some(RoomState {
                registered: false,
                modes: "(none)".into(),
                topic: None,
            })
        );

        let actions = slash(&mut router, [1; 16], [11; 16], "lobby", "/MODE lobby +M");
        assert!(!actions.is_empty());
        assert!(router.state.rooms["lobby"].moderated);
        assert!(actions.iter().map(action_envelope).all(|envelope| {
            envelope
                .room_state()
                .is_some_and(|state| state.modes == "+m")
        }));

        let actions = slash(&mut router, [1; 16], [11; 16], "lobby", "/LiSt");
        assert_eq!(
            action_envelope(&actions[0]).text(K_BODY),
            Some("No public rooms registered")
        );
    }

    #[test]
    fn structured_extensions_are_only_sent_to_capable_peers() {
        let mut router = Router::new(config(), [9; 16]);
        connect_legacy(&mut router, [1; 16], [11; 16], "legacy");

        let mut join_request = client(T_JOIN, [11; 16]);
        join_request.set(K_ROOM, Value::Text("lobby".into()));
        let actions = router.packet([1; 16], &join_request.encode().unwrap());
        let joined = actions
            .iter()
            .map(action_envelope)
            .find(|envelope| envelope.integer(K_T) == Some(T_JOINED))
            .unwrap();
        assert_eq!(joined.room_state(), None);

        let actions = slash(&mut router, [1; 16], [11; 16], "lobby", "/who lobby");
        let who = actions
            .iter()
            .map(action_envelope)
            .find(|envelope| {
                envelope
                    .text(K_BODY)
                    .is_some_and(|body| body.starts_with("members in lobby:"))
            })
            .unwrap();
        assert_eq!(who.user_list(), None);
        assert!(who.text(K_BODY).unwrap().contains("legacy"));

        connect(&mut router, [2; 16], [22; 16], "modern");
        let mut join_request = client(T_JOIN, [22; 16]);
        join_request.set(K_ROOM, Value::Text("lobby".into()));
        let actions = router.packet([2; 16], &join_request.encode().unwrap());
        let joined_for = |recipient| {
            actions.iter().find_map(|action| {
                let Action::Send(target, payload) = action else {
                    return None;
                };
                let envelope = Envelope::decode(payload).unwrap();
                (*target == recipient && envelope.integer(K_T) == Some(T_JOINED))
                    .then_some(envelope)
            })
        };
        assert_eq!(joined_for([1; 16]).unwrap().room_state(), None);
        assert!(joined_for([2; 16]).unwrap().room_state().is_some());
    }

    #[test]
    fn part_and_close_suppress_false_departure_while_same_identity_remains() {
        let mut router = Router::new(config(), [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "alice-phone");
        connect(&mut router, [2; 16], [11; 16], "alice-laptop");
        connect(&mut router, [3; 16], [33; 16], "bob");
        for (link, peer) in [
            ([1; 16], [11; 16]),
            ([2; 16], [11; 16]),
            ([3; 16], [33; 16]),
        ] {
            join(&mut router, link, peer, "lobby");
        }

        assert!(router.closed([1; 16]).is_empty());

        let mut envelope = client(T_PART, [11; 16]);
        envelope.set(K_ROOM, Value::Text("lobby".into()));
        let actions = router.packet([2; 16], &envelope.encode().unwrap());
        let bob_notification = actions.iter().find_map(|action| {
            let Action::Send(recipient, payload) = action else {
                return None;
            };
            (*recipient == [3; 16]).then(|| Envelope::decode(payload).unwrap())
        });
        let bob_notification = bob_notification.expect("bob must receive final PARTED");
        assert_eq!(bob_notification.integer(K_T), Some(T_PARTED));
        assert_eq!(bob_notification.text(K_NICK), Some("alice-laptop"));
        assert_eq!(
            bob_notification.get(K_BODY),
            Some(&Value::Array(vec![Value::Bytes([11; 16].to_vec())]))
        );
    }

    #[test]
    fn direct_notice_has_exactly_one_recipient() {
        let mut router = Router::new(config(), [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "alice");
        connect(&mut router, [2; 16], [22; 16], "bob");
        let mut notice = client(T_NOTICE, [11; 16]);
        notice.set(K_DST, Value::Bytes(vec![22; 16]));
        notice.set(K_BODY, Value::Text("private".into()));
        let actions = router.packet([1; 16], &notice.encode().unwrap());
        assert!(matches!(actions.as_slice(), [Action::Send(link, _)] if *link == [2; 16]));
    }

    #[test]
    fn direct_notice_falls_back_to_another_link_for_same_identity() {
        let mut router = Router::new(config(), [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "alice-phone");
        connect(&mut router, [2; 16], [11; 16], "alice-laptop");
        connect(&mut router, [3; 16], [33; 16], "bob");
        router.closed([2; 16]);

        let mut notice = client(T_NOTICE, [33; 16]);
        notice.set(K_DST, Value::Bytes([11; 16].to_vec()));
        notice.set(K_BODY, Value::Text("hello".into()));
        let actions = router.packet([3; 16], &notice.encode().unwrap());
        assert!(matches!(actions.as_slice(), [Action::Send(link, _)] if *link == [1; 16]));
    }

    #[test]
    fn kline_closes_every_link_for_target_identity() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config");
        HubConfig::write_default(&config_path).unwrap();
        let mut cfg = HubConfig::load(&config_path).unwrap();
        cfg.trusted_identities.push([99; 16]);
        let mut router = Router::new(cfg, [9; 16]);
        connect(&mut router, [9; 16], [99; 16], "operator");
        connect(&mut router, [1; 16], [11; 16], "alice-phone");
        connect(&mut router, [2; 16], [11; 16], "alice-laptop");

        let actions = slash(
            &mut router,
            [9; 16],
            [99; 16],
            "ignored",
            &format!("/kline add {}", hex::encode([11; 16])),
        );
        let mut closed = actions
            .iter()
            .filter_map(|action| match action {
                Action::Close(link) => Some(*link),
                _ => None,
            })
            .collect::<Vec<_>>();
        closed.sort();
        assert_eq!(closed, vec![[1; 16], [2; 16]]);
        assert!(router.config.banned_identities.contains(&[11; 16]));
        assert!(
            HubConfig::load(&config_path)
                .unwrap()
                .banned_identities
                .contains(&[11; 16])
        );
    }

    #[test]
    fn expected_notice_resource_is_verified_and_forwarded() {
        let mut router = Router::new(config(), [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "alice");
        connect(&mut router, [2; 16], [22; 16], "bob");
        for (link, peer) in [([1; 16], [11; 16]), ([2; 16], [22; 16])] {
            let mut join = client(T_JOIN, peer);
            join.set(K_ROOM, Value::Text("lobby".into()));
            router.packet(link, &join.encode().unwrap());
        }
        let payload = vec![b'x'; 1024];
        let mut body = Map::new();
        body.insert(Value::Integer(B_RES_ID), Value::Bytes(vec![7; 8]));
        body.insert(Value::Integer(B_RES_KIND), Value::Text("notice".into()));
        body.insert(
            Value::Integer(B_RES_SIZE),
            Value::Integer(payload.len() as i128),
        );
        body.insert(
            Value::Integer(B_RES_SHA256),
            Value::Bytes(Sha256::digest(&payload).to_vec()),
        );
        body.insert(Value::Integer(B_RES_ENCODING), Value::Text("utf-8".into()));
        let mut announcement = client(T_RESOURCE_ENVELOPE, [11; 16]);
        announcement.set(K_ROOM, Value::Text("lobby".into()));
        announcement.set(K_BODY, Value::Map(body));
        assert!(
            router
                .packet([1; 16], &announcement.encode().unwrap())
                .is_empty()
        );
        let actions = router.resource_received([1; 16], payload);
        assert!(matches!(
            actions.as_slice(),
            [
                Action::Send(link, envelope),
                Action::SendResource(resource_link, resource)
            ] if *link == [2; 16] && *resource_link == [2; 16] && resource.len() == 1024
                && Envelope::decode(envelope).unwrap().integer(K_T) == Some(T_RESOURCE_ENVELOPE)
                && Envelope::decode(envelope).unwrap().bytes(K_SRC) == Some(&[11; 16][..])
                && Envelope::decode(envelope).unwrap().text(K_NICK) == Some("alice")
        ));
        assert_eq!(router.state.counters.resources_received, 1);
        assert_eq!(router.state.counters.resources_sent, 1);
        assert_eq!(router.state.counters.resource_bytes_received, 1024);
        assert_eq!(router.state.counters.resource_bytes_sent, 1024);
    }

    #[test]
    fn expected_message_resource_is_forwarded_as_message() {
        let mut router = Router::new(config(), [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "alice");
        connect(&mut router, [2; 16], [22; 16], "bob");
        for (link, peer) in [([1; 16], [11; 16]), ([2; 16], [22; 16])] {
            let mut join = client(T_JOIN, peer);
            join.set(K_ROOM, Value::Text("lobby".into()));
            router.packet(link, &join.encode().unwrap());
        }
        let payload = vec![b'x'; 400];
        let announcement =
            Envelope::resource(&[11; 16], Some("lobby"), "message", &payload, Some("utf-8"))
                .unwrap();
        assert!(
            router
                .packet([1; 16], &announcement.encode().unwrap())
                .is_empty()
        );

        let actions = router.resource_received([1; 16], payload);
        assert_eq!(actions.len(), 2);
        assert!(actions.iter().all(|action| {
            matches!(
                action,
                Action::Send(_, envelope)
                    if Envelope::decode(envelope).unwrap().integer(K_T) == Some(T_MSG)
            )
        }));
        assert_eq!(router.state.counters.messages_forwarded, 1);
    }

    #[test]
    fn large_greeting_uses_resource_envelope_and_payload() {
        let mut cfg = config();
        cfg.greeting = Some("x".repeat(1024));
        let mut router = Router::new(cfg, [9; 16]);
        router.established([1; 16]);
        router.identified([1; 16], [11; 16]);
        let hello = client(T_HELLO, [11; 16]);
        let actions = router.packet([1; 16], &hello.encode().unwrap());
        assert!(matches!(
            actions.as_slice(),
            [
                Action::Send(_, _),
                Action::Send(_, _),
                Action::SendResource(_, payload)
            ] if payload.len() == 1024
        ));
    }

    #[test]
    fn large_notice_falls_back_to_utf8_safe_packets_when_resources_are_disabled() {
        let mut cfg = config();
        cfg.enable_resource_transfer = false;
        cfg.greeting = Some("🙂".repeat(300));
        let mut router = Router::new(cfg, [9; 16]);
        router.established([1; 16]);
        router.identified([1; 16], [11; 16]);
        let hello = client(T_HELLO, [11; 16]);
        let actions = router.packet([1; 16], &hello.encode().unwrap());
        assert!(actions.len() > 2);
        let chunks: Vec<_> = actions
            .iter()
            .skip(1)
            .map(|action| match action {
                Action::Send(_, payload) => Envelope::decode(payload)
                    .unwrap()
                    .text(K_BODY)
                    .unwrap()
                    .to_string(),
                Action::SendResource(_, _) => panic!("resource fallback was not used"),
                Action::Close(_) => panic!("unexpected close"),
            })
            .collect();
        assert!(chunks.iter().all(|chunk| chunk.len() <= 300));
        assert_eq!(chunks.concat(), "🙂".repeat(300));
    }

    #[test]
    fn utf8_chunker_never_splits_codepoints() {
        let value = "a🙂б".repeat(100);
        let chunks = utf8_chunks(&value, 17);
        assert!(chunks.iter().all(|chunk| chunk.len() <= 17));
        assert_eq!(chunks.concat(), value);
    }

    #[test]
    fn room_ban_command_matches_python_syntax_and_evicts_member() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config();
        cfg.room_registry_path = dir.path().join("rooms");
        let mut router = Router::new(cfg, [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "alice");
        connect(&mut router, [2; 16], [22; 16], "bob");
        join(&mut router, [1; 16], [11; 16], "lobby");
        join(&mut router, [2; 16], [22; 16], "lobby");
        let actions = slash(
            &mut router,
            [1; 16],
            [11; 16],
            "lobby",
            "/ban lobby add bob",
        );
        assert!(router.state.rooms["lobby"].banned.contains(&[22; 16]));
        assert!(!router.state.rooms["lobby"].members.contains(&[2; 16]));
        assert!(!router.state.sessions[&[2; 16]].rooms.contains("lobby"));
        assert_eq!(actions.len(), 2);
        let loaded = RoomRegistry::load(&router.config.room_registry_path).unwrap();
        // Ephemeral rooms are deliberately not persisted until /register.
        assert!(loaded.is_empty());
    }

    #[test]
    fn every_boolean_and_key_room_mode_is_broadcast() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config();
        cfg.room_registry_path = dir.path().join("rooms");
        let mut router = Router::new(cfg, [9; 16]);
        for (link, peer, nick) in [
            ([1; 16], [11; 16], "alice"),
            ([2; 16], [22; 16], "bob"),
            ([3; 16], [33; 16], "carol"),
        ] {
            connect(&mut router, link, peer, nick);
            join(&mut router, link, peer, "lobby");
        }
        for command in [
            "/mode lobby +m",
            "/mode lobby +i",
            "/mode lobby +t",
            "/mode lobby +n",
            "/mode lobby +p",
            "/mode lobby +k secret phrase",
        ] {
            let actions = slash(&mut router, [1; 16], [11; 16], "lobby", command);
            assert_eq!(actions.len(), 3, "{command} was not broadcast");
        }
        let room = &router.state.rooms["lobby"];
        assert!(room.moderated);
        assert!(room.invite_only);
        assert!(room.topic_ops_only);
        assert!(room.no_outside_messages);
        assert!(room.private);
        assert_eq!(room.key.as_deref(), Some("secret phrase"));
        assert_eq!(router.room_mode_string("lobby"), "+ikmnpt");

        for command in [
            "/mode lobby -m",
            "/mode lobby -i",
            "/mode lobby -t",
            "/mode lobby -n",
            "/mode lobby -p",
            "/mode lobby -k",
        ] {
            let actions = slash(&mut router, [1; 16], [11; 16], "lobby", command);
            assert_eq!(actions.len(), 3, "{command} was not broadcast");
        }
        assert_eq!(router.room_mode_string("lobby"), "(none)");
    }

    #[test]
    fn member_modes_broadcast_and_invite_notifies_target() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config();
        cfg.room_registry_path = dir.path().join("rooms");
        let mut router = Router::new(cfg, [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "alice");
        connect(&mut router, [2; 16], [22; 16], "bob");
        join(&mut router, [1; 16], [11; 16], "lobby");
        join(&mut router, [2; 16], [22; 16], "lobby");

        for command in [
            "/op lobby bob",
            "/deop lobby bob",
            "/voice lobby bob",
            "/devoice lobby bob",
            "/mode lobby +o bob",
            "/mode lobby -o bob",
            "/mode lobby +v bob",
            "/mode lobby -v bob",
        ] {
            let actions = slash(&mut router, [1; 16], [11; 16], "lobby", command);
            assert_eq!(actions.len(), 2, "{command} was not broadcast");
        }

        router
            .state
            .sessions
            .get_mut(&[2; 16])
            .unwrap()
            .rooms
            .clear();
        router
            .state
            .rooms
            .get_mut("lobby")
            .unwrap()
            .members
            .remove(&[2; 16]);
        router.state.rooms.get_mut("lobby").unwrap().invite_only = true;
        let actions = slash(
            &mut router,
            [1; 16],
            [11; 16],
            "lobby",
            "/invite lobby add bob",
        );
        assert_eq!(actions.len(), 2);
        assert!(router.state.rooms["lobby"].is_invited(&[22; 16], now()));
    }

    #[test]
    fn ambiguous_nickname_reports_candidates_without_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config();
        cfg.room_registry_path = dir.path().join("rooms");
        let mut router = Router::new(cfg, [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "alice");
        connect(&mut router, [2; 16], [22; 16], "duplicate");
        connect(&mut router, [3; 16], [33; 16], "duplicate");
        for (link, peer) in [
            ([1; 16], [11; 16]),
            ([2; 16], [22; 16]),
            ([3; 16], [33; 16]),
        ] {
            join(&mut router, link, peer, "lobby");
        }
        let actions = slash(
            &mut router,
            [1; 16],
            [11; 16],
            "lobby",
            "/kick lobby duplicate",
        );
        assert_eq!(actions.len(), 1);
        let Action::Send(_, payload) = &actions[0] else {
            panic!("expected ambiguity notice");
        };
        let envelope = Envelope::decode(payload).unwrap();
        assert!(
            envelope
                .text(K_BODY)
                .unwrap()
                .contains("matches 2 identities")
        );
        assert_eq!(router.state.rooms["lobby"].members.len(), 3);
    }

    #[test]
    fn registered_and_topic_modes_enforce_founder_and_operator_rules() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config();
        cfg.room_registry_path = dir.path().join("rooms");
        let mut router = Router::new(cfg, [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "alice");
        connect(&mut router, [2; 16], [22; 16], "bob");
        join(&mut router, [1; 16], [11; 16], "lobby");
        join(&mut router, [2; 16], [22; 16], "lobby");

        slash(&mut router, [1; 16], [11; 16], "lobby", "/register lobby");
        assert!(router.state.rooms["lobby"].registered);
        assert_eq!(router.room_mode_string("lobby"), "+nrt");

        let denied = slash(
            &mut router,
            [2; 16],
            [22; 16],
            "lobby",
            "/topic lobby unauthorized",
        );
        let Action::Send(_, payload) = &denied[0] else {
            panic!("expected authorization error");
        };
        assert_eq!(
            Envelope::decode(payload).unwrap().integer(K_T),
            Some(T_ERROR)
        );
        assert!(router.state.rooms["lobby"].topic.is_none());

        let topic = slash(
            &mut router,
            [1; 16],
            [11; 16],
            "lobby",
            "/topic lobby Welcome everyone",
        );
        assert_eq!(topic.len(), 2);
        assert_eq!(
            router.state.rooms["lobby"].topic.as_deref(),
            Some("Welcome everyone")
        );

        let read_only = slash(&mut router, [1; 16], [11; 16], "lobby", "/mode lobby -r");
        assert_eq!(read_only.len(), 1);
        assert!(router.state.rooms["lobby"].registered);
    }

    #[test]
    fn registered_room_survives_restart_and_reload_preserves_live_members() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config");
        HubConfig::write_default(&config_path).unwrap();
        let cfg = HubConfig::load(&config_path).unwrap();
        let mut first = Router::new(cfg.clone(), [9; 16]);
        connect(&mut first, [1; 16], [11; 16], "alice");
        join(&mut first, [1; 16], [11; 16], "lobby");
        slash(&mut first, [1; 16], [11; 16], "lobby", "/register lobby");
        slash(&mut first, [1; 16], [11; 16], "lobby", "/mode lobby +m");
        slash(
            &mut first,
            [1; 16],
            [11; 16],
            "lobby",
            "/topic lobby Persistent topic",
        );

        let mut restarted = Router::new(cfg, [9; 16]);
        restarted.state.rooms = RoomRegistry::load(&restarted.config.room_registry_path).unwrap();
        let room = &restarted.state.rooms["lobby"];
        assert!(room.registered);
        assert!(room.moderated);
        assert_eq!(room.topic.as_deref(), Some("Persistent topic"));

        connect(&mut restarted, [2; 16], [22; 16], "bob");
        join(&mut restarted, [2; 16], [22; 16], "lobby");
        let mut disk = RoomRegistry::load(&restarted.config.room_registry_path).unwrap();
        disk.get_mut("lobby").unwrap().topic = Some("Changed on disk".into());
        RoomRegistry::save(&restarted.config.room_registry_path, &disk).unwrap();
        restarted.reload().unwrap();
        assert_eq!(
            restarted.state.rooms["lobby"].topic.as_deref(),
            Some("Changed on disk")
        );
        assert!(restarted.state.rooms["lobby"].members.contains(&[2; 16]));
        assert!(restarted.state.sessions[&[2; 16]].rooms.contains("lobby"));
    }

    #[test]
    fn list_returns_only_sorted_public_registered_rooms_with_topics() {
        let mut router = Router::new(config(), [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "alice");
        router.state.rooms.insert(
            "zeta".into(),
            Room {
                registered: true,
                topic: Some("Last room".into()),
                ..Room::default()
            },
        );
        router.state.rooms.insert(
            "alpha".into(),
            Room {
                registered: true,
                ..Room::default()
            },
        );
        router.state.rooms.insert(
            "secret".into(),
            Room {
                registered: true,
                private: true,
                ..Room::default()
            },
        );
        router
            .state
            .rooms
            .insert("temporary".into(), Room::default());

        let actions = slash(&mut router, [1; 16], [11; 16], "ignored", "/list");
        assert_eq!(actions.len(), 1);
        let response = action_envelope(&actions[0]);
        assert_eq!(response.integer(K_T), Some(T_NOTICE));
        assert_eq!(response.text(K_ROOM), None);
        assert_eq!(
            response.text(K_BODY),
            Some("Registered public rooms:\n  alpha\n  zeta - Last room")
        );
    }

    #[test]
    fn help_lists_available_commands_in_a_single_notice() {
        let mut router = Router::new(config(), [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "alice");

        let actions = slash(&mut router, [1; 16], [11; 16], "ignored", "/help");
        assert_eq!(actions.len(), 1);
        let response = action_envelope(&actions[0]);
        assert_eq!(response.integer(K_T), Some(T_NOTICE));
        assert_eq!(response.text(K_ROOM), None);
        let help = response.text(K_BODY).unwrap();
        for command in [
            "/help",
            "/list",
            "/who",
            "/names",
            "/topic",
            "/register",
            "/unregister",
            "/mode",
            "/op",
            "/deop",
            "/voice",
            "/devoice",
            "/invite",
            "/ban",
            "/unban",
            "/kick",
            "/stats",
            "/reload",
            "/kline",
        ] {
            assert!(help.contains(command), "help omits {command}");
        }
    }

    #[test]
    fn who_normalizes_room_and_formats_nicks_hashes_and_private_denial() {
        let mut router = Router::new(config(), [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "alice");
        connect(&mut router, [2; 16], [22; 16], "bob");
        join(&mut router, [1; 16], [11; 16], "Lobby");
        join(&mut router, [2; 16], [22; 16], "Lobby");
        router.state.set_nick([2; 16], None);

        let actions = slash(&mut router, [1; 16], [11; 16], "ignored", "/names LOBBY");
        assert_eq!(actions.len(), 1);
        assert_eq!(
            action_envelope(&actions[0]).text(K_BODY),
            Some("members in lobby: 16161616161616161616161616161616, alice (0b0b0b0b0b0b)")
        );

        router.state.rooms.get_mut("lobby").unwrap().private = true;
        let actions = slash(&mut router, [1; 16], [11; 16], "lobby", "/who");
        assert_eq!(actions.len(), 1);
        let response = action_envelope(&actions[0]);
        assert_eq!(response.integer(K_T), Some(T_NOTICE));
        assert_eq!(response.text(K_BODY), Some("room lobby is private"));
    }

    #[test]
    fn nickname_change_on_command_updates_who_and_notifies_room() {
        let mut router = Router::new(config(), [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "alice");
        connect(&mut router, [2; 16], [22; 16], "bob");
        join(&mut router, [1; 16], [11; 16], "lobby");
        join(&mut router, [2; 16], [22; 16], "lobby");

        let mut command = client(T_MSG, [11; 16]);
        command.set(K_ROOM, Value::Text("lobby".into()));
        command.set(K_BODY, Value::Text("/who lobby".into()));
        command.set_nick("alice-new");
        let actions = router.packet([1; 16], &command.encode().unwrap());

        assert_eq!(
            router.state.sessions[&[1; 16]].nick.as_deref(),
            Some("alice-new")
        );
        let who = actions
            .iter()
            .map(action_envelope)
            .find(|envelope| {
                envelope
                    .text(K_BODY)
                    .is_some_and(|body| body.starts_with("members in lobby:"))
            })
            .unwrap();
        assert!(
            who.user_list()
                .unwrap()
                .iter()
                .any(|user| user.nick.as_deref() == Some("alice-new"))
        );
        assert_eq!(
            actions
                .iter()
                .map(action_envelope)
                .filter(|envelope| {
                    envelope
                        .text(K_BODY)
                        .is_some_and(|body| body == "nick changed: alice -> alice-new")
                })
                .count(),
            2
        );
    }

    #[test]
    fn stats_require_server_operator_and_report_detailed_counters() {
        let mut cfg = config();
        cfg.trusted_identities.push([11; 16]);
        let mut router = Router::new(cfg, [9; 16]);
        connect(&mut router, [1; 16], [11; 16], "operator");
        connect(&mut router, [2; 16], [22; 16], "guest");

        let denied = slash(&mut router, [2; 16], [22; 16], "ignored", "/stats");
        assert_eq!(denied.len(), 1);
        let denied = action_envelope(&denied[0]);
        assert_eq!(denied.integer(K_T), Some(T_ERROR));
        assert_eq!(denied.text(K_BODY), Some("not authorized"));

        router.count_forwarded(T_MSG);
        router.count_forwarded(T_NOTICE);
        router.count_forwarded(T_ACTION);
        router.state.counters.pings_in = 2;
        router.state.counters.pings_out = 3;
        router.state.counters.pongs_in = 4;
        router.state.counters.pongs_out = 5;
        router.state.counters.announces = 6;
        let report = router.format_stats();
        assert!(report.contains("clients_total=2 clients_identified=2 clients_welcomed=2"));
        assert!(report.contains("msgs_fwd=1 notices_fwd=1 actions_fwd=1"));
        assert!(report.contains("pings: in=2 out=3 pongs: in=4 out=5"));
        assert!(report.contains("announces=6"));

        let allowed = slash(&mut router, [1; 16], [11; 16], "ignored", "/stats");
        assert!(!allowed.is_empty());
        let envelope = action_envelope(&allowed[0]);
        assert!(matches!(
            envelope.integer(K_T),
            Some(T_NOTICE) | Some(T_RESOURCE_ENVELOPE)
        ));
    }

    #[test]
    fn maintenance_expires_invites_and_prunes_only_stale_empty_registered_rooms() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config();
        cfg.room_registry_path = dir.path().join("rooms");
        cfg.room_registry_prune_after_s = 10.0;
        cfg.room_registry_prune_interval_s = 1.0;
        let mut router = Router::new(cfg, [9; 16]);
        let mut stale = crate::state::Room {
            registered: true,
            last_used_ts: now() - 20.0,
            ..Default::default()
        };
        stale.invited.insert([1; 16], now() - 1.0);
        router.state.rooms.insert("stale".into(), stale);
        router.state.rooms.insert(
            "active".into(),
            crate::state::Room {
                registered: true,
                last_used_ts: now() - 20.0,
                members: [[7; 16]].into_iter().collect(),
                ..Default::default()
            },
        );
        router.last_registry_prune = Instant::now() - Duration::from_secs(2);
        router.liveness_tick();
        assert!(!router.state.rooms.contains_key("stale"));
        assert!(router.state.rooms.contains_key("active"));
        let loaded = RoomRegistry::load(&router.config.room_registry_path).unwrap();
        assert!(!loaded.contains_key("stale"));
        assert!(loaded.contains_key("active"));
    }
}
