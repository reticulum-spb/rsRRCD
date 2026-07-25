# rsRRCD

`rsRRCD` is a standalone RRC hub daemon (server) written in Rust on top of
the adjacent [rsReticulum](../rsReticulum) workspace. It is the Rust analogue
of the Python implementation in [rrcd](../rrcd).

- License: MIT
- RRC destination namespace: `rrc.hub`
- Application and room configuration: rsReticulum
  `rns_runtime::config::Config` (ConfigObj/INI format)

## Build and test

From the `rsRRCD/` directory:

```text
cargo build --release
cargo test --offline
cargo clippy --offline --all-targets -- -D warnings
```

The resulting release binary is:

```text
target/release/rrcd-rs
```

`rsRRCD` uses path dependencies from the adjacent `rsReticulum/` workspace, so
both directories must remain next to each other when building from this source
tree.

The package also exposes the transport-independent hub core as the `rsrrcd`
library. Test harnesses and alternate frontends can drive `Router` with
identified Link IDs and apply the returned `Action` values without duplicating
room, command, persistence, or protocol logic.

## Run

Start the debug build with:

```text
cargo run
```

or run a release build directly:

```text
target/release/rrcd-rs
```

On first run, the daemon creates the following files and exits so that the
operator can review the configuration:

```text
~/.rsRRC/config
~/.rsRRC/hub_identity
~/.rsRRC/rooms
```

Run it again after editing `~/.rsRRC/config`.

To override the application state directory, set `RSRRCD_HOME`:

```text
RSRRCD_HOME=/tmp/rsRRC rrcd-rs
```

By default, Reticulum configuration is read from:

```text
~/.rsReticulum
```

This default works with a configured `shared_instance`. Override it with either
the application config or the CLI:

```text
rrcd-rs --rns-config /path/to/rsReticulum
```

Useful path overrides:

```text
rrcd-rs --config /path/to/config
rrcd-rs --identity /path/to/hub_identity
rrcd-rs --room-registry /path/to/rooms
```

The destination namespace is fixed at `rrc.hub`, allowing clients to discover
hubs consistently. Startup and periodic announces include CBOR app-data:

```text
{"proto": "rrc", "v": 1, "hub": "<hub_name>"}
```

Print the destination hash and exit with:

```text
rrcd-rs --print-destination
```

Run `rrcd-rs --help` for all CLI overrides.

## Configuration

Configuration is deliberately read and written through rsReticulum's standard
ConfigObj implementation. It is not TOML.

A generated configuration looks like:

```ini
[hub]
configdir =
identity_path = hub_identity
room_registry_path = rooms

announce_on_start = yes
announce_period_s = 0
hub_name = rrc
greeting =

trusted_identities = ,
banned_identities = ,

include_joined_member_list = no
room_invite_timeout_s = 900
room_registry_prune_after_s = 2592000
room_registry_prune_interval_s = 3600

max_nick_bytes = 32
max_room_name_bytes = 64
max_msg_body_bytes = 350
max_rooms_per_session = 32
rate_limit_msgs_per_minute = 240

ping_interval_s = 0
ping_timeout_s = 0

enable_resource_transfer = yes
max_resource_bytes = 262144
max_pending_resource_expectations = 8
resource_expectation_ttl_s = 30

[logging]
level = INFO
console = yes
file =
```

Relative `identity_path`, `room_registry_path`, and logging file paths are
resolved relative to the application config file.

Identity lists contain full 16-byte Reticulum identity hashes in hexadecimal.
Invalid entries cause startup or `/reload` to fail instead of silently
weakening policy.

## Logging

By default, `rsRRCD` logs to stderr, which works well with systemd/journald.

```ini
[logging]
level = INFO
console = yes
file =
```

To write to both stderr and a file:

```ini
[logging]
level = DEBUG
console = yes
file = rrcd-rs.log
```

CLI overrides:

```text
rrcd-rs --log-level DEBUG
rrcd-rs --log-file ~/.rsRRC/rrcd-rs.log
rrcd-rs -v
rrcd-rs -vv
```

`/reload` applies a changed log level immediately. Changing the log destination
(`console` or `file`) takes effect on the next process start.

## Compatibility

`rsRRCD` implements the core RRC protocol used by the Python daemon:

- `HELLO` / `WELCOME`
- `JOIN` / `JOINED`
- `PART` / `PARTED`
- room `MSG`, `NOTICE`, and `ACTION`
- direct identity-addressed `NOTICE`
- `PING` / `PONG`
- `ERROR`
- `RESOURCE_ENVELOPE` followed by a native Reticulum Resource

Protocol alignment notes:

- Envelopes are CBOR maps with unsigned integer keys.
- The required envelope fields are validated before routing.
- Unknown non-negative extension keys are accepted.
- Capabilities are advertised in WELCOME body key `2` as a CBOR map.
- WELCOME advertises ACTION, direct NOTICE, and Resource support.
- A greeting/MOTD is delivered after WELCOME, rather than being embedded in
  WELCOME.
- JOIN is followed by a NOTICE containing registration state, room modes, and
  the topic.
- Commands and mode flags are case-insensitive.
- ACTION bodies are forwarded as room content and are not interpreted as slash
  commands.

JOINED/PARTED handling is link-aware. A repeated JOIN does not duplicate the
joining link's notification, departure includes the cached nickname, and
closing one link does not announce an identity as departed while another link
for that identity remains in the room.

## Extensions

`rsRRCD` does not require extra operator-specific on-wire message types. A
recognized slash command in a string `MSG` or `NOTICE` body is handled locally
by the hub and is not forwarded.

### Optional nickname

Envelope key `K_NICK = 7` carries an optional nickname associated with
`K_SRC`. Nicknames are display hints, not identities.

The hub trims nicknames and accepts them only if they:

- are non-empty;
- contain no newline, carriage return, or NUL;
- fit within `max_nick_bytes` UTF-8 bytes.

Clients must fall back to the identity in `K_SRC` when `K_NICK` is absent.

### Direct NOTICE

Envelope key `K_DST = 8` may contain the full destination identity hash for a
client-to-client NOTICE. The hub delivers it to exactly one live link for that
identity and preserves `K_DST` in the forwarded envelope.

A direct NOTICE must not also contain `K_ROOM`. Mixed room/direct semantics are
rejected with ERROR. If one link for the identity closes, delivery falls back
to another live link.

Support is advertised as `CAP_DIRECT_NOTICE = 2`.

### Large payload transfer

Large payloads use a two-step protocol:

1. A `RESOURCE_ENVELOPE` packet announces the resource ID, kind, size, optional
   encoding, and optional SHA-256 digest.
2. The payload is transferred as a native rsReticulum Resource.

Supported kinds:

- `notice`: UTF-8 NOTICE text;
- `motd`: hub greeting/MOTD;
- `blob`: generic binary data.

Safety controls:

- a Resource must match a recent per-link expectation;
- expectation count and lifetime are bounded;
- payload size is limited by `max_resource_bytes`;
- declared size and SHA-256 are verified;
- unexpected, expired, oversized, or malformed resources are rejected.

When native Resource transfer is disabled, or generated NOTICE text exceeds
the configured Resource limit, text falls back to UTF-8-safe packet chunks.

## Commands

### Client discovery commands

- `/list` — list sorted public registered rooms and their topics.
- `/who [room]` or `/names [room]` — list room members as nickname/hash.

Private rooms (`+p`) are omitted from `/list`. Their membership is visible only
to identities in `trusted_identities`.

### Server operator commands

These require the caller's full identity hash in `trusted_identities`:

- `/stats` — show uptime, clients, rooms, memberships, busiest rooms, trust
  counts, limits, packet/byte counters, MSG/NOTICE/ACTION counters, ping/pong,
  announce, and Resource totals.
- `/reload` — reload application policy and the room registry without dropping
  active sessions or memberships.
- `/kline add <nick|hashprefix|hash>` — add and persist a global identity ban;
  all active links for that identity are closed.
- `/kline del <hash>` — remove and persist a global identity ban.
- `/kline list` — list global bans.

### Room commands

Room founder/operator commands:

- `/kick <room> <nick|hashprefix>`
- `/register <room>`
- `/unregister <room>`
- `/topic <room> [topic]`
- `/mode <room> +m|-m`
- `/mode <room> +i|-i`
- `/mode <room> +k <key>`
- `/mode <room> -k`
- `/mode <room> +p|-p`
- `/mode <room> +t|-t`
- `/mode <room> +n|-n`
- `/mode <room> +o|-o <nick|hashprefix|hash>`
- `/mode <room> +v|-v <nick|hashprefix|hash>`
- `/op <room> <nick|hashprefix|hash>`
- `/deop <room> <nick|hashprefix|hash>`
- `/voice <room> <nick|hashprefix|hash>`
- `/devoice <room> <nick|hashprefix|hash>`
- `/ban <room> add|del|list [nick|hashprefix|hash]`
- `/unban <room> <nick|hashprefix|hash>`
- `/invite <room> add|del|list [nick|hashprefix|hash]`

`+r` is read-only through `/mode`; use `/register` and `/unregister`.

Registration is founder-only and requires the founder to be present in the
room. Registering enables `+nrt`, records the founder as an operator, and
persists the room.

Invites are sent to a currently connected target. For `+i` and `+k` rooms, the
hub also stores an expiring identity-bound invitation. A `+k` invitation allows
JOIN without the key. Invitations are consumed on successful JOIN and removed
after `room_invite_timeout_s`.

Ambiguous nicknames or hash prefixes are never resolved arbitrarily. The hub
returns the matching identities and asks the operator to use a longer hash.

## Room registry format

The default registry is `~/.rsRRC/rooms`. It is maintained with the same
rsReticulum ConfigObj writer as the application configuration.

Example:

```ini
[rooms]

[[lobby]]
founder = 0123456789abcdef0123456789abcdef
topic = Welcome
moderated = no
invite_only = no
private = no
topic_ops_only = yes
no_outside_msgs = yes
operators = 0123456789abcdef0123456789abcdef
voiced = ,
bans = ,
invited = ,
last_used_ts = 1730000000.0
```

Supported per-room fields:

- `founder`: full identity hash;
- `topic`: optional room topic;
- `moderated`: `+m`;
- `invite_only`: `+i`;
- `private`: `+p`;
- `topic_ops_only`: `+t`;
- `no_outside_msgs`: `+n`;
- `key`: optional `+k` key;
- `operators`: identity list;
- `voiced`: identity list;
- `bans`: identity list;
- `invited`: comma-separated `identity_hash:expiry_timestamp` entries;
- `last_used_ts`: Unix timestamp used for pruning.

Registered-room state, modes, invites, bans, operators, voice, topics, keys,
and activity timestamps survive restart. `/reload` merges disk policy into the
live room map while preserving active memberships.

Empty registered rooms older than `room_registry_prune_after_s` are removed at
`room_registry_prune_interval_s`. Rooms with live members are retained.

## Security and threat model

Assumptions:

- Reticulum link establishment and remote identification are authoritative for
  peer identity.
- The host, application config, room registry, and hub private identity are
  trusted by the operator.

The daemon aims to provide:

- HELLO/WELCOME gating before room operations;
- authoritative source identity on forwarded envelopes;
- per-link token-bucket rate limiting;
- room, nickname, message, and Resource limits;
- global and room-local identity bans;
- moderated, invite-only, keyed, private, topic-restricted, and
  no-outside-message room modes;
- expectation-bound and integrity-checked Resource reception.

Non-goals:

- complete protection against denial of service;
- protection from a malicious trusted operator;
- confidentiality from the hub itself;
- hiding Reticulum/network metadata outside the hub's control.

Operational guidance:

- protect `~/.rsRRC/hub_identity` as a private key;
- treat `trusted_identities` as administrator credentials;
- keep the config and room registry writable only by the hub operator;
- run the daemon under a dedicated unprivileged OS account;
- do not run it as root.

## Live end-to-end test

The live test uses the configured Reticulum shared instance:

```text
tests/live_e2e.sh ~/.rsReticulum
```

It performs:

- real Link establishment and identification;
- HELLO/WELCOME and room JOIN;
- room registration, `+m`, and topic persistence;
- MSG round trip;
- a 4096-byte native Resource forwarded through the hub;
- rsRRC-client WELCOME/JOIN and large-message Resource round trip;
- graceful daemon shutdown and restart;
- destination identity, room mode, and topic verification after restart.

The test needs permission to create/use the local shared-instance socket and
the interfaces configured in rsReticulum. Link establishment may occasionally
race with shared-instance path propagation; rerunning the test is safe.

## Troubleshooting

### Link closes or times out during establishment

Confirm that:

- `~/.rsReticulum/config` exists and its shared instance is running;
- the hub and client use the same Reticulum configuration;
- no stale process owns the shared-instance socket;
- the destination has been announced and a path is available.

The current shared-instance transport can occasionally close a newly
established Link during path propagation. A clean immediate retry normally
succeeds.

### Client times out waiting for WELCOME

Enable debug logging:

```text
rrcd-rs --log-level DEBUG
```

Check that the client identifies the Link and sends a valid HELLO envelope.
Large greetings are sent after WELCOME as a native Resource when enabled, so
they do not inflate the WELCOME packet.
