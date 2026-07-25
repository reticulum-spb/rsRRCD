#!/usr/bin/env python3
"""Live NomadNet RRC client compatibility check against an RRC hub."""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
from pathlib import Path

import RNS


def wait_until(predicate, timeout: float, description: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for {description}")


class Manager:
    def __init__(self, identity):
        self.identity = identity
        self.app = type(
            "App",
            (),
            {
                "rrc_history_per_room_cap": 0,
                "rrc_filter_loaded_history": True,
                "rrc_ephemeral_notices": 600,
            },
        )()
        self.messages = []
        self._history = tempfile.TemporaryDirectory(prefix="nomadnet-rrc-compat.")

    def get_nickname(self):
        return "nomadnet-compat"

    def save(self):
        pass

    def _notify_change(self, hub=None):
        pass

    def _notify_messages(self, hub, message):
        self.messages.append(message)

    def _on_welcome(self, hub):
        pass

    def active_room_for(self, hub):
        return None

    def _ensure_history_dir(self, hub):
        return self._history.name

    def _history_path(self, hub, room):
        return str(Path(self._history.name) / f"{room}.log")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("hub_hash")
    parser.add_argument("rns_config")
    parser.add_argument("nomadnet_source")
    parser.add_argument("--room", default="nomadnet-compat")
    args = parser.parse_args()

    sys.path.insert(0, str(Path(args.nomadnet_source).resolve()))
    from nomadnet.RRC import RRCHub

    RNS.Reticulum(configdir=args.rns_config)
    identity = RNS.Identity()
    manager = Manager(identity)
    hub = RRCHub(manager, bytes.fromhex(args.hub_hash), name="rust-compat")
    hub.connect()
    wait_until(
        lambda: hub.status == RRCHub.STATUS_CONNECTED,
        30,
        f"WELCOME ({hub.status_text})",
    )

    hub.join_room(args.room)
    wait_until(lambda: args.room in hub.rooms, 30, "JOINED")
    hub.send_ping(args.room)
    wait_until(lambda: not hub._pending_pings, 30, "PONG")

    hub.send_command("/list")
    wait_until(
        lambda: any("public rooms" in m.text.lower() for m in hub.notices),
        30,
        "LIST response",
    )
    hub.send_command(f"/who {args.room}", room=args.room)
    wait_until(
        lambda: any(
            f"members in {args.room}" in m.text.lower() for m in hub.notices
        ),
        30,
        "WHO response",
    )

    text = "NomadNet Python client to Rust hub"
    hub.send_message(args.room, text)
    hub.send_action(args.room, "tests compatibility")
    hub.send_command("/help", room=args.room)
    try:
        wait_until(
            lambda: any("/help" in m.text.lower() for m in hub.notices),
            30,
            "HELP response",
        )
    except TimeoutError:
        print("notices:", [m.text for m in hub.notices], file=sys.stderr)
        print("expectations:", hub._resource_expectations, file=sys.stderr)
        raise

    member_count = len(hub.get_members(args.room))
    hub.disconnect()
    print(
        "NOMADNET COMPAT OK: "
        f"hub={hub.hub_name} room={args.room} "
        f"members={member_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
