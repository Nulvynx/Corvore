"""Receive live passive Bettercap events and relay them through the spool.

No radio commands, Bettercap control endpoints, or replay-file inputs.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path
import stat
import sys
from typing import Any, Callable, Iterable

from corvore.bridge_spool import DurableEventSpool
from corvore.collector import (
    MAX_EVENT_BYTES,
    PASSIVE_WIFI_TAGS,
    parse_bettercap_event,
)


class BridgeSourceError(RuntimeError):
    """Invalid source configuration or interrupted live collection."""


def read_api_credentials(path: str | os.PathLike[str]) -> tuple[str, str]:
    """Read a private collector-owned JSON credentials file."""

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)

    try:
        info = os.fstat(fd)

        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) & 0o077
        ):
            raise BridgeSourceError("unsafe Bettercap credentials file")

        with os.fdopen(fd, "rb", closefd=False) as handle:
            encoded = handle.read(4097)

        if not encoded or len(encoded) > 4096:
            raise BridgeSourceError("invalid credentials file size")

        try:
            value = json.loads(encoded.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise BridgeSourceError(
                "credentials file is not valid JSON"
            ) from exc

        if not isinstance(value, dict) or set(value) != {
            "username", "password"
        }:
            raise BridgeSourceError("invalid credentials file fields")

        username = value["username"]
        password = value["password"]

        if (
            not isinstance(username, str)
            or not 1 <= len(username) <= 128
            or any(char in username for char in ":\r\n")
        ):
            raise BridgeSourceError("invalid API username")

        if (
            not isinstance(password, str)
            or not 1 <= len(password) <= 256
            or any(char in password for char in "\r\n")
        ):
            raise BridgeSourceError("invalid API password")

        return username, password

    finally:
        os.close(fd)


def live_bettercap_events(
    *,
    port: int,
    credentials_file: str | os.PathLike[str],
    connector: Callable[..., Any] | None = None,
) -> Iterable[bytes]:
    """Yield validated passive events from a loopback-only WebSocket."""

    if type(port) is not int or not 1024 <= port <= 65535:
        raise ValueError("invalid Bettercap API port")

    username, password = read_api_credentials(credentials_file)

    if connector is None:
        try:
            from websockets.sync.client import connect
        except ImportError as exc:
            raise BridgeSourceError(
                "Python websockets dependency is unavailable"
            ) from exc

        connector = connect

    token = base64.b64encode(
        f"{username}:{password}".encode("utf-8")
    ).decode("ascii")

    # Never accept a remote host or an implicit HTTP proxy.
    uri = f"ws://127.0.0.1:{port}/api/events"

    with connector(
        uri,
        additional_headers={
            "Authorization": f"Basic {token}",
        },
        compression=None,
        proxy=None,
        open_timeout=5,
        close_timeout=2,
        ping_interval=20,
        ping_timeout=20,
        max_size=MAX_EVENT_BYTES,
        max_queue=1,
    ) as connection:
        for message in connection:
            # Bettercap emits WebSocket text messages.
            if not isinstance(message, str):
                raise BridgeSourceError(
                    "Bettercap emitted a non-text WebSocket message"
                )

            try:
                raw = message.encode("utf-8")
            except UnicodeError as exc:
                raise BridgeSourceError(
                    "Bettercap event encoding failed"
                ) from exc

            if not raw or len(raw) > MAX_EVENT_BYTES:
                raise BridgeSourceError(
                    "Bettercap event exceeds source size limit"
                )

            try:
                envelope = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise BridgeSourceError(
                    "Bettercap event is not valid JSON"
                ) from exc

            if (
                not isinstance(envelope, dict)
                or not isinstance(envelope.get("tag"), str)
            ):
                raise BridgeSourceError(
                    "Bettercap event has an invalid envelope"
                )

            # Other Bettercap modules also emit events. Do not
            # persist their contents or turn them into observations.
            if envelope["tag"] not in PASSIVE_WIFI_TAGS:
                continue

            # Validate the complete passive event before enqueue.
            parse_bettercap_event(raw)

            yield raw


def relay_events(
    spool: DurableEventSpool,
    ingress_socket: str | os.PathLike[str],
    source_instance: str,
    events: Iterable[bytes],
) -> int:
    """Drain older events first; persist each new emission before sending."""

    spool.dispatch_pending(ingress_socket, max_events=2048)

    accepted = 0

    for raw in events:
        spool.enqueue(raw, source_instance=source_instance)
        accepted += 1

        # The event remains in SQLite if ingress or ACK fails.
        spool.dispatch_pending(ingress_socket, max_events=128)

    return accepted


def run_bridge(
    *,
    spool_dir: str | os.PathLike[str],
    ingress_socket: str | os.PathLike[str],
    source_instance: str,
    credentials_file: str | os.PathLike[str],
    api_port: int,
) -> None:
    """Run live collection. An ended stream is a service failure."""

    with DurableEventSpool(spool_dir) as spool:
        relay_events(
            spool,
            ingress_socket,
            source_instance,
            live_bettercap_events(
                port=api_port,
                credentials_file=credentials_file,
            ),
        )

    raise BridgeSourceError("Bettercap event stream ended")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="CORVORE live passive Bettercap bridge"
    )

    parser.add_argument("--spool-dir", required=True)
    parser.add_argument("--ingress-socket", required=True)
    parser.add_argument("--source-instance", required=True)
    parser.add_argument("--credentials-file", required=True)
    parser.add_argument("--api-port", type=int, default=8081)

    args = parser.parse_args()

    try:
        run_bridge(
            spool_dir=args.spool_dir,
            ingress_socket=args.ingress_socket,
            source_instance=args.source_instance,
            credentials_file=args.credentials_file,
            api_port=args.api_port,
        )
    except Exception as exc:
        # Do not print event contents, credentials, or request headers.
        print(
            f"CORVORE_BRIDGE_FAILED={type(exc).__name__}",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
