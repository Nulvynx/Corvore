"""Bounded Unix-domain ingress for passive collection."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import socket
import stat
import struct
import threading
import time
from typing import Any
import uuid

from corvore.collector import (
    CollectorEventError,
    MAX_EVENT_BYTES,
    build_observation,
    parse_bettercap_event,
)
from corvore.storage import (
    append_observation_idempotent,
    ObservationIntegrityError,
)


INGRESS_SCHEMA_VERSION = 2

MAX_HEADER_BYTES = 1024

MAX_INGRESS_FRAME_BYTES = (
    MAX_EVENT_BYTES
    + MAX_HEADER_BYTES
    + 1
)

MAX_RESPONSE_BYTES = 4096

_IDENTIFIER = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
)


class IngressError(RuntimeError):
    """Passive ingress runtime failure."""


class IngressProtocolError(ValueError):
    """Passive ingress frame violates the protocol."""


def _validate_source_instance(
    value: str,
) -> str:
    if (
        not isinstance(value, str)
        or not _IDENTIFIER.fullmatch(value)
    ):
        raise IngressProtocolError(
            "source_instance is invalid"
        )

    return value


def _validate_delivery_id(value: str) -> str:
    if not isinstance(value, str):
        raise IngressProtocolError("delivery_id must be a UUIDv4 string")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise IngressProtocolError("delivery_id must be a UUIDv4 string") from exc
    if parsed.version != 4 or str(parsed) != value:
        raise IngressProtocolError("delivery_id must be a canonical UUIDv4")
    return value


def _event_bytes(
    raw_event: bytes | str,
) -> bytes:
    if isinstance(raw_event, bytes):
        encoded = raw_event

    elif isinstance(raw_event, str):
        try:
            encoded = raw_event.encode(
                "utf-8"
            )
        except UnicodeEncodeError as exc:
            raise IngressProtocolError(
                "event cannot be encoded as UTF-8"
            ) from exc

    else:
        raise IngressProtocolError(
            "event must be bytes or text"
        )

    if not encoded:
        raise IngressProtocolError(
            "event must not be empty"
        )

    if len(encoded) > MAX_EVENT_BYTES:
        raise IngressProtocolError(
            "event exceeds source size limit"
        )

    return encoded


def encode_event_frame(
    raw_event: bytes | str,
    *,
    source_instance: str,
    delivery_id: str,
) -> bytes:
    source_instance = (
        _validate_source_instance(
            source_instance
        )
    )

    delivery_id = _validate_delivery_id(delivery_id)
    event = _event_bytes(raw_event)

    header = json.dumps(
        {
            "message_type":
                "bettercap_event",
            "schema_version":
                INGRESS_SCHEMA_VERSION,
            "source_instance":
                source_instance,
            "delivery_id":
                delivery_id,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

    if len(header) > MAX_HEADER_BYTES:
        raise IngressProtocolError(
            "ingress header exceeds size limit"
        )

    frame = (
        header
        + b"\n"
        + event
    )

    if len(frame) > MAX_INGRESS_FRAME_BYTES:
        raise IngressProtocolError(
            "ingress frame exceeds size limit"
        )

    return frame


def decode_event_frame(
    frame: bytes,
) -> tuple[str, str, bytes]:
    if not isinstance(frame, bytes):
        raise IngressProtocolError(
            "ingress frame must be bytes"
        )

    if (
        not frame
        or len(frame)
        > MAX_INGRESS_FRAME_BYTES
    ):
        raise IngressProtocolError(
            "ingress frame size is invalid"
        )

    (
        header_raw,
        separator,
        event,
    ) = frame.partition(b"\n")

    if not separator:
        raise IngressProtocolError(
            "ingress header separator missing"
        )

    if (
        not header_raw
        or len(header_raw)
        > MAX_HEADER_BYTES
    ):
        raise IngressProtocolError(
            "ingress header size is invalid"
        )

    event = _event_bytes(event)

    try:
        header = json.loads(
            header_raw.decode("utf-8")
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise IngressProtocolError(
            "ingress header is invalid JSON"
        ) from exc

    if not isinstance(header, dict):
        raise IngressProtocolError(
            "ingress header must be an object"
        )

    if set(header) != {
        "message_type",
        "schema_version",
        "source_instance",
        "delivery_id",
    }:
        raise IngressProtocolError(
            "ingress header fields are invalid"
        )

    schema_version = header[
        "schema_version"
    ]

    if (
        not isinstance(
            schema_version,
            int,
        )
        or isinstance(
            schema_version,
            bool,
        )
        or schema_version
        != INGRESS_SCHEMA_VERSION
    ):
        raise IngressProtocolError(
            "unsupported ingress schema"
        )

    if (
        header["message_type"]
        != "bettercap_event"
    ):
        raise IngressProtocolError(
            "unsupported ingress message type"
        )

    source_instance = (
        _validate_source_instance(
            header["source_instance"]
        )
    )

    delivery_id = _validate_delivery_id(header["delivery_id"])

    return (
        source_instance,
        delivery_id,
        event,
    )


def _response_bytes(
    payload: dict[str, Any],
) -> bytes:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

    if len(encoded) > MAX_RESPONSE_BYTES:
        raise IngressError(
            "ingress response exceeds limit"
        )

    return encoded


def decode_response(
    raw: bytes,
) -> dict[str, Any]:
    if (
        not isinstance(raw, bytes)
        or not raw
        or len(raw) > MAX_RESPONSE_BYTES
    ):
        raise IngressProtocolError(
            "ingress response size invalid"
        )

    try:
        value = json.loads(
            raw.decode("utf-8")
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise IngressProtocolError(
            "ingress response is invalid"
        ) from exc

    if not isinstance(value, dict):
        raise IngressProtocolError(
            "ingress response must be an object"
        )

    schema_version = value.get(
        "schema_version"
    )

    if (
        not isinstance(
            schema_version,
            int,
        )
        or isinstance(
            schema_version,
            bool,
        )
        or schema_version
        != INGRESS_SCHEMA_VERSION
    ):
        raise IngressProtocolError(
            "ingress response schema invalid"
        )

    status = value.get("status")

    if status == "rejected":
        if set(value) != {
            "schema_version",
            "status",
        }:
            raise IngressProtocolError(
                "rejected response fields invalid"
            )

        return value

    if status == "committed":
        if set(value) != {
            "schema_version",
            "status",
            "ingest_seq",
            "observation_id",
        }:
            raise IngressProtocolError(
                "committed response fields invalid"
            )

        if (
            not isinstance(
                value["ingest_seq"],
                int,
            )
            or isinstance(
                value["ingest_seq"],
                bool,
            )
            or value["ingest_seq"] < 1
        ):
            raise IngressProtocolError(
                "committed ingest sequence invalid"
            )

        if not isinstance(
            value["observation_id"],
            str,
        ):
            raise IngressProtocolError(
                "committed observation ID invalid"
            )

        return value

    raise IngressProtocolError(
        "ingress response status invalid"
    )


def _peer_uid(
    connection: socket.socket,
) -> int:
    option = getattr(
        socket,
        "SO_PEERCRED",
        None,
    )

    if option is None:
        raise IngressError(
            "SO_PEERCRED is unavailable"
        )

    size = struct.calcsize("3i")

    raw = connection.getsockopt(
        socket.SOL_SOCKET,
        option,
        size,
    )

    _pid, uid, _gid = struct.unpack(
        "3i",
        raw,
    )

    return int(uid)


class IngressServer:
    """SOCK_SEQPACKET passive event ingress."""

    def __init__(
        self,
        *,
        state_dir: str | os.PathLike[str],
        socket_path: str | os.PathLike[str],
        boot_id: str,
        allowed_uid: int,
        stop_event: threading.Event,
        socket_gid: int | None = None,
    ) -> None:
        if (
            not isinstance(allowed_uid, int)
            or isinstance(allowed_uid, bool)
            or allowed_uid < 0
        ):
            raise ValueError(
                "allowed_uid is invalid"
            )

        if (
            socket_gid is not None
            and (
                not isinstance(socket_gid, int)
                or isinstance(socket_gid, bool)
                or socket_gid < 0
            )
        ):
            raise ValueError(
                "socket_gid is invalid"
            )

        self.state_dir = Path(state_dir)
        self.socket_path = Path(
            socket_path
        )
        self.boot_id = boot_id
        self.allowed_uid = allowed_uid
        self.socket_gid = socket_gid
        self.stop_event = stop_event

        self.ready_event = threading.Event()
        self.failure: Exception | None = None

    def _prepare_socket_path(
        self,
    ) -> None:
        parent = self.socket_path.parent

        if parent.is_symlink():
            raise IngressError(
                "ingress directory must not "
                "be a symlink"
            )

        parent.mkdir(
            parents=True,
            exist_ok=True,
            mode=0o750,
        )

        if not parent.is_dir():
            raise IngressError(
                "ingress parent is not "
                "a directory"
            )

        if self.socket_gid is not None:
            os.chown(
                parent,
                -1,
                self.socket_gid,
            )

            os.chmod(
                parent,
                0o750,
            )

        try:
            existing = (
                self.socket_path.lstat()
            )
        except FileNotFoundError:
            return

        if stat.S_ISLNK(
            existing.st_mode
        ):
            raise IngressError(
                "ingress socket path must "
                "not be a symlink"
            )

        if not stat.S_ISSOCK(
            existing.st_mode
        ):
            raise IngressError(
                "ingress socket path exists "
                "and is not a socket"
            )

        self.socket_path.unlink()

    def _cleanup_socket(
        self,
    ) -> None:
        try:
            existing = (
                self.socket_path.lstat()
            )
        except FileNotFoundError:
            return

        if stat.S_ISSOCK(
            existing.st_mode
        ):
            self.socket_path.unlink()

    @staticmethod
    def _send_response(
        connection: socket.socket,
        payload: dict[str, Any],
    ) -> bool:
        encoded = _response_bytes(
            payload
        )

        try:
            sent = connection.send(
                encoded
            )
        except OSError:
            return False

        return sent == len(encoded)

    def _serve_connection(
        self,
        connection: socket.socket,
    ) -> None:
        if (
            _peer_uid(connection)
            != self.allowed_uid
        ):
            return

        # One source event per connection prevents
        # an authorized but faulty peer from holding
        # the ingress worker indefinitely.
        connection.settimeout(1.0)

        try:
            (
                frame,
                _ancillary,
                flags,
                _address,
            ) = connection.recvmsg(
                MAX_INGRESS_FRAME_BYTES
            )

        except (
            socket.timeout,
            OSError,
        ):
            return

        if not frame:
            return

        if flags & socket.MSG_TRUNC:
            self._send_response(
                connection,
                {
                    "schema_version":
                        INGRESS_SCHEMA_VERSION,
                    "status":
                        "rejected",
                },
            )
            return

        try:
            (
                source_instance,
                delivery_id,
                raw_event,
            ) = decode_event_frame(
                frame
            )

            event = parse_bettercap_event(
                raw_event
            )

            observation = build_observation(
                event,
                boot_id=self.boot_id,
                source_instance=(
                    source_instance
                ),
                delivery_id=delivery_id,
                monotonic_ns=(
                    time.monotonic_ns()
                ),
            )

        except (
            IngressProtocolError,
            CollectorEventError,
        ):
            self._send_response(
                connection,
                {
                    "schema_version":
                        INGRESS_SCHEMA_VERSION,
                    "status":
                        "rejected",
                },
            )
            return

        try:
            result = append_observation_idempotent(
                self.state_dir,
                **observation,
            )
        except ObservationIntegrityError:
            # Reusing a delivery ID with different content is a
            # protocol violation, not a reason to stop the daemon.
            self._send_response(
                connection,
                {
                    "schema_version": INGRESS_SCHEMA_VERSION,
                    "status": "rejected",
                },
            )
            return

        # Persistence has already committed here.
        # If this response is lost, deterministic
        # identity makes retransmission idempotent.
        self._send_response(
            connection,
            {
                "schema_version":
                    INGRESS_SCHEMA_VERSION,
                "status":
                    "committed",
                "ingest_seq":
                    result["ingest_seq"],
                "observation_id":
                    result[
                        "observation_id"
                    ],
            },
        )

    def _serve(
        self,
    ) -> None:
        self._prepare_socket_path()

        server = socket.socket(
            socket.AF_UNIX,
            socket.SOCK_SEQPACKET,
        )

        try:
            server.bind(
                str(self.socket_path)
            )

            os.chmod(
                self.socket_path,
                0o660,
            )

            if self.socket_gid is not None:
                os.chown(
                    self.socket_path,
                    -1,
                    self.socket_gid,
                )

            server.listen(8)
            server.settimeout(0.2)

            self.ready_event.set()

            while not self.stop_event.is_set():
                try:
                    connection, _ = (
                        server.accept()
                    )
                except socket.timeout:
                    continue

                except OSError:
                    if self.stop_event.is_set():
                        return
                    raise

                with connection:
                    self._serve_connection(
                        connection
                    )

        finally:
            server.close()
            self._cleanup_socket()

    def run(self) -> None:
        try:
            self._serve()

        except Exception as exc:
            self.failure = exc
            self.stop_event.set()

        finally:
            self.ready_event.set()


def send_event(
    socket_path: str | os.PathLike[str],
    raw_event: bytes | str,
    *,
    source_instance: str,
    delivery_id: str,
    timeout: float = 2.0,
) -> dict[str, Any]:
    frame = encode_event_frame(
        raw_event,
        source_instance=source_instance,
        delivery_id=delivery_id,
    )

    client = socket.socket(
        socket.AF_UNIX,
        socket.SOCK_SEQPACKET,
    )

    client.settimeout(timeout)

    try:
        try:
            client.connect(
                str(socket_path)
            )

            sent = client.send(frame)

            if sent != len(frame):
                raise IngressProtocolError(
                    "partial ingress send"
                )

            (
                response,
                _ancillary,
                flags,
                _address,
            ) = client.recvmsg(
                MAX_RESPONSE_BYTES
            )

        except OSError as exc:
            raise IngressProtocolError(
                "ingress transport failed"
            ) from exc

        if flags & socket.MSG_TRUNC:
            raise IngressProtocolError(
                "ingress response truncated"
            )

        if not response:
            raise IngressProtocolError(
                "ingress closed without response"
            )

        return decode_response(
            response
        )

    finally:
        client.close()
