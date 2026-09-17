"""Passive Bettercap event normalization boundary."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import re
from typing import Any
import uuid


MAX_EVENT_BYTES = 64 * 1024
MAX_DATA_DEPTH = 16
MAX_CONTAINER_ITEMS = 1024
MAX_STRING_CHARS = 4096
MAX_KEY_CHARS = 128

MIN_JSON_INTEGER = -(2 ** 63)
MAX_JSON_INTEGER = (2 ** 63) - 1

PASSIVE_WIFI_TAGS = frozenset({
    "wifi.ap.new",
    "wifi.ap.lost",
    "wifi.client.new",
    "wifi.client.lost",
    "wifi.client.probe",
    "wifi.client.deauthentication",
    "wifi.client.handshake",
})

_IDENTIFIER = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
)


class CollectorEventError(ValueError):
    """A collector event violates the ingress contract."""


@dataclass(frozen=True)
class BettercapEvent:
    tag: str
    source_time: str
    data: dict[str, Any]
    canonical_sha256: str


def _validate_identifier(
    name: str,
    value: str,
) -> str:
    if (
        not isinstance(value, str)
        or not _IDENTIFIER.fullmatch(value)
    ):
        raise CollectorEventError(
            f"{name} has invalid syntax or length"
        )

    return value


def _reject_json_constant(
    value: str,
) -> None:
    raise CollectorEventError(
        f"non-finite JSON constant rejected: {value}"
    )


def _validate_value(
    value: Any,
    *,
    depth: int = 0,
) -> None:
    if depth > MAX_DATA_DEPTH:
        raise CollectorEventError(
            "event data exceeds maximum nesting depth"
        )

    if value is None:
        return

    if isinstance(value, bool):
        return

    if isinstance(value, str):
        if len(value) > MAX_STRING_CHARS:
            raise CollectorEventError(
                "event string exceeds maximum length"
            )
        return

    if isinstance(value, int):
        if not (
            MIN_JSON_INTEGER
            <= value
            <= MAX_JSON_INTEGER
        ):
            raise CollectorEventError(
                "event integer outside signed 64-bit range"
            )
        return

    if isinstance(value, float):
        if not math.isfinite(value):
            raise CollectorEventError(
                "event float must be finite"
            )
        return

    if isinstance(value, list):
        if len(value) > MAX_CONTAINER_ITEMS:
            raise CollectorEventError(
                "event list exceeds maximum item count"
            )

        for item in value:
            _validate_value(
                item,
                depth=depth + 1,
            )

        return

    if isinstance(value, dict):
        if len(value) > MAX_CONTAINER_ITEMS:
            raise CollectorEventError(
                "event object exceeds maximum item count"
            )

        for key, item in value.items():
            if not isinstance(key, str):
                raise CollectorEventError(
                    "event object keys must be strings"
                )

            if len(key) > MAX_KEY_CHARS:
                raise CollectorEventError(
                    "event object key exceeds maximum length"
                )

            _validate_value(
                item,
                depth=depth + 1,
            )

        return

    raise CollectorEventError(
        "event contains unsupported JSON value type"
    )


def parse_bettercap_event(
    raw: bytes | str,
) -> BettercapEvent:
    if isinstance(raw, bytes):
        encoded = raw

        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise CollectorEventError(
                "event is not valid UTF-8"
            ) from exc

    elif isinstance(raw, str):
        text = raw

        try:
            encoded = raw.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise CollectorEventError(
                "event cannot be encoded as UTF-8"
            ) from exc

    else:
        raise CollectorEventError(
            "event must be bytes or text"
        )

    if not encoded:
        raise CollectorEventError(
            "event must not be empty"
        )

    if len(encoded) > MAX_EVENT_BYTES:
        raise CollectorEventError(
            "event exceeds 64 KiB"
        )

    try:
        value = json.loads(
            text,
            parse_constant=_reject_json_constant,
        )
    except CollectorEventError:
        raise
    except (
        json.JSONDecodeError,
        UnicodeError,
    ) as exc:
        raise CollectorEventError(
            "event is not valid JSON"
        ) from exc

    if not isinstance(value, dict):
        raise CollectorEventError(
            "event root must be an object"
        )

    if set(value) != {
        "tag",
        "time",
        "data",
    }:
        raise CollectorEventError(
            "event envelope fields are invalid"
        )

    tag = value["tag"]
    source_time = value["time"]
    data = value["data"]

    if (
        not isinstance(tag, str)
        or tag not in PASSIVE_WIFI_TAGS
    ):
        raise CollectorEventError(
            "event tag is not an allowed passive WiFi event"
        )

    if (
        not isinstance(source_time, str)
        or not 1 <= len(source_time) <= 64
        or any(
            ord(character) < 0x20
            for character in source_time
        )
    ):
        raise CollectorEventError(
            "event source time is invalid"
        )

    if not isinstance(data, dict):
        raise CollectorEventError(
            "event data must be an object"
        )

    _validate_value(data)

    canonical = json.dumps(
        {
            "data": data,
            "tag": tag,
            "time": source_time,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

    if len(canonical) > MAX_EVENT_BYTES:
        raise CollectorEventError(
            "canonical event exceeds 64 KiB"
        )

    return BettercapEvent(
        tag=tag,
        source_time=source_time,
        data=data,
        canonical_sha256=hashlib.sha256(
            canonical
        ).hexdigest(),
    )


def _validate_delivery_id(value: str) -> str:
    if not isinstance(value, str):
        raise CollectorEventError("delivery_id must be a UUIDv4 string")

    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise CollectorEventError("delivery_id must be a UUIDv4 string") from exc

    if parsed.version != 4 or str(parsed) != value:
        raise CollectorEventError("delivery_id must be a canonical UUIDv4")

    return value


def deterministic_observation_id(
    *,
    boot_id: str,
    source_instance: str,
    delivery_id: str,
) -> str:
    boot_id = _validate_identifier("boot_id", boot_id)
    source_instance = _validate_identifier("source_instance", source_instance)
    delivery_id = _validate_delivery_id(delivery_id)

    # A new source emission gets a new ID even if payload bytes match.
    # A retransmission MUST reuse its original delivery_id.
    name = f"corvore:bettercap:{boot_id}:{source_instance}:{delivery_id}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, name))


def build_observation(
    event: BettercapEvent,
    *,
    boot_id: str,
    source_instance: str,
    delivery_id: str,
    monotonic_ns: int,
) -> dict[str, Any]:
    boot_id = _validate_identifier(
        "boot_id",
        boot_id,
    )

    source_instance = _validate_identifier(
        "source_instance",
        source_instance,
    )

    if (
        not isinstance(monotonic_ns, int)
        or isinstance(monotonic_ns, bool)
        or monotonic_ns < 0
    ):
        raise CollectorEventError(
            "monotonic_ns must be a non-negative integer"
        )

    return {
        "boot_id": boot_id,
        "monotonic_ns": monotonic_ns,
        "source_kind": "bettercap",
        "source_instance": source_instance,
        "observation_kind": event.tag,
        "payload": {
            "schema_version": 1,
            "source": {
                "tag": event.tag,
                "time": event.source_time,
                "sha256": event.canonical_sha256,
            },
            "data": event.data,
        },
        # Bettercap's wall-clock timestamp is upstream
        # evidence, not a verified CORVORE TimeProvider value.
        "observed_at_utc": None,
        "clock_accuracy_verified": False,
        "observation_id":
            deterministic_observation_id(
                boot_id=boot_id,
                source_instance=source_instance,
                delivery_id=delivery_id,
            ),
    }
