"""Split SQLite persistence foundation for CORVORE."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
from importlib import resources
import json
import math
import os
from pathlib import Path
import re
import sqlite3
from typing import Any
from urllib.parse import quote
import uuid


CORE_DB_NAME = "core.db"
OBSERVATIONS_DB_NAME = "observations.db"

CORE_APPLICATION_ID = 0x43565243
OBSERVATIONS_APPLICATION_ID = 0x4356524F

MAX_PAYLOAD_BYTES = 1024 * 1024
MAX_READ_LIMIT = 1000
MAX_JSON_DEPTH = 32
MIN_JSON_INTEGER = -(2 ** 63)
MAX_JSON_INTEGER = (2 ** 63) - 1

_IDENTIFIER = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
)

_MIGRATION_NAME = re.compile(
    r"^([0-9]{4})_([a-z0-9_]+)\.sql$"
)


class StorageError(RuntimeError):
    """Base persistence error."""


class MigrationError(StorageError):
    """Migration state is inconsistent."""


class ObservationIntegrityError(StorageError):
    """Stored observation failed integrity validation."""


class ProcessingSequenceError(StorageError):
    """Observation processing order is invalid."""


class ObservationNotFoundError(StorageError):
    """Referenced observation does not exist."""


class OutboxEventNotFoundError(StorageError):
    """Referenced outbox event does not exist."""


@dataclass(frozen=True)
class OutboxEvent:
    event_id: str
    topic: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class DatabasePaths:
    state_dir: Path
    core: Path
    observations: Path

    @classmethod
    def from_state_dir(
        cls,
        state_dir: str | os.PathLike[str],
    ) -> "DatabasePaths":
        root = Path(state_dir)

        return cls(
            state_dir=root,
            core=root / CORE_DB_NAME,
            observations=root / OBSERVATIONS_DB_NAME,
        )


_ROLE = {
    "core": {
        "application_id": CORE_APPLICATION_ID,
        "synchronous": "FULL",
    },
    "observations": {
        "application_id":
            OBSERVATIONS_APPLICATION_ID,
        "synchronous": "NORMAL",
    },
}


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _validate_identifier(
    name: str,
    value: str,
    *,
    min_length: int = 1,
) -> str:
    if not isinstance(value, str):
        raise ValueError(
            f"{name} must be a string"
        )

    if (
        len(value) < min_length
        or not _IDENTIFIER.fullmatch(value)
    ):
        raise ValueError(
            f"{name} has invalid syntax "
            "or length"
        )

    return value


def _validate_json_value(
    value: Any,
    *,
    depth: int = 0,
) -> None:
    if depth > MAX_JSON_DEPTH:
        raise ValueError(
            "payload exceeds maximum "
            "JSON nesting depth"
        )

    if value is None:
        return

    if isinstance(value, bool):
        return

    if isinstance(value, str):
        return

    if isinstance(value, int):
        if not (
            MIN_JSON_INTEGER
            <= value
            <= MAX_JSON_INTEGER
        ):
            raise ValueError(
                "JSON integer outside "
                "signed 64-bit range"
            )
        return

    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(
                "JSON float must be finite"
            )
        return

    if isinstance(value, list):
        for item in value:
            _validate_json_value(
                item,
                depth=depth + 1,
            )
        return

    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(
                    "JSON object keys must "
                    "be strings"
                )

            _validate_json_value(
                item,
                depth=depth + 1,
            )
        return

    raise ValueError(
        "payload contains unsupported "
        "JSON value type"
    )


def _canonical_payload(
    payload: dict[str, Any],
) -> tuple[str, str]:
    if not isinstance(payload, dict):
        raise ValueError(
            "payload must be a JSON object"
        )

    _validate_json_value(payload)

    try:
        raw = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")

    except (
        TypeError,
        ValueError,
        UnicodeError,
    ) as exc:
        raise ValueError(
            "payload cannot be encoded "
            "as deterministic JSON"
        ) from exc

    if len(raw) > MAX_PAYLOAD_BYTES:
        raise ValueError(
            "payload exceeds 1 MiB"
        )

    return (
        raw.decode("utf-8"),
        hashlib.sha256(raw).hexdigest(),
    )


def _normalize_observed_at_utc(
    value: str | None,
    *,
    clock_accuracy_verified: bool,
) -> str | None:
    if value is None:
        if clock_accuracy_verified:
            raise ValueError(
                "verified clock requires "
                "observed_at_utc"
            )

        return None

    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 64
    ):
        raise ValueError(
            "observed_at_utc must be "
            "null or bounded text"
        )

    candidate = value

    if candidate.endswith("Z"):
        candidate = (
            candidate[:-1]
            + "+00:00"
        )

    try:
        parsed = datetime.fromisoformat(
            candidate
        )
    except ValueError as exc:
        raise ValueError(
            "observed_at_utc must be "
            "valid ISO 8601"
        ) from exc

    if parsed.tzinfo is None:
        raise ValueError(
            "observed_at_utc must include "
            "timezone information"
        )

    if parsed.utcoffset() != timedelta(0):
        raise ValueError(
            "observed_at_utc must be UTC"
        )

    normalized = (
        parsed.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )

    if len(normalized) > 64:
        raise ValueError(
            "normalized observed_at_utc "
            "exceeds size limit"
        )

    return normalized


def _strip_leading_sql_comments(
    statement: str,
) -> str:
    value = statement.lstrip()

    while True:
        if value.startswith("--"):
            newline = value.find("\n")

            if newline < 0:
                return ""

            value = value[
                newline + 1:
            ].lstrip()

            continue

        if value.startswith("/*"):
            end = value.find("*/", 2)

            if end < 0:
                raise MigrationError(
                    "unterminated SQL comment"
                )

            value = value[
                end + 2:
            ].lstrip()

            continue

        return value


def _migration_statements(
    sql: str,
) -> list[str]:
    statements = []
    buffer = ""

    # Parse incrementally instead of by line.
    # A migration may legally contain several
    # SQL statements on the same physical line.
    for character in sql:
        buffer += character

        if sqlite3.complete_statement(
            buffer
        ):
            statement = buffer.strip()

            if statement:
                statements.append(
                    statement
                )

            buffer = ""

    if buffer.strip():
        raise MigrationError(
            "migration contains "
            "incomplete SQL"
        )

    return statements


def _validate_migration_sql(
    sql: str,
) -> None:
    prohibited = {
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE",
        "END",
    }

    for statement in             _migration_statements(sql):
        visible =             _strip_leading_sql_comments(
                statement
            )

        if not visible:
            continue

        first = visible.split(
            None,
            1,
        )[0].rstrip(";").upper()

        if first in prohibited:
            raise MigrationError(
                "migration files must not "
                "control transactions"
            )


def _migration_set(
    role: str,
) -> list[tuple[int, str, str, str]]:
    if role not in _ROLE:
        raise ValueError(
            "unknown database role"
        )

    root = (
        resources.files("corvore")
        .joinpath("migrations")
        .joinpath(role)
    )

    files = sorted(
        (
            item
            for item in root.iterdir()
            if item.is_file()
            and item.name.endswith(".sql")
        ),
        key=lambda item: item.name,
    )

    result = []
    expected = 1

    for item in files:
        match = _MIGRATION_NAME.fullmatch(
            item.name
        )

        if not match:
            raise MigrationError(
                f"invalid migration name: "
                f"{item.name}"
            )

        version = int(match.group(1))

        if version != expected:
            raise MigrationError(
                "migration versions must "
                "be contiguous from 0001"
            )

        sql = item.read_text(
            encoding="utf-8"
        )

        _validate_migration_sql(sql)

        digest = hashlib.sha256(
            sql.encode("utf-8")
        ).hexdigest()

        result.append(
            (
                version,
                item.name,
                sql,
                digest,
            )
        )

        expected += 1

    if not result:
        raise MigrationError(
            f"no migrations for {role}"
        )

    return result


def _connect(
    path: Path,
    role: str,
) -> sqlite3.Connection:
    if role not in _ROLE:
        raise ValueError(
            "unknown database role"
        )

    if path.is_symlink():
        raise StorageError(
            "database path must not "
            "be a symlink"
        )

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
        mode=0o750,
    )

    was_new = (
        not path.exists()
        or path.stat().st_size == 0
    )

    connection = sqlite3.connect(
        path,
        timeout=5.0,
        isolation_level=None,
    )

    connection.row_factory = sqlite3.Row

    try:
        connection.execute(
            "PRAGMA busy_timeout = 5000"
        )

        connection.execute(
            "PRAGMA foreign_keys = ON"
        )

        journal_mode = connection.execute(
            "PRAGMA journal_mode = WAL"
        ).fetchone()[0]

        if str(journal_mode).lower() != "wal":
            raise StorageError(
                f"{role}: WAL unavailable"
            )

        connection.execute(
            "PRAGMA synchronous = "
            + _ROLE[role]["synchronous"]
        )

        application_id = int(
            connection.execute(
                "PRAGMA application_id"
            ).fetchone()[0]
        )

        expected_id = int(
            _ROLE[role]["application_id"]
        )

        if application_id == 0 and was_new:
            connection.execute(
                "PRAGMA application_id = "
                + str(expected_id)
            )

            application_id = int(
                connection.execute(
                    "PRAGMA application_id"
                ).fetchone()[0]
            )

        if application_id != expected_id:
            raise StorageError(
                f"{role}: application_id "
                "mismatch"
            )

        os.chmod(path, 0o600)

        return connection

    except Exception:
        connection.close()
        raise


def _readonly_connection(
    path: Path,
    role: str,
) -> sqlite3.Connection:
    if role not in _ROLE:
        raise ValueError(
            "unknown database role"
        )

    if path.is_symlink():
        raise StorageError(
            "database path must not "
            "be a symlink"
        )

    if not path.is_file():
        raise StorageError(
            f"database missing: "
            f"{path.name}"
        )

    resolved = path.resolve(
        strict=True
    )

    uri = (
        "file:"
        + quote(
            str(resolved),
            safe="/",
        )
        + "?mode=ro"
    )

    connection = sqlite3.connect(
        uri,
        uri=True,
        timeout=5.0,
        isolation_level=None,
    )

    connection.row_factory =         sqlite3.Row

    try:
        connection.execute(
            "PRAGMA busy_timeout = 5000"
        )

        connection.execute(
            "PRAGMA query_only = ON"
        )

        connection.execute(
            "PRAGMA foreign_keys = ON"
        )

        foreign_keys = int(
            connection.execute(
                "PRAGMA foreign_keys"
            ).fetchone()[0]
        )

        if foreign_keys != 1:
            raise StorageError(
                f"{role}: foreign key "
                "enforcement unavailable"
            )

        application_id = int(
            connection.execute(
                "PRAGMA application_id"
            ).fetchone()[0]
        )

        expected_id = int(
            _ROLE[role][
                "application_id"
            ]
        )

        if application_id != expected_id:
            raise StorageError(
                f"{role}: application_id "
                "mismatch"
            )

        return connection

    except Exception:
        connection.close()
        raise


def _ensure_migration_table(
    connection: sqlite3.Connection,
) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS
        schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            sha256 TEXT NOT NULL
                CHECK (length(sha256) = 64)
        )
        """
    )


def _apply_migrations(
    connection: sqlite3.Connection,
    role: str,
) -> None:
    _ensure_migration_table(
        connection
    )

    available = _migration_set(role)

    rows = connection.execute(
        """
        SELECT version, name, sha256
        FROM schema_migrations
        ORDER BY version
        """
    ).fetchall()

    recorded = {
        int(row["version"]): (
            str(row["name"]),
            str(row["sha256"]),
        )
        for row in rows
    }

    available_versions = {
        version
        for version, _, _, _ in available
    }

    if set(recorded) - available_versions:
        raise MigrationError(
            "database contains unknown "
            "migration versions"
        )

    for version, name, sql, digest in available:
        existing = recorded.get(version)

        if existing is not None:
            if existing != (
                name,
                digest,
            ):
                raise MigrationError(
                    f"{role}: applied migration "
                    f"{version:04d} differs "
                    "from source"
                )

            continue

        migration_record = (
            "INSERT INTO schema_migrations"
            "(version, name, sha256) VALUES ("
            f"{version}, "
            f"{_sql_quote(name)}, "
            f"{_sql_quote(digest)}"
            ");"
        )

        script = (
            "BEGIN IMMEDIATE;\n"
            + sql.rstrip()
            + "\n"
            + migration_record
            + "\n"
            + "PRAGMA user_version = "
            + str(version)
            + ";\n"
            + "COMMIT;\n"
        )

        try:
            connection.executescript(
                script
            )

        except Exception:
            try:
                connection.execute(
                    "ROLLBACK"
                )
            except sqlite3.Error:
                pass

            raise

    latest = available[-1][0]

    current = int(
        connection.execute(
            "PRAGMA user_version"
        ).fetchone()[0]
    )

    if current != latest:
        raise MigrationError(
            f"{role}: user_version "
            "mismatch"
        )


def _ready_connection(
    path: Path,
    role: str,
) -> sqlite3.Connection:
    connection = _connect(
        path,
        role,
    )

    try:
        _apply_migrations(
            connection,
            role,
        )

        return connection

    except Exception:
        connection.close()
        raise


def _sync_name(
    value: int,
) -> str:
    return {
        0: "OFF",
        1: "NORMAL",
        2: "FULL",
        3: "EXTRA",
    }.get(
        value,
        f"UNKNOWN:{value}",
    )


def _verified_migrations(
    connection: sqlite3.Connection,
    role: str,
) -> list[dict[str, Any]]:
    available = _migration_set(
        role
    )

    exists = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
          AND name = 'schema_migrations'
        """
    ).fetchone()

    if exists is None:
        raise MigrationError(
            f"{role}: migration metadata "
            "missing"
        )

    rows = connection.execute(
        """
        SELECT version, name, sha256
        FROM schema_migrations
        ORDER BY version
        """
    ).fetchall()

    expected_versions = [
        version
        for version, _, _, _
        in available
    ]

    actual_versions = [
        int(row["version"])
        for row in rows
    ]

    if actual_versions !=             expected_versions:
        raise MigrationError(
            f"{role}: migration "
            "sequence mismatch"
        )

    result = []

    for row, expected in zip(
        rows,
        available,
        strict=True,
    ):
        (
            version,
            filename,
            _sql,
            digest,
        ) = expected

        if (
            str(row["name"])
            != filename
            or str(row["sha256"])
            != digest
        ):
            raise MigrationError(
                f"{role}: migration "
                f"{version:04d} differs "
                "from source"
            )

        result.append({
            "version": version,
            "name": filename,
            "sha256": digest,
        })

    latest = available[-1][0]

    user_version = int(
        connection.execute(
            "PRAGMA user_version"
        ).fetchone()[0]
    )

    if user_version != latest:
        raise MigrationError(
            f"{role}: user_version "
            "mismatch"
        )

    return result


def _database_status(
    path: Path,
    role: str,
) -> dict[str, Any]:
    connection = _readonly_connection(
        path,
        role,
    )

    try:
        migrations =             _verified_migrations(
                connection,
                role,
            )

        tables = [
            str(row[0])
            for row in connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                ORDER BY name
                """
            )
        ]

        return {
            "role": role,
            "path": str(path),
            "application_id": int(
                connection.execute(
                    "PRAGMA application_id"
                ).fetchone()[0]
            ),
            "user_version": int(
                connection.execute(
                    "PRAGMA user_version"
                ).fetchone()[0]
            ),
            "journal_mode": str(
                connection.execute(
                    "PRAGMA journal_mode"
                ).fetchone()[0]
            ),
            "synchronous_policy":
                _ROLE[role][
                    "synchronous"
                ],
            "foreign_keys_enabled":
                int(
                    connection.execute(
                        "PRAGMA foreign_keys"
                    ).fetchone()[0]
                ) == 1,
            "integrity_check": str(
                connection.execute(
                    "PRAGMA integrity_check"
                ).fetchone()[0]
            ),
            "migrations": migrations,
            "tables": tables,
        }

    finally:
        connection.close()


def initialize_databases(
    state_dir: str | os.PathLike[str],
) -> dict[str, Any]:
    paths = DatabasePaths.from_state_dir(
        state_dir
    )

    if paths.state_dir.is_symlink():
        raise StorageError(
            "state directory must not "
            "be a symlink"
        )

    paths.state_dir.mkdir(
        parents=True,
        exist_ok=True,
        mode=0o750,
    )

    for role, path in (
        ("core", paths.core),
        (
            "observations",
            paths.observations,
        ),
    ):
        connection = _ready_connection(
            path,
            role,
        )
        connection.close()

    return storage_status(
        paths.state_dir
    )


def storage_status(
    state_dir: str | os.PathLike[str],
) -> dict[str, Any]:
    paths = DatabasePaths.from_state_dir(
        state_dir
    )

    if paths.state_dir.is_symlink():
        raise StorageError(
            "state directory must not "
            "be a symlink"
        )

    missing = [
        path.name
        for path in (
            paths.core,
            paths.observations,
        )
        if not path.is_file()
    ]

    if missing:
        raise StorageError(
            "database files missing: "
            + ", ".join(missing)
        )

    return {
        "schema_version": 1,
        "state_dir": str(
            paths.state_dir.resolve()
        ),
        "databases": {
            "core": _database_status(
                paths.core,
                "core",
            ),
            "observations":
                _database_status(
                    paths.observations,
                    "observations",
                ),
        },
    }


def append_observation(
    state_dir: str | os.PathLike[str],
    *,
    boot_id: str,
    monotonic_ns: int,
    source_kind: str,
    source_instance: str,
    observation_kind: str,
    payload: dict[str, Any],
    observed_at_utc: str | None = None,
    clock_accuracy_verified: bool = False,
    observation_id: str | None = None,
) -> dict[str, Any]:
    if observation_id is None:
        observation_id = str(
            uuid.uuid4()
        )

    observation_id = _validate_identifier(
        "observation_id",
        observation_id,
        min_length=16,
    )

    boot_id = _validate_identifier(
        "boot_id",
        boot_id,
    )

    source_kind = _validate_identifier(
        "source_kind",
        source_kind,
    )

    source_instance = _validate_identifier(
        "source_instance",
        source_instance,
    )

    observation_kind = _validate_identifier(
        "observation_kind",
        observation_kind,
    )

    if (
        not isinstance(monotonic_ns, int)
        or isinstance(monotonic_ns, bool)
        or monotonic_ns < 0
    ):
        raise ValueError(
            "monotonic_ns must be a "
            "non-negative integer"
        )

    if not isinstance(
        clock_accuracy_verified,
        bool,
    ):
        raise ValueError(
            "clock_accuracy_verified "
            "must be bool"
        )

    observed_at_utc = (
        _normalize_observed_at_utc(
            observed_at_utc,
            clock_accuracy_verified=(
                clock_accuracy_verified
            ),
        )
    )

    payload_json, payload_sha256 = \
        _canonical_payload(payload)

    paths = DatabasePaths.from_state_dir(
        state_dir
    )

    connection = _ready_connection(
        paths.observations,
        "observations",
    )

    try:
        connection.execute(
            "BEGIN IMMEDIATE"
        )

        insert_result = connection.execute(
            """
            INSERT INTO observation_log(
                observation_id,
                boot_id,
                monotonic_ns,
                observed_at_utc,
                clock_accuracy_verified,
                source_kind,
                source_instance,
                observation_kind,
                payload_json,
                payload_sha256
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                observation_id,
                boot_id,
                monotonic_ns,
                observed_at_utc,
                int(
                    clock_accuracy_verified
                ),
                source_kind,
                source_instance,
                observation_kind,
                payload_json,
                payload_sha256,
            ),
        )

        ingest_seq = int(
            insert_result.lastrowid
        )

        connection.execute(
            "COMMIT"
        )

    except Exception:
        try:
            connection.execute(
                "ROLLBACK"
            )
        except sqlite3.Error:
            pass

        raise

    finally:
        connection.close()

    return {
        "ingest_seq": ingest_seq,
        "observation_id":
            observation_id,
        "payload_sha256":
            payload_sha256,
    }


def processing_checkpoint(
    state_dir: str | os.PathLike[str],
) -> int:
    paths = DatabasePaths.from_state_dir(
        state_dir
    )

    connection = _readonly_connection(
        paths.core,
        "core",
    )

    try:
        _verified_migrations(
            connection,
            "core",
        )

        row = connection.execute(
            """
            SELECT last_applied_ingest_seq
            FROM ingest_state
            WHERE singleton = 1
            """
        ).fetchone()

        if row is None:
            raise StorageError(
                "processing checkpoint missing"
            )

        value = int(
            row[
                "last_applied_ingest_seq"
            ]
        )

        if value < 0:
            raise StorageError(
                "processing checkpoint invalid"
            )

        return value

    finally:
        connection.close()


def _require_observation(
    state_dir: str | os.PathLike[str],
    ingest_seq: int,
) -> str:
    paths = DatabasePaths.from_state_dir(
        state_dir
    )

    connection = _readonly_connection(
        paths.observations,
        "observations",
    )

    try:
        _verified_migrations(
            connection,
            "observations",
        )

        row = connection.execute(
            """
            SELECT observation_id
            FROM observation_log
            WHERE ingest_seq = ?
            """,
            (ingest_seq,),
        ).fetchone()

        if row is None:
            raise ObservationNotFoundError(
                "observation does not exist"
            )

        return str(
            row["observation_id"]
        )

    finally:
        connection.close()


def _prepare_outbox_event(
    event: OutboxEvent,
) -> tuple[str, str, str, str]:
    if not isinstance(
        event,
        OutboxEvent,
    ):
        raise ValueError(
            "outbox events must use "
            "OutboxEvent"
        )

    event_id = _validate_identifier(
        "event_id",
        event.event_id,
        min_length=16,
    )

    topic = _validate_identifier(
        "topic",
        event.topic,
    )

    payload_json, payload_sha256 =         _canonical_payload(
            event.payload
        )

    return (
        event_id,
        topic,
        payload_json,
        payload_sha256,
    )


def commit_processing_step(
    state_dir: str | os.PathLike[str],
    *,
    ingest_seq: int,
    events: (
        list[OutboxEvent]
        | tuple[OutboxEvent, ...]
    ) = (),
) -> dict[str, Any]:
    if (
        not isinstance(ingest_seq, int)
        or isinstance(ingest_seq, bool)
        or ingest_seq < 1
    ):
        raise ValueError(
            "ingest_seq must be a "
            "positive integer"
        )

    if not isinstance(
        events,
        (list, tuple),
    ):
        raise ValueError(
            "events must be a list "
            "or tuple"
        )

    observation_id =         _require_observation(
            state_dir,
            ingest_seq,
        )

    prepared = [
        _prepare_outbox_event(
            event
        )
        for event in events
    ]

    paths = DatabasePaths.from_state_dir(
        state_dir
    )

    connection = _ready_connection(
        paths.core,
        "core",
    )

    try:
        connection.execute(
            "BEGIN IMMEDIATE"
        )

        row = connection.execute(
            """
            SELECT last_applied_ingest_seq
            FROM ingest_state
            WHERE singleton = 1
            """
        ).fetchone()

        if row is None:
            raise StorageError(
                "processing checkpoint missing"
            )

        current = int(
            row[
                "last_applied_ingest_seq"
            ]
        )

        if ingest_seq <= current:
            connection.execute(
                "ROLLBACK"
            )

            return {
                "status":
                    "already_applied",
                "ingest_seq":
                    ingest_seq,
                "observation_id":
                    observation_id,
                "processing_checkpoint":
                    current,
                "outbox_events_written":
                    0,
            }

        expected = current + 1

        if ingest_seq != expected:
            connection.execute(
                "ROLLBACK"
            )

            raise ProcessingSequenceError(
                "observation processing "
                f"gap: expected {expected}, "
                f"received {ingest_seq}"
            )

        for (
            event_id,
            topic,
            payload_json,
            payload_sha256,
        ) in prepared:
            connection.execute(
                """
                INSERT INTO outbox(
                    event_id,
                    topic,
                    created_ingest_seq,
                    payload_json,
                    payload_sha256,
                    acknowledged
                )
                VALUES (?, ?, ?, ?, ?, 0)
                """,
                (
                    event_id,
                    topic,
                    ingest_seq,
                    payload_json,
                    payload_sha256,
                ),
            )

        updated = connection.execute(
            """
            UPDATE ingest_state
            SET last_applied_ingest_seq = ?
            WHERE singleton = 1
              AND last_applied_ingest_seq = ?
            """,
            (
                ingest_seq,
                current,
            ),
        )

        if updated.rowcount != 1:
            raise StorageError(
                "processing checkpoint update "
                "lost serialization"
            )

        connection.execute(
            "COMMIT"
        )

        return {
            "status": "applied",
            "ingest_seq": ingest_seq,
            "observation_id":
                observation_id,
            "processing_checkpoint":
                ingest_seq,
            "outbox_events_written":
                len(prepared),
        }

    except Exception:
        try:
            connection.execute(
                "ROLLBACK"
            )
        except sqlite3.Error:
            pass

        raise

    finally:
        connection.close()


def read_pending_outbox(
    state_dir: str | os.PathLike[str],
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    if (
        not isinstance(limit, int)
        or isinstance(limit, bool)
        or not 1 <= limit <= MAX_READ_LIMIT
    ):
        raise ValueError(
            "limit outside allowed range"
        )

    paths = DatabasePaths.from_state_dir(
        state_dir
    )

    connection = _readonly_connection(
        paths.core,
        "core",
    )

    try:
        _verified_migrations(
            connection,
            "core",
        )

        rows = connection.execute(
            """
            SELECT
                outbox_seq,
                event_id,
                topic,
                created_ingest_seq,
                payload_json,
                payload_sha256
            FROM outbox
            WHERE acknowledged = 0
            ORDER BY outbox_seq
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        result = []

        for row in rows:
            payload_json = str(
                row["payload_json"]
            )

            raw = payload_json.encode(
                "utf-8"
            )

            expected = str(
                row["payload_sha256"]
            )

            actual = hashlib.sha256(
                raw
            ).hexdigest()

            if actual != expected:
                raise ObservationIntegrityError(
                    "outbox payload "
                    "hash mismatch"
                )

            payload = json.loads(
                payload_json
            )

            if not isinstance(
                payload,
                dict,
            ):
                raise ObservationIntegrityError(
                    "outbox payload is not "
                    "a JSON object"
                )

            result.append({
                "outbox_seq":
                    int(
                        row[
                            "outbox_seq"
                        ]
                    ),
                "event_id":
                    str(
                        row[
                            "event_id"
                        ]
                    ),
                "topic":
                    str(row["topic"]),
                "created_ingest_seq":
                    int(
                        row[
                            "created_ingest_seq"
                        ]
                    ),
                "payload": payload,
                "payload_sha256":
                    expected,
            })

        return result

    finally:
        connection.close()


def acknowledge_outbox_event(
    state_dir: str | os.PathLike[str],
    *,
    event_id: str,
) -> dict[str, Any]:
    event_id = _validate_identifier(
        "event_id",
        event_id,
        min_length=16,
    )

    paths = DatabasePaths.from_state_dir(
        state_dir
    )

    connection = _ready_connection(
        paths.core,
        "core",
    )

    try:
        connection.execute(
            "BEGIN IMMEDIATE"
        )

        row = connection.execute(
            """
            SELECT acknowledged
            FROM outbox
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()

        if row is None:
            raise OutboxEventNotFoundError(
                "outbox event does "
                "not exist"
            )

        if int(
            row["acknowledged"]
        ) == 1:
            connection.execute(
                "ROLLBACK"
            )

            return {
                "status":
                    "already_acknowledged",
                "event_id": event_id,
            }

        updated = connection.execute(
            """
            UPDATE outbox
            SET acknowledged = 1
            WHERE event_id = ?
              AND acknowledged = 0
            """,
            (event_id,),
        )

        if updated.rowcount != 1:
            raise StorageError(
                "outbox acknowledgement "
                "lost serialization"
            )

        connection.execute(
            "COMMIT"
        )

        return {
            "status": "acknowledged",
            "event_id": event_id,
        }

    except Exception:
        try:
            connection.execute(
                "ROLLBACK"
            )
        except sqlite3.Error:
            pass

        raise

    finally:
        connection.close()


def read_observations_after(
    state_dir: str | os.PathLike[str],
    *,
    after_ingest_seq: int,
    limit: int = 100,
) -> list[dict[str, Any]]:
    if (
        not isinstance(
            after_ingest_seq,
            int,
        )
        or isinstance(
            after_ingest_seq,
            bool,
        )
        or after_ingest_seq < 0
    ):
        raise ValueError(
            "after_ingest_seq must be "
            "a non-negative integer"
        )

    if (
        not isinstance(limit, int)
        or isinstance(limit, bool)
        or not 1 <= limit <= MAX_READ_LIMIT
    ):
        raise ValueError(
            "limit outside allowed range"
        )

    paths = DatabasePaths.from_state_dir(
        state_dir
    )

    connection = _ready_connection(
        paths.observations,
        "observations",
    )

    try:
        rows = connection.execute(
            """
            SELECT
                ingest_seq,
                observation_id,
                boot_id,
                monotonic_ns,
                observed_at_utc,
                clock_accuracy_verified,
                source_kind,
                source_instance,
                observation_kind,
                payload_json,
                payload_sha256
            FROM observation_log
            WHERE ingest_seq > ?
            ORDER BY ingest_seq
            LIMIT ?
            """,
            (
                after_ingest_seq,
                limit,
            ),
        ).fetchall()

        result = []

        for row in rows:
            encoded = str(
                row["payload_json"]
            ).encode("utf-8")

            actual = hashlib.sha256(
                encoded
            ).hexdigest()

            expected = str(
                row["payload_sha256"]
            )

            if actual != expected:
                raise ObservationIntegrityError(
                    "observation payload "
                    "hash mismatch"
                )

            payload = json.loads(
                encoded
            )

            if not isinstance(
                payload,
                dict,
            ):
                raise ObservationIntegrityError(
                    "stored payload is not "
                    "a JSON object"
                )

            result.append({
                "ingest_seq":
                    int(row["ingest_seq"]),
                "observation_id":
                    str(
                        row[
                            "observation_id"
                        ]
                    ),
                "boot_id":
                    str(row["boot_id"]),
                "monotonic_ns":
                    int(
                        row[
                            "monotonic_ns"
                        ]
                    ),
                "observed_at_utc":
                    row[
                        "observed_at_utc"
                    ],
                "clock_accuracy_verified":
                    bool(
                        row[
                            "clock_accuracy_verified"
                        ]
                    ),
                "source_kind":
                    str(
                        row[
                            "source_kind"
                        ]
                    ),
                "source_instance":
                    str(
                        row[
                            "source_instance"
                        ]
                    ),
                "observation_kind":
                    str(
                        row[
                            "observation_kind"
                        ]
                    ),
                "payload": payload,
                "payload_sha256":
                    expected,
            })

        return result

    finally:
        connection.close()
