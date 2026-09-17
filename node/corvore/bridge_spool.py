"""Bounded, persistent retry spool for the passive Bettercap event bridge.

This module does not start Bettercap, configure a radio, or read a replay file.
Each source emission must be enqueued exactly once before its first delivery.
"""

from __future__ import annotations

from dataclasses import dataclass
import fcntl
import hashlib
import os
from pathlib import Path
import sqlite3
import stat
from typing import Callable
import uuid

from corvore.collector import (
    MAX_EVENT_BYTES,
    deterministic_observation_id,
    parse_bettercap_event,
)
from corvore.ingress import send_event


DEFAULT_MAX_EVENTS = 2048
DEFAULT_MAX_BYTES = 16 * 1024 * 1024
SPOOL_APPLICATION_ID = 0x43565251  # CVRQ
SPOOL_USER_VERSION = 1


class SpoolError(RuntimeError):
    """Spool integrity, ownership, or lifecycle failure."""


class SpoolFullError(SpoolError):
    """Bounded spool cannot accept another emission; fail without dropping."""


class SpoolBootMismatch(SpoolError):
    """The current ingress v2 contract cannot replay across Linux boots."""


class SpoolDeliveryRejected(SpoolError):
    """Ingress rejected a queued event; retain it for investigation."""


@dataclass(frozen=True)
class PendingDelivery:
    seq: int
    delivery_id: str
    boot_id: str
    source_instance: str
    raw_event: bytes


def _linux_boot_id() -> str:
    return Path('/proc/sys/kernel/random/boot_id').read_text(
        encoding='ascii'
    ).strip()


def _canonical_uuid4(value: str, label: str) -> str:
    if not isinstance(value, str):
        raise SpoolError(f'{label} must be a canonical UUIDv4')
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise SpoolError(f'{label} must be a canonical UUIDv4') from exc
    if parsed.version != 4 or str(parsed) != value:
        raise SpoolError(f'{label} must be a canonical UUIDv4')
    return value


def _ensure_private_directory(path: Path) -> None:
    if path.is_symlink():
        raise SpoolError('spool directory must not be a symlink')
    if not path.exists():
        path.mkdir(parents=True, mode=0o700)
    if not path.is_dir():
        raise SpoolError('spool directory is not a directory')
    info = path.stat()
    if info.st_uid != os.geteuid():
        raise SpoolError('spool directory has an unexpected owner')
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise SpoolError('spool directory must not allow group/other access')


def _ensure_private_file(path: Path) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
        raise SpoolError(f'unsafe spool file: {path.name}')
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise SpoolError(f'spool file is not private: {path.name}')


class DurableEventSpool:
    """Single-owner SQLite WAL/FULL queue; use via a context manager.

    No event is removed unless ingress returns an authenticated response
    whose observation ID matches this event and status is committed.
    """

    def __init__(
        self,
        directory: str | os.PathLike[str],
        *,
        max_events: int = DEFAULT_MAX_EVENTS,
        max_bytes: int = DEFAULT_MAX_BYTES,
        boot_id_provider: Callable[[], str] = _linux_boot_id,
    ) -> None:
        if type(max_events) is not int or not 1 <= max_events <= 100_000:
            raise ValueError('max_events must be between 1 and 100000')
        if type(max_bytes) is not int or not MAX_EVENT_BYTES <= max_bytes <= 1024**3:
            raise ValueError('max_bytes outside allowed range')
        if not callable(boot_id_provider):
            raise ValueError('boot_id_provider must be callable')
        self.directory = Path(directory)
        self.max_events = max_events
        self.max_bytes = max_bytes
        self.boot_id_provider = boot_id_provider
        self._lock_fd: int | None = None
        self._db: sqlite3.Connection | None = None

    def __enter__(self) -> 'DurableEventSpool':
        _ensure_private_directory(self.directory)
        lock_path = self.directory / 'bridge.lock'
        db_path = self.directory / 'bridge-queue.db'
        for path in (lock_path, db_path, db_path.with_name(db_path.name + '-wal'),
                     db_path.with_name(db_path.name + '-shm')):
            _ensure_private_file(path)
        flags = os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0)
        fd = os.open(lock_path, flags, 0o600)
        self._lock_fd = fd
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise SpoolError('unsafe spool lock')
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise SpoolError('another bridge spool owner is running') from exc
            for path in (db_path, db_path.with_name(db_path.name + '-wal'),
                         db_path.with_name(db_path.name + '-shm')):
                _ensure_private_file(path)
            # SQLite uses the process umask for a new database; create its
            # inode explicitly with private permissions before connecting.
            if not db_path.exists():
                try:
                    db_fd = os.open(
                        db_path,
                        os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0),
                        0o600,
                    )
                except FileExistsError:
                    _ensure_private_file(db_path)
                else:
                    os.close(db_fd)
            _ensure_private_file(db_path)
            self._db = sqlite3.connect(str(db_path), timeout=5, isolation_level=None)
            self._db.row_factory = sqlite3.Row
            self._db.execute('PRAGMA busy_timeout=5000')
            self._db.execute('PRAGMA journal_mode=WAL')
            self._db.execute('PRAGMA synchronous=FULL')
            self._initialize_schema()
            for path in (db_path, db_path.with_name(db_path.name + '-wal'),
                         db_path.with_name(db_path.name + '-shm')):
                _ensure_private_file(path)
            return self
        except BaseException:
            self.close()
            raise

    def _initialize_schema(self) -> None:
        db = self._connection()
        version = int(db.execute('PRAGMA user_version').fetchone()[0])
        application_id = int(db.execute('PRAGMA application_id').fetchone()[0])
        if version == 0 and application_id == 0:
            # Refuse to repurpose any existing non-empty SQLite database.
            if db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchone():
                raise SpoolError('unrecognized non-empty spool database')
            db.execute('BEGIN IMMEDIATE')
            try:
                db.execute('''CREATE TABLE pending (
                    seq INTEGER PRIMARY KEY,
                    delivery_id TEXT UNIQUE NOT NULL,
                    boot_id TEXT NOT NULL,
                    source_instance TEXT NOT NULL,
                    raw_event BLOB NOT NULL,
                    sha256 TEXT NOT NULL,
                    event_bytes INTEGER NOT NULL CHECK(event_bytes > 0)
                )''')
                db.execute(f'PRAGMA application_id={SPOOL_APPLICATION_ID}')
                db.execute(f'PRAGMA user_version={SPOOL_USER_VERSION}')
                db.execute('COMMIT')
            except BaseException:
                db.execute('ROLLBACK')
                raise
        elif version != SPOOL_USER_VERSION or application_id != SPOOL_APPLICATION_ID:
            raise SpoolError('spool database identity or version mismatch')
        if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise SpoolError('spool database integrity check failed')
        expected_columns = {
            'seq', 'delivery_id', 'boot_id', 'source_instance',
            'raw_event', 'sha256', 'event_bytes',
        }
        if {row[1] for row in db.execute('PRAGMA table_info(pending)')} != expected_columns:
            raise SpoolError('spool schema differs from expected columns')

    def _connection(self) -> sqlite3.Connection:
        if self._db is None:
            raise SpoolError('spool is not open')
        return self._db

    def close(self) -> None:
        if self._db is not None:
            try:
                self._db.close()
            finally:
                self._db = None
        if self._lock_fd is not None:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(self._lock_fd)
                self._lock_fd = None

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        self.close()

    def pending_count(self) -> int:
        return int(self._connection().execute('SELECT COUNT(*) FROM pending').fetchone()[0])

    def enqueue(self, raw_event: bytes, *, source_instance: str) -> str:
        """Persist before sending; one call means one *new* source emission."""
        if not isinstance(raw_event, bytes):
            raise TypeError('source event must be bytes')
        # Validate the Bettercap envelope at the untrusted boundary.
        parse_bettercap_event(raw_event)
        if not isinstance(source_instance, str) or not source_instance:
            raise SpoolError('invalid source_instance')
        # Reuse ingress validation, without opening a socket.
        from corvore.ingress import encode_event_frame
        boot_id = _canonical_uuid4(self.boot_id_provider(), 'boot_id')
        delivery_id = str(uuid.uuid4())
        encode_event_frame(raw_event, source_instance=source_instance, delivery_id=delivery_id)
        size = len(raw_event)
        db = self._connection()
        db.execute('BEGIN IMMEDIATE')
        try:
            count, total = db.execute(
                'SELECT COUNT(*), COALESCE(SUM(event_bytes), 0) FROM pending'
            ).fetchone()
            if count >= self.max_events or total + size > self.max_bytes:
                raise SpoolFullError('spool full: upstream collection must pause')
            db.execute('''INSERT INTO pending
                (delivery_id, boot_id, source_instance, raw_event, sha256, event_bytes)
                VALUES (?, ?, ?, ?, ?, ?)''',
                (delivery_id, boot_id, source_instance, raw_event,
                 hashlib.sha256(raw_event).hexdigest(), size))
            db.execute('COMMIT')
        except BaseException:
            db.execute('ROLLBACK')
            raise
        return delivery_id

    def peek(self) -> PendingDelivery | None:
        row = self._connection().execute('''SELECT seq, delivery_id, boot_id,
             source_instance, raw_event, sha256, event_bytes
             FROM pending ORDER BY seq LIMIT 1''').fetchone()
        if row is None:
            return None
        raw = bytes(row['raw_event'])
        if (len(raw) != row['event_bytes'] or
                hashlib.sha256(raw).hexdigest() != row['sha256']):
            raise SpoolError('queued event integrity mismatch')
        _canonical_uuid4(row['delivery_id'], 'delivery_id')
        _canonical_uuid4(row['boot_id'], 'boot_id')
        return PendingDelivery(
            seq=int(row['seq']), delivery_id=row['delivery_id'],
            boot_id=row['boot_id'], source_instance=row['source_instance'],
            raw_event=raw,
        )

    def dispatch_one(
        self,
        socket_path: str | os.PathLike[str],
        *,
        sender: Callable[..., dict[str, object]] = send_event,
    ) -> bool:
        """Deliver oldest event, remove only after matching committed ACK.

        A crash after core commit but before queue deletion causes a retry
        with the same delivery ID. Never retry a v2 frame after reboot.
        """
        entry = self.peek()
        if entry is None:
            return False
        if _canonical_uuid4(self.boot_id_provider(), 'boot_id') != entry.boot_id:
            raise SpoolBootMismatch(
                'queued event originates from a previous boot; ingress v2 '
                'cannot preserve its boot identity. Retained, not replayed.'
            )
        response = sender(
            socket_path, entry.raw_event,
            source_instance=entry.source_instance,
            delivery_id=entry.delivery_id,
        )
        if not isinstance(response, dict) or response.get('status') != 'committed':
            raise SpoolDeliveryRejected('ingress did not commit queued event')
        expected_id = deterministic_observation_id(
            boot_id=entry.boot_id, source_instance=entry.source_instance,
            delivery_id=entry.delivery_id,
        )
        if response.get('observation_id') != expected_id:
            raise SpoolDeliveryRejected('committed response has wrong observation ID')
        seq = response.get('ingest_seq')
        if type(seq) is not int or seq < 1:
            raise SpoolDeliveryRejected('committed response has invalid ingest sequence')
        db = self._connection()
        db.execute('BEGIN IMMEDIATE')
        try:
            result = db.execute('''DELETE FROM pending WHERE seq=?
                AND delivery_id=? AND sha256=?''',
                (entry.seq, entry.delivery_id, hashlib.sha256(entry.raw_event).hexdigest()))
            if result.rowcount != 1:
                raise SpoolError('queued event changed during acknowledgement')
            db.execute('COMMIT')
        except BaseException:
            db.execute('ROLLBACK')
            raise
        return True

    def dispatch_pending(
        self,
        socket_path: str | os.PathLike[str],
        *,
        max_events: int = 128,
    ) -> int:
        if type(max_events) is not int or not 1 <= max_events <= 2048:
            raise ValueError('invalid dispatch limit')
        delivered = 0
        while delivered < max_events and self.dispatch_one(socket_path):
            delivered += 1
        return delivered
