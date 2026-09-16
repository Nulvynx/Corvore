# CORVORE Persistence Architecture

## Purpose

CORVORE uses separate persistence stores for raw ingress and durable
application state.

This separation allows observations to be persisted cheaply while
retaining stronger durability for model state and processing progress.

## observations.db

`observations.db` is the durable append-only ingress log.

Policy:

- SQLite WAL mode
- `synchronous=NORMAL`
- monotonically increasing local `ingest_seq`
- immutable observation rows
- canonical JSON payloads
- SHA-256 stored with each payload
- wall-clock time is not authoritative unless explicitly verified

## core.db

`core.db` stores durable application and processing state.

Policy:

- SQLite WAL mode
- `synchronous=FULL`
- durable observation-processing checkpoint
- local transactional outbox
- derived state, checkpoint advancement and related outbox writes must be
  committed atomically inside this database

## Cross-database crash model

CORVORE does not depend on an atomic transaction spanning both database
files.

Required processing sequence:

1. persist an observation in `observations.db`;
2. obtain its durable `ingest_seq`;
3. process observations in increasing `ingest_seq` order;
4. update derived state and `last_applied_ingest_seq` atomically in
   `core.db`;
5. after restart, replay observations newer than the durable processing
   checkpoint.

A crash between ingress persistence and model application is therefore
recoverable through replay.

## Observation envelope

Canonical ingress fields are:

- `ingest_seq`
- `observation_id`
- `boot_id`
- `monotonic_ns`
- `observed_at_utc`
- `clock_accuracy_verified`
- `source_kind`
- `source_instance`
- `observation_kind`
- `payload_json`
- `payload_sha256`

`ingest_seq` is the authoritative local processing order.

`monotonic_ns` is interpreted together with `boot_id`.

Absolute UTC chronology remains a separate time-provider responsibility.

## Payload rules

Payloads are canonical JSON objects:

- UTF-8
- deterministic key ordering
- compact separators
- NaN and Infinity rejected
- maximum encoded size of 1 MiB
- SHA-256 calculated over exact canonical bytes

## Migration contract

Each database has an independent ordered migration set.

Rules:

- numbering begins at `0001`
- versions are contiguous
- migration filename and SHA-256 are persisted
- modifying an already-applied migration is an integrity failure
- SQLite `user_version` equals the latest applied migration

## Database identity

Each database uses a distinct SQLite `application_id`.

A database with an unexpected application ID must be rejected instead of
being silently interpreted as a CORVORE database.

## Test separation

Synthetic observations, fixtures and replay material belong only to
engineering validation and automated tests.

Production runtime code must not depend on synthetic or replay-only data
sources.

## Current boundaries

The persistence layer does not define:

- live radio collection
- BSS identity semantics
- inferred network grouping
- hostile-input normalization
- evidence semantics
- WebUI behavior
- active radio operations

Those components consume the persistence contract rather than being
embedded in it.

## Inspection semantics

Persistence inspection is read-only.

The status command must not apply migrations, create databases, change
journal mode, alter database permissions or modify application data.

Initialization and migration remain explicit operations.

## Deterministic JSON restrictions

Observation payloads use a portable JSON subset:

- object keys are strings
- integers fit the signed 64-bit range
- floating-point values are finite
- nesting depth is bounded
- unsupported language-specific values are rejected

The stored payload digest is calculated over the exact deterministic
UTF-8 JSON representation.

## UTC timestamp validation

When `observed_at_utc` is present it must be a timezone-aware ISO 8601
UTC timestamp.

When `clock_accuracy_verified` is true, `observed_at_utc` is mandatory.

Absolute time quality does not alter local processing order, which
remains defined by `ingest_seq`.

## Processing checkpoint

`core.db` stores the highest observation sequence that has been applied
to durable application state.

Processing must be contiguous.

If the current checkpoint is `N`, the next observation eligible for
application is `N + 1`.

An attempt to skip ahead is rejected.

Reprocessing an observation whose sequence is already at or below the
durable checkpoint is treated as an idempotent no-op.

## Cross-database recovery

Observation ingress and application-state mutation intentionally use
different database files.

The recovery invariant is:

- an observation is durable before it can be applied;
- the application checkpoint never advances past an absent observation;
- after restart, observations newer than the core checkpoint are replayed.

This avoids requiring distributed or cross-file SQLite transactions.

## Transactional outbox

Derived local events are inserted into the `core.db` outbox in the same
transaction that advances the processing checkpoint.

Therefore either:

- both the outbox event and checkpoint advancement commit; or
- neither commits.

Outbox delivery is at-least-once until explicit acknowledgement.

Consumers must use the stable `event_id` for idempotency.

Acknowledgement is itself idempotent.

## Migration transaction ownership

Migration files contain schema/data operations only.

Migration source files must not issue transaction-control statements such
as `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT` or `RELEASE`.

Transaction boundaries are owned by the migration framework so that
migration metadata and schema changes commit atomically.

## Observation sequence allocation

`observation_log.ingest_seq` uses SQLite `INTEGER PRIMARY KEY` without
`AUTOINCREMENT`.

The observation log is append-only and database-level triggers reject
row deletion. Consequently the normal SQLite ROWID allocator provides
the required increasing local sequence without maintaining
`sqlite_sequence` for every observation.

Avoiding `AUTOINCREMENT` on the high-volume ingress path reduces
unnecessary metadata writes on flash-backed storage.

The transactional outbox may use an independent sequence policy because
its write volume and retention semantics differ from observation ingress.
