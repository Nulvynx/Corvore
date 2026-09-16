CREATE TABLE observation_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO observation_meta(key, value)
VALUES
    ('database_role', 'observations'),
    ('schema_family', 'corvore-observations-v1');

CREATE TABLE observation_log (
    ingest_seq INTEGER PRIMARY KEY,

    observation_id TEXT NOT NULL UNIQUE
        CHECK (
            length(observation_id) BETWEEN 16 AND 128
        ),

    boot_id TEXT NOT NULL
        CHECK (
            length(boot_id) BETWEEN 1 AND 128
        ),

    monotonic_ns INTEGER NOT NULL
        CHECK (monotonic_ns >= 0),

    observed_at_utc TEXT,

    clock_accuracy_verified INTEGER NOT NULL
        CHECK (clock_accuracy_verified IN (0, 1)),

    source_kind TEXT NOT NULL
        CHECK (
            length(source_kind) BETWEEN 1 AND 128
        ),

    source_instance TEXT NOT NULL
        CHECK (
            length(source_instance) BETWEEN 1 AND 128
        ),

    observation_kind TEXT NOT NULL
        CHECK (
            length(observation_kind) BETWEEN 1 AND 128
        ),

    payload_json TEXT NOT NULL,

    payload_sha256 TEXT NOT NULL
        CHECK (length(payload_sha256) = 64)
);

CREATE INDEX observation_log_kind_seq_idx
ON observation_log(observation_kind, ingest_seq);

CREATE INDEX observation_log_source_seq_idx
ON observation_log(
    source_kind,
    source_instance,
    ingest_seq
);

CREATE TRIGGER observation_log_no_update
BEFORE UPDATE ON observation_log
BEGIN
    SELECT RAISE(
        ABORT,
        'observation_log is append-only'
    );
END;

CREATE TRIGGER observation_log_no_delete
BEFORE DELETE ON observation_log
BEGIN
    SELECT RAISE(
        ABORT,
        'observation_log is append-only'
    );
END;
