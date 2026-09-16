CREATE TABLE core_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO core_meta(key, value)
VALUES
    ('database_role', 'core'),
    ('schema_family', 'corvore-core-v1');

CREATE TABLE ingest_state (
    singleton INTEGER PRIMARY KEY
        CHECK (singleton = 1),

    last_applied_ingest_seq INTEGER NOT NULL
        DEFAULT 0
        CHECK (last_applied_ingest_seq >= 0)
);

INSERT INTO ingest_state(
    singleton,
    last_applied_ingest_seq
)
VALUES (1, 0);

CREATE TABLE outbox (
    outbox_seq INTEGER PRIMARY KEY AUTOINCREMENT,

    event_id TEXT NOT NULL UNIQUE
        CHECK (
            length(event_id) BETWEEN 16 AND 128
        ),

    topic TEXT NOT NULL
        CHECK (
            length(topic) BETWEEN 1 AND 128
        ),

    created_ingest_seq INTEGER NOT NULL
        CHECK (created_ingest_seq >= 0),

    payload_json TEXT NOT NULL,

    payload_sha256 TEXT NOT NULL
        CHECK (length(payload_sha256) = 64),

    acknowledged INTEGER NOT NULL
        DEFAULT 0
        CHECK (acknowledged IN (0, 1))
);

CREATE INDEX outbox_pending_idx
ON outbox(acknowledged, outbox_seq);
