-- BabelQueue transactional outbox table (ADR-0029).
--
-- The `body` column stores the FROZEN wire envelope verbatim (schema_version 1) — the
-- exact EnvelopeCodec::encode() output. The outbox columns around it (id, status,
-- attempts, …) are bookkeeping for the relay; they are NOT part of the envelope and
-- never travel on the wire (GR-1).
--
-- The producer INSERTs into this table inside the SAME transaction as its business
-- write, so a crash can never commit the order without its message. The relay then
-- SELECTs `status = 'pending'` rows oldest-first, publishes each, and flips them to
-- 'published' (or bumps `attempts` + `last_error` on failure, leaving them pending).

-- ---------------------------------------------------------------------------
-- MySQL / MariaDB
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS babelqueue_outbox (
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    queue        VARCHAR(255)    NOT NULL,
    body         LONGTEXT        NOT NULL,                 -- encoded envelope JSON, verbatim
    status       VARCHAR(16)     NOT NULL DEFAULT 'pending', -- 'pending' | 'published'
    attempts     INT UNSIGNED    NOT NULL DEFAULT 0,
    last_error   VARCHAR(500)    NULL,
    created_at   DATETIME        NOT NULL,
    published_at DATETIME        NULL,
    failed_at    DATETIME        NULL,
    PRIMARY KEY (id),
    -- The relay's hot query: pending rows, oldest first.
    KEY idx_outbox_pending (status, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- PostgreSQL (equivalent)
-- ---------------------------------------------------------------------------
-- CREATE TABLE IF NOT EXISTS babelqueue_outbox (
--     id           BIGSERIAL    PRIMARY KEY,
--     queue        VARCHAR(255) NOT NULL,
--     body         TEXT         NOT NULL,
--     status       VARCHAR(16)  NOT NULL DEFAULT 'pending',
--     attempts     INTEGER      NOT NULL DEFAULT 0,
--     last_error   VARCHAR(500),
--     created_at   TIMESTAMP    NOT NULL,
--     published_at TIMESTAMP,
--     failed_at    TIMESTAMP
-- );
-- CREATE INDEX IF NOT EXISTS idx_outbox_pending ON babelqueue_outbox (status, id);

-- ---------------------------------------------------------------------------
-- SQLite (used by this example's test)
-- ---------------------------------------------------------------------------
-- CREATE TABLE IF NOT EXISTS babelqueue_outbox (
--     id           INTEGER PRIMARY KEY AUTOINCREMENT,
--     queue        TEXT    NOT NULL,
--     body         TEXT    NOT NULL,
--     status       TEXT    NOT NULL DEFAULT 'pending',
--     attempts     INTEGER NOT NULL DEFAULT 0,
--     last_error   TEXT,
--     created_at   TEXT    NOT NULL,
--     published_at TEXT,
--     failed_at    TEXT
-- );
-- CREATE INDEX IF NOT EXISTS idx_outbox_pending ON babelqueue_outbox (status, id);
