# Transactional Outbox — commit the order and its message together

A plain producer makes a **dual write**: it commits the business row (`INSERT INTO
orders …`) *and* publishes to the broker. Those are two systems that can disagree on a
crash — the order is saved but the message is lost, or the message is sent but the order
rolled back. The **transactional outbox** ([ADR-0029](https://babelqueue.com)) removes the
dual write: the message is written **into the same database, inside the same transaction**
as the business data, so they commit or roll back **atomically**. A separate **relay**
publishes the stored rows afterwards. Exactly-once *handoff* into the broker, no
distributed transaction.

BabelQueue's php-sdk ships the dependency-free helper
([`BabelQueue\Outbox`](https://github.com/BabelQueue/php-sdk/tree/main/src/Outbox)); this
example binds it to a real database with **[InitORM](https://github.com/initorm)**
(`initorm/database`). The SDK core stays `ext-json`-only (GR-7) — the InitORM glue lives
here, in the [`src/InitOrmOutboxStore.php`](src/InitOrmOutboxStore.php) adapter.

## How it works

```
produce.php                                  relay.php
┌──────────────────────────────┐            ┌────────────────────────────┐
│ $db->transaction(function(){ │            │ $rows = fetchUnpublished() │
│    INSERT INTO orders …       │            │ foreach: transport.publish │
│    $outbox->write($envelope)  │  ── DB ──▶ │ markPublished() / markFailed
│ });   // both, or neither     │            │ (at-least-once, retry)     │
└──────────────────────────────┘            └────────────────────────────┘
        atomic commit                          drains the durable outbox
```

1. **Write side** (`produce.php`) — the **caller owns the transaction boundary**. We open
   *one* `$db->transaction(…)`, do the business `INSERT INTO orders` and
   `$outbox->write($envelope)` inside it, and InitORM commits both together. The
   `InitOrmOutboxStore::save()` adapter runs its `INSERT` on the connection's
   already-open transaction — it never begins or commits one itself.
2. **Read side** (`relay.php`) — `OutboxRelay::drain()` `SELECT`s pending rows oldest-first,
   publishes each through a BabelQueue `Transport` (here `RedisTransport`), marks it
   `published`, or — on a publish failure — bumps `attempts` + `last_error` and leaves it
   **pending** for the next pass. At-least-once: a downstream consumer dedupes on the
   canonical `meta.id` (the php-sdk's `Idempotent::wrap`, [ADR-0022], is the consumer-side
   mirror).

The envelope is the **frozen** `schema_version: 1` wire format. The outbox **stores it
verbatim** in the `body` column and the relay **publishes those exact bytes** — it never
decodes or rebuilds the envelope — so `trace_id` and every field reach the broker
byte-for-byte as written (GR-1 / GR-4 / GR-5). The outbox table's own columns (`id`,
`status`, `attempts`, …) are bookkeeping around the envelope; they never travel on the
wire.

## The outbox table

Production DDL (MySQL / PostgreSQL / SQLite variants) is in [`schema.sql`](schema.sql).
The demo's `bootstrap.php` creates a SQLite version automatically so it runs with no
server.

```sql
CREATE TABLE babelqueue_outbox (
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    queue        VARCHAR(255)    NOT NULL,
    body         LONGTEXT        NOT NULL,                 -- encoded envelope, verbatim
    status       VARCHAR(16)     NOT NULL DEFAULT 'pending',
    attempts     INT UNSIGNED    NOT NULL DEFAULT 0,
    last_error   VARCHAR(500)    NULL,
    created_at   DATETIME        NOT NULL,
    published_at DATETIME        NULL,
    failed_at    DATETIME        NULL,
    PRIMARY KEY (id),
    KEY idx_outbox_pending (status, id)                    -- the relay's hot query
);
```

> **Concurrency note:** a multi-worker relay should reserve rows with
> `SELECT … FOR UPDATE SKIP LOCKED` (or a `picked_at` claim) so two relays never publish
> the same row. At-least-once tolerates the rare overlap regardless. The adapter's
> `fetchUnpublished()` marks the spot.

## Run it

```bash
composer install

# 1) write 3 orders, each committed atomically with its outbox message (SQLite, no server)
DB_DSN="sqlite:/tmp/babelqueue-outbox.sqlite" php produce.php

# 2) start Redis, then relay the durable rows onto the broker
docker run -d -p 6379:6379 redis:7
DB_DSN="sqlite:/tmp/babelqueue-outbox.sqlite" BROKER_URL="redis://localhost:6379/0" php relay.php
```

Any other-language BabelQueue consumer (Go, Java, Python, …) on the `orders` queue reads
the relayed envelopes natively — same canonical envelope, same `trace_id`.

## Test

```bash
composer install
vendor/bin/phpunit
```

The test drives `InitOrmOutboxStore` against an **in-memory SQLite** database: save →
fetch (oldest-first, limited) → `markPublished`; `markFailed` bumps `attempts` and keeps
the row pending; a full `Outbox` write → `OutboxRelay` flush preserves `trace_id`; and a
throwing transport marks the row failed. It skips cleanly if `pdo_sqlite` is unavailable.

[ADR-0022]: https://babelqueue.com

## License

[MIT](../LICENSE) © Muhammet Şafak
