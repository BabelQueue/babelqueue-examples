# BabelQueue — examples

[![live interop](https://img.shields.io/endpoint?url=https%3A%2F%2Fbabelqueue.com%2Fbadges%2Fnightly.json)](https://github.com/BabelQueue/babelqueue-examples/actions/workflows/nightly-interop.yml)

Runnable, cross-language demos that prove the BabelQueue promise: **a message
produced in one language is consumed natively in another**, over the broker you
already run, using one strict JSON envelope.

| Example | Producer → Consumer | Broker | What it shows |
| :--- | :--- | :--- | :--- |
| [`redis-orders/`](redis-orders) | Python · PHP → Go / Java | Redis | Same canonical envelope on a shared Redis queue (§1 reliable-queue, URN routing, `trace_id`, `meta.lang`) — swap in either producer, and a Go **or** Java consumer reads it unchanged |
| [`rabbitmq-orders/`](rabbitmq-orders) | Python → Node | RabbitMQ | The same envelope over RabbitMQ with the §2 AMQP 0-9-1 property projection (`type` = URN, `correlation_id`, `x-` headers) — Node routes a Python-produced message by `properties.type` |
| [`sqs-orders/`](sqs-orders) | Python → Go | Amazon SQS | The same envelope over SQS with the §3 native `MessageAttributes` projection — runs on free, SQS-compatible ElasticMQ (no AWS account needed) |
| [`pulsar-orders/`](pulsar-orders) | Java · PHP → .NET · PHP | Apache Pulsar | The same envelope over Pulsar with the §5 message-property projection — Java (native) **or** PHP (pure-PHP WebSocket API) produces; .NET (native) **or** PHP (WebSocket consumer API) consumes on a Pulsar standalone container |
| [`kafka-orders/`](kafka-orders) | Java · PHP → Go · PHP | Apache Kafka | The record value is the envelope with the §6 `bq-` header projection — Java (native) **or** PHP (`ext-rdkafka`) produces; Go (native) **or** PHP (`ext-rdkafka`) consumes process-then-commit on a Redpanda (Kafka-API) container |
| [`artemis-orders/`](artemis-orders) | Java (JMS) · PHP (STOMP) → Python (AMQP 1.0) · Laravel (STOMP) | Apache ActiveMQ Artemis | **One envelope, three protocols:** Java produces over JMS, PHP over STOMP; Python consumes over AMQP 1.0 and a Laravel worker over STOMP (the `babelqueue-artemis` driver) — Artemis bridges them on the same address (§7) |
| [`asb-orders/`](asb-orders) | Python → Go | Azure Service Bus | The same envelope over Azure Service Bus with the §4 AMQP `ApplicationProperties` projection — Go routes a Python-produced message by the `bq-job` property; runs on the official Service Bus emulator (no Azure subscription needed). PHP has no Service Bus path (ADR-0021) |
| [`idempotency-payments/`](idempotency-payments) | Python → Go | Redis | At-least-once means the **same** message can arrive twice — a Python producer sends the identical payment envelope twice (same `meta.id`), and a Go consumer wrapped with `idempotency.Wrap` (§ ADR-0022) charges **once** and skips the duplicate: no double charge, deduped on the canonical `meta.id` across languages |
| [`idempotency-inmemory/`](idempotency-inmemory) | Python → Python | **none** (in-memory) | The **broker-free** companion to `idempotency-payments` — one `python charge_once.py`, no Redis/Docker. Publishes the **same** payment envelope twice on the `memory://` transport and wraps the handler with `idempotency.wrap` (§ ADR-0022); the side-effect fires **once** and the script asserts it (doubles as a smoke test). The runnable form of the cross-SDK `idempotency` conformance fixtures: at-least-once delivery → exactly-once **effect** |
| [`dlq-redrive/`](dlq-redrive) | Python triage + re-drive | Redis | A dead-letter queue is just a queue of canonical envelopes — a Python consumer triages `orders.dlq` (printing the additive top-level `dead_letter` block: `reason`/`error`/`attempts`/`lang`), and a re-drive helper moves messages back to the source `orders` queue for a clean re-run (cross-language: the message a Go/PHP consumer dead-lettered re-drives here). Now also shows **replay-bypass** (`--bypass`, ADR-0027): re-drive a fixed message and skip the external side-effect (charge / email) that already fired |
| [`otel-tracing/`](otel-tracing) | Python → Go | Redis | One **OpenTelemetry** trace across a polyglot hop (§ ADR-0025) — a Python producer publishes inside a `PRODUCER` span and stamps its trace id into the envelope's `trace_id`; a Go consumer wraps its handler with `otel.WrapHandler`, so the `CONSUMER` span lands in the **same** trace via the `trace_id` ↔ OTel TraceID bijection. A console/stdout exporter on both ends prints the spans — same trace id on both hops, no collector needed (W3C `traceparent` parent-child linkage is a documented follow-up) |
| [`outbox-initorm/`](outbox-initorm) | PHP produce (outbox) + relay → any consumer | Redis + InitORM (SQL) | The **transactional outbox** (§ ADR-0029) — a PHP producer commits the business row *and* the BabelQueue message in **one database transaction** (no dual write, no distributed transaction), and a separate relay drains the durable outbox rows onto Redis later. The message is stored as the frozen envelope **verbatim** and relayed byte-for-byte (`trace_id` intact), so any other-language consumer reads it natively. The php-sdk ships the dependency-free `Outbox` helper; this example binds it to a real DB with `initorm/database` — at-least-once handoff, deduped downstream on `meta.id` (the consumer-side mirror of ADR-0022) |
| [`schema-validation/`](schema-validation) | Go produce + consume | Redis | Per-URN **payload schema validation** (§ ADR-0024) — a Go producer validates `data` with `schema.Check` against a draft-07 schema for `urn:babel:orders:created` and publishes only the valid order (the invalid one is rejected **before send**); a Go consumer adds `schema.Wrap` as a safety net (invalid data dead-letters instead of reaching the handler). Both read one governed `registry.json` via a `DirProvider` — the same manifest babelqueue-registry owns |

Each example uses the **published** SDKs (`pip install "babelqueue[redis]"`,
`go get github.com/babelqueue/babelqueue-go/redis`, …) — nothing vendored.

The full standard is documented at **[babelqueue.com](https://babelqueue.com)**.

## Nightly live interop

Every night (03:17 UTC, and on demand via **Run workflow**) the
[`nightly-interop`](.github/workflows/nightly-interop.yml) workflow runs one producer → consumer
pair per broker against a **real broker container**, using the published SDKs from their
registries. Seven brokers, one job each:

| Broker | Producer → Consumer | Container | Example |
| :--- | :--- | :--- | :--- |
| Redis | Python → Go | `redis:7` | [`redis-orders/`](redis-orders) |
| RabbitMQ | Python → Node | `rabbitmq:3-management` | [`rabbitmq-orders/`](rabbitmq-orders) |
| Amazon SQS | Python → Go | LocalStack | [`sqs-orders/`](sqs-orders) |
| Apache Pulsar | Java → .NET | Pulsar standalone | [`pulsar-orders/`](pulsar-orders) |
| Apache Kafka | Java → Go | Redpanda | [`kafka-orders/`](kafka-orders) |
| ActiveMQ Artemis | Java (JMS) → Python (AMQP 1.0) | Artemis | [`artemis-orders/`](artemis-orders) |
| Azure Service Bus | Python → Go | Service Bus emulator + SQL Server 2022 | [`asb-orders/`](asb-orders) |

A job passes only when the consumer prints the expected number of messages. The last job merges
the per-broker records with [`scripts/interop-merge.sh`](scripts/interop-merge.sh) into
`interop-results.json` (`generated_at`, `run_url`, `brokers[]`) and a shields.io endpoint
`badge.json`, and uploads both to the rolling `nightly` prerelease (`gh release upload --clobber`;
the tag is never moved). The badge at the top of this page reads that `badge.json`.

**Staleness.** GitHub disables `schedule` triggers in a repository with 60 days without activity,
and the badge would then keep showing the last result forever. `interop-results.json` carries
`generated_at`; treat a result older than ~48 hours as stale and re-enable the workflow (or run it
with **Run workflow**) when that happens.

This is extra evidence of cross-language compatibility, not a condition for the "Compatible" label
(ADR-0032).

## R1 and post-R1 behaviour scenarios

Beyond the one-pair-per-broker smoke test, six scenarios prove the R1 behaviours (Redis delayed
delivery and crash recovery, Kafka retry topics) and the post-R1 Pulsar exhaustion rules
([ADR-0036](https://babelqueue.com/docs/spec/1.x/broker-bindings/#apache-pulsar)) across languages.
Each script starts its broker with `docker compose` (`redis-orders/`, `kafka-orders/` or
`pulsar-orders/`), builds the probes it needs, runs the scenario and exits 0 only if every check
passes. They also run in the nightly workflow as the `r1-scenarios` matrix.

| Script | What it proves |
| :--- | :--- |
| [`scripts/redis-delay.sh`](scripts/redis-delay.sh) | Go publishes with `delay=2s`; Node, Java, .NET and Python each receive it. Measured delay is logged and must be within [1.0, 3.5] s. |
| [`scripts/redis-recovery.sh`](scripts/redis-recovery.sh) | `kill -9` of a consumer that reserved a message (`retry_after` 5 s); another language receives it. Zset layout: `attempts` +1. Legacy `:processing` layout: `attempts` unchanged and `:processing` / `:processing:deadlines` end empty. |
| [`scripts/redis-late-ack.sh`](scripts/redis-late-ack.sh) | Legacy layout: consumer A holds a message past `retry_after`, B receives the re-queued copy, A's late ack is a no-op ("reservation expired" warning), B acks; the message is processed once and the keys end empty. |
| [`scripts/redis-laravel-shared.sh`](scripts/redis-laravel-shared.sh) | A Laravel `babelqueue-redis` connection and a Go consumer share one zset queue; every id is acked exactly once. |
| [`scripts/kafka-retry.sh`](scripts/kafka-retry.sh) | Java consumer fails, the record goes to `<topic>.retry.1`, is re-injected and a Python consumer processes it. Checks `bq-attempts` and body `attempts` both +1, value and key byte-identical, no `bq-delay` / `bq-original-topic` on the re-injected record, `traceparent` preserved, wait equals the tier. |
| [`scripts/pulsar-exhaustion.sh`](scripts/pulsar-exhaustion.sh) | A Go producer and DLQ reader against Java and Node consumers whose handler always throws. A valid envelope: the handler runs exactly 3 times (attempts 0, 1, 2) and exactly one record lands on `<queue>.dlq` with the same `meta.id` / `trace_id`, `dead_letter.reason: "failed"`, `dead_letter.attempts` 3, body `attempts` 3 and `bq-attempts` 3, on an `Exclusive` subscription (a re-publish retry; the native redelivery count stays 0) and on a `Shared` one (the nack fallback; the count goes 0, 1, 2). Poison: a JSON object with no `job` / `urn` and a body that is not valid UTF-8 never reach the handler and are recorded once as the [ADR-0035](https://babelqueue.com/docs/spec/1.x/dead-letter-and-tracing/) splice and base64 wrapper, the raw bytes intact. Nothing else arrives during the DLQ reader's quiet window. |

```bash
scripts/redis-delay.sh
scripts/redis-recovery.sh
scripts/redis-late-ack.sh
scripts/redis-laravel-shared.sh
scripts/kafka-retry.sh
scripts/pulsar-exhaustion.sh

# against the in-repo SDK sources instead of the published packages (pre-release check)
BQ_SDK_ROOT=/path/to/BabelQueue scripts/redis-delay.sh
```

Needs Docker, bash 3.2+ and the toolchains of the probes a scenario uses (Go, Node, JDK 17 + Maven,
.NET 8, Python 3, PHP + Composer). Probes live under `redis-orders/interop/`,
`kafka-orders/interop/` and `pulsar-orders/interop/`; the shared helpers are in
`scripts/lib/interop-common.sh`.

`pulsar-exhaustion.sh` pins the first SDK releases that carry the ADR-0036 behaviour
(`com.babelqueue:babelqueue-pulsar` 1.2.0 in `pulsar-orders/interop/java/pom.xml` and
`@babelqueue/pulsar` ^1.3.0 in `pulsar-orders/interop/node/package.json`; the Go probe only
produces and reads, so it stays on the published Go modules). Until those versions are on Maven
Central and npm, run it with `BQ_SDK_ROOT` as above (it builds the in-repo Java and Node packages);
the nightly job is red until then, like the other pinned scenarios. `PULSAR_CONSUMERS="java"` or
`"node"` limits it to one consumer language, and `PULSAR_NO_COMPOSE=1` uses a broker you already run
(`PULSAR_URL`, default `pulsar://localhost:6650`; the topic stats come from `PULSAR_ADMIN_URL`, default
`http://localhost:8080`). It also needs `curl` and `jq`, and each case asserts that the work topic ends with
no backlog, so the retried original was acknowledged.

## License

[MIT](LICENSE) © Muhammet Şafak
