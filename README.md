# BabelQueue — examples

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
| [`idempotency-payments/`](idempotency-payments) | Python → Go | Redis | At-least-once means the **same** message can arrive twice — a Python producer sends the identical payment envelope twice (same `meta.id`), and a Go consumer wrapped with `idempotency.Wrap` (§ ADR-0022) charges **once** and skips the duplicate: no double charge, deduped on the canonical `meta.id` across languages |
| [`dlq-redrive/`](dlq-redrive) | Python triage + re-drive | Redis | A dead-letter queue is just a queue of canonical envelopes — a Python consumer triages `orders.dlq` (printing the additive top-level `dead_letter` block: `reason`/`error`/`attempts`/`lang`), and a re-drive helper moves messages back to the source `orders` queue for a clean re-run (cross-language: the message a Go/PHP consumer dead-lettered re-drives here). Now also shows **replay-bypass** (`--bypass`, ADR-0027): re-drive a fixed message and skip the external side-effect (charge / email) that already fired |
| [`otel-tracing/`](otel-tracing) | Python → Go | Redis | One **OpenTelemetry** trace across a polyglot hop (§ ADR-0025) — a Python producer publishes inside a `PRODUCER` span and stamps its trace id into the envelope's `trace_id`; a Go consumer wraps its handler with `otel.WrapHandler`, so the `CONSUMER` span lands in the **same** trace via the `trace_id` ↔ OTel TraceID bijection. A console/stdout exporter on both ends prints the spans — same trace id on both hops, no collector needed (W3C `traceparent` parent-child linkage is a documented follow-up) |
| [`outbox-initorm/`](outbox-initorm) | PHP produce (outbox) + relay → any consumer | Redis + InitORM (SQL) | The **transactional outbox** (§ ADR-0029) — a PHP producer commits the business row *and* the BabelQueue message in **one database transaction** (no dual write, no distributed transaction), and a separate relay drains the durable outbox rows onto Redis later. The message is stored as the frozen envelope **verbatim** and relayed byte-for-byte (`trace_id` intact), so any other-language consumer reads it natively. The php-sdk ships the dependency-free `Outbox` helper; this example binds it to a real DB with `initorm/database` — at-least-once handoff, deduped downstream on `meta.id` (the consumer-side mirror of ADR-0022) |
| [`schema-validation/`](schema-validation) | Go produce + consume | Redis | Per-URN **payload schema validation** (§ ADR-0024) — a Go producer validates `data` with `schema.Check` against a draft-07 schema for `urn:babel:orders:created` and publishes only the valid order (the invalid one is rejected **before send**); a Go consumer adds `schema.Wrap` as a safety net (invalid data dead-letters instead of reaching the handler). Both read one governed `registry.json` via a `DirProvider` — the same manifest babelqueue-registry owns |

Each example uses the **published** SDKs (`pip install "babelqueue[redis]"`,
`go get github.com/babelqueue/babelqueue-go/redis`, …) — nothing vendored.

The full standard is documented at **[babelqueue.com](https://babelqueue.com)**.

## License

[MIT](LICENSE) © Muhammet Şafak
