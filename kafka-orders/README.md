# Kafka orders — Java produces, Go consumes

A **Java** producer publishes canonical BabelQueue envelopes to an Apache Kafka topic; a
**Go** service reads the *same* topic and routes by URN. Same wire envelope, different
languages, one broker — no PHP `serialize()`, no language-specific format. The Go consumer
never knows which language wrote the record; it only sees the canonical envelope and
`meta.lang`.

Each SDK uses the [§6 Kafka binding](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-kafka):
the record **value** is the envelope JSON, the contract fields are mirrored onto `bq-` record
headers (`bq-job` = URN, `bq-trace-id`, `bq-message-id`, …) so a consumer routes and traces
without decoding the body, and the record timestamp mirrors `meta.created_at`. Consume is
**process-then-commit** (manual offset commit, at-least-once); `bq-attempts` is the
authoritative attempt counter.

The demo runs against **Redpanda** — a single-binary, Kafka-API-compatible broker — so it
needs no managed Kafka. Point `KAFKA_BROKERS` at any Kafka cluster to run it there unchanged.

## Run it

```bash
# 1) start Redpanda (Kafka API on 9092)
docker compose up -d
# wait until healthy (~10s): docker compose ps
docker compose exec redpanda rpk topic create orders   # create the work topic
```

```bash
# 2) producer — Java   (needs com.babelqueue:babelqueue-kafka ^1.0)
cd producer-java
mvn compile exec:java
```

```bash
# 3) consumer — Go   (needs babelqueue-go/kafka ^1.0)
cd consumer-go
go run .
```

The Go consumer (a new group reads from the earliest offset) prints each order as it routes
it by URN:

```
[go] orders:created  order_id=1001  amount=19.99  from lang=java  trace=…  attempts=0
[go] orders:created  order_id=1002  amount=39.98  from lang=java  trace=…  attempts=0
[go] orders:created  order_id=1003  amount=59.97  from lang=java  trace=…  attempts=0
[go] orders:shipped  order_id=1002  carrier=DHL    from lang=java
```

`KAFKA_BROKERS` (both sides, default `localhost:9092`) and `CONSUME_SECONDS` (consumer,
default `20`) are configurable via env vars.

## What this proves

- **One envelope, two languages.** Java's `KafkaPublisher` and Go's `kafka.Transport` agree
  on the byte-identical envelope value and the §6 `bq-` header projection — this is the
  binding's reference parity pair (Java → Go).
- **URN routing without decoding.** The consumer routes on the `bq-job` header
  (`urn:babel:orders:created` vs `urn:babel:orders:shipped`); it never parses a record it has
  no handler for.
- **Trace propagation.** Each record's `trace_id` survives the hop unchanged.
- **`attempts` from the authoritative header.** A first delivery reads `attempts = 0` from
  `bq-attempts` (Kafka has no native delivery count; the header is the home).
- **Process-then-commit.** The Go consumer commits the offset only after the handler returns
  (at-least-once).

## Cleanup

```bash
docker compose down
```
