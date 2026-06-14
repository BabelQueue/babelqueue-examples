# Kafka orders — Java and PHP produce, Go consumes

A **Java** producer *or* a **PHP** producer publishes canonical BabelQueue envelopes to an
Apache Kafka topic; a **Go** service reads the *same* topic and routes by URN. Same wire
envelope, different languages, one broker — no PHP `serialize()`, no language-specific format.
The Go consumer never knows which language wrote the record; it only sees the canonical
envelope and `meta.lang`. PHP reaches Kafka over **`ext-rdkafka`** (the only mature PHP Kafka
client — an opt-in transport, [ADR-0019](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-kafka)).

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
# 2) producer — pick one (both emit byte-identical envelopes)

# Java   (needs com.babelqueue:babelqueue-kafka ^1.0)
cd producer-java && mvn compile exec:java

# …or PHP   (needs babelqueue/php-sdk ^1.3 + the ext-rdkafka extension)
cd producer-php && composer install && php produce.php
```

```bash
# 3) consumer — Go   (needs babelqueue-go/kafka ^1.0)
cd consumer-go
go run .
```

The Go consumer (a new group reads from the earliest offset) prints each order as it routes
it by URN. Producing from **PHP** (`lang=php`), the Go side reads the same shape it reads from
Java — proven live over Redpanda:

```
[go] orders:created  order_id=3001  amount=19.99  from lang=php  trace=769c9e53-…  attempts=0
[go] orders:created  order_id=3002  amount=39.98  from lang=php  trace=f70c10dc-…  attempts=0
[go] orders:created  order_id=3003  amount=59.97  from lang=php  trace=6dfb4fe6-…  attempts=0
[go] orders:shipped  order_id=3002  carrier=DHL Express ✈  from lang=php
```

(Run `producer-java` instead and the same consumer prints `from lang=java` — the consumer
code is identical; only `meta.lang` differs.)

`KAFKA_BROKERS` (both sides, default `localhost:9092`) and `CONSUME_SECONDS` (consumer,
default `20`) are configurable via env vars.

## What this proves

- **One envelope, three languages.** Java's `KafkaPublisher`, PHP's `KafkaTransport`
  (`ext-rdkafka`) and Go's `kafka.Transport` agree on the byte-identical envelope value and the
  §6 `bq-` header projection — Java **or** PHP can produce, Go reads either unchanged.
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
