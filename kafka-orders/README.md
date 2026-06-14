# Kafka orders — Java · PHP produce, Go · PHP consume

A **Java** producer *or* a **PHP** producer publishes canonical BabelQueue envelopes to an
Apache Kafka topic; a **Go** service *or* a **PHP** service reads the *same* topic and routes by
URN. Same wire envelope, different languages, one broker — no PHP `serialize()`, no
language-specific format. A consumer never knows which language wrote the record; it only sees the
canonical envelope and `meta.lang`. PHP reaches Kafka — both **produce and consume** — over
**`ext-rdkafka`** (the only mature PHP Kafka client — an opt-in, GR-7-relaxed path,
[ADR-0019](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-kafka)).

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
# 3) consumer — pick one (both read the same topic, route by URN)

# Go   (needs babelqueue-go/kafka ^1.0)
cd consumer-go && go run .

# …or PHP over ext-rdkafka, process-then-commit   (needs babelqueue/php-sdk ^1.6 + the ext-rdkafka extension)
cd consumer-php && composer install && php consume.php
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
- **PHP consumes too — produce *and* consume over `ext-rdkafka`.** PHP's `KafkaConsumer` reads a
  **Java**-produced record, routes it by URN (`from lang=java`), takes `attempts` from the
  authoritative `bq-attempts` header, and commits the offset only after the handler succeeds
  (process-then-commit) — proven live:

  ```
  [php] urn:babel:orders:created   data={"amount":19.99,"order_id":1001}  from lang=java  attempts=0
  [php] urn:babel:orders:created   data={"amount":39.98,"order_id":1002}  from lang=java  attempts=0
  [php] urn:babel:orders:created   data={"amount":59.97,"order_id":1003}  from lang=java  attempts=0
  [php] urn:babel:orders:shipped   data={"carrier":"DHL","order_id":1002}  from lang=java  attempts=0
  ```

## Cleanup

```bash
docker compose down
```
