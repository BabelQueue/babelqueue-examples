# Artemis orders — Java (JMS) · PHP (STOMP) produce, Python (AMQP 1.0) · Laravel (STOMP) consume

A **Java** producer publishes canonical BabelQueue envelopes to an Apache ActiveMQ **Artemis**
address over **JMS** (the CORE protocol); a **Python** service reads the *same* address over
**AMQP 1.0**, and a **Laravel** worker reads it over **STOMP** — each routes by URN. Same wire
envelope, **three protocols**, multiple languages, one broker — no PHP `serialize()`, no
language-specific format. A consumer never knows which language (or protocol) wrote a message; it
sees only the canonical envelope and `meta.lang`.

This is the cross-protocol proof at the heart of the
[§7 Artemis binding](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-activemq-artemis):
the envelope JSON is the message **body**, the URN rides `JMSType` (the `x-opt-jms-type`
annotation over AMQP) so a consumer routes without decoding the body, `trace_id` rides
`JMSCorrelationID` / `correlation-id`, and the `bq_` application properties carry the rest.
Artemis bridges JMS and AMQP 1.0 on the same address — that is the whole point.

> The `bq_` properties use **underscores** (`bq_schema_version`), not the hyphens of the
> Kafka/Pulsar bindings: a JMS property name must be a valid Java identifier. See
> [ADR-0017](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-activemq-artemis).

## Run it

```bash
# 1) start Artemis (CORE/JMS on 61616, AMQP 1.0 on 5672, console on 8161)
docker compose up -d
# wait until healthy (~15s): docker compose ps
```

```bash
# 2) producer — Java   (needs com.babelqueue:babelqueue-artemis ^1.0)
cd producer-java
mvn compile exec:java
```

```bash
# 2b) producer — PHP over STOMP   (needs babelqueue/php-sdk ^1.1 + stomp-php ^5; §7 STOMP path)
cd producer-php-stomp
composer install
php produce.php
```

```bash
# 3) consumer — pick one (both read the same 'orders' address, each over its own protocol)

# Python over AMQP 1.0   (needs babelqueue[artemis] ^1.5)
#   Start it FIRST (it creates the anycast 'orders' queue), then run a producer.
cd consumer-python
python -m venv .venv && . .venv/bin/activate
pip install "babelqueue[artemis]"
python consume.py

# …or Laravel over STOMP   (needs babelqueue/laravel ^1.2 + stomp-php ^5; the babelqueue-artemis driver)
cd consumer-laravel && composer install && php consume.php
```

A consumer reads the messages a producer sent — over **JMS** *or* **STOMP** — and routes each by
URN. Producing from **Java (JMS)** and consuming with the **Laravel STOMP driver** (proven live):

```
[laravel] urn:babel:orders:created   data={"order_id":1001,"amount":19.99}  from lang=java  trace=…  attempts=1
[laravel] urn:babel:orders:created   data={"order_id":1002,"amount":39.98}  from lang=java  trace=…  attempts=1
[laravel] urn:babel:orders:created   data={"order_id":1003,"amount":59.97}  from lang=java  trace=…  attempts=1
[laravel] urn:babel:orders:shipped   data={"order_id":1002,"carrier":"DHL"}  from lang=java  trace=…  attempts=1
[laravel] done — consumed 4 message(s), all ACKed.
```

Java produced over **JMS** (CORE, 61616); Laravel consumed over **STOMP** (61613); Artemis bridged
the protocols on the same `orders` address. The Laravel `babelqueue-artemis` driver subscribes with
`client-individual` ack, routes **body-authoritatively** on the envelope's `job` URN (§7.8), and
ACKs each message via `delete()` — in a real app it is a drop-in `php artisan queue:work
babelqueue-artemis` (see `consumer-laravel/consume.php`). `ARTEMIS_URL` / `ARTEMIS_STOMP` /
`ARTEMIS_STOMP_PORT` are env-configurable.

## What this proves

- **One envelope, two protocols.** Java's `ArtemisPublisher` sends over **JMS**; Python's
  `ArtemisTransport` consumes over **AMQP 1.0**. Artemis bridges them on the `orders` address —
  this is the binding's reference interop pair (Java → Python).
- **URN routing without decoding.** The consumer routes on the URN carried by `JMSType` /
  `x-opt-jms-type` (`urn:babel:orders:created` vs `urn:babel:orders:shipped`); it never parses a
  message it has no handler for.
- **Trace propagation.** Each message's `trace_id` (carried on `JMSCorrelationID` /
  `correlation-id`) survives the protocol hop unchanged.
- **The body is authoritative.** Python reads `meta.lang=java`, `order_id`, `amount`, `carrier`
  straight from the byte-identical envelope body — the `bq_` properties are a redundant view.
- **PHP consumes too — three protocols on one address.** The Laravel `babelqueue-artemis` driver
  reads a **Java(JMS)**-produced message over **STOMP** and routes it by URN (`from lang=java`):
  Java↔JMS, Python↔AMQP 1.0 and Laravel↔STOMP all meet on the `orders` address. This is the §7
  PHP **consume** half (the producer half is the `php-sdk` `StompTransport`).

## Cleanup

```bash
docker compose down
```
