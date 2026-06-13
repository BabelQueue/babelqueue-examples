# Artemis orders — Java (JMS) produces, Python (AMQP 1.0) consumes

A **Java** producer publishes canonical BabelQueue envelopes to an Apache ActiveMQ **Artemis**
address over **JMS** (the CORE protocol); a **Python** service reads the *same* address over
**AMQP 1.0** and routes by URN. Same wire envelope, **two protocols**, two languages, one broker —
no PHP `serialize()`, no language-specific format. Python never knows which language (or protocol)
wrote a message; it sees only the canonical envelope and `meta.lang`.

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
# 3) consumer — Python   (needs babelqueue[artemis] ^1.5)
cd consumer-python
python -m venv .venv && . .venv/bin/activate
pip install "babelqueue[artemis]"
python consume.py
```

The Python consumer reads the four messages Java produced and routes each by URN:

```
[python] orders:created  order_id=1001  amount=19.99  from lang=java
[python] orders:created  order_id=1002  amount=39.98  from lang=java
[python] orders:created  order_id=1003  amount=59.97  from lang=java
[python] orders:shipped  order_id=1002  carrier=DHL  from lang=java
[python] done — consumed 4 message(s).
```

`ARTEMIS_URL` is configurable on both sides: the Java producer defaults to
`tcp://localhost:61616` (CORE/JMS), the Python consumer to `artemis://localhost:5672` (AMQP 1.0).

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

## Cleanup

```bash
docker compose down
```
