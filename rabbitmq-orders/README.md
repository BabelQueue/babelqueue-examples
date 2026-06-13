# RabbitMQ orders — Python produces, Node consumes

A **Python** producer publishes canonical BabelQueue envelopes to a RabbitMQ queue; a
**Node** service reads the *same* queue and routes by URN. Same wire envelope, different
languages, one broker — no PHP `serialize()`, no language-specific format. The Node consumer
never knows which language wrote the message; it only sees the canonical envelope and
`meta.lang`.

Each SDK uses the [§2 RabbitMQ binding](https://babelqueue.com/docs/spec/1.x/broker-bindings#rabbitmq):
the message **body** is the envelope JSON, and the contract fields are projected onto native
AMQP 0-9-1 properties so a consumer routes without decoding the body — `type` = URN,
`correlation_id` = `trace_id`, `message_id` = `meta.id`, `app_id` = `babelqueue`, plus the
native-typed `x-schema-version` / `x-source-lang` / `x-attempts` headers (AMQP field-tables
carry typed values — integers stay integers). Consume is `basic.get` + manual ack
(at-least-once); routing reads `properties.type`. `x-attempts` is the authoritative attempt
counter — RabbitMQ has no native delivery count for this transport, so `attempts` lives in
the body.

The demo runs against a stock **`rabbitmq:3-management`** container, so it needs no managed
broker. Point `BROKER_URL` at any RabbitMQ to run it there unchanged.

## Run it

```bash
# 1) start RabbitMQ (AMQP on 5672, management UI on http://localhost:15672 — guest/guest)
docker compose up -d
# wait until healthy (~15-20s): docker compose ps
```

```bash
# 2) producer — Python   (needs babelqueue[amqp] from PyPI)
cd producer-python
python -m venv .venv && .venv/bin/pip install "babelqueue[amqp]"
.venv/bin/python produce.py
```

```bash
# 3) consumer — Node   (needs @babelqueue/rabbitmq ^1.0.0 + amqplib)
cd consumer-node
npm install
node consume.mjs
```

The Node consumer polls the `orders` queue and prints each order as it routes it by URN:

```
[node] consuming from 'orders' until 4 message(s) handled...
[node] orders:created  order_id=1001  amount=19.99  from lang=python  trace=b3e1d875-1be1-4ae4-9d9d-d2523910de27  attempts=0
[node] orders:created  order_id=1002  amount=39.98  from lang=python  trace=ca00b24c-5200-47fe-b995-333cb8a4eb3b  attempts=0
[node] orders:created  order_id=1003  amount=59.97  from lang=python  trace=8af7b35d-3a0f-4c95-a087-b8bf0cbce6f3  attempts=0
[node] orders:shipped  order_id=1002  carrier=DHL  from lang=python
[node] done — handled 4 message(s).
```

`BROKER_URL` (both sides, default `amqp://guest:guest@localhost:5672/`) and
`CONSUME_MESSAGES` (consumer, default `4`) are configurable via env vars.

## What this proves

- **One envelope, two languages.** Python's `PikaTransport` and Node's `RabbitMQConsumer`
  agree on the byte-identical envelope body and the §2 AMQP property projection
  (Python → Node).
- **URN routing without decoding.** The consumer routes on `properties.type`
  (`urn:babel:orders:created` vs `urn:babel:orders:shipped`); it never parses a message it
  has no handler for.
- **Trace propagation.** Each message's `trace_id` (carried as `correlation_id`) survives the
  hop unchanged.
- **`attempts` from the body.** A first delivery reads `attempts = 0` (RabbitMQ has no native
  delivery count for this transport; the body — mirrored to `x-attempts` — is the home).
- **Manual ack (at-least-once).** The Node consumer `ack`s a message only after the handler
  returns.

## Cleanup

```bash
docker compose down
```
