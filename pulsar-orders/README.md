# Pulsar orders — Java and PHP produce, .NET consumes

A **Java** producer *or* a **PHP** producer publishes canonical BabelQueue envelopes to an
Apache Pulsar topic; a **.NET** service reads the *same* topic and routes by URN. Same wire
envelope, different languages, one broker — no PHP `serialize()`, no language-specific format.
The .NET consumer never knows which language wrote the message; it only sees the canonical
envelope and `meta.lang`. PHP reaches Pulsar over Pulsar's native **WebSocket API** (a pure-PHP
client — no C extension; [ADR-0020](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-pulsar)).

Each SDK uses the [§5 Pulsar binding](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-pulsar):
the envelope is the message payload, projected onto native Pulsar message properties
(`bq-job` = URN, `bq-trace-id`, `bq-message-id`, …) so a consumer can route and trace
without decoding the body.

The demo runs against **Apache Pulsar standalone** — a single-node broker in one
container — so it needs no managed Pulsar. Point `PULSAR_URL` at any Pulsar cluster
(`pulsar://…` or `pulsar+ssl://…`) to run it there unchanged.

## Run it

```bash
# 1) start Pulsar standalone (binary protocol on 6650, admin on 8080)
docker compose up -d
# wait until healthy (~30s): docker compose ps
```

```bash
# 2) consumer — .NET   (needs BabelQueue.Pulsar ^1.0, which ships PulsarConsumer)
#    Start it FIRST so its 'babelqueue' subscription exists before the producer publishes
#    (a Shared subscription only receives messages sent after it is created).
cd consumer-dotnet
dotnet run
```

```bash
# 3) producer — pick one (both emit byte-identical envelopes + the same §5 properties)

# Java   (needs com.babelqueue:babelqueue-pulsar ^1.0, binary protocol on 6650)
cd producer-java && mvn compile exec:java

# …or PHP   (needs babelqueue/php-sdk ^1.4 + textalk/websocket, the WebSocket API on 8080)
cd producer-php && composer install && php produce.php
```

The .NET consumer prints each order as it routes it by URN. From **Java** it reads
`from lang=java`; from **PHP** (`PulsarTransport` over the WebSocket API) it reads the same
shape with `from lang=php` — the consumer code is identical, only `meta.lang` differs:

```
[dotnet] orders:created  order_id=5001  amount=19.99  from lang=php  trace=…  attempts=0
[dotnet] orders:created  order_id=5002  amount=39.98  from lang=php  trace=…  attempts=0
[dotnet] orders:created  order_id=5003  amount=59.97  from lang=php  trace=…  attempts=0
[dotnet] orders:shipped  order_id=5002  carrier=DHL Express ✈  from lang=php
```

`PULSAR_URL` (Java + .NET, default `pulsar://localhost:6650`), `PULSAR_WS` (PHP, default
`ws://localhost:8080/ws/v2`) and `CONSUME_SECONDS` (consumer, default `15`) are configurable via
env vars.

> The PHP producer publishes over the WebSocket API; the `Shared`-subscription note above still
> applies (start the .NET consumer first, or it only sees messages sent after its subscription
> exists).

## What this proves

- **One envelope, three languages.** Java's `PulsarPublisher` (native client), PHP's
  `PulsarTransport` (WebSocket API) and .NET's `PulsarConsumer` agree on the byte-identical
  envelope and the §5 `bq-` property projection — Java **or** PHP can produce, .NET reads either
  unchanged. PHP's WebSocket client is **pure-PHP** (no C extension — GR-7 intact).
- **URN routing without decoding.** The consumer routes on the `bq-job` property
  (`urn:babel:orders:created` vs `urn:babel:orders:shipped`); it never parses a message it
  has no handler for.
- **Trace propagation.** Each message's `trace_id` survives the hop unchanged.
- **`attempts` reconciliation.** A first delivery reads `attempts = 0`
  (`max(body, RedeliveryCount)`, and Pulsar's redelivery count is 0-based).

## Cleanup

```bash
docker compose down
```
