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

Each example uses the **published** SDKs (`pip install "babelqueue[redis]"`,
`go get github.com/babelqueue/babelqueue-go/redis`, …) — nothing vendored.

The full standard is documented at **[babelqueue.com](https://babelqueue.com)**.

## License

[MIT](LICENSE) © Muhammet Şafak
