# BabelQueue — examples

Runnable, cross-language demos that prove the BabelQueue promise: **a message
produced in one language is consumed natively in another**, over the broker you
already run, using one strict JSON envelope.

| Example | Producer → Consumer | Broker | What it shows |
| :--- | :--- | :--- | :--- |
| [`redis-orders/`](redis-orders) | Python · PHP → Go | Redis | Same canonical envelope across languages on a shared Redis queue (URN routing, `trace_id`, `meta.lang`) — swap in either producer, the Go consumer is unchanged |
| [`sqs-orders/`](sqs-orders) | Python → Go | Amazon SQS | The same envelope over SQS with the §3 native `MessageAttributes` projection — runs on free, SQS-compatible ElasticMQ (no AWS account needed) |
| [`pulsar-orders/`](pulsar-orders) | Java → .NET | Apache Pulsar | The same envelope over Pulsar with the §5 message-property projection — runs on a Pulsar standalone container |
| [`kafka-orders/`](kafka-orders) | Java → Go | Apache Kafka | The record value is the envelope with the §6 `bq-` header projection — process-then-commit consume on a Redpanda (Kafka-API) container |
| [`artemis-orders/`](artemis-orders) | Java (JMS) → Python (AMQP 1.0) | Apache ActiveMQ Artemis | **One envelope, two protocols:** Java produces over JMS, Python consumes over AMQP 1.0, Artemis bridges them on the same address (§7 projection) |

Each example uses the **published** SDKs (`pip install "babelqueue[redis]"`,
`go get github.com/babelqueue/babelqueue-go/redis`, …) — nothing vendored.

The full standard is documented at **[babelqueue.com](https://babelqueue.com)**.

## License

[MIT](LICENSE) © Muhammet Şafak
