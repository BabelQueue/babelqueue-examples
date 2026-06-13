# BabelQueue — examples

Runnable, cross-language demos that prove the BabelQueue promise: **a message
produced in one language is consumed natively in another**, over the broker you
already run, using one strict JSON envelope.

| Example | Producer → Consumer | Broker | What it shows |
| :--- | :--- | :--- | :--- |
| [`redis-orders/`](redis-orders) | Python · PHP → Go | Redis | Same canonical envelope across languages on a shared Redis queue (URN routing, `trace_id`, `meta.lang`) — swap in either producer, the Go consumer is unchanged |
| [`sqs-orders/`](sqs-orders) | Python → Go | Amazon SQS | The same envelope over SQS with the §3 native `MessageAttributes` projection — runs on free, SQS-compatible ElasticMQ (no AWS account needed) |

Each example uses the **published** SDKs (`pip install "babelqueue[redis]"`,
`go get github.com/babelqueue/babelqueue-go/redis`, …) — nothing vendored.

The full standard is documented at **[babelqueue.com](https://babelqueue.com)**.

## License

[MIT](LICENSE) © Muhammet Şafak
