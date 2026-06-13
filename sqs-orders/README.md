# SQS orders — Python produces, Go consumes

A **Python** producer publishes canonical BabelQueue envelopes to an Amazon SQS
queue; a **Go** service reads the *same* queue and routes by URN. Same wire
envelope, different languages, one broker — no PHP `serialize()`, no
language-specific format. The Go consumer never knows which language wrote the
message; it only sees the canonical envelope and `meta.lang`.

Each SDK uses the [§3 SQS binding](https://babelqueue.com/docs/spec/1.x/broker-bindings):
the envelope is the `MessageBody`, projected onto native `MessageAttributes`
(`bq-job` = URN, `bq-trace-id`, `bq-message-id`, …) so a consumer can route and trace
without decoding the body.

The demo runs against **ElasticMQ** — a free, SQS-compatible broker — so it needs no AWS
account. Point `SQS_ENDPOINT` / `AWS_REGION` (and real credentials) at Amazon SQS to run
it there unchanged.

## Run it

```bash
# 1) start ElasticMQ (pre-creates the `orders` queue via elasticmq.conf)
docker compose up -d
```

```bash
# 2) producer — Python   (needs babelqueue[sqs] ^1.1, which ships SqsTransport)
cd producer-python
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python produce.py
cd ..
```

```bash
# 3) consumer — Go
cd consumer-go
go run .
```

The producer/consumer default to the local ElasticMQ (`http://localhost:9324`,
region `eu-central-1`, dummy credentials), so the commands above work as-is.

Expected consumer output — a Go program reading messages it never produced. Note
**`produced by "python"`**: the data, `trace_id` and unicode all survive the language
boundary.

```
[go] order created  id=1042 amount=99.9 USD  trace=…  (produced by "python")
[go] order created  id=1043 amount=12.5 EUR  trace=…  (produced by "python")
[go] item indexed   sku=WIDGET-1 title="Café Widget ☕"  (produced by "python")
[go] processed 3 message(s) — same envelope, different language.
```

## Point it at real Amazon SQS

```bash
export SQS_ENDPOINT=                       # leave empty to use the default AWS endpoint
export AWS_REGION=eu-central-1
export AWS_ACCESS_KEY_ID=…  AWS_SECRET_ACCESS_KEY=…
# create an `orders` queue in that region, then run the producer + consumer as above.
```

For a FIFO queue (`orders.fifo`), the producer sets `MessageGroupId` /
`MessageDeduplicationId` automatically; see the
[SQS binding](https://babelqueue.com/docs/spec/1.x/broker-bindings#amazon-sqs).

## Swap the ends

The queue carries the canonical envelope, so any SDK can be on either side:

- **Go producer:** `sqs.New(ctx, …)` + `app.Publish(...)`.
- **Python consumer:** `@app.handler("urn:...")` + `app.run()` on a `sqs://` URL.
- **Node / Java / PHP / .NET** read/write the identical envelope on their own SQS
  transports — see [babelqueue.com](https://babelqueue.com).
