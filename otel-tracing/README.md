# OpenTelemetry tracing — one trace across a polyglot hop

A message produced in one language and consumed in another is **one logical
operation** — and OpenTelemetry should see it as one trace. BabelQueue's optional
OTel module ([ADR-0025](https://babelqueue.com)) gives you that without touching
the wire envelope: it correlates the produce and consume spans through the
envelope's `trace_id`, which maps **1:1** to a 128-bit OTel TraceID (the `trace_id`
↔ TraceID bijection). The zero-dependency core never imports OpenTelemetry —
wiring a `TracerProvider` is opt-in.

This example proves the correlation end-to-end, cross-language:

1. A **Python** producer publishes an order with
   `otel.publish(tracer, app, urn, data)` — it opens a **PRODUCER** span
   `publish urn:babel:orders:created` and stamps that span's trace id (as a UUID)
   into the envelope's `trace_id`.
2. A **Go** consumer wraps its handler with `otel.WrapHandler(tracer, handler)` —
   it opens a **CONSUMER** span `process urn:babel:orders:created` whose trace is
   **derived from the same `trace_id`** via the bijection. So the consume span
   lands in the **same trace** as the producer's publish span.

Both ends wire a **console span exporter** (Python `ConsoleSpanExporter`, Go
`stdouttrace`), so each span prints to stdout — the demo is fully self-contained
and runs **without an OTel collector**. Everything else runs on Redis (§1).

## What to look for

The point of the demo is **one trace id on both hops**. The producer prints the
`trace_id` it put on the wire; the consumer's CONSUMER span (and the `trace_id` it
logs) shows the **same** id:

```
[python] PRODUCER span emitted above; the wire trace_id = 663f57bf-0870-a778-a0c8-cd43299a6413
```
```
[go] processed   order_id=1042 amount=99.9 EUR  meta.id=…  trace_id=663f57bf-0870-a778-a0c8-cd43299a6413
```

and the Go CONSUMER span's `TraceID` is `663f57bf0870a778a0c8cd43299a6413` — the
same id with the dashes removed. That is the bijection: the UUID Python stamped on
the wire and the TraceID Go traces under are the same 16 bytes.

## Run it

```bash
# 1) start Redis
docker compose up -d            # or: docker run -d -p 6379:6379 redis:7
```

```bash
# 2) producer — Python   (PRODUCER span; stamps the trace id onto the wire)
#    the [otel] extra brings the OTel API; opentelemetry-sdk wires the console exporter
cd producer-python
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python produce.py
cd ..
```

The producer prints its PRODUCER span (`publish urn:babel:orders:created`) and the
`trace_id` it carried onto the wire:

```
{
    "name": "publish urn:babel:orders:created",
    "context": { "trace_id": "0x663f57bf0870a778a0c8cd43299a6413", ... },
    "kind": "SpanKind.PRODUCER",
    "attributes": { "messaging.system": "babelqueue", "messaging.operation": "publish", ... }
}
[python] published urn:babel:orders:created  meta.id=…
[python] PRODUCER span emitted above; the wire trace_id = 663f57bf-0870-a778-a0c8-cd43299a6413
[python] now run the Go consumer — its CONSUMER span must show the SAME trace id.
```

```bash
# 3) consumer — Go   (CONSUMER span derived from the wire trace_id)
#    needs babelqueue-go ^1.5 + the …/otel submodule (its own module, so OTel stays
#    out of the core); go.mod already pins it. Redis transport is …/redis
cd consumer-go
go run .
```

The consumer prints its CONSUMER span (`process urn:babel:orders:created`) — note
its `TraceID` matches the producer's `trace_id`:

```
{
	"Name": "process urn:babel:orders:created",
	"SpanContext": { "TraceID": "663f57bf0870a778a0c8cd43299a6413", ... },
	"Parent": { "TraceID": "663f57bf0870a778a0c8cd43299a6413", "Remote": true },
	"SpanKind": 5,
	"Attributes": [
		{ "Key": "messaging.system", "Value": { "Type": "STRING", "Value": "babelqueue" } },
		{ "Key": "messaging.message.conversation_id", "Value": { "Type": "STRING", "Value": "663f57bf-0870-a778-a0c8-cd43299a6413" } },
		{ "Key": "messaging.babelqueue.attempts", "Value": { "Type": "INT64", "Value": 0 } }
	]
}
[go] processed   order_id=1042 amount=99.9 EUR  meta.id=…  trace_id=663f57bf-0870-a778-a0c8-cd43299a6413
[go] handled 1 message(s) — each CONSUMER span shares the producer's trace via trace_id.
```

(Go's `stdouttrace` prints `SpanKind: 5` for CONSUMER, `4` for PRODUCER.) The two
spans now share one TraceID, so any OTel backend stitches them into a single
cross-language trace.

## The two span kinds

Both spans carry the [messaging semantic-convention](https://opentelemetry.io/docs/specs/semconv/messaging/)
attributes, drawn from the envelope:

| Span | Kind | Name | Key attributes |
| :--- | :--- | :--- | :--- |
| Publish | `PRODUCER` | `publish <urn>` | `messaging.system=babelqueue`, `messaging.operation=publish`, `messaging.message.id` |
| Consume | `CONSUMER` | `process <urn>` | `messaging.destination.name` (`meta.queue`), `messaging.message.id`, `messaging.message.conversation_id` (`trace_id`), `messaging.babelqueue.attempts` |

Because `attempts` is on every CONSUMER span, a redelivery (`attempts: 2`) is
distinguishable from a first attempt (`attempts: 0`) right on the trace; a handler
that throws records the error and sets the span status to **Error**.

## Honest limit — trace identity today, parent-child linkage next

This bridges trace **identity**: one shared `trace_id` → one OTel trace, so both
hops appear under the same trace. Exact span **parent-child linkage** across the
hop — the consume span pointing at the publish span as its parent, via the W3C
`traceparent` carried as a transport header — is a **documented follow-up**, the
same shape as the broker bindings themselves. Until then the consume span is rooted
in a deterministic remote parent derived from the `trace_id` (so it is valid and in
the right trace), not the producer's exact span. One shared `trace_id` = one trace,
today.

## Configuration

All scripts read these environment variables:

| Variable | Default | Meaning |
| :--- | :--- | :--- |
| `BROKER_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `QUEUE` | `orders` | queue the order envelopes flow over |

## Swap the ends

The correlation key is the canonical `trace_id`, so any SDK can be on either side —
same two span names, same messaging-semconv attributes, same bijection:

- **Go producer:** `bqotel.Publish(ctx, tracer, app, urn, data)` opens the PRODUCER
  span and stamps the trace id.
- **Python consumer:** `from babelqueue import otel`, then
  `app.register(urn, otel.wrap_handler(tracer, handler))` for the CONSUMER span.
- **PHP / Java / Node:** the same `wrapHandler` / `publish` entry points and the
  same `trace_id` ↔ TraceID bijection (`traceIdOf` / `uuidOf`).

See [babelqueue.com](https://babelqueue.com) for the per-SDK OTel APIs.
