# Idempotent payments — the same charge twice, charged once

Queues are **at-least-once**: a client retries, a broker redelivers, a proxy
double-sends — and the *same* message arrives more than once. For a payment that
means a **double charge**. BabelQueue ships an optional, dependency-free
idempotency guard ([ADR-0022](https://babelqueue.com)) that dedupes a consumer on
`meta.id` — the canonical per-message identity — so a message whose id was already
processed is acked and **skipped** instead of run again.

This example proves it end-to-end, cross-language:

1. A **Python** producer publishes the **same** payment envelope **twice** — the
   identical `meta.id`, `trace_id` and `data`, byte-for-byte (what a redelivery
   actually looks like on the wire).
2. A **Go** consumer wraps its charge handler with `idempotency.Wrap(store, …)`.
   The first delivery **charges**; the second is recognised as a replay of an
   already-processed id and **skipped** — no double charge.

The dedupe key is `meta.id`, so it works across SDKs: a payment a **PHP** or
**Python** producer sent is de-duplicated by a **Go**, **Java** or **Python**
consumer — same envelope, same identity, one charge. The guard is purely additive,
so the wire envelope stays frozen at `schema_version: 1`.

> Idempotency here is **seen-set, post-success dedupe** under at-least-once with an
> idempotent handler — not exactly-once and not an in-flight concurrency lock. It
> stops an *accidental* duplicate from re-running the side-effect. (A *deliberate*
> replay off a DLQ is the [`dlq-redrive/`](../dlq-redrive) replay-bypass story.)

## Run it

```bash
# 1) start Redis
docker compose up -d            # or: docker run -d -p 6379:6379 redis:7
```

```bash
# 2) producer — Python   (sends the SAME payment twice, same meta.id)
cd producer-python
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python charge.py
cd ..
```

```
[python] published attempt 1  meta.id=…  amount=4200 EUR
[python] published attempt 2  meta.id=…  amount=4200 EUR
[python] 2 deliveries of the SAME payment (meta.id=…) on the 'payments' Redis list — now run the consumer; it must charge exactly once.
```

```bash
# 3) consumer — Go   (needs babelqueue-go ^1.5, which ships the idempotency helper
#    in the core module — no extra `go get`; the Redis transport is …/redis)
cd consumer-go
go run .
```

Expected consumer output — **two** deliveries arrive, but the side-effect runs
**once**. The second is skipped because its `meta.id` was already processed:

```
[go] charged       payment_id=pay_7f3a amount=4200 EUR  meta.id=…  (charged once)
[go] skipped       payment_id=pay_7f3a  meta.id=…  (duplicate — not charged again)
[go] handled 2 delivery(ies) — charged exactly once, no double charge.
```

## How the guard works

`idempotency.Wrap(store, handler)` returns a handler that, on each delivery:

- looks up the envelope's `meta.id` in a `Store` (a "seen-set");
- if **already seen** → returns `nil` (the runtime acks it, so the broker stops
  redelivering) **without** calling your handler — the charge does not re-run;
- if **new** → runs your handler, then records the id as processed **only on
  success**. A handler that raises leaves the id unrecorded, so retry / dead-letter
  still apply and a later delivery runs it again.

The reference `InMemoryStore` is for tests and single-process consumers. A
production fleet uses a **Redis- or database-backed** `Store` (the same three
methods — `Seen` / `Remember` / `Forget`) so dedupe is shared across workers.

## Configuration

All scripts read these environment variables:

| Variable | Default | Meaning |
| :--- | :--- | :--- |
| `BROKER_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `QUEUE` | `payments` | queue the payment envelopes flow over |

## Swap the ends

The dedupe key is the canonical `meta.id`, so any SDK can be on either side:

- **Python consumer:** `from babelqueue.idempotency import InMemoryStore, wrap`,
  then `app.register("urn:babel:payments:charge", wrap(store, handler))`.
- **PHP consumer:** the `BabelQueue\Idempotency` helper, same `meta.id` dedupe.
- **PHP / Go / Java / Node producer:** publish the envelope; a redelivery (same
  `meta.id`) is what the guard collapses — the producer needs no special code.

See [babelqueue.com](https://babelqueue.com) for the per-SDK idempotency APIs.
