# DLQ re-drive — triage a dead-letter queue, then move messages back

A **dead-letter queue is just an ordinary queue holding canonical envelopes**
([ADR-0009](https://babelqueue.com/docs/spec/1.x/error-handling#6-dead-letter-queues-dlq)),
so any SDK can read it — no special format, no PHP `serialize()`. This example
shows the two operator workflows for a `<queue>.dlq`:

1. **Triage** — a small **Python** consumer reads `orders.dlq` and prints each
   quarantined message, surfacing the additive top-level `dead_letter` block
   (`reason` / `error` / `failed_at` / `original_queue` / `attempts` / `lang`)
   that records *why* it failed.
2. **Re-drive** — a small helper moves messages from `orders.dlq` back to the
   source `orders` queue (after you have fixed the underlying fault), stripping
   the `dead_letter` block and resetting `attempts` so they get a clean re-run.

The DLQ holds the *original* envelope verbatim (same `trace_id`, `meta.id`,
`data`), so the message a **Go** or **PHP** consumer dead-lettered is re-driven
here and can then be picked up by a consumer in **any** language — the
cross-language re-drive case. Everything runs on the simplest broker, Redis (§1).

A `dead_letter` block is **additive and optional**, so the wire envelope stays
frozen at `schema_version: 1`; consumers of normal queues ignore it.

## Run it

```bash
# 1) start Redis
docker compose up -d            # or: docker run -d -p 6379:6379 redis:7
```

```bash
# 2) seed the DLQ with a few dead-lettered envelopes
#    (stands in for what a failing consumer would have quarantined)
cd seed-dlq
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python seed.py
cd ..
```

Then **triage** the DLQ — read and print every quarantined message:

```bash
# 3) DLQ consumer — Python   (needs babelqueue[redis] ^1.0, which ships RedisTransport)
cd consumer-python
pip install -r requirements.txt
python consume_dlq.py
cd ..
```

Expected output — each dead-lettered envelope with the reason it landed on the
DLQ. Note the preserved `trace_id` and intact unicode `data`; the consumer drains
the DLQ as it triages (it `ack`s each message):

```
[dlq] urn:babel:orders:created
      reason=failed  attempts=3  lang=python
      error='payment gateway timeout'  (PaymentError)
      original_queue=orders  trace=dbe33f76-60bf-4410-9a71-2465908a3e17
      data={'order_id': 1042, 'amount': 99.9, 'currency': 'USD'}
[dlq] urn:babel:catalog:item.indexed
      reason=unknown_urn  attempts=1  lang=python
      error=None  (None)
      original_queue=orders  trace=b57b1185-1071-4d7f-bc1d-f0a27c315a95
      data={'sku': 'WIDGET-1', 'title': 'Café Widget ☕'}
[dlq] triaged 3 dead-lettered message(s) on 'orders.dlq'.
```

Or **re-drive** the DLQ back to the source queue instead — run this on a *freshly
seeded* DLQ (the triage step above drains it, so re-seed with `python seed.py`
first if you ran it):

```bash
# 4) re-drive — move orders.dlq → orders
pip install -r requirements.txt
python redrive.py                  # move every message back
# python redrive.py --max 1        # move at most one
# python redrive.py --keep-dead-letter   # leave the dead_letter block in place
# python redrive.py --bypass       # replay, but skip side-effects that already fired (see below)
```

```
[redrive] urn:babel:orders:created  trace=884907a7-…  orders.dlq -> orders
[redrive] urn:babel:orders:created  trace=ebe6fd85-…  orders.dlq -> orders
[redrive] urn:babel:catalog:item.indexed  trace=21e373db-…  orders.dlq -> orders
[redrive] moved 3 message(s) from 'orders.dlq' back to 'orders'.
```

Each message is reserved on the DLQ, re-published to `orders`, then `ack`ed off
the DLQ — so an interrupted run never loses a message (at-least-once). The
re-driven envelope keeps its `trace_id` / `meta.id` / `data`; by default the
`dead_letter` block is dropped and `attempts` reset to `0` for a clean re-run.
A `redis-orders` consumer (Go, Java, …) will then pick the message up unchanged.

> Re-drive **after** the underlying fault is fixed, or the message will just fail
> and dead-letter again.

## Replay-bypass — re-drive without re-firing side-effects

A re-drive is a **deliberate replay**: the handler runs again. That is exactly what
you want for the *idempotent core* (save the order, re-index the item), but a
message often also did something **external and non-idempotent** before it failed —
charged a card, sent a confirmation email, called a third party. Replaying it
naively re-fires that effect: a second charge, a duplicate email.

The **Replay-Bypass** guard
([ADR-0027](https://babelqueue.com)) closes that gap. `redrive.py --bypass` stamps
an out-of-band `bq-replay-bypass` transport header on each re-driven message; the
runtime surfaces it to the handler, which wraps its external effect so a replay
**skips** it:

```python
from babelqueue import bypass_external_effects

@app.handler("urn:babel:orders:created")
def on_order_created(data, meta):
    save_order(data)                                   # idempotent core — always runs
    bypass_external_effects(lambda: send_email(data))  # external effect — skipped on replay
```

The marker rides **out of band** as a transport header, so the wire envelope stays
frozen at `schema_version: 1` (a normal first-time delivery has no header, runs the
effect as usual). It propagates over a transport that carries per-message headers —
today the in-memory transport does, so the end-to-end demo runs **with no broker**:

```bash
# replay-bypass — full loop on the in-memory transport, no Redis needed
pip install -r requirements.txt        # (memory:// needs no broker extra)
python replay_bypass_demo.py
```

The demo replays the *same* fixed message twice — once plain, once with
`--bypass` — so you can see the difference: the idempotent core runs both times,
but the email fires only on the plain replay, not the bypassed one:

```
=== Plain redrive — the email RE-FIRES (bypass=False) ===
[seed] dead-lettered order 1042 on 'orders.dlq' (email had already gone out)
[redrive] redriven=1  bq-replay-bypass stamped=False  orders.dlq -> orders
[handler] processed order 1042 (idempotent core — always runs)
[handler]   -> sent confirmation email for order 1042
[consume] handled 1 message(s); emails sent on the replay: [1042]

=== Redrive WITH replay-bypass — the email is SKIPPED (bypass=True) ===
[seed] dead-lettered order 1042 on 'orders.dlq' (email had already gone out)
[redrive] redriven=1  bq-replay-bypass stamped=True  orders.dlq -> orders
[handler] processed order 1042 (idempotent core — always runs)
[consume] handled 1 message(s); emails sent on the replay: []
```

Over a **real broker** the header propagates only once that broker's transport
implements the optional `HeaderPublisher` capability — a follow-up, like the broker
bindings themselves. Until then `python redrive.py --bypass` over Redis re-drives
the message and prints a notice that the marker was not carried (so the replay would
re-fire effects); the in-memory demo above proves the contract end-to-end today.

> Replay-bypass is the inverse of [`idempotency-payments/`](../idempotency-payments):
> idempotency stops an **accidental** duplicate from re-running the effect; bypass
> lets an **intended** replay re-run the core while skipping the effect that already
> happened.

## How a message gets here

In production you do not seed the DLQ by hand — a consumer dead-letters a message
when retries are exhausted, or on an unroutable URN with `on_unknown_urn:
dead_letter`. With the Python runtime that is built in:

```python
app = BabelQueue("redis://localhost:6379/0", queue="orders",
                 dead_letter=True, max_attempts=3)
```

On the third failing attempt the runtime annotates the envelope with the
`dead_letter` block and moves it to `orders.dlq` — exactly the shape this example
triages and re-drives. Every SDK that supports a DLQ uses the same block and the
same `<original_queue>.dlq` naming.

## Configuration

All scripts read these environment variables:

| Variable | Default | Meaning |
| :--- | :--- | :--- |
| `BROKER_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `QUEUE` | `orders` | source queue; the DLQ is `<QUEUE>.dlq` |

## Swap the ends

A DLQ is an ordinary queue of canonical envelopes, so any SDK can triage or
re-drive it:

- **Go / Java / Node / .NET / PHP triage:** point the same consumer pattern
  (`@app.handler(...)` / `app.Handle(...)`, `RedisConsumer.builder(...)`) at the
  `orders.dlq` queue and read the `dead_letter` block off the envelope.
- **Re-drive in any language:** pop from `orders.dlq`, drop the `dead_letter`
  block, re-publish to `orders` — the three lines this helper runs.

See [babelqueue.com](https://babelqueue.com) for the per-SDK consumer APIs.
