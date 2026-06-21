# Idempotency, broker-free — the same charge twice, charged once

Queues are **at-least-once**: a client retries, a broker redelivers, a worker crashes
before ack — and the *same* message arrives more than once. For a payment that means a
**double charge**. BabelQueue ships an optional, dependency-free idempotency guard
([ADR-0022](https://babelqueue.com/docs/spec/1.x/idempotency)) that dedupes a consumer on
`meta.id` — the canonical per-message identity — so a message whose id was already
processed is acked and **skipped** instead of run again.

This is the **broker-free** companion to [`idempotency-payments/`](../idempotency-payments)
(Python -> Go over Redis). It needs **no Redis, no Docker, no broker** — it runs the whole
produce -> consume round-trip in-process on the published `babelqueue` package's `memory://`
transport, so it is a single `python charge_once.py` away. It is the runnable form of the
cross-SDK [idempotency conformance fixtures](https://github.com/BabelQueue/conformance)
(`manifest.json` -> `idempotency`): at-least-once delivery collapsed to an **exactly-once
effect** by the consumer.

## What it does

1. Builds one `BabelQueue("memory://")` app (no infrastructure).
2. Encodes **one** payment envelope and publishes that exact body **twice** — the identical
   `meta.id`, `trace_id` and `data`, byte-for-byte (what a redelivery actually looks like on
   the wire). `app.publish` would mint a fresh `meta.id` each call — a *new* message, not a
   duplicate — so we publish the pre-encoded `body` directly.
3. Wraps the charge handler with `idempotency.wrap(store, charge)`. The first delivery
   **charges**; the second is recognised as a replay of an already-processed id and
   **skipped** — no double charge. The script asserts the side-effect fired exactly once, so
   it doubles as a smoke test.

> Idempotency here is **seen-set, post-success dedupe** under at-least-once with an
> idempotent handler — not exactly-once and not an in-flight concurrency lock. It stops an
> *accidental* duplicate from re-running the side-effect. (A *deliberate* replay off a DLQ is
> the [`dlq-redrive/`](../dlq-redrive) replay-bypass story.)

## Run it

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python charge_once.py
```

Expected output — **two** deliveries arrive, but the side-effect runs **once**:

```
[publish] delivery 1  meta.id=…  (same envelope, byte-for-byte)
[publish] delivery 2  meta.id=…  (same envelope, byte-for-byte)
[charge] payment_id=pay_7f3a amount=4200 EUR  meta.id=…  (charged)

[result] 2 deliveries of the SAME payment (meta.id=…) handled; side-effect fired 1 time(s).
[ok] exactly-once effect under at-least-once delivery — no double charge.
```

## How the guard works

`idempotency.wrap(store, handler)` returns a handler that, on each delivery:

- looks up the envelope's `meta.id` in a `Store` (a "seen-set");
- if **already seen** → returns (the runtime acks it, so the broker stops redelivering)
  **without** calling your handler — the charge does not re-run;
- if **new** → runs your handler, then records the id as processed **only on success**. A
  handler that raises leaves the id unrecorded, so retry / dead-letter still apply and a
  later delivery runs it again.

A message with **no usable `meta.id`** runs unchanged (**fail-open**) — the guard never
drops work it cannot identify.

## The `Store`

The reference `InMemoryStore` (in the core, used here) is process-local — fine for a
single-process consumer and tests. A production **fleet** needs a **shared** store so two
workers see each other's "seen" set: implement the same three methods —
`seen(id)` / `remember(id)` / `forget(id)` — over a Redis key, a database table, or a
PSR-16-style cache, and pass it to `wrap` unchanged. The interface is stable, so swapping
the backend is a one-line change.

## Make it real / cross-language

The dedupe key is the canonical `meta.id`, so any SDK can be on either side and the same
payment is deduped end-to-end:

- **Real broker:** pass `redis://localhost:6379/0` instead of `memory://` — the code is
  otherwise identical.
- **Cross-language:** see [`idempotency-payments/`](../idempotency-payments), where a Python
  producer sends the duplicate and a **Go** consumer (`idempotency.Wrap`) dedupes it.
- **Other consumers:** PHP `BabelQueue\Idempotency\Idempotent::wrap`, Java
  `Idempotent.wrap`, Node `Wrap` — same `meta.id` dedupe, same seen-set semantics.

See [babelqueue.com](https://babelqueue.com/docs/spec/1.x/idempotency) for the full
idempotency contract and the per-SDK APIs.
