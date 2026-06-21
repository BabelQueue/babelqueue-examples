"""Broker-free idempotency demo — the SAME payment delivered twice, charged once.

No Redis, no Docker, no broker: this runs entirely in-process on the published
``babelqueue`` package's ``memory://`` transport, so it is a single ``python charge_once.py``
away. It proves the dedupe contract (ADR-0022) the cross-SDK conformance suite locks:
at-least-once delivery collapsed to an **exactly-once effect** by the consumer.

What it shows
-------------
At-least-once delivery means the *same* message can arrive more than once — a worker
crashes before ack, a broker redelivers, a proxy double-sends. ``meta.id`` is the canonical
per-message identity, so a true duplicate is one with the same id, byte-for-byte.

We build ONE envelope with :meth:`EnvelopeCodec.make` and publish that exact encoded body
TWICE (``app.publish`` would mint a fresh ``meta.id`` each call — a *new* message, not a
duplicate). The handler is wrapped with :func:`babelqueue.idempotency.wrap`, which dedupes
on ``meta.id``: the first delivery charges; the second is recognised as a replay of an
already-processed id and skipped — no double charge.

    pip install babelqueue        # no extras — the in-memory transport is in the core
    python charge_once.py

The same code works against a real broker by passing ``redis://…`` instead of ``memory://``
(see ``../idempotency-payments`` for the cross-language Python -> Go version over Redis).
"""

from __future__ import annotations

from babelqueue import BabelQueue, EnvelopeCodec, InMemoryStore
from babelqueue.idempotency import wrap

QUEUE = "payments"
URN = "urn:babel:payments:charge"

# A broker-free app: the in-memory transport keeps the published bodies in-process, so the
# whole produce -> consume round-trip runs without any infrastructure.
app = BabelQueue("memory://", queue=QUEUE)

# The seen-set store backing the idempotency guard. InMemoryStore is the reference
# implementation every SDK ships; a production fleet swaps in a Redis- or database-backed
# store behind the same three methods (seen / remember / forget) so dedupe is shared.
store = InMemoryStore()

charges = 0  # the side-effect counter — must end at 1, not 2


def charge(data: dict, meta: dict) -> None:
    """The business handler — the external side-effect we must run AT MOST ONCE."""
    global charges
    charges += 1
    print(
        f"[charge] payment_id={data['payment_id']} amount={data['amount']} {data['currency']}"
        f"  meta.id={meta['id']}  (charged)"
    )


# Register the idempotent handler. wrap(store, charge) skips an already-seen meta.id and
# returns, so the runtime acks the duplicate without re-running the charge.
app.register(URN, wrap(store, charge))

# --- Produce: the SAME payment, twice (a textbook at-least-once redelivery) --------------
payment = {"payment_id": "pay_7f3a", "amount": 4200, "currency": "EUR", "customer": "cus_19c2"}
envelope = EnvelopeCodec.make(URN, payment, queue=QUEUE)
body = EnvelopeCodec.encode(envelope)  # encode ONCE so both sends are byte-identical
message_id = envelope["meta"]["id"]

for attempt in (1, 2):
    app.transport.publish(QUEUE, body)  # publish the identical body — same meta.id
    print(f"[publish] delivery {attempt}  meta.id={message_id}  (same envelope, byte-for-byte)")

# --- Consume: drain both deliveries; the guard charges once --------------------------------
processed = app.consume(max_messages=2)

print(
    f"\n[result] {processed} deliveries of the SAME payment (meta.id={message_id}) handled; "
    f"side-effect fired {charges} time(s)."
)

# Assert the contract so the demo doubles as a smoke test (exit non-zero if it ever regresses).
assert processed == 2, f"expected 2 deliveries, got {processed}"
assert charges == 1, f"double charge! expected exactly 1 charge, got {charges}"
print("[ok] exactly-once effect under at-least-once delivery — no double charge.")
