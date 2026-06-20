"""Producer (Python) — sends the SAME payment twice with the SAME ``meta.id``.

This stands in for an at-least-once delivery hiccup: a client retries a request, a
broker redelivers, or a proxy double-sends — and the *identical* canonical envelope
lands on the queue twice. ``meta.id`` is the canonical per-message identity, so a
true duplicate is one with the same id, byte-for-byte.

    pip install "babelqueue[redis]"
    python charge.py

We build ONE envelope with :meth:`EnvelopeCodec.make` and publish that exact encoded
body twice (``app.publish`` would mint a fresh ``meta.id`` each call, which is a *new*
message, not a duplicate). The idempotent consumer on the other end charges once and
skips the replay — no double charge.
"""

import os

from babelqueue import EnvelopeCodec
from babelqueue.transport import make_transport

BROKER_URL = os.environ.get("BROKER_URL", "redis://localhost:6379/0")
QUEUE = os.environ.get("QUEUE", "payments")

# One payment, one canonical envelope, one stable meta.id.
payment = {"payment_id": "pay_7f3a", "amount": 4200, "currency": "EUR", "customer": "cus_19c2"}
envelope = EnvelopeCodec.make("urn:babel:payments:charge", payment, queue=QUEUE)
body = EnvelopeCodec.encode(envelope)  # encode ONCE so both sends are byte-identical

message_id = envelope["meta"]["id"]
transport = make_transport(BROKER_URL)

# Publish the identical body twice — same meta.id, same trace_id, same data.
for attempt in (1, 2):
    transport.publish(QUEUE, body)
    print(f"[python] published attempt {attempt}  meta.id={message_id}  amount={payment['amount']} {payment['currency']}")

print(
    f"[python] 2 deliveries of the SAME payment (meta.id={message_id}) on the "
    f"'{QUEUE}' Redis list — now run the consumer; it must charge exactly once."
)
