"""Seed (Python) — puts a few *dead-lettered* canonical envelopes on the
``orders.dlq`` Redis list so the consumer and re-drive demos have something to
read. This stands in for what a failing consumer would have quarantined: the
original envelope, preserved verbatim, plus the additive top-level ``dead_letter``
block (``reason`` / ``error`` / ``failed_at`` / ``original_queue`` / ``attempts`` /
``lang``). Any SDK could have produced these — they are just JSON on a queue.

    pip install "babelqueue[redis]"
    python seed.py
"""

import os

from babelqueue import EnvelopeCodec, dead_letter
from babelqueue.transport import make_transport

BROKER_URL = os.environ.get("BROKER_URL", "redis://localhost:6379/0")
QUEUE = os.environ.get("QUEUE", "orders")
DLQ = f"{QUEUE}.dlq"

# (urn, data, reason, error, exception, attempts) — the failures a worker would
# have hit. `lang` defaults to the seeding SDK ("python"); on a real queue it is
# whatever language dead-lettered the message.
failures = [
    ("urn:babel:orders:created", {"order_id": 1042, "amount": 99.90, "currency": "USD"},
     "failed", "payment gateway timeout", "PaymentError", 3),
    ("urn:babel:orders:created", {"order_id": 1043, "amount": 12.50, "currency": "EUR"},
     "failed", "downstream 503 from inventory service", "HttpError", 3),
    ("urn:babel:catalog:item.indexed", {"sku": "WIDGET-1", "title": "Café Widget ☕"},
     "unknown_urn", None, None, 1),
]

transport = make_transport(BROKER_URL)

for urn, data, reason, error, exception, attempts in failures:
    envelope = EnvelopeCodec.make(urn, data, queue=QUEUE)
    envelope["attempts"] = attempts
    annotated = dead_letter.annotate(
        envelope, reason, QUEUE, attempts, error=error, exception=exception
    )
    transport.publish(DLQ, EnvelopeCodec.encode(annotated))
    print(f"[seed] dead-lettered {urn}  reason={reason}  trace={annotated['trace_id']}")

print(f"[seed] {len(failures)} message(s) on the '{DLQ}' Redis list — now run the DLQ consumer.")
