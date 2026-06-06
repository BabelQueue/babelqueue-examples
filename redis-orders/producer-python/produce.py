"""Producer (Python) — publishes canonical BabelQueue envelopes to a shared Redis
queue that a consumer in *any other language* can read.

    pip install "babelqueue[redis]"
    python produce.py
"""

import os

from babelqueue import BabelQueue

BROKER_URL = os.environ.get("BROKER_URL", "redis://localhost:6379/0")

app = BabelQueue(BROKER_URL, queue="orders")

messages = [
    ("urn:babel:orders:created", {"order_id": 1042, "amount": 99.90, "currency": "USD"}),
    ("urn:babel:orders:created", {"order_id": 1043, "amount": 12.50, "currency": "EUR"}),
    ("urn:babel:catalog:item.indexed", {"sku": "WIDGET-1", "title": "Café Widget ☕"}),
]

for urn, data in messages:
    message_id = app.publish(urn, data)
    print(f"[python] published {urn}  id={message_id}  data={data}")

print(f"[python] {len(messages)} message(s) on the 'orders' Redis list — now run the Go consumer.")
