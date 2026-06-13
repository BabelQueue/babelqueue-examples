"""Consumer (Python) — reads the Artemis "orders" address over AMQP 1.0 and routes by URN.

The messages were produced by the Java service (producer-java) over JMS (the CORE protocol);
Artemis bridges the two protocols on the same address. Python never knows which language wrote
a message — it sees only the canonical envelope and ``meta.lang``.

    pip install "babelqueue[artemis]"
    python consume.py
"""

import os

from babelqueue import BabelQueue

BROKER_URL = os.environ.get("ARTEMIS_URL", "artemis://localhost:5672")

app = BabelQueue(BROKER_URL, queue="orders")


@app.handler("urn:babel:orders:created")
def on_created(data, meta):
    print(
        f"[python] orders:created  order_id={data['order_id']}  amount={data['amount']}  "
        f"from lang={meta['lang']}"
    )


@app.handler("urn:babel:orders:shipped")
def on_shipped(data, meta):
    print(
        f"[python] orders:shipped  order_id={data['order_id']}  carrier={data['carrier']}  "
        f"from lang={meta['lang']}"
    )


print("[python] consuming up to 4 messages from 'orders' over AMQP 1.0 ...")
count = app.consume(max_messages=4, timeout=10.0)
print(f"[python] done — consumed {count} message(s).")
