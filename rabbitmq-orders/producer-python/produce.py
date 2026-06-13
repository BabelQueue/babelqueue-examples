"""Producer (Python) — publishes canonical BabelQueue envelopes to a RabbitMQ
queue that a consumer in *any other language* can read.

    pip install "babelqueue[amqp]"
    python produce.py

Each message is projected onto native AMQP 0-9-1 properties (§2 of the broker-bindings
contract): ``type`` = URN, ``correlation_id`` = trace_id, ``message_id`` = meta.id,
``app_id`` = babelqueue, plus ``x-schema-version`` / ``x-source-lang`` / ``x-attempts``
headers — so the Node consumer routes on ``properties.type`` without parsing the body.

Defaults target the local RabbitMQ from docker-compose. Point BROKER_URL at any
RabbitMQ to run it there.
"""

import os

from babelqueue import BabelQueue

BROKER_URL = os.environ.get("BROKER_URL", "amqp://guest:guest@localhost:5672/")

app = BabelQueue(BROKER_URL, queue="orders")

messages = [
    ("urn:babel:orders:created", {"order_id": 1001, "amount": 19.99}),
    ("urn:babel:orders:created", {"order_id": 1002, "amount": 39.98}),
    ("urn:babel:orders:created", {"order_id": 1003, "amount": 59.97}),
    ("urn:babel:orders:shipped", {"order_id": 1002, "carrier": "DHL"}),
]

for urn, data in messages:
    message_id = app.publish(urn, data)
    print(f"[python] published {urn}  id={message_id}  data={data}")

print(f"[python] {len(messages)} message(s) on the 'orders' RabbitMQ queue — now run the Node consumer.")
