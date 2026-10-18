"""Producer (Python) — publishes canonical BabelQueue envelopes to an Azure Service Bus
queue that a consumer in *any other language* can read.

    pip install -r requirements.txt
    python produce.py

Defaults target the local Service Bus emulator from docker-compose. Point
ASB_CONNECTION_STRING at a real namespace (and create an `orders` queue there) to run it
against Azure unchanged.
"""

import os

from babelqueue import BabelQueue
from babelqueue.asb_transport import AsbTransport

# The emulator's well-known development connection string (no secret: it only works locally).
EMULATOR_CONNECTION_STRING = (
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
)
CONNECTION_STRING = os.environ.get("ASB_CONNECTION_STRING", EMULATOR_CONNECTION_STRING)

transport = AsbTransport(connection_string=CONNECTION_STRING)
app = BabelQueue(transport=transport, queue="orders")

messages = [
    ("urn:babel:orders:created", {"order_id": 1042, "amount": 99.90, "currency": "USD"}),
    ("urn:babel:orders:created", {"order_id": 1043, "amount": 12.50, "currency": "EUR"}),
    ("urn:babel:catalog:item.indexed", {"sku": "WIDGET-1", "title": "Café Widget ☕"}),
]

try:
    for urn, data in messages:
        message_id = app.publish(urn, data)
        print(f"[python] published {urn}  id={message_id}  data={data}")
finally:
    transport.close()

print(f"[python] {len(messages)} message(s) on the 'orders' Service Bus queue — now run the Go consumer.")
