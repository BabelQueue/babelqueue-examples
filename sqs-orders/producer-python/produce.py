"""Producer (Python) — publishes canonical BabelQueue envelopes to an Amazon SQS
queue that a consumer in *any other language* can read.

    pip install "babelqueue[sqs]"
    python produce.py

Defaults target the local ElasticMQ from docker-compose. Point SQS_ENDPOINT / AWS_REGION
(and AWS credentials — any value works for ElasticMQ) at real Amazon SQS to run it there.
"""

import os

from babelqueue import BabelQueue

REGION = os.environ.get("AWS_REGION", "eu-central-1")
ENDPOINT = os.environ.get("SQS_ENDPOINT", "http://localhost:9324")
# ElasticMQ accepts any credentials; real SQS uses your configured ones.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

BROKER_URL = os.environ.get("BROKER_URL", f"sqs://{REGION}?endpoint={ENDPOINT}")

app = BabelQueue(BROKER_URL, queue="orders")

messages = [
    ("urn:babel:orders:created", {"order_id": 1042, "amount": 99.90, "currency": "USD"}),
    ("urn:babel:orders:created", {"order_id": 1043, "amount": 12.50, "currency": "EUR"}),
    ("urn:babel:catalog:item.indexed", {"sku": "WIDGET-1", "title": "Café Widget ☕"}),
]

for urn, data in messages:
    message_id = app.publish(urn, data)
    print(f"[python] published {urn}  id={message_id}  data={data}")

print(f"[python] {len(messages)} message(s) on the 'orders' SQS queue — now run the Go consumer.")
