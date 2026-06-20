"""Producer (Python) — publishes an order inside a PRODUCER span, carrying the active trace's
id into the envelope's ``trace_id`` so a consumer in another language lands in the SAME trace
(ADR-0025). ``otel.publish(tracer, app, urn, data)`` does both: it opens a ``publish <urn>``
span and stamps ``trace_id`` with that span's trace id (a UUID — which maps 1:1 to a 128-bit
OTel trace id via the ``trace_id`` ↔ TraceID bijection).

    pip install "babelqueue[redis,otel]" opentelemetry-sdk
    python produce.py

We wire a **console span exporter** (``ConsoleSpanExporter``) so the demo is self-contained:
the producer's ``publish urn:babel:orders:created`` span is printed to stdout, no OTel
collector required. Note its ``trace_id`` in the printed span — the Go consumer's
``process …`` span will print the *same* trace id, because both are derived from the one
``trace_id`` on the wire.

The wire envelope is untouched (the core never imports OpenTelemetry); only the optional
``[otel]`` module is involved, exactly like the optional transport drivers.
"""

import os

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor

from babelqueue import BabelQueue, otel

BROKER_URL = os.environ.get("BROKER_URL", "redis://localhost:6379/0")
QUEUE = os.environ.get("QUEUE", "orders")
URN = "urn:babel:orders:created"

# Wire a TracerProvider with a console exporter so spans print to stdout (self-contained demo).
provider = TracerProvider()
provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("orders-producer")

app = BabelQueue(BROKER_URL, queue=QUEUE)

# Open our OWN span so we can read the trace id that otel.publish will carry onto the wire.
# (otel.publish opens a child PRODUCER span internally; a child shares its parent's trace id,
# so the trace id we read here is exactly the one stamped into the message's trace_id.)
order = {"order_id": 1042, "amount": 99.90, "currency": "EUR"}
with tracer.start_as_current_span("order-request") as root:
    wire_trace_id = otel.uuid_of(root.get_span_context().trace_id)
    message_id = otel.publish(tracer, app, URN, order, queue=QUEUE)

print(
    f"[python] published {URN}  meta.id={message_id}\n"
    f"[python] PRODUCER span emitted above; the wire trace_id = {wire_trace_id}\n"
    f"[python] now run the Go consumer — its CONSUMER span must show the SAME trace id."
)

# Flush the console exporter so the span is printed before the process exits.
provider.force_flush()
