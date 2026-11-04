"""Consumer (Python, AMQP 1.0) for the OTel interop on Artemis: handles the messages a Java (JMS)
producer sent, each inside a CONSUMER span ``process <queue>`` written to ``OTEL_SPAN_FILE``.

    OTEL_SPAN_FILE=consumer.jsonl python consume_artemis.py

ARTEMIS_AMQP_URL (default ``artemis://localhost:5672``) is the broker's AMQP 1.0 URL; it is not ARTEMIS_URL,
which scripts/lib/interop-common.sh sets to an ``amqp://`` URL for the Go and .NET probes.

The JMS producer carries the ``traceparent`` as a message property (ADR-0028); this consumer reads it
through the runtime's header seam, so its span must be a true child of the Java producer span.
"""

import os
import sys

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

from babelqueue import BabelQueue, otel
from spanfile import JsonLinesSpanExporter

URN = "urn:babel:orders:created"


def main() -> None:
    span_file = os.environ.get("OTEL_SPAN_FILE", "")
    if not span_file:
        sys.exit("OTEL_SPAN_FILE is required")
    queue = os.environ.get("QUEUE", "orders")
    expected = int(os.environ.get("OTEL_MESSAGES", "3"))
    timeout = float(os.environ.get("CONSUME_TIMEOUT", "60"))

    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(JsonLinesSpanExporter(span_file)))
    tracer = provider.get_tracer("otel-interop-consumer", schema_url=otel.SEMCONV_SCHEMA_URL)

    app = BabelQueue(os.environ.get("ARTEMIS_AMQP_URL", "artemis://localhost:5672"), queue=queue)

    def on_created(data, meta):  # type: ignore[no-untyped-def]
        print(f"PROBE python HANDLED id={meta['id']} order_id={data['order_id']}", flush=True)

    app.register(URN, otel.wrap_handler(tracer, on_created, system="activemq"))

    print("PROBE python READY", flush=True)
    count = app.consume(max_messages=expected, timeout=timeout)
    provider.force_flush()
    provider.shutdown()
    print(f"PROBE python DONE consumed={count}", flush=True)
    if count < expected:
        sys.exit(f"consumed {count} of {expected} message(s)")


if __name__ == "__main__":
    main()
