"""Producer (Python) for the OTel interop: publishes inside a PRODUCER span ``send <queue>`` and
writes the finished spans to ``OTEL_SPAN_FILE``.

    BROKER=redis|rabbitmq|sqs|azureservicebus OTEL_SPAN_FILE=producer.jsonl python produce.py

``system`` is the broker name of ADR-0038 section 3 (``messaging.system``). The ``traceparent``
travels out of band on the transport (ADR-0028), so the consumer's span in another language must be a
true child of the span written here. Defaults target the compose files of the matching ``*-orders``
example; ``BROKER_URL`` / ``SQS_ENDPOINT`` / ``ASB_CONNECTION_STRING`` override them.
"""

import os
import sys

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

from babelqueue import BabelQueue, otel
from spanfile import JsonLinesSpanExporter

URN = "urn:babel:orders:created"
SYSTEMS = {"redis": "redis", "rabbitmq": "rabbitmq", "sqs": "aws_sqs", "azureservicebus": "servicebus"}


def connect(broker: str, queue: str):  # type: ignore[no-untyped-def]
    if broker == "redis":
        return BabelQueue(os.environ.get("BROKER_URL", "redis://localhost:6379/0"), queue=queue), None
    if broker == "rabbitmq":
        return BabelQueue(os.environ.get("BROKER_URL", "amqp://guest:guest@localhost:5672/"), queue=queue), None
    if broker == "sqs":
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
        region = os.environ.get("AWS_REGION", "eu-central-1")
        endpoint = os.environ.get("SQS_ENDPOINT", "http://localhost:9324")
        return BabelQueue(os.environ.get("BROKER_URL", f"sqs://{region}?endpoint={endpoint}"), queue=queue), None
    if broker == "azureservicebus":
        from babelqueue.asb_transport import AsbTransport

        # The emulator's well-known development connection string (no secret: it only works locally).
        connection_string = os.environ.get(
            "ASB_CONNECTION_STRING",
            "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;",
        )
        transport = AsbTransport(connection_string=connection_string)
        return BabelQueue(transport=transport, queue=queue), transport
    sys.exit(f"unknown BROKER {broker!r} (redis, rabbitmq, sqs, azureservicebus)")


def main() -> None:
    broker = os.environ.get("BROKER", "")
    span_file = os.environ.get("OTEL_SPAN_FILE", "")
    if not span_file:
        sys.exit("OTEL_SPAN_FILE is required")
    queue = os.environ.get("QUEUE", "orders")
    count = int(os.environ.get("OTEL_MESSAGES", "3"))
    system = SYSTEMS.get(broker)
    if system is None:
        sys.exit(f"unknown BROKER {broker!r} (redis, rabbitmq, sqs, azureservicebus)")

    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(JsonLinesSpanExporter(span_file)))
    tracer = provider.get_tracer("otel-interop-producer", schema_url=otel.SEMCONV_SCHEMA_URL)

    app, transport = connect(broker, queue)
    try:
        for index in range(1, count + 1):
            message_id = otel.publish(
                tracer, app, URN, {"order_id": 1000 + index, "amount": 10.0 * index}, queue=queue, system=system
            )
            print(f"PROBE python PUBLISHED id={message_id} queue={queue} system={system}", flush=True)
    finally:
        if transport is not None:
            transport.close()
        provider.force_flush()
        provider.shutdown()
    print(f"PROBE python DONE published={count}", flush=True)


if __name__ == "__main__":
    main()
