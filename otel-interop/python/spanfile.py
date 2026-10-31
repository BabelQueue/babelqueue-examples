"""Span file exporter for the OTel interop probes (Python).

Every probe of ``otel-interop/`` (Python, Go, Node, Java, .NET) appends one JSON object per finished
span to the file named by ``OTEL_SPAN_FILE``, in the same flat shape, so ``scripts/otel-verify.sh``
can compare a producer's file with a consumer's file without knowing which SDK wrote them:

    {"name", "kind", "trace_id", "span_id", "parent_span_id", "status", "attributes": {...}}

``trace_id`` / ``span_id`` / ``parent_span_id`` are lowercase hex (32 / 16 / 16 digits, the parent is
"" for a root span).
"""

import json
import threading

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult


class JsonLinesSpanExporter(SpanExporter):
    def __init__(self, path: str) -> None:
        self._path = path
        self._lock = threading.Lock()

    def export(self, spans):  # type: ignore[no-untyped-def]
        with self._lock, open(self._path, "a", encoding="utf-8") as out:
            for span in spans:
                out.write(json.dumps(_record(span), sort_keys=True) + "\n")
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        return None


def _record(span: ReadableSpan) -> dict:
    context = span.get_span_context()
    parent = span.parent
    return {
        "name": span.name,
        "kind": span.kind.name,
        "trace_id": format(context.trace_id, "032x"),
        "span_id": format(context.span_id, "016x"),
        "parent_span_id": format(parent.span_id, "016x") if parent is not None else "",
        "status": span.status.status_code.name,
        "attributes": {key: _plain(value) for key, value in (span.attributes or {}).items()},
    }


def _plain(value):  # type: ignore[no-untyped-def]
    return list(value) if isinstance(value, tuple) else value
