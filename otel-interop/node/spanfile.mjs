// Span file exporter for the OTel interop probes (Node). Appends one JSON object per finished span
// to a file, in the flat shape every otel-interop probe writes (see ../python/spanfile.py): name,
// kind, trace_id, span_id, parent_span_id, status, attributes. scripts/otel-verify.sh compares them.
import { appendFileSync } from "node:fs";

const KINDS = ["INTERNAL", "SERVER", "CLIENT", "PRODUCER", "CONSUMER"];
const STATUSES = ["UNSET", "OK", "ERROR"];

export class JsonLinesSpanExporter {
  constructor(path) {
    this.path = path;
  }

  export(spans, resultCallback) {
    try {
      for (const span of spans) {
        const context = span.spanContext();
        const record = {
          name: span.name,
          kind: KINDS[span.kind] ?? String(span.kind),
          trace_id: context.traceId,
          span_id: context.spanId,
          // sdk-trace-base 2.x: parentSpanContext (1.x had parentSpanId).
          parent_span_id: span.parentSpanContext?.spanId ?? span.parentSpanId ?? "",
          status: STATUSES[span.status.code] ?? String(span.status.code),
          attributes: { ...span.attributes },
        };
        appendFileSync(this.path, JSON.stringify(record) + "\n");
      }
      resultCallback({ code: 0 });
    } catch (error) {
      resultCallback({ code: 1, error });
    }
  }

  shutdown() {
    return Promise.resolve();
  }

  forceFlush() {
    return Promise.resolve();
  }
}
