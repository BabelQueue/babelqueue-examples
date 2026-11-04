import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Appends one JSON object per finished span to a file, in the flat shape every otel-interop probe
 * writes (see python/spanfile.py): name, kind, trace_id, span_id, parent_span_id, status,
 * attributes. scripts/otel-verify.sh compares these files. No JSON library: the values are strings,
 * numbers, booleans and lists of those.
 */
final class JsonLinesSpanExporter implements SpanExporter {
    private final Path path;

    JsonLinesSpanExporter(String path) {
        this.path = Path.of(path);
    }

    @Override
    public synchronized CompletableResultCode export(Collection<SpanData> spans) {
        StringBuilder out = new StringBuilder();
        for (SpanData span : spans) {
            Map<String, Object> attributes = new TreeMap<>();
            span.getAttributes().forEach((key, value) -> attributes.put(key.getKey(), value));
            out.append("{\"name\":").append(quote(span.getName()))
                .append(",\"kind\":").append(quote(span.getKind().name()))
                .append(",\"trace_id\":").append(quote(span.getSpanContext().getTraceId()))
                .append(",\"span_id\":").append(quote(span.getSpanContext().getSpanId()))
                .append(",\"parent_span_id\":")
                .append(quote(span.getParentSpanContext().isValid() ? span.getParentSpanContext().getSpanId() : ""))
                .append(",\"status\":").append(quote(span.getStatus().getStatusCode().name()))
                .append(",\"attributes\":{");
            boolean first = true;
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                if (!first) {
                    out.append(',');
                }
                first = false;
                out.append(quote(entry.getKey())).append(':').append(value(entry.getValue()));
            }
            out.append("}}\n");
        }
        try {
            Files.writeString(path, out.toString(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
        return CompletableResultCode.ofSuccess();
    }

    private static String value(Object value) {
        if (value instanceof List<?> list) {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(value(list.get(i)));
            }
            return sb.append(']').toString();
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        return quote(String.valueOf(value));
    }

    private static String quote(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }
}
