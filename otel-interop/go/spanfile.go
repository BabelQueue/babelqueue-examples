package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// fileExporter appends one JSON object per finished span to a file, in the flat shape every
// otel-interop probe writes (see python/spanfile.py): name, kind, trace_id, span_id,
// parent_span_id, status, attributes. scripts/otel-verify.sh compares these files.
type fileExporter struct {
	mu   sync.Mutex
	file *os.File
}

func newFileExporter(path string) (*fileExporter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &fileExporter{file: f}, nil
}

func (e *fileExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	enc := json.NewEncoder(e.file)
	for _, s := range spans {
		attrs := make(map[string]any, len(s.Attributes()))
		for _, kv := range s.Attributes() {
			attrs[string(kv.Key)] = kv.Value.AsInterface()
		}
		parent := ""
		if p := s.Parent(); p.SpanID().IsValid() {
			parent = p.SpanID().String()
		}
		if err := enc.Encode(map[string]any{
			"name":           s.Name(),
			"kind":           strings.ToUpper(s.SpanKind().String()),
			"trace_id":       s.SpanContext().TraceID().String(),
			"span_id":        s.SpanContext().SpanID().String(),
			"parent_span_id": parent,
			"status":         strings.ToUpper(s.Status().Code.String()),
			"attributes":     attrs,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (e *fileExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.file.Close()
}
