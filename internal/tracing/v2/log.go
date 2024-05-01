package tracing

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

const (
	TraceIdField    = "trace_id"
	SpanIdField     = "span_id"
	TraceFlagsField = "trace_flags"
)

func LogFields(ctx context.Context) map[string]string {
	span := trace.SpanContextFromContext(ctx)
	if !span.IsValid() {
		return nil
	}

	return map[string]string{
		TraceIdField:    span.TraceID().String(),
		SpanIdField:     span.SpanID().String(),
		TraceFlagsField: span.TraceFlags().String(),
	}
}
