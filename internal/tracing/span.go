package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Span abstracts the span type of our global tracing system in order to allow
// it to be replaced in future.
type Span struct {
	ctx context.Context
	w   trace.Span
}

func otelSpan(ctx context.Context, s trace.Span) *Span {
	if s == nil {
		return nil
	}
	return &Span{ctx: ctx, w: s}
}

func (s *Span) unwrap() trace.Span {
	if s == nil {
		return nil
	}
	return s.w
}

// LogKV adds log key/value pairs to the span.
func (s *Span) LogKV(name string, kv ...string) {
	if s == nil {
		return
	}
	var attrs []attribute.KeyValue
	for i := 0; i < len(kv)-1; i += 2 {
		attrs = append(attrs, attribute.String(kv[i], kv[i+1]))
	}
	s.w.AddEvent(name, trace.WithAttributes(attrs...))
}

// SetTag sets a given tag to a value.
func (s *Span) SetTag(key, value string) {
	if s == nil {
		return
	}
	s.w.SetAttributes(attribute.String(key, value))
}

// Finish the span.
func (s *Span) Finish() {
	if s == nil {
		return
	}
	s.w.End()
}

// TextMap attempts to inject a span into a map object in text map format.
func (s *Span) TextMap() (map[string]any, error) {
	if s == nil {
		return nil, nil
	}
	c := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(s.ctx, c)

	spanMapGeneric := make(map[string]any, len(c))
	for k, v := range c {
		spanMapGeneric[k] = v
	}
	return spanMapGeneric, nil
}
