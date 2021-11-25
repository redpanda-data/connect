package tracing

import (
	"github.com/opentracing/opentracing-go"
)

// Span abstracts the span type of our global tracing system in order to allow
// it to be replaced in future.
type Span struct {
	w opentracing.Span
}

func openTracingSpan(s opentracing.Span) *Span {
	if s == nil {
		return nil
	}
	return &Span{w: s}
}

func (s *Span) unwrap() opentracing.Span {
	if s == nil {
		return nil
	}
	return s.w
}

// LogKV adds log key/value pairs to the span.
func (s *Span) LogKV(kv ...string) {
	alts := make([]interface{}, 0, len(kv))
	for _, v := range kv {
		alts = append(alts, v)
	}
	s.w.LogKV(alts...)
}

// SetTag sets a given tag to a value.
func (s *Span) SetTag(key string, value interface{}) {
	s.w.SetTag(key, value)
}

// Finish the span.
func (s *Span) Finish() {
	s.w.Finish()
}

// TextMap attempts to inject a span into a map object in text map format.
func (s *Span) TextMap() (map[string]interface{}, error) {
	spanMap := opentracing.TextMapCarrier{}

	if err := opentracing.GlobalTracer().Inject(s.w.Context(), opentracing.TextMap, spanMap); err != nil {
		return nil, err
	}

	spanMapGeneric := make(map[string]interface{}, len(spanMap))
	for k, v := range spanMap {
		spanMapGeneric[k] = v
	}

	return spanMapGeneric, nil
}
