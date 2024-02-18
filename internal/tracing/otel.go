package tracing

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/message"
)

const (
	name                   = "benthos"
	componentTypeAttribute = "benthos.component.type"
)

// GetSpan returns a span attached to a message part. Returns nil if the part
// doesn't have a span attached.
func GetSpan(p *message.Part) *Span {
	ctx := message.GetContext(p)
	return GetSpanFromContext(ctx)
}

// GetSpan returns a span within a context. Returns nil if the context doesn't
// have a span attached.
func GetSpanFromContext(ctx context.Context) *Span {
	t := trace.SpanFromContext(ctx)
	return OtelSpan(ctx, t)
}

// GetActiveSpan returns a span attached to a message part. Returns nil if the
// part doesn't have a span attached or it is inactive.
func GetActiveSpan(p *message.Part) *Span {
	ctx := message.GetContext(p)
	t := trace.SpanFromContext(ctx)
	if !t.IsRecording() {
		return nil
	}
	return OtelSpan(ctx, t)
}

// GetTraceID returns the traceID from a span attached to a message part. Returns a zeroed traceID if the part
// doesn't have a span attached.
func GetTraceID(p *message.Part) string {
	ctx := message.GetContext(p)
	span := trace.SpanFromContext(ctx)
	return span.SpanContext().TraceID().String()
}

// WithChildSpan takes a message, extracts a span, creates a new child span,
// and returns a new message with that span embedded. The original message is
// unchanged.
func WithChildSpan(prov trace.TracerProvider, componentType string, componentLabel string, part *message.Part) (*message.Part, *Span) {
	span := GetActiveSpan(part)

	var (
		spanName      = GetSpanName(componentType, componentLabel)
		spanAttribute = attribute.String(componentTypeAttribute, componentType)
	)

	if span == nil {
		ctx, t := prov.Tracer(name).Start(part.GetContext(), spanName, trace.WithAttributes(spanAttribute))
		span = OtelSpan(ctx, t)
		part = part.WithContext(ctx)
	} else {
		ctx, t := prov.Tracer(name).Start(span.ctx, spanName, trace.WithAttributes(spanAttribute))
		span = OtelSpan(ctx, t)
		part = part.WithContext(ctx)
	}
	return part, span
}

// WithChildSpans takes a message, extracts spans per message part, creates new
// child spans, and returns a new message with those spans embedded. The
// original message is unchanged.
func WithChildSpans(prov trace.TracerProvider, componentType string, componentLabel string, batch message.Batch) (message.Batch, []*Span) {
	spans := make([]*Span, 0, len(batch))
	newParts := make(message.Batch, len(batch))
	for i, part := range batch {
		if part == nil {
			continue
		}
		var otSpan *Span
		newParts[i], otSpan = WithChildSpan(prov, componentType, componentLabel, part)
		spans = append(spans, otSpan)
	}
	return newParts, spans
}

// WithSiblingSpans takes a message, extracts spans per message part, creates
// new sibling spans, and returns a new message with those spans embedded. The
// original message is unchanged.
func WithSiblingSpans(prov trace.TracerProvider, componentType string, componentLabel string, batch message.Batch) (message.Batch, []*Span) {
	spans := make([]*Span, 0, len(batch))
	newParts := make([]*message.Part, batch.Len())

	var (
		spanName      = GetSpanName(componentType, componentLabel)
		spanAttribute = attribute.String(componentTypeAttribute, componentType)
	)

	for i, part := range batch {
		if part == nil {
			continue
		}
		otSpan := GetActiveSpan(part)
		if otSpan == nil {
			ctx, t := prov.Tracer(name).Start(part.GetContext(), spanName, trace.WithAttributes(spanAttribute))
			otSpan = OtelSpan(ctx, t)
		} else {
			ctx, t := prov.Tracer(name).Start(
				part.GetContext(), spanName,
				trace.WithLinks(trace.LinkFromContext(otSpan.ctx)),
				trace.WithAttributes(spanAttribute),
			)
			otSpan = OtelSpan(ctx, t)
		}
		newParts[i] = message.WithContext(otSpan.ctx, part)
		spans = append(spans, otSpan)
	}
	return newParts, spans
}

//------------------------------------------------------------------------------

// InitSpans sets up OpenTracing spans on each message part if one does not
// already exist.
func InitSpans(prov trace.TracerProvider, componentType string, componentLabel string, batch message.Batch) {
	for i, p := range batch {
		batch[i] = InitSpan(prov, componentType, componentLabel, p)
	}
}

// InitSpan sets up an OpenTracing span on a message part if one does not
// already exist.
func InitSpan(prov trace.TracerProvider, componentType string, componentLabel string, part *message.Part) *message.Part {
	if GetActiveSpan(part) != nil {
		return part
	}
	ctx, _ := prov.Tracer(name).Start(part.GetContext(), GetSpanName(componentType, componentLabel), trace.WithAttributes(attribute.String(componentTypeAttribute, componentType)))
	return message.WithContext(ctx, part)
}

// InitSpansFromParentTextMap obtains a span parent reference from a text map
// and creates child spans for each message.
func InitSpansFromParentTextMap(prov trace.TracerProvider, componentType string, componentLabel string, textMapGeneric map[string]any, batch message.Batch) error {
	c := propagation.MapCarrier{}
	for k, v := range textMapGeneric {
		if vStr, ok := v.(string); ok {
			c[strings.ToLower(k)] = vStr
		}
	}

	textProp := otel.GetTextMapPropagator()
	for i, p := range batch {
		ctx := textProp.Extract(p.GetContext(), c)
		pCtx, _ := prov.Tracer(name).Start(ctx, GetSpanName(componentType, componentLabel), trace.WithAttributes(attribute.String(componentTypeAttribute, componentType)))
		batch[i] = message.WithContext(pCtx, p)
	}
	return nil
}

// FinishSpans calls Finish on all message parts containing a span.
func FinishSpans(batch message.Batch) {
	for _, p := range batch {
		if span := GetActiveSpan(p); span != nil {
			span.unwrap().End()
		}
	}
}
