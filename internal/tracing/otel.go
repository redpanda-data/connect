package tracing

import (
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/message"
)

const (
	name = "benthos"
)

// GetSpan returns a span attached to a message part. Returns nil if the part
// doesn't have a span attached.
func GetSpan(p *message.Part) *Span {
	ctx := message.GetContext(p)
	t := trace.SpanFromContext(ctx)
	return otelSpan(ctx, t)
}

// GetActiveSpan returns a span attached to a message part. Returns nil if the
// part doesn't have a span attached or it is inactive.
func GetActiveSpan(p *message.Part) *Span {
	ctx := message.GetContext(p)
	t := trace.SpanFromContext(ctx)
	if !t.IsRecording() {
		return nil
	}
	return otelSpan(ctx, t)
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
func WithChildSpan(prov trace.TracerProvider, operationName string, part *message.Part) (*message.Part, *Span) {
	span := GetActiveSpan(part)
	if span == nil {
		ctx, t := prov.Tracer(name).Start(part.GetContext(), operationName)
		span = otelSpan(ctx, t)
		part = part.WithContext(ctx)
	} else {
		ctx, t := prov.Tracer(name).Start(span.ctx, operationName)
		span = otelSpan(ctx, t)
		part = part.WithContext(ctx)
	}
	return part, span
}

// WithChildSpans takes a message, extracts spans per message part, creates new
// child spans, and returns a new message with those spans embedded. The
// original message is unchanged.
func WithChildSpans(prov trace.TracerProvider, operationName string, batch message.Batch) (message.Batch, []*Span) {
	spans := make([]*Span, 0, len(batch))
	newParts := make(message.Batch, len(batch))
	for i, part := range batch {
		if part == nil {
			continue
		}
		var otSpan *Span
		newParts[i], otSpan = WithChildSpan(prov, operationName, part)
		spans = append(spans, otSpan)
	}
	return newParts, spans
}

// WithSiblingSpans takes a message, extracts spans per message part, creates
// new sibling spans, and returns a new message with those spans embedded. The
// original message is unchanged.
func WithSiblingSpans(prov trace.TracerProvider, operationName string, batch message.Batch) (message.Batch, []*Span) {
	spans := make([]*Span, 0, len(batch))
	newParts := make([]*message.Part, batch.Len())
	for i, part := range batch {
		if part == nil {
			continue
		}
		otSpan := GetActiveSpan(part)
		if otSpan == nil {
			ctx, t := prov.Tracer(name).Start(part.GetContext(), operationName)
			otSpan = otelSpan(ctx, t)
		} else {
			ctx, t := prov.Tracer(name).Start(
				part.GetContext(), operationName,
				trace.WithLinks(trace.LinkFromContext(otSpan.ctx)),
			)
			otSpan = otelSpan(ctx, t)
		}
		newParts[i] = message.WithContext(otSpan.ctx, part)
		spans = append(spans, otSpan)
	}
	return newParts, spans
}

//------------------------------------------------------------------------------

// InitSpans sets up OpenTracing spans on each message part if one does not
// already exist.
func InitSpans(prov trace.TracerProvider, operationName string, batch message.Batch) {
	for i, p := range batch {
		batch[i] = InitSpan(prov, operationName, p)
	}
}

// InitSpan sets up an OpenTracing span on a message part if one does not
// already exist.
func InitSpan(prov trace.TracerProvider, operationName string, part *message.Part) *message.Part {
	if GetActiveSpan(part) != nil {
		return part
	}
	ctx, _ := prov.Tracer(name).Start(part.GetContext(), operationName)
	return message.WithContext(ctx, part)
}

// InitSpansFromParentTextMap obtains a span parent reference from a text map
// and creates child spans for each message.
func InitSpansFromParentTextMap(prov trace.TracerProvider, operationName string, textMapGeneric map[string]any, batch message.Batch) error {
	c := propagation.MapCarrier{}
	for k, v := range textMapGeneric {
		if vStr, ok := v.(string); ok {
			c[strings.ToLower(k)] = vStr
		}
	}

	textProp := otel.GetTextMapPropagator()
	for i, p := range batch {
		ctx := textProp.Extract(p.GetContext(), c)
		pCtx, _ := prov.Tracer(name).Start(ctx, operationName)
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
