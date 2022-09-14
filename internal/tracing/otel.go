package tracing

import (
	"context"
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

// CreateChildSpan takes a message part, extracts an existing span if there is
// one and returns child span.
func CreateChildSpan(prov trace.TracerProvider, operationName string, part *message.Part) *Span {
	span := GetActiveSpan(part)
	if span == nil {
		ctx, t := prov.Tracer(name).Start(context.Background(), operationName)
		span = otelSpan(ctx, t)
	} else {
		ctx, t := prov.Tracer(name).Start(span.ctx, operationName)
		span = otelSpan(ctx, t)
	}
	return span
}

// CreateChildSpans takes a message, extracts spans per message part and returns
// a slice of child spans. The length of the returned slice is guaranteed to
// match the message size.
func CreateChildSpans(prov trace.TracerProvider, operationName string, msg message.Batch) []*Span {
	spans := make([]*Span, msg.Len())
	_ = msg.Iter(func(i int, part *message.Part) error {
		spans[i] = CreateChildSpan(prov, operationName, part)
		return nil
	})
	return spans
}

// PartsWithChildSpans takes a slice of message parts, extracts spans per part,
// creates new child spans, and returns a new slice of parts with those spans
// embedded. The original parts are unchanged.
func PartsWithChildSpans(prov trace.TracerProvider, operationName string, parts []*message.Part) ([]*message.Part, []*Span) {
	spans := make([]*Span, 0, len(parts))
	newParts := make([]*message.Part, len(parts))
	for i, part := range parts {
		if part == nil {
			continue
		}
		otSpan := CreateChildSpan(prov, operationName, part)
		newParts[i] = message.WithContext(otSpan.ctx, part)
		spans = append(spans, otSpan)
	}
	return newParts, spans
}

// WithChildSpans takes a message, extracts spans per message part, creates new
// child spans, and returns a new message with those spans embedded. The
// original message is unchanged.
func WithChildSpans(prov trace.TracerProvider, operationName string, msg message.Batch) (message.Batch, []*Span) {
	parts := make([]*message.Part, 0, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		parts = append(parts, p)
		return nil
	})
	return PartsWithChildSpans(prov, operationName, parts)
}

// WithSiblingSpans takes a message, extracts spans per message part, creates
// new sibling spans, and returns a new message with those spans embedded. The
// original message is unchanged.
func WithSiblingSpans(prov trace.TracerProvider, operationName string, msg message.Batch) message.Batch {
	parts := make([]*message.Part, msg.Len())
	_ = msg.Iter(func(i int, part *message.Part) error {
		otSpan := GetActiveSpan(part)
		if otSpan == nil {
			ctx, t := prov.Tracer(name).Start(context.Background(), operationName)
			otSpan = otelSpan(ctx, t)
		} else {
			ctx, t := prov.Tracer(name).Start(
				context.Background(), operationName,
				trace.WithLinks(trace.LinkFromContext(otSpan.ctx)),
			)
			otSpan = otelSpan(ctx, t)
		}
		parts[i] = message.WithContext(otSpan.ctx, part)
		return nil
	})
	return parts
}

//------------------------------------------------------------------------------

// IterateWithChildSpans iterates all the parts of a message and, for each part,
// creates a new span from an existing span attached to the part and calls a
// func with that span before finishing the child span.
func IterateWithChildSpans(prov trace.TracerProvider, operationName string, msg message.Batch, iter func(int, *Span, *message.Part) error) error {
	return msg.Iter(func(i int, p *message.Part) error {
		otSpan := CreateChildSpan(prov, operationName, p)
		err := iter(i, otSpan, p)
		otSpan.Finish()
		return err
	})
}

// InitSpans sets up OpenTracing spans on each message part if one does not
// already exist.
func InitSpans(prov trace.TracerProvider, operationName string, msg message.Batch) {
	for i, p := range msg {
		msg[i] = InitSpan(prov, operationName, p)
	}
}

// InitSpan sets up an OpenTracing span on a message part if one does not
// already exist.
func InitSpan(prov trace.TracerProvider, operationName string, part *message.Part) *message.Part {
	if GetActiveSpan(part) != nil {
		return part
	}
	ctx, _ := prov.Tracer(name).Start(context.Background(), operationName)
	return message.WithContext(ctx, part)
}

// InitSpansFromParent sets up OpenTracing spans as children of a parent span on
// each message part if one does not already exist.
func InitSpansFromParent(prov trace.TracerProvider, operationName string, parent *Span, msg message.Batch) {
	for i, p := range msg {
		msg[i] = InitSpanFromParent(prov, operationName, parent, p)
	}
}

// InitSpanFromParent sets up an OpenTracing span as children of a parent
// span on a message part if one does not already exist.
func InitSpanFromParent(prov trace.TracerProvider, operationName string, parent *Span, part *message.Part) *message.Part {
	if GetActiveSpan(part) != nil {
		return part
	}
	ctx, _ := prov.Tracer(name).Start(parent.ctx, operationName)
	return message.WithContext(ctx, part)
}

// InitSpansFromParentTextMap obtains a span parent reference from a text map
// and creates child spans for each message.
func InitSpansFromParentTextMap(prov trace.TracerProvider, operationName string, textMapGeneric map[string]any, msg message.Batch) error {
	c := propagation.MapCarrier{}
	for k, v := range textMapGeneric {
		if vStr, ok := v.(string); ok {
			c[strings.ToLower(k)] = vStr
		}
	}

	ctx := otel.GetTextMapPropagator().Extract(context.Background(), c)
	for i, p := range msg {
		pCtx, _ := prov.Tracer(name).Start(ctx, operationName)
		msg[i] = message.WithContext(pCtx, p)
	}
	return nil
}

// FinishSpans calls Finish on all message parts containing a span.
func FinishSpans(msg message.Batch) {
	for _, p := range msg {
		if span := GetActiveSpan(p); span != nil {
			span.unwrap().End()
		}
	}
}
