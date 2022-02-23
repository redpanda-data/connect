package tracing

import (
	"context"

	"github.com/Jeffail/benthos/v3/lib/message"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	name = "benthos"
)

// GetSpan returns a span attached to a message part. Returns nil if the part
// doesn't have a span attached.
func GetSpan(p *message.Part) *Span {
	ctx := message.GetContext(p)
	t := trace.SpanFromContext(ctx)
	if !t.IsRecording() {
		return nil
	}
	return otelSpan(ctx, t)
}

// CreateChildSpan takes a message part, extracts an existing span if there is
// one and returns child span.
func CreateChildSpan(operationName string, part *message.Part) *Span {
	span := GetSpan(part)
	if span == nil {
		ctx, t := otel.GetTracerProvider().Tracer(name).Start(context.Background(), operationName)
		span = otelSpan(ctx, t)
	} else {
		ctx, t := otel.GetTracerProvider().Tracer(name).Start(span.ctx, operationName)
		span = otelSpan(ctx, t)
	}
	return span
}

// CreateChildSpans takes a message, extracts spans per message part and returns
// a slice of child spans. The length of the returned slice is guaranteed to
// match the message size.
func CreateChildSpans(operationName string, msg *message.Batch) []*Span {
	spans := make([]*Span, msg.Len())
	_ = msg.Iter(func(i int, part *message.Part) error {
		spans[i] = CreateChildSpan(operationName, part)
		return nil
	})
	return spans
}

// PartsWithChildSpans takes a slice of message parts, extracts spans per part,
// creates new child spans, and returns a new slice of parts with those spans
// embedded. The original parts are unchanged.
func PartsWithChildSpans(operationName string, parts []*message.Part) ([]*message.Part, []*Span) {
	spans := make([]*Span, 0, len(parts))
	newParts := make([]*message.Part, len(parts))
	for i, part := range parts {
		if part == nil {
			continue
		}
		otSpan := CreateChildSpan(operationName, part)
		newParts[i] = message.WithContext(otSpan.ctx, part)
		spans = append(spans, otSpan)
	}
	return newParts, spans
}

// WithChildSpans takes a message, extracts spans per message part, creates new
// child spans, and returns a new message with those spans embedded. The
// original message is unchanged.
func WithChildSpans(operationName string, msg *message.Batch) (*message.Batch, []*Span) {
	parts := make([]*message.Part, 0, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		parts = append(parts, p)
		return nil
	})

	newParts, spans := PartsWithChildSpans(operationName, parts)
	newMsg := message.QuickBatch(nil)
	newMsg.SetAll(newParts)

	return newMsg, spans
}

// WithSiblingSpans takes a message, extracts spans per message part, creates
// new sibling spans, and returns a new message with those spans embedded. The
// original message is unchanged.
func WithSiblingSpans(operationName string, msg *message.Batch) *message.Batch {
	parts := make([]*message.Part, msg.Len())
	_ = msg.Iter(func(i int, part *message.Part) error {
		otSpan := GetSpan(part)
		if otSpan == nil {
			ctx, t := otel.GetTracerProvider().Tracer(name).Start(context.Background(), operationName)
			otSpan = otelSpan(ctx, t)
		} else {
			ctx, t := otel.GetTracerProvider().Tracer(name).Start(
				context.Background(), operationName,
				trace.WithLinks(trace.LinkFromContext(otSpan.ctx)),
			)
			otSpan = otelSpan(ctx, t)
		}
		parts[i] = message.WithContext(otSpan.ctx, part)
		return nil
	})

	newMsg := message.QuickBatch(nil)
	newMsg.SetAll(parts)
	return newMsg
}

//------------------------------------------------------------------------------

// IterateWithChildSpans iterates all the parts of a message and, for each part,
// creates a new span from an existing span attached to the part and calls a
// func with that span before finishing the child span.
func IterateWithChildSpans(operationName string, msg *message.Batch, iter func(int, *Span, *message.Part) error) error {
	return msg.Iter(func(i int, p *message.Part) error {
		otSpan := CreateChildSpan(operationName, p)
		err := iter(i, otSpan, p)
		otSpan.Finish()
		return err
	})
}

// InitSpans sets up OpenTracing spans on each message part if one does not
// already exist.
func InitSpans(operationName string, msg *message.Batch) {
	tracedParts := make([]*message.Part, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		tracedParts[i] = InitSpan(operationName, p)
		return nil
	})
	msg.SetAll(tracedParts)
}

// InitSpan sets up an OpenTracing span on a message part if one does not
// already exist.
func InitSpan(operationName string, part *message.Part) *message.Part {
	if GetSpan(part) != nil {
		return part
	}
	ctx, _ := otel.GetTracerProvider().Tracer(name).Start(context.Background(), operationName)
	return message.WithContext(ctx, part)
}

// InitSpansFromParent sets up OpenTracing spans as children of a parent span on
// each message part if one does not already exist.
func InitSpansFromParent(operationName string, parent *Span, msg *message.Batch) {
	tracedParts := make([]*message.Part, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		tracedParts[i] = InitSpanFromParent(operationName, parent, p)
		return nil
	})
	msg.SetAll(tracedParts)
}

// InitSpanFromParent sets up an OpenTracing span as children of a parent
// span on a message part if one does not already exist.
func InitSpanFromParent(operationName string, parent *Span, part *message.Part) *message.Part {
	if GetSpan(part) != nil {
		return part
	}
	ctx, _ := otel.GetTracerProvider().Tracer(name).Start(parent.ctx, operationName)
	return message.WithContext(ctx, part)
}

// InitSpansFromParentTextMap obtains a span parent reference from a text map
// and creates child spans for each message.
func InitSpansFromParentTextMap(operationName string, textMapGeneric map[string]interface{}, msg *message.Batch) error {
	c := propagation.MapCarrier{}
	for k, v := range textMapGeneric {
		if vStr, ok := v.(string); ok {
			c[k] = vStr
		}
	}

	ctx := otel.GetTextMapPropagator().Extract(context.Background(), c)

	tracedParts := make([]*message.Part, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		pCtx, _ := otel.GetTracerProvider().Tracer(name).Start(ctx, operationName)
		tracedParts[i] = message.WithContext(pCtx, p)
		return nil
	})

	msg.SetAll(tracedParts)
	return nil
}

// FinishSpans calls Finish on all message parts containing a span.
func FinishSpans(msg *message.Batch) {
	_ = msg.Iter(func(i int, p *message.Part) error {
		span := GetSpan(p)
		if span == nil {
			return nil
		}
		span.unwrap().End()
		return nil
	})
}
