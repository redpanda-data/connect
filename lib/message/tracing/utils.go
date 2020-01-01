package tracing

import (
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

// GetSpan returns a span attached to a message part. Returns nil if the part
// doesn't have a span attached.
func GetSpan(p types.Part) opentracing.Span {
	return opentracing.SpanFromContext(message.GetContext(p))
}

// CreateChildSpan takes a message part, extracts an existing span if there is
// one and returns child span.
func CreateChildSpan(operationName string, part types.Part) opentracing.Span {
	span := GetSpan(part)
	if span == nil {
		span = opentracing.StartSpan(operationName)
	} else {
		span = opentracing.StartSpan(
			operationName,
			opentracing.ChildOf(span.Context()),
		)
	}
	return span
}

// CreateChildSpans takes a message, extracts spans per message part and returns
// a slice of child spans. The length of the returned slice is guaranteed to
// match the message size.
func CreateChildSpans(operationName string, msg types.Message) []opentracing.Span {
	spans := make([]opentracing.Span, msg.Len())
	msg.Iter(func(i int, part types.Part) error {
		spans[i] = CreateChildSpan(operationName, part)
		return nil
	})
	return spans
}

// WithChildSpans takes a message, extracts spans per message part, creates new
// child spans, and returns a new message with those spans embedded. The
// original message is unchanged.
func WithChildSpans(operationName string, msg types.Message) (types.Message, []opentracing.Span) {
	spans := make([]opentracing.Span, msg.Len())
	parts := make([]types.Part, msg.Len())
	msg.Iter(func(i int, part types.Part) error {
		ctx := message.GetContext(part)
		span := opentracing.SpanFromContext(ctx)
		if span == nil {
			span = opentracing.StartSpan(operationName)
		} else {
			span = opentracing.StartSpan(
				operationName,
				opentracing.ChildOf(span.Context()),
			)
		}
		ctx = opentracing.ContextWithSpan(ctx, span)
		parts[i] = message.WithContext(ctx, part)
		spans[i] = span
		return nil
	})

	newMsg := message.New(nil)
	newMsg.SetAll(parts)
	return newMsg, spans
}

// WithSiblingSpans takes a message, extracts spans per message part, creates
// new sibling spans, and returns a new message with those spans embedded. The
// original message is unchanged.
func WithSiblingSpans(operationName string, msg types.Message) types.Message {
	parts := make([]types.Part, msg.Len())
	msg.Iter(func(i int, part types.Part) error {
		ctx := message.GetContext(part)
		span := opentracing.SpanFromContext(ctx)
		if span == nil {
			span = opentracing.StartSpan(operationName)
		} else {
			span = opentracing.StartSpan(
				operationName,
				opentracing.FollowsFrom(span.Context()),
			)
		}
		ctx = opentracing.ContextWithSpan(ctx, span)
		parts[i] = message.WithContext(ctx, part)
		return nil
	})

	newMsg := message.New(nil)
	newMsg.SetAll(parts)
	return newMsg
}

//------------------------------------------------------------------------------

// IterateWithChildSpans iterates all the parts of a message and, for each part,
// creates a new span from an existing span attached to the part and calls a
// func with that span before finishing the child span.
func IterateWithChildSpans(operationName string, msg types.Message, iter func(int, opentracing.Span, types.Part) error) error {
	return msg.Iter(func(i int, p types.Part) error {
		span, _ := opentracing.StartSpanFromContext(message.GetContext(p), operationName)
		err := iter(i, span, p)
		span.Finish()
		return err
	})
}

// InitSpans sets up OpenTracing spans on each message part if one does not
// already exist.
func InitSpans(operationName string, msg types.Message) {
	tracedParts := make([]types.Part, msg.Len())
	msg.Iter(func(i int, p types.Part) error {
		if GetSpan(p) != nil {
			tracedParts[i] = p
			return nil
		}
		span := opentracing.StartSpan(operationName)
		ctx := opentracing.ContextWithSpan(message.GetContext(p), span)
		tracedParts[i] = message.WithContext(ctx, p)
		return nil
	})
	msg.SetAll(tracedParts)
}

// InitSpansFromParent sets up OpenTracing spans as children of a parent span on
// each message part if one does not already exist.
func InitSpansFromParent(operationName string, parent opentracing.SpanContext, msg types.Message) {
	tracedParts := make([]types.Part, msg.Len())
	msg.Iter(func(i int, p types.Part) error {
		if GetSpan(p) != nil {
			tracedParts[i] = p
			return nil
		}
		span := opentracing.StartSpan(operationName, opentracing.ChildOf(parent))
		ctx := opentracing.ContextWithSpan(message.GetContext(p), span)
		tracedParts[i] = message.WithContext(ctx, p)
		return nil
	})
	msg.SetAll(tracedParts)
}

// FinishSpans calls Finish on all message parts containing a span.
func FinishSpans(msg types.Message) {
	msg.Iter(func(i int, p types.Part) error {
		span := GetSpan(p)
		if span == nil {
			return nil
		}
		span.Finish()
		return nil
	})
}

//------------------------------------------------------------------------------
