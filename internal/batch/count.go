package batch

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/message"
)

type batchedCountKeyType int

const batchedCountKey batchedCountKeyType = iota

// CtxCollapsedCount attempts to extract the actual number of messages that were
// collapsed into the resulting message part. This value could be greater than 1
// when users configure processors that archive batched message parts.
func CtxCollapsedCount(ctx context.Context) int {
	if v, ok := ctx.Value(batchedCountKey).(int); ok {
		return v
	}
	return 1
}

// CollapsedCount attempts to extract the actual number of messages that were
// collapsed into the resulting message part. This value could be greater than 1
// when users configure processors that archive batched message parts.
func CollapsedCount(p *message.Part) int {
	return CtxCollapsedCount(message.GetContext(p))
}

// MessageCollapsedCount attempts to extract the actual number of messages that
// were combined into the resulting batched message parts. This value could
// differ from message.Len() when users configure processors that archive
// batched message parts.
func MessageCollapsedCount(m message.Batch) int {
	total := 0
	_ = m.Iter(func(i int, p *message.Part) error {
		total += CollapsedCount(p)
		return nil
	})
	return total
}

// WithCollapsedCount returns a message part with a context indicating that this
// message is the result of collapsing a number of messages. This allows
// downstream components to know how many total messages were combined.
func WithCollapsedCount(p *message.Part, count int) *message.Part {
	// Start with the previous length which could also be >1.
	ctx := CtxWithCollapsedCount(message.GetContext(p), count)
	return message.WithContext(ctx, p)
}

// CtxWithCollapsedCount returns a message part with a context indicating that this
// message is the result of collapsing a number of messages. This allows
// downstream components to know how many total messages were combined.
func CtxWithCollapsedCount(ctx context.Context, count int) context.Context {
	base := 1
	if v, ok := ctx.Value(batchedCountKey).(int); ok {
		base = v
	}

	// The new length is the previous length plus the total messages put into
	// this batch (minus one to prevent double counting the original part).
	return context.WithValue(ctx, batchedCountKey, base-1+count)
}
