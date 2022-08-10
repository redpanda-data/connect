package mock

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/message"
)

// Processor provides a mock processor implementation around a closure.
type Processor func(message.Batch) ([]message.Batch, error)

// ProcessBatch returns the closure result executed on a batch.
func (p Processor) ProcessBatch(ctx context.Context, b message.Batch) ([]message.Batch, error) {
	return p(b)
}

// Close does nothing.
func (p Processor) Close(context.Context) error {
	return nil
}
