package processor

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/message"
)

// V1 is a common interface implemented by processors. The implementation of a
// V1 processor is responsible for all expected observability and error handling
// behaviour described within Benthos documentation.
type V1 interface {
	// Process a batch of messages into one or more resulting batches, or return
	// an error if the entire batch could not be processed, currently the only
	// valid reason for returning an error is if the context was cancelled.
	//
	// If zero messages are returned and the error is nil then all messages are
	// filtered.
	ProcessBatch(ctx context.Context, b message.Batch) ([]message.Batch, error)

	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}

// Pipeline is an interface that implements channel based consumer and
// producer methods for streaming data through a processing pipeline.
type Pipeline interface {
	// TransactionChan returns a channel used for consuming transactions from
	// this type. Every transaction received must be resolved before another
	// transaction will be sent.
	TransactionChan() <-chan message.Transaction

	// Consume starts the type receiving transactions from a Transactor.
	Consume(<-chan message.Transaction) error

	// TriggerCloseNow signals that the component should close immediately,
	// messages in flight will be dropped.
	TriggerCloseNow()

	// WaitForClose blocks until the component has closed down or the context is
	// cancelled. Closing occurs either when the input transaction channel is
	// closed and messages are flushed (and acked), or when CloseNowAsync is
	// called.
	WaitForClose(ctx context.Context) error
}

// PipelineConstructorFunc is a constructor to be called for each parallel
// stream pipeline thread in order to construct a custom pipeline
// implementation.
type PipelineConstructorFunc func() (Pipeline, error)

// Unwrap attempts to access a wrapped processor from the provided
// implementation where applicable, otherwise the provided processor is
// returned. This is necessary when access raw implementations that could have
// been wrapped in a tracing mechanism (or other).
func Unwrap(p V1) V1 {
	if w, ok := p.(interface {
		UnwrapProc() V1
	}); ok {
		return Unwrap(w.UnwrapProc())
	}
	return p
}
