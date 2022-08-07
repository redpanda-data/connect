package buffer

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/message"
)

// Streamed is an interface implemented by all buffer types that provides stream
// based methods.
type Streamed interface {
	// TransactionChan returns a channel used for consuming transactions from
	// this type. Every transaction received must be resolved before another
	// transaction will be sent.
	TransactionChan() <-chan message.Transaction

	// Consume starts the type receiving transactions from a Transactor.
	Consume(<-chan message.Transaction) error

	// TriggerStopConsuming instructs the buffer to cut off the producer it is
	// consuming from. It will then enter a mode whereby messages can only be
	// read, and when the buffer is empty it will shut down.
	TriggerStopConsuming()

	// TriggerCloseNow triggers the shut down of this component but should not
	// block the calling goroutine.
	TriggerCloseNow()

	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(ctx context.Context) error
}
