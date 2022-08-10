package output

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/message"
)

// Sync is a common interface implemented by outputs and provides synchronous
// based writing APIs.
type Sync interface {
	// WriteTransaction attempts to write a transaction to an output.
	WriteTransaction(context.Context, message.Transaction) error

	// Connected returns a boolean indicating whether this output is currently
	// connected to its target.
	Connected() bool

	// TriggerStopConsuming instructs the output to start shutting down
	// resources once all pending messages are delivered and acknowledged.
	TriggerStopConsuming()

	// TriggerCloseNow triggers the shut down of this component but should not
	// block the calling goroutine.
	TriggerCloseNow()

	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(ctx context.Context) error
}

// Streamed is a common interface implemented by outputs and provides channel
// based streaming APIs.
type Streamed interface {
	// Consume starts the type receiving transactions from a Transactor.
	Consume(<-chan message.Transaction) error

	// Connected returns a boolean indicating whether this output is currently
	// connected to its target.
	Connected() bool

	// TriggerCloseNow triggers the shut down of this component but should not
	// block the calling goroutine.
	TriggerCloseNow()

	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(ctx context.Context) error
}
