package input

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/message"
)

// Streamed is a common interface implemented by inputs and provides channel
// based streaming APIs.
type Streamed interface {
	// TransactionChan returns a channel used for consuming transactions from
	// this type. Every transaction received must be resolved before another
	// transaction will be sent.
	TransactionChan() <-chan message.Transaction

	// Connected returns a boolean indicating whether this input is currently
	// connected to its target.
	Connected() bool

	// TriggerStopConsuming instructs the input to start shutting down resources
	// once all pending messages are delivered and acknowledged. This call does
	// not block.
	TriggerStopConsuming()

	// TriggerCloseNow triggers the shut down of this component but should not
	// block the calling goroutine.
	TriggerCloseNow()

	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(ctx context.Context) error
}

// AsyncAckFn is a function used to acknowledge receipt of a message batch. The
// provided response indicates whether the message batch was successfully
// delivered. Returns an error if the acknowledge was not propagated.
type AsyncAckFn func(context.Context, error) error

// Async is a type that reads Benthos messages from an external source and
// allows acknowledgements for a message batch to be propagated asynchronously.
type Async interface {
	// Connect attempts to establish a connection to the source, if
	// unsuccessful returns an error. If the attempt is successful (or not
	// necessary) returns nil.
	Connect(ctx context.Context) error

	// ReadBatch attempts to read a new message from the source. If
	// successful a message is returned along with a function used to
	// acknowledge receipt of the returned message. It's safe to process the
	// returned message and read the next message asynchronously.
	ReadBatch(ctx context.Context) (message.Batch, AsyncAckFn, error)

	// Close triggers the shut down of this component and blocks until
	// completion or context cancellation.
	Close(ctx context.Context) error
}
