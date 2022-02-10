package reader

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/response"
)

// AsyncAckFn is a function used to acknowledge receipt of a message batch. The
// provided response indicates whether the message batch was successfully
// delivered. Returns an error if the acknowledge was not propagated.
type AsyncAckFn func(context.Context, response.Error) error

var noopAsyncAckFn AsyncAckFn = func(context.Context, response.Error) error {
	return nil
}

// Async is a type that reads Benthos messages from an external source and
// allows acknowledgements for a message batch to be propagated asynchronously.
type Async interface {
	// ConnectWithContext attempts to establish a connection to the source, if
	// unsuccessful returns an error. If the attempt is successful (or not
	// necessary) returns nil.
	ConnectWithContext(ctx context.Context) error

	// ReadWithContext attempts to read a new message from the source. If
	// successful a message is returned along with a function used to
	// acknowledge receipt of the returned message. It's safe to process the
	// returned message and read the next message asynchronously.
	ReadWithContext(ctx context.Context) (*message.Batch, AsyncAckFn, error)

	// CloseAsync triggers the shut down of this component but should not block
	// the calling goroutine.
	CloseAsync()

	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(timeout time.Duration) error
}
