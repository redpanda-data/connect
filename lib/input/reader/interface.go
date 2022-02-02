package reader

import (
	"context"

	"github.com/Jeffail/benthos/v3/lib/types"
)

// AsyncAckFn is a function used to acknowledge receipt of a message batch. The
// provided response indicates whether the message batch was successfully
// delivered. Returns an error if the acknowledge was not propagated.
type AsyncAckFn func(context.Context, types.Response) error

var noopAsyncAckFn AsyncAckFn = func(context.Context, types.Response) error {
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
	ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error)

	types.Closable
}
