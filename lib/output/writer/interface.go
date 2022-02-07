package writer

import (
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// Type is a type that writes Benthos messages to a third party sink. If the
// protocol supports a form of acknowledgement then it will be returned by the
// call to Write.
type Type interface {
	// Connect attempts to establish a connection to the sink, if unsuccessful
	// returns an error. If the attempt is successful (or not necessary) returns
	// nil.
	Connect() error

	// Write should block until either the message is sent (and acknowledged) to
	// a sink, or a transport specific error has occurred, or the Type is
	// closed.
	Write(msg *message.Batch) error

	types.Closable
}
