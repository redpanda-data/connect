package processor

import (
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Type reads a message, performs a processing operation, and returns either a
// slice of messages resulting from the process to be propagated through the
// pipeline, or a response that should be sent back to the source instead.
type Type interface {
	// ProcessMessage attempts to process a message. Since processing can fail
	// this call returns both a slice of messages in case of success or a
	// response in case of failure. If the slice of messages is empty the
	// response should be returned to the source.
	ProcessMessage(msg *message.Batch) ([]*message.Batch, error)

	types.Closable
}

//------------------------------------------------------------------------------
