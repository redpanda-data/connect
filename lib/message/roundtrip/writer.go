package roundtrip

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Writer is a writer implementation that adds messages to a ResultStore located
// in the context of the first message part of each batch. This is essentially a
// mechanism that returns the result of a pipeline directly back to the origin
// of the message.
type Writer struct{}

// Connect is a noop.
func (s Writer) Connect() error {
	return nil
}

// Write a message batch to a ResultStore located in the first message of the
// batch.
func (s Writer) Write(msg types.Message) error {
	return SetAsResponse(msg)
}

// CloseAsync is a noop.
func (s Writer) CloseAsync() {}

// WaitForClose is a noop.
func (s Writer) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
