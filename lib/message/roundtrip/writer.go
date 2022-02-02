package roundtrip

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

// Writer is a writer implementation that adds messages to a ResultStore located
// in the context of the first message part of each batch. This is essentially a
// mechanism that returns the result of a pipeline directly back to the origin
// of the message.
type Writer struct{}

// ConnectWithContext is a noop.
func (s Writer) ConnectWithContext(ctx context.Context) error {
	return nil
}

// WriteWithContext writes a message batch to a ResultStore located in the first
// message of the batch.
func (s Writer) WriteWithContext(ctx context.Context, msg types.Message) error {
	return SetAsResponse(msg)
}

// CloseAsync is a noop.
func (s Writer) CloseAsync() {}

// WaitForClose is a noop.
func (s Writer) WaitForClose(time.Duration) error {
	return nil
}
