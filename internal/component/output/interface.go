package output

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
)

// Sync is a common interface implemented by outputs and provides synchronous
// based writing APIs.
type Sync interface {
	// WriteTransaction attempts to write a transaction to an output.
	WriteTransaction(context.Context, message.Transaction) error

	// Connected returns a boolean indicating whether this output is currently
	// connected to its target.
	Connected() bool

	// CloseAsync triggers the shut down of this component but should not block
	// the calling goroutine.
	CloseAsync()

	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(timeout time.Duration) error
}

// Streamed is a common interface implemented by outputs and provides channel
// based streaming APIs.
type Streamed interface {
	// Consume starts the type receiving transactions from a Transactor.
	Consume(<-chan message.Transaction) error

	// Connected returns a boolean indicating whether this output is currently
	// connected to its target.
	Connected() bool

	// CloseAsync triggers the shut down of this component but should not block
	// the calling goroutine.
	CloseAsync()

	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(timeout time.Duration) error
}
