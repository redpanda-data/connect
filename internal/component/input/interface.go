package input

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
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

	// CloseAsync triggers the shut down of this component but should not block
	// the calling goroutine.
	CloseAsync()

	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(timeout time.Duration) error
}
