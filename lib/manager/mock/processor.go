package mock

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
)

// Processor provides a mock processor implementation around a closure.
type Processor func(*message.Batch) ([]*message.Batch, error)

// ProcessMessage returns the closure result executed on a batch.
func (p Processor) ProcessMessage(b *message.Batch) ([]*message.Batch, error) {
	return p(b)
}

// CloseAsync does nothing.
func (p Processor) CloseAsync() {
}

// WaitForClose does nothing.
func (p Processor) WaitForClose(time.Duration) error {
	return nil
}
