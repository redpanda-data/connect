package mock

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// Processor provides a mock processor implementation around a closure.
type Processor func(*message.Batch) ([]*message.Batch, error)

// ProcessMessage returns the closure result executed on a batch.
func (p Processor) ProcessMessage(b *message.Batch) ([]*message.Batch, types.Response) {
	res, err := p(b)
	if err != nil {
		return nil, response.NewError(err)
	}
	return res, nil
}

// CloseAsync does nothing.
func (p Processor) CloseAsync() {
}

// WaitForClose does nothing.
func (p Processor) WaitForClose(time.Duration) error {
	return nil
}
