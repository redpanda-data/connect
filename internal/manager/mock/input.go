package mock

import (
	"context"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/message"
)

// Input provides a mocked input implementation.
type Input struct {
	TChan     chan message.Transaction
	closeOnce sync.Once
}

// NewInput creates a new mock input that will return transactions containing a
// list of batches, then exit.
func NewInput(batches []message.Batch) *Input {
	ts := make(chan message.Transaction, len(batches))
	resChan := make(chan error, len(batches))
	go func() {
		defer close(ts)
		for _, b := range batches {
			ts <- message.NewTransaction(b, resChan)
		}
	}()
	return &Input{TChan: ts}
}

// Connected always returns true.
func (f *Input) Connected() bool {
	return true
}

// TransactionChan returns a transaction channel.
func (f *Input) TransactionChan() <-chan message.Transaction {
	return f.TChan
}

// TriggerStopConsuming closes the input transaction channel.
func (f *Input) TriggerStopConsuming() {
	f.closeOnce.Do(func() {
		close(f.TChan)
	})
}

// TriggerCloseNow closes the input transaction channel.
func (f *Input) TriggerCloseNow() {
	f.closeOnce.Do(func() {
		close(f.TChan)
	})
}

// WaitForClose does nothing.
func (f *Input) WaitForClose(ctx context.Context) error {
	return nil
}
