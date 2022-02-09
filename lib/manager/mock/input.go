package mock

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// Input provides a mocked input implementation.
type Input struct {
	ts chan types.Transaction
}

// NewInput creates a new mock input that will return transactions containing a
// list of batches, then exit.
func NewInput(batches []*message.Batch) *Input {
	ts := make(chan types.Transaction, len(batches))
	resChan := make(chan response.Error, len(batches))
	go func() {
		defer close(ts)
		for _, b := range batches {
			ts <- types.NewTransaction(b, resChan)
		}
	}()
	return &Input{ts: ts}
}

// Connected always returns true.
func (f *Input) Connected() bool {
	return true
}

// TransactionChan returns a transaction channel.
func (f *Input) TransactionChan() <-chan types.Transaction {
	return f.ts
}

// CloseAsync does nothing.
func (f *Input) CloseAsync() {
}

// WaitForClose does nothing.
func (f *Input) WaitForClose(time.Duration) error {
	return nil
}
