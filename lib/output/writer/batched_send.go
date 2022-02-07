package writer

import (
	"errors"

	"github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// Returns true if the error should break a batch send loop.
func sendErrIsFatal(err error) bool {
	if errors.Is(err, types.ErrTypeClosed) {
		return true
	}
	if errors.Is(err, types.ErrNotConnected) {
		return true
	}
	if errors.Is(err, types.ErrTimeout) {
		return true
	}
	return false
}

// IterateBatchedSend executes a closure fn on each message of a batch, where
// the closure is expected to attempt a send and return an error. If an error is
// returned then it is added to a batch error in order to support index specific
// error handling.
//
// However, if a fatal error is returned such as a connection loss or shut down
// then it is returned immediately.
func IterateBatchedSend(msg *message.Batch, fn func(int, *message.Part) error) error {
	if msg.Len() == 1 {
		return fn(0, msg.Get(0))
	}
	var batchErr *batch.Error
	if err := msg.Iter(func(i int, p *message.Part) error {
		tmpErr := fn(i, p)
		if tmpErr != nil {
			if sendErrIsFatal(tmpErr) {
				return tmpErr
			}
			if batchErr == nil {
				batchErr = batch.NewError(msg, tmpErr)
			}
			batchErr.Failed(i, tmpErr)
		}
		return nil
	}); err != nil {
		return err
	}
	if batchErr != nil {
		return batchErr
	}
	return nil
}
