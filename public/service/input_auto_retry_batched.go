package service

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	"github.com/benthosdev/benthos/v4/internal/autoretry"
	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// AutoRetryNacksBatched wraps a batched input implementation with a component
// that automatically reattempts messages that fail downstream. This is useful
// for inputs that do not support nacks, and therefore don't have an answer for
// when an ack func is called with an error.
//
// When messages fail to be delivered they will be reattempted with back off
// until success or the stream is stopped.
func AutoRetryNacksBatched(i BatchInput) BatchInput {
	return &autoRetryInputBatched{
		retryList: autoretry.NewList(func(t MessageBatch, err error) MessageBatch {
			var bErr *batch.Error
			if !errors.As(err, &bErr) || bErr.IndexedErrors() == 0 {
				return t
			}

			newBatch := make(MessageBatch, 0, bErr.IndexedErrors())
			bErr.WalkParts(func(i int, p *message.Part, err error) bool {
				if err == nil {
					return true
				}
				newBatch = append(newBatch, &Message{part: p})
				return true
			})
			return newBatch
		}),
		child: i,
	}
}

//------------------------------------------------------------------------------

type autoRetryInputBatched struct {
	retryList   *autoretry.List[MessageBatch]
	child       BatchInput
	inputClosed int32
}

func (i *autoRetryInputBatched) Connect(ctx context.Context) error {
	err := i.child.Connect(ctx)
	// If our source has finished but we still have messages in flight then
	// we act like we're still open. Read will be called and we can either
	// return the pending messages or wait for them.
	if errors.Is(err, ErrEndOfInput) && i.retryList.Exhausted() {
		atomic.StoreInt32(&i.inputClosed, 1)
		err = nil
	}
	return err
}

func (i *autoRetryInputBatched) ReadBatch(ctx context.Context) (MessageBatch, AckFunc, error) {
	if batch, rAckFn, exists := i.retryList.TryShift(ctx); exists {
		return batch.Copy(), AckFunc(rAckFn), nil
	}

	var (
		batch MessageBatch
		aFn   AckFunc
		err   error
	)

	if atomic.LoadInt32(&i.inputClosed) > 0 {
		err = ErrEndOfInput
	} else {
		batch, aFn, err = i.child.ReadBatch(ctx)
	}
	if err != nil {
		// If our source has finished but we still have messages in flight then
		// we block, ideally until the messages are acked.
		if errors.Is(err, ErrEndOfInput) {
			batch, rAckFn, err := i.retryList.Shift(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = ErrEndOfInput
				}
				return nil, nil, err
			}
			return batch.Copy(), AckFunc(rAckFn), nil
		}
		return nil, nil, err
	}

	rAckFn := i.retryList.Adopt(ctx, batch, autoretry.AckFunc(aFn))
	return batch.Copy(), AckFunc(rAckFn), nil
}

func (i *autoRetryInputBatched) Close(ctx context.Context) error {
	return i.child.Close(ctx)
}
