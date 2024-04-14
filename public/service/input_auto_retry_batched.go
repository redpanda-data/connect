package service

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/benthosdev/benthos/v4/internal/autoretry"
	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// AutoRetryNacksBatchedToggled wraps an input implementation with
// AutoRetryNacksBatched only if a field defined by NewAutoRetryNacksToggleField
// has been set to true.
func AutoRetryNacksBatchedToggled(c *ParsedConfig, i BatchInput) (BatchInput, error) {
	b, err := c.FieldBool(AutoRetryNacksToggleFieldName)
	if err != nil {
		return nil, err
	}
	if b {
		return AutoRetryNacksBatched(i), nil
	}
	return i, nil
}

// AutoRetryNacksBatched wraps a batched input implementation with a component
// that automatically reattempts messages that fail downstream. This is useful
// for inputs that do not support nacks, and therefore don't have an answer for
// when an ack func is called with an error.
//
// When messages fail to be delivered they will be reattempted with back off
// until success or the stream is stopped.
func AutoRetryNacksBatched(i BatchInput) BatchInput {
	return &autoRetryInputBatched{
		retryList: autoretry.NewList(
			func(ctx context.Context) (MessageBatch, autoretry.AckFunc, error) {
				t, aFn, err := i.ReadBatch(ctx)

				// Make sure we're able to track the position of messages in
				// order to reassociate them after a batch-wide error
				// downstream.
				iParts := make([]*message.Part, len(t))
				for i, p := range t {
					iParts[i] = p.part
				}

				_, iParts = message.NewSortGroup(iParts)
				for i, p := range iParts {
					t[i] = NewInternalMessage(p)
				}

				return t, autoretry.AckFunc(aFn), err
			},
			func(t MessageBatch, err error) MessageBatch {
				var bErr *batch.Error
				if len(t) == 0 || !errors.As(err, &bErr) || bErr.IndexedErrors() == 0 {
					return t
				}

				sortGroup := message.TopLevelSortGroup(t[0].part)
				if sortGroup == nil {
					// We can't associate our source batch with the one that's associated
					// with the batch error, therefore we fall back towards treating every
					// message as if it was errored the same.
					return t
				}

				sortBatch := make(message.Batch, len(t))
				for i, p := range t {
					sortBatch[i] = p.part
				}

				seenIndexes := map[int]struct{}{}
				newBatch := make(MessageBatch, 0, bErr.IndexedErrors())
				bErr.WalkPartsBySource(sortGroup, sortBatch, func(i int, p *message.Part, err error) bool {
					if err == nil {
						return true
					}
					if _, exists := seenIndexes[i]; exists {
						return true
					}
					seenIndexes[i] = struct{}{}
					newBatch = append(newBatch, &Message{part: p})
					return true
				})
				if len(newBatch) == 0 {
					return t
				}
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
	if errors.Is(err, ErrEndOfInput) && !i.retryList.Exhausted() {
		atomic.StoreInt32(&i.inputClosed, 1)
		err = nil
	}
	return err
}

func (i *autoRetryInputBatched) ReadBatch(ctx context.Context) (MessageBatch, AckFunc, error) {
	batch, rAckFn, err := i.retryList.Shift(ctx, atomic.LoadInt32(&i.inputClosed) == 0)
	if err != nil {
		if errors.Is(err, autoretry.ErrExhausted) {
			return nil, nil, ErrEndOfInput
		}
		if errors.Is(err, ErrEndOfInput) {
			// Mark our input as being closed and trigger an immediate re-read
			// in order to clear any pending retries.
			atomic.StoreInt32(&i.inputClosed, 1)
			return i.ReadBatch(ctx)
		}
		// Otherwise we have an unknown error from our reader that we should
		// escalate, this is most likely an ErrNotConnected or ErrTimeout.
		return nil, nil, err
	}
	return batch.Copy(), AckFunc(rAckFn), nil
}

func (i *autoRetryInputBatched) Close(ctx context.Context) error {
	_ = i.retryList.Close(ctx)
	return i.child.Close(ctx)
}
