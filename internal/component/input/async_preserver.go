package input

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/benthosdev/benthos/v4/internal/autoretry"
	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// AsyncPreserver is a wrapper for input.Async implementations that keeps a
// buffer of sent messages until they are acknowledged. If an error occurs
// during message propagation the contents of the buffer will be resent instead
// of reading new messages until it is depleted. AsyncPreserver implements
// input.Async.
//
// Wrapping an input with this type is useful when your source of messages
// doesn't have a concept of a NoAck (like Kafka), and instead of "rejecting"
// messages we always intend to simply retry them until success.
type AsyncPreserver struct {
	retryList *autoretry.List[message.Batch]

	inputClosed int32
	r           Async
}

// NewAsyncPreserver returns a new AsyncPreserver wrapper around a input.Async.
func NewAsyncPreserver(r Async) *AsyncPreserver {
	return &AsyncPreserver{
		retryList: autoretry.NewList(
			func(ctx context.Context) (message.Batch, autoretry.AckFunc, error) {
				t, aFn, err := r.ReadBatch(ctx)

				// Make sure we're able to track the position of messages in
				// order to reassociate them after a batch-wide error
				// downstream.
				_, t = message.NewSortGroup(t)

				return t, autoretry.AckFunc(aFn), err
			},
			func(t message.Batch, err error) message.Batch {
				var bErr *batch.Error
				if len(t) == 0 || !errors.As(err, &bErr) || bErr.IndexedErrors() == 0 {
					return t
				}

				sortGroup := message.TopLevelSortGroup(t[0])
				if sortGroup == nil {
					// We can't associate our source batch with the one that's associated
					// with the batch error, therefore we fall back towards treating every
					// message as if it was errored the same.
					return t
				}

				newBatch := make(message.Batch, 0, bErr.IndexedErrors())
				bErr.WalkParts(sortGroup, t, func(i int, p *message.Part, err error) bool {
					if err == nil {
						return true
					}
					newBatch = append(newBatch, p)
					return true
				})
				if len(newBatch) == 0 {
					return t
				}
				return newBatch
			}),
		r: r,
	}
}

//------------------------------------------------------------------------------

// Connect attempts to establish a connection to the source, if
// unsuccessful returns an error. If the attempt is successful (or not
// necessary) returns nil.
func (p *AsyncPreserver) Connect(ctx context.Context) error {
	err := p.r.Connect(ctx)
	// If our source has finished but we still have messages in flight then
	// we act like we're still open. Read will be called and we can either
	// return the pending messages or wait for them.
	if errors.Is(err, component.ErrTypeClosed) && !p.retryList.Exhausted() {
		atomic.StoreInt32(&p.inputClosed, 1)
		err = nil
	}
	return err
}

// ReadBatch attempts to read a new message from the source.
func (p *AsyncPreserver) ReadBatch(ctx context.Context) (message.Batch, AsyncAckFn, error) {
	batch, rAckFn, err := p.retryList.Shift(ctx, atomic.LoadInt32(&p.inputClosed) == 0)
	if err != nil {
		if errors.Is(err, autoretry.ErrExhausted) {
			return nil, nil, component.ErrTypeClosed
		}
		if errors.Is(err, component.ErrTypeClosed) {
			// Mark our input as being closed and trigger an immediate re-read
			// in order to clear any pending retries.
			atomic.StoreInt32(&p.inputClosed, 1)
			return p.ReadBatch(ctx)
		}
		// Otherwise we have an unknown error from our reader that we should
		// escalate, this is most likely an ErrNotConnected or ErrTimeout.
		return nil, nil, err
	}
	return batch.ShallowCopy(), AsyncAckFn(rAckFn), nil
}

// Close triggers the shut down of this component and blocks until completion or
// context cancellation.
func (p *AsyncPreserver) Close(ctx context.Context) error {
	_ = p.retryList.Close(ctx)
	return p.r.Close(ctx)
}
