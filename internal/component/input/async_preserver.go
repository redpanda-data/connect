package input

import (
	"context"
	"errors"
	"io"
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
		retryList: autoretry.NewList(func(t message.Batch, err error) message.Batch {
			var bErr *batch.Error
			if !errors.As(err, &bErr) || bErr.IndexedErrors() == 0 {
				return t
			}

			newBatch := make(message.Batch, 0, bErr.IndexedErrors())
			bErr.WalkParts(func(i int, p *message.Part, err error) bool {
				if err == nil {
					return true
				}
				newBatch = append(newBatch, p)
				return true
			})
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
	if errors.Is(err, component.ErrTypeClosed) && p.retryList.Exhausted() {
		atomic.StoreInt32(&p.inputClosed, 1)
		err = nil
	}
	return err
}

// ReadBatch attempts to read a new message from the source.
func (p *AsyncPreserver) ReadBatch(ctx context.Context) (message.Batch, AsyncAckFn, error) {
	if batch, rAckFn, exists := p.retryList.TryShift(ctx); exists {
		return batch.ShallowCopy(), AsyncAckFn(rAckFn), nil
	}

	var (
		batch message.Batch
		aFn   AsyncAckFn
		err   error
	)

	if atomic.LoadInt32(&p.inputClosed) > 0 {
		err = component.ErrTypeClosed
	} else {
		batch, aFn, err = p.r.ReadBatch(ctx)
	}
	if err != nil {
		// If our source has finished but we still have messages in flight then
		// we block, ideally until the messages are acked.
		if errors.Is(err, component.ErrTypeClosed) {
			batch, rAckFn, err := p.retryList.Shift(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = component.ErrTypeClosed
				}
				return nil, nil, err
			}
			return batch.ShallowCopy(), AsyncAckFn(rAckFn), nil
		}
		return nil, nil, err
	}

	rAckFn := p.retryList.Adopt(ctx, batch, autoretry.AckFunc(aFn))
	return batch.ShallowCopy(), AsyncAckFn(rAckFn), nil
}

// Close triggers the shut down of this component and blocks until completion or
// context cancellation.
func (p *AsyncPreserver) Close(ctx context.Context) error {
	return p.r.Close(ctx)
}
