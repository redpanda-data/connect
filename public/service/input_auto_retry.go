package service

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	"github.com/benthosdev/benthos/v4/internal/autoretry"
)

// AutoRetryNacks wraps an input implementation with a component that
// automatically reattempts messages that fail downstream. This is useful for
// inputs that do not support nacks, and therefore don't have an answer for
// when an ack func is called with an error.
//
// When messages fail to be delivered they will be reattempted with back off
// until success or the stream is stopped.
func AutoRetryNacks(i Input) Input {
	return &autoRetryInput{
		retryList: autoretry.NewList[*Message](nil),
		child:     i,
	}
}

//------------------------------------------------------------------------------

type autoRetryInput struct {
	retryList   *autoretry.List[*Message]
	child       Input
	inputClosed int32
}

func (i *autoRetryInput) Connect(ctx context.Context) error {
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

func (i *autoRetryInput) Read(ctx context.Context) (*Message, AckFunc, error) {
	if msg, rAckFn, exists := i.retryList.TryShift(ctx); exists {
		return msg.Copy(), AckFunc(rAckFn), nil
	}

	var (
		msg *Message
		aFn AckFunc
		err error
	)

	if atomic.LoadInt32(&i.inputClosed) > 0 {
		err = ErrEndOfInput
	} else {
		msg, aFn, err = i.child.Read(ctx)
	}
	if err != nil {
		// If our source has finished but we still have messages in flight then
		// we block, ideally until the messages are acked.
		if errors.Is(err, ErrEndOfInput) {
			msg, rAckFn, err := i.retryList.Shift(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = ErrEndOfInput
				}
				return nil, nil, err
			}
			return msg.Copy(), AckFunc(rAckFn), nil
		}
		return nil, nil, err
	}

	rAckFn := i.retryList.Adopt(ctx, msg, autoretry.AckFunc(aFn))
	return msg.Copy(), AckFunc(rAckFn), nil
}

func (i *autoRetryInput) Close(ctx context.Context) error {
	return i.child.Close(ctx)
}
