package service

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/benthosdev/benthos/v4/internal/autoretry"
)

// AutoRetryNacksToggled wraps an input implementation with AutoRetryNacks only
// if a field defined by NewAutoRetryNacksToggleField has been set to true.
func AutoRetryNacksToggled(c *ParsedConfig, i Input) (Input, error) {
	b, err := c.FieldBool(AutoRetryNacksToggleFieldName)
	if err != nil {
		return nil, err
	}
	if b {
		return AutoRetryNacks(i), nil
	}
	return i, nil
}

// AutoRetryNacks wraps an input implementation with a component that
// automatically reattempts messages that fail downstream. This is useful for
// inputs that do not support nacks, and therefore don't have an answer for
// when an ack func is called with an error.
//
// When messages fail to be delivered they will be reattempted with back off
// until success or the stream is stopped.
func AutoRetryNacks(i Input) Input {
	return &autoRetryInput{
		retryList: autoretry.NewList(func(ctx context.Context) (*Message, autoretry.AckFunc, error) {
			t, aFn, err := i.Read(ctx)
			return t, autoretry.AckFunc(aFn), err
		}, nil),
		child: i,
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
	if errors.Is(err, ErrEndOfInput) && !i.retryList.Exhausted() {
		atomic.StoreInt32(&i.inputClosed, 1)
		err = nil
	}
	return err
}

func (i *autoRetryInput) Read(ctx context.Context) (*Message, AckFunc, error) {
	msg, rAckFn, err := i.retryList.Shift(ctx, atomic.LoadInt32(&i.inputClosed) == 0)
	if err != nil {
		if errors.Is(err, autoretry.ErrExhausted) {
			return nil, nil, ErrEndOfInput
		}
		if errors.Is(err, ErrEndOfInput) {
			// Mark our input as being closed and trigger an immediate re-read
			// in order to clear any pending retries.
			atomic.StoreInt32(&i.inputClosed, 1)
			return i.Read(ctx)
		}
		// Otherwise we have an unknown error from our reader that we should
		// escalate, this is most likely an ErrNotConnected or ErrTimeout.
		return nil, nil, err
	}
	return msg.Copy(), AckFunc(rAckFn), nil
}

func (i *autoRetryInput) Close(ctx context.Context) error {
	_ = i.retryList.Close(ctx)
	return i.child.Close(ctx)
}
