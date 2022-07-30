package service

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
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
		child:           i,
		resendInterrupt: func() {},
	}
}

//------------------------------------------------------------------------------

type messageRetry struct {
	boff     backoff.BackOff
	attempts int
	msg      *Message
	ackFn    AckFunc
}

func newMessageRetry(msg *Message, ackFn AckFunc) messageRetry {
	boff := backoff.NewExponentialBackOff()
	boff.InitialInterval = time.Millisecond
	boff.MaxInterval = time.Second
	boff.Multiplier = 1.1
	boff.MaxElapsedTime = 0
	return messageRetry{boff: boff, attempts: 0, msg: msg, ackFn: ackFn}
}

type autoRetryInput struct {
	resendMessages  []messageRetry
	resendInterrupt func()
	msgsMut         sync.Mutex

	child Input
}

func (i *autoRetryInput) Connect(ctx context.Context) error {
	return i.child.Connect(ctx)
}

func (i *autoRetryInput) wrapAckFunc(m messageRetry) (*Message, AckFunc) {
	return m.msg.Copy(), func(ctx context.Context, err error) error {
		if err != nil {
			i.msgsMut.Lock()
			i.resendMessages = append(i.resendMessages, m)
			i.resendInterrupt()
			i.msgsMut.Unlock()
			return nil
		}
		return m.ackFn(ctx, nil)
	}
}

func (i *autoRetryInput) Read(ctx context.Context) (*Message, AckFunc, error) {
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	// If we have messages queued to be resent we prioritise them over reading
	// new messages.
	i.msgsMut.Lock()
	if lMsgs := len(i.resendMessages); lMsgs > 0 {
		resend := i.resendMessages[0]
		if lMsgs > 1 {
			i.resendMessages = i.resendMessages[1:]
		} else {
			i.resendMessages = nil
		}
		i.msgsMut.Unlock()

		resend.attempts++
		if resend.attempts > 2 {
			// This sleep prevents a busy loop on permanently failed messages.
			if tout := resend.boff.NextBackOff(); tout > 0 {
				select {
				case <-time.After(tout):
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
		}
		sendMsg, ackFn := i.wrapAckFunc(resend)
		return sendMsg, ackFn, nil
	}
	i.resendInterrupt = cancel
	i.msgsMut.Unlock()

	msg, aFn, err := i.child.Read(ctx)
	if err != nil {
		return nil, nil, err
	}
	sendMsg, ackFn := i.wrapAckFunc(newMessageRetry(msg, aFn))
	return sendMsg, ackFn, nil
}

func (i *autoRetryInput) Close(ctx context.Context) error {
	return i.child.Close(ctx)
}
