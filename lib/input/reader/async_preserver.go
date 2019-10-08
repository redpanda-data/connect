// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package reader

import (
	"context"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

type asyncPreserverResend struct {
	msg   types.Message
	ackFn AsyncAckFn
}

// AsyncPreserver is a wrapper for reader.Async implementations that keeps a
// buffer of sent messages until they are acknowledged. If an error occurs
// during message propagation the contents of the buffer will be resent instead
// of reading new messages until it is depleted. AsyncPreserver implements
// reader.Async.
//
// Wrapping an input with this type is useful when your source of messages
// doesn't have a concept of a NoAck (like Kafka), and instead of "rejecting"
// messages we always intend to simply retry them until success.
type AsyncPreserver struct {
	resendMessages  []asyncPreserverResend
	resendInterrupt func()
	msgsMut         sync.Mutex

	r Async
}

// NewAsyncPreserver returns a new AsyncPreserver wrapper around a reader.Async.
func NewAsyncPreserver(r Async) *AsyncPreserver {
	return &AsyncPreserver{
		r:               r,
		resendInterrupt: func() {},
	}
}

//------------------------------------------------------------------------------

// ConnectWithContext attempts to establish a connection to the source, if
// unsuccessful returns an error. If the attempt is successful (or not
// necessary) returns nil.
func (p *AsyncPreserver) ConnectWithContext(ctx context.Context) error {
	return p.r.ConnectWithContext(ctx)
}

func (p *AsyncPreserver) wrapAckFn(msg types.Message, ackFn AsyncAckFn) AsyncAckFn {
	return func(ctx context.Context, res types.Response) error {
		if res.Error() != nil {
			p.msgsMut.Lock()
			p.resendMessages = append(p.resendMessages, asyncPreserverResend{
				msg:   msg,
				ackFn: ackFn,
			})
			p.resendInterrupt()
			p.msgsMut.Unlock()
			return nil
		}
		return ackFn(ctx, res)
	}
}

// ReadWithContext attempts to read a new message from the source.
func (p *AsyncPreserver) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	// If we have messages queued to be resent we prioritise them over reading
	// new messages.
	p.msgsMut.Lock()
	if lMsgs := len(p.resendMessages); lMsgs > 0 {
		resend := p.resendMessages[0]
		if lMsgs > 1 {
			p.resendMessages = p.resendMessages[1:]
		} else {
			p.resendMessages = nil
		}
		p.msgsMut.Unlock()
		return resend.msg, p.wrapAckFn(resend.msg, resend.ackFn), nil
	}
	p.resendInterrupt = cancel
	p.msgsMut.Unlock()

	msg, aFn, err := p.r.ReadWithContext(ctx)
	if err != nil {
		return nil, nil, err
	}
	return msg, p.wrapAckFn(msg, aFn), nil
}

// CloseAsync triggers the asynchronous closing of the reader.
func (p *AsyncPreserver) CloseAsync() {
	p.r.CloseAsync()
}

// WaitForClose blocks until either the reader is finished closing or a timeout
// occurs.
func (p *AsyncPreserver) WaitForClose(tout time.Duration) error {
	return p.r.WaitForClose(tout)
}

//------------------------------------------------------------------------------
