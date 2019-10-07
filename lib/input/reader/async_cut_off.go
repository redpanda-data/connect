// Copyright (c) 2019 Ashley Jeffs
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
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

type asyncCutOffMsg struct {
	msg   types.Message
	ackFn AsyncAckFn
}

// AsyncCutOff is a wrapper for reader.Async implementations that exits from
// WaitForClose immediately. This is only useful when the underlying readable
// resource cannot be closed reliably and can block forever.
type AsyncCutOff struct {
	msgChan   chan asyncCutOffMsg
	errChan   chan error
	closeChan chan struct{}

	r Async
}

// NewAsyncCutOff returns a new AsyncCutOff wrapper around a reader.Async.
func NewAsyncCutOff(r Async) *AsyncCutOff {
	return &AsyncCutOff{
		msgChan:   make(chan asyncCutOffMsg),
		errChan:   make(chan error),
		closeChan: make(chan struct{}),
		r:         r,
	}
}

//------------------------------------------------------------------------------

// ConnectWithContext attempts to establish a connection to the source, if
// unsuccessful returns an error. If the attempt is successful (or not
// necessary) returns nil.
func (c *AsyncCutOff) ConnectWithContext(ctx context.Context) error {
	return c.r.ConnectWithContext(ctx)
}

// ReadWithContext attempts to read a new message from the source.
func (c *AsyncCutOff) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	go func() {
		msg, ackFn, err := c.r.ReadWithContext(ctx)
		if err == nil {
			c.msgChan <- asyncCutOffMsg{
				msg:   msg,
				ackFn: ackFn,
			}
		} else {
			c.errChan <- err
		}
	}()
	select {
	case m := <-c.msgChan:
		return m.msg, m.ackFn, nil
	case e := <-c.errChan:
		return nil, nil, e
	case <-ctx.Done():
		return nil, nil, types.ErrTimeout
	case <-c.closeChan:
	}
	return nil, nil, types.ErrTypeClosed
}

// CloseAsync triggers the asynchronous closing of the reader.
func (c *AsyncCutOff) CloseAsync() {
	c.r.CloseAsync()
	close(c.closeChan)
}

// WaitForClose blocks until either the reader is finished closing or a timeout
// occurs.
func (c *AsyncCutOff) WaitForClose(tout time.Duration) error {
	return nil // We don't block here.
}

//------------------------------------------------------------------------------
