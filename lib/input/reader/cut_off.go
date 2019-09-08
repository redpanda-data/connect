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
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// CutOff is a wrapper for reader.Type implementations that exits from
// WaitForClose immediately. This is only useful when the underlying readable
// resource cannot be closed reliably and can block forever.
type CutOff struct {
	msgChan   chan types.Message
	errChan   chan error
	closeChan chan struct{}

	r Type
}

// NewCutOff returns a new CutOff wrapper around a reader.Type.
func NewCutOff(r Type) *CutOff {
	return &CutOff{
		msgChan:   make(chan types.Message),
		errChan:   make(chan error),
		closeChan: make(chan struct{}),
		r:         r,
	}
}

//------------------------------------------------------------------------------

// Connect attempts to establish a connection to the source, if unsuccessful
// returns an error. If the attempt is successful (or not necessary) returns
// nil.
func (c *CutOff) Connect() error {
	return c.r.Connect()
}

// Acknowledge instructs whether messages read since the last Acknowledge call
// were successfully propagated. If the error is nil this will be forwarded to
// the underlying wrapped reader. If a non-nil error is returned the buffer of
// messages will be resent.
func (c *CutOff) Acknowledge(err error) error {
	return c.r.Acknowledge(err)
}

// Read attempts to read a new message from the source.
func (c *CutOff) Read() (types.Message, error) {
	go func() {
		msg, err := c.r.Read()
		if err == nil {
			c.msgChan <- msg
		} else {
			c.errChan <- err
		}
	}()
	select {
	case m := <-c.msgChan:
		return m, nil
	case e := <-c.errChan:
		return nil, e
	case <-c.closeChan:
	}
	return nil, types.ErrTypeClosed
}

// CloseAsync triggers the asynchronous closing of the reader.
func (c *CutOff) CloseAsync() {
	c.r.CloseAsync()
	close(c.closeChan)
}

// WaitForClose blocks until either the reader is finished closing or a timeout
// occurs.
func (c *CutOff) WaitForClose(tout time.Duration) error {
	return nil // We don't block here.
}

//------------------------------------------------------------------------------
