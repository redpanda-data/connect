// Copyright (c) 2014 Ashley Jeffs
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

package buffer

import (
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNone] = TypeSpec{
		constructor: NewEmpty,
		description: `
Selecting no buffer (default) means the output layer is directly coupled with
the input layer. This is the safest and lowest latency option since
acknowledgements from at-least-once protocols can be propagated all the way from
the output protocol to the input protocol.

If the output layer is hit with back pressure it will propagate all the way to
the input layer, and further up the data stream. If you need to relieve your
pipeline of this back pressure consider using a more robust buffering solution
such as Kafka before resorting to alternatives.`,
	}
}

//------------------------------------------------------------------------------

// Empty is an empty buffer, simply forwards messages on directly.
type Empty struct {
	running int32

	messagesOut chan types.Transaction
	messagesIn  <-chan types.Transaction

	closeChan chan struct{}
	closed    chan struct{}
}

// NewEmpty creates a new buffer interface but doesn't buffer messages.
func NewEmpty(config Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	e := &Empty{
		running:     1,
		messagesOut: make(chan types.Transaction),
		closeChan:   make(chan struct{}),
		closed:      make(chan struct{}),
	}
	return e, nil
}

//------------------------------------------------------------------------------

// loop is an internal loop of the empty buffer.
func (e *Empty) loop() {
	defer func() {
		atomic.StoreInt32(&e.running, 0)

		close(e.messagesOut)
		close(e.closed)
	}()

	var open bool
	for atomic.LoadInt32(&e.running) == 1 {
		var inT types.Transaction
		select {
		case inT, open = <-e.messagesIn:
			if !open {
				return
			}
		case <-e.closeChan:
			return
		}
		select {
		case e.messagesOut <- inT:
		case <-e.closeChan:
			return
		}
	}
}

//------------------------------------------------------------------------------

// Consume assigns a messages channel for the output to read.
func (e *Empty) Consume(msgs <-chan types.Transaction) error {
	if e.messagesIn != nil {
		return types.ErrAlreadyStarted
	}
	e.messagesIn = msgs
	go e.loop()
	return nil
}

// TransactionChan returns the channel used for consuming messages from this
// input.
func (e *Empty) TransactionChan() <-chan types.Transaction {
	return e.messagesOut
}

// ErrorsChan returns the errors channel.
func (e *Empty) ErrorsChan() <-chan []error {
	return nil
}

// StopConsuming instructs the buffer to no longer consume data.
func (e *Empty) StopConsuming() {
	e.CloseAsync()
}

// CloseAsync shuts down the StackBuffer output and stops processing messages.
func (e *Empty) CloseAsync() {
	if atomic.CompareAndSwapInt32(&e.running, 1, 0) {
		close(e.closeChan)
	}
}

// WaitForClose blocks until the StackBuffer output has closed down.
func (e *Empty) WaitForClose(timeout time.Duration) error {
	select {
	case <-e.closed:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
