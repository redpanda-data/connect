/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package agent

import (
	"errors"
	"time"

	"github.com/jeffail/benthos/output"
	"github.com/jeffail/benthos/types"
)

//--------------------------------------------------------------------------------------------------

// Errors for the buffered agent type.
var (
	ErrOutOfBounds        = errors.New("index out of bounds")
	ErrBufferReachedLimit = errors.New("buffer reached its limit")
)

//--------------------------------------------------------------------------------------------------

// Buffered - An agent that wraps an output with a message buffer.
type Buffered struct {
	output output.Type

	buffer []types.Message
	limit  int
	used   int

	messages     chan types.Message
	responseChan chan output.Response

	outMessages chan types.Message

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewBuffered - Create a new buffered agent type.
func NewBuffered(out output.Type, limit int) *Buffered {
	b := Buffered{
		output:       out,
		buffer:       []types.Message{},
		limit:        limit,
		used:         0,
		messages:     make(chan types.Message),
		outMessages:  make(chan types.Message),
		responseChan: make(chan output.Response),
		closedChan:   make(chan struct{}),
		closeChan:    make(chan struct{}),
	}

	b.output.SetReadChan(b.outMessages)

	go b.loop()

	return &b
}

//--------------------------------------------------------------------------------------------------

func (b *Buffered) shiftMessage() (types.Message, error) {
	if len(b.buffer) == 0 {
		return types.Message{}, ErrOutOfBounds
	}

	msg := b.buffer[0]
	b.used = b.used - cap(msg.Content)
	b.buffer[0].Content = nil
	b.buffer = b.buffer[1:]

	return msg, nil
}

func (b *Buffered) unshiftMessage(msg types.Message) error {
	if b.used+cap(msg.Content) > b.limit {
		return ErrBufferReachedLimit
	}
	b.used = b.used + cap(msg.Content)
	b.buffer = append(b.buffer, msg)
	return nil
}

// loop - Internal loop brokers incoming messages to output pipe.
func (b *Buffered) loop() {
	running := true
	outChan := b.outMessages
	for running {
		if len(b.buffer) == 0 {
			outChan = nil
		} else {
			outChan = b.outMessages
		}
		select {
		case msg := <-b.messages:
			err := b.unshiftMessage(msg)
			for err != nil && len(b.buffer) > 0 {
				outChan <- b.buffer[0]
				b.shiftMessage()

				err = b.unshiftMessage(msg)
			}
			b.responseChan <- err
		case outChan <- b.buffer[0]:
			if _, err := b.shiftMessage(); err != nil {
				panic(err) // TODO
			}
		case _, running = <-b.closeChan:
			running = false
		}
	}

	close(b.outMessages)
	b.output.CloseAsync()

	close(b.responseChan)
	close(b.closedChan)
}

// MessageChan - Returns the messages input channel.
func (b *Buffered) MessageChan() chan<- types.Message {
	return b.messages
}

// ResponseChan - Returns the errors channel.
func (b *Buffered) ResponseChan() <-chan output.Response {
	return b.responseChan
}

// CloseAsync - Shuts down the Buffered output and stops processing messages.
func (b *Buffered) CloseAsync() {
	close(b.closeChan)
	close(b.messages)
}

// WaitForClose - Blocks until the Buffered output has closed down.
func (b *Buffered) WaitForClose(timeout time.Duration) error {
	if err := b.output.WaitForClose(timeout); err != nil {
		return err
	}
	select {
	case <-b.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
