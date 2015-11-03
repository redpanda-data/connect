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
	responseChan chan types.Response
	errorsChan   chan []error

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
		responseChan: make(chan types.Response),
		errorsChan:   make(chan []error),
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

	size := 0
	for i := range msg.Parts {
		size += cap(msg.Parts[i])
	}

	b.used = b.used - size
	b.buffer[0].Parts = nil
	b.buffer = b.buffer[1:]

	return msg, nil
}

func (b *Buffered) pushMessage(msg types.Message) error {
	size := 0
	for i := range msg.Parts {
		size += cap(msg.Parts[i])
	}

	if b.used+size > b.limit {
		return ErrBufferReachedLimit
	}
	b.used = b.used + size
	b.buffer = append(b.buffer, msg)
	return nil
}

// loop - Internal loop brokers incoming messages to output pipe.
func (b *Buffered) loop() {
	running := true

	var outChan chan types.Message
	var lastMsg types.Message

	var errChan chan []error
	errors := []error{}

	for running {
		// If we do not have buffered messages then set the output chan to nil
		if len(b.buffer) == 0 {
			outChan = nil
			lastMsg = types.Message{Parts: nil}
		} else {
			outChan = b.outMessages
			lastMsg = b.buffer[0]
		}

		// If we do not have errors to propagate then set the error chan to nil
		if len(errors) == 0 {
			errChan = nil
		} else {
			errChan = b.errorsChan
		}

		select {
		case msg, open := <-b.messages:
			if running = open; !open {
				continue
			}
			err := b.pushMessage(msg)
			for err != nil && len(b.buffer) > 0 {
				outChan <- b.buffer[0]
				if _, err = b.shiftMessage(); err != nil {
					errors = append(errors, err)
				}
				res := <-b.output.ResponseChan()
				if res.Error() != nil {
					errors = append(errors, res.Error())
				}
				err = b.pushMessage(msg)
			}
			b.responseChan <- types.NewSimpleResponse(err)
		case outChan <- lastMsg:
			if _, err := b.shiftMessage(); err != nil {
				errors = append(errors, err)
			}
			res := <-b.output.ResponseChan()
			if res.Error() != nil {
				errors = append(errors, res.Error())
			}
		case errChan <- errors:
			errors = []error{}
		case _, running = <-b.closeChan:
		}
	}

	close(b.outMessages)
	b.output.CloseAsync()

	close(b.errorsChan)
	close(b.responseChan)
	close(b.closedChan)
}

// MessageChan - Returns the messages input channel.
func (b *Buffered) MessageChan() chan<- types.Message {
	return b.messages
}

// ResponseChan - Returns the response channel.
func (b *Buffered) ResponseChan() <-chan types.Response {
	return b.responseChan
}

// ErrorsChan - Returns the errors channel.
func (b *Buffered) ErrorsChan() <-chan []error {
	return b.errorsChan
}

// CloseAsync - Shuts down the Buffered output and stops processing messages.
func (b *Buffered) CloseAsync() {
	b.output.CloseAsync()
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
