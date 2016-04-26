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

package buffer

import (
	"time"

	"github.com/jeffail/benthos/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["none"] = NewEmpty
}

//--------------------------------------------------------------------------------------------------

// Empty - Empty buffer, simply forwards messages on directly.
type Empty struct {
	messagesOut  chan types.Message
	responsesOut chan types.Response

	messagesIn  <-chan types.Message
	responsesIn <-chan types.Response

	closed chan struct{}
}

/*
NewEmpty - Supports buffer interface but doesn't buffer messages.
*/
func NewEmpty(config Config, log log.Modular, stats metrics.Aggregator) (Type, error) {
	return &Empty{
		messagesOut:  make(chan types.Message),
		responsesOut: make(chan types.Response),
		closed:       make(chan struct{}),
	}, nil
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop of the empty buffer.
func (e *Empty) loop() {
	defer close(e.closed)
	defer close(e.messagesOut)
	defer close(e.responsesOut)

	for {
		msg, open := <-e.messagesIn
		if !open {
			return
		}
		e.messagesOut <- msg
		res := <-e.responsesIn
		e.responsesOut <- res
	}
}

//--------------------------------------------------------------------------------------------------

// StartReceiving - Assigns a messages channel for the output to read.
func (e *Empty) StartReceiving(msgs <-chan types.Message) error {
	if e.messagesIn != nil {
		return types.ErrAlreadyStarted
	}
	e.messagesIn = msgs
	if e.responsesIn != nil {
		go e.loop()
	}
	return nil
}

// MessageChan - Returns the channel used for consuming messages from this input.
func (e *Empty) MessageChan() <-chan types.Message {
	return e.messagesOut
}

// StartListening - Sets the channel for reading responses.
func (e *Empty) StartListening(responses <-chan types.Response) error {
	if e.responsesIn != nil {
		return types.ErrAlreadyStarted
	}
	e.responsesIn = responses
	if e.messagesIn != nil {
		go e.loop()
	}
	return nil
}

// ResponseChan - Returns the response channel.
func (e *Empty) ResponseChan() <-chan types.Response {
	return e.responsesOut
}

// ErrorsChan - Returns the errors channel.
func (e *Empty) ErrorsChan() <-chan []error {
	return nil
}

// CloseAsync - Shuts down the StackBuffer output and stops processing messages.
func (e *Empty) CloseAsync() {
}

// WaitForClose - Blocks until the StackBuffer output has closed down.
func (e *Empty) WaitForClose(timeout time.Duration) error {
	select {
	case <-e.closed:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
