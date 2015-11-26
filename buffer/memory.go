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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/types"
)

//--------------------------------------------------------------------------------------------------

// Errors for the buffered agent type.
var (
	ErrOutOfBounds        = errors.New("index out of bounds")
	ErrBufferReachedLimit = errors.New("buffer reached its limit")
)

//--------------------------------------------------------------------------------------------------

// Memory - An agent that wraps an output with a message buffer.
type Memory struct {
	buffer *types.MessageBlock

	running int32

	messagesIn   <-chan types.Message
	messagesOut  chan types.Message
	responsesIn  <-chan types.Response
	responsesOut chan types.Response
	errorsChan   chan []error

	closedWG sync.WaitGroup
}

// NewMemory - Create a new buffered agent type.
func NewMemory(limit int) *Memory {
	m := Memory{
		buffer:       types.NewMessageBlock(limit),
		running:      1,
		messagesOut:  make(chan types.Message),
		responsesOut: make(chan types.Response),
		errorsChan:   make(chan []error),
	}

	return &m
}

//--------------------------------------------------------------------------------------------------

// inputLoop - Internal loop brokers incoming messages to output pipe.
func (m *Memory) inputLoop() {
	defer close(m.responsesOut)
	defer m.closedWG.Done()

	for atomic.LoadInt32(&m.running) == 1 {
		msg, open := <-m.messagesIn
		if !open {
			return
		}
		m.buffer.PushMessage(msg)
		m.responsesOut <- types.NewSimpleResponse(nil)
	}
}

// outputLoop - Internal loop brokers incoming messages to output pipe.
func (m *Memory) outputLoop() {
	defer close(m.errorsChan)
	defer close(m.messagesOut)
	defer m.closedWG.Done()

	var errMap map[error]struct{}
	var errs []error

	var msg types.Message
	for atomic.LoadInt32(&m.running) == 1 {
		if msg.Parts == nil {
			var err error
			msg, err = m.buffer.NextMessage()
			if err != nil && err != types.ErrTypeClosed {
				if _, exists := errMap[err]; !exists {
					errMap[err] = struct{}{}
					errs = append(errs, err)
				}
			}
		}

		if msg.Parts != nil {
			m.messagesOut <- msg
			res, open := <-m.responsesIn
			if !open {
				return
			}
			if res.Error() == nil {
				msg = types.Message{}
				m.buffer.ShiftMessage()
			} else {
				if _, exists := errMap[res.Error()]; !exists {
					errMap[res.Error()] = struct{}{}
					errs = append(errs, res.Error())
				}
			}
		}

		// If we have errors built up.
		if len(errs) > 0 {
			select {
			case m.errorsChan <- errs:
				errMap = map[error]struct{}{}
				errs = []error{}
			default:
				// Reader not ready, do not block here.
			}
		}
	}
}

// StartReceiving - Assigns a messages channel for the output to read.
func (m *Memory) StartReceiving(msgs <-chan types.Message) error {
	if m.messagesIn != nil {
		return types.ErrAlreadyStarted
	}
	m.messagesIn = msgs

	if m.responsesIn != nil {
		m.closedWG.Add(2)
		go m.inputLoop()
		go m.outputLoop()
	}
	return nil
}

// MessageChan - Returns the channel used for consuming messages from this input.
func (m *Memory) MessageChan() <-chan types.Message {
	return m.messagesOut
}

// StartListening - Sets the channel for reading responses.
func (m *Memory) StartListening(responses <-chan types.Response) error {
	if m.responsesIn != nil {
		return types.ErrAlreadyStarted
	}
	m.responsesIn = responses

	if m.messagesIn != nil {
		m.closedWG.Add(2)
		go m.inputLoop()
		go m.outputLoop()
	}
	return nil
}

// ResponseChan - Returns the response channel.
func (m *Memory) ResponseChan() <-chan types.Response {
	return m.responsesOut
}

// ErrorsChan - Returns the errors channel.
func (m *Memory) ErrorsChan() <-chan []error {
	return m.errorsChan
}

// CloseAsync - Shuts down the Memory output and stops processing messages.
func (m *Memory) CloseAsync() {
	atomic.StoreInt32(&m.running, 0)
}

// WaitForClose - Blocks until the Memory output has closed down.
func (m *Memory) WaitForClose(timeout time.Duration) error {
	closed := make(chan struct{})
	go func() {
		m.closedWG.Wait()
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
