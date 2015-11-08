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
	buffer []types.Message
	limit  int
	used   int

	messagesIn   <-chan types.Message
	messagesOut  chan types.Message
	responsesIn  <-chan types.Response
	responsesOut chan types.Response
	errorsChan   chan []error

	newMessagesChan  chan (<-chan types.Message)
	newResponsesChan chan (<-chan types.Response)

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewMemory - Create a new buffered agent type.
func NewMemory(limit int) *Memory {
	m := Memory{
		buffer:           []types.Message{},
		limit:            limit,
		used:             0,
		messagesOut:      make(chan types.Message),
		responsesOut:     make(chan types.Response),
		errorsChan:       make(chan []error),
		newMessagesChan:  make(chan (<-chan types.Message)),
		newResponsesChan: make(chan (<-chan types.Response)),
		closedChan:       make(chan struct{}),
		closeChan:        make(chan struct{}),
	}

	go m.loop()

	return &m
}

//--------------------------------------------------------------------------------------------------

func (m *Memory) shiftMessage() (types.Message, error) {
	if len(m.buffer) == 0 {
		return types.Message{}, ErrOutOfBounds
	}

	msg := m.buffer[0]

	size := 0
	for i := range msg.Parts {
		size += cap(msg.Parts[i])
	}

	m.used = m.used - size
	m.buffer[0].Parts = nil
	m.buffer = m.buffer[1:]

	return msg, nil
}

func (m *Memory) pushMessage(msg types.Message) bool {
	size := 0
	for i := range msg.Parts {
		size += cap(msg.Parts[i])
	}

	m.used = m.used + size
	m.buffer = append(m.buffer, msg)

	return m.limitReached()
}

func (m *Memory) limitReached() bool {
	return m.used > m.limit
}

// loop - Internal loop brokers incoming messages to output pipe.
func (m *Memory) loop() {
	running := true

	var inMsgChan <-chan types.Message
	var outMsgChan chan types.Message
	var outResChan chan types.Response
	var nextMsg types.Message

	var errChan chan []error
	errors := []error{}

	responseInPending, responseOutPending := false, false

	for running {
		// If we are waiting for our output to respond, or do not have buffered messages then set
		// the output chan to nil.
		if !responseInPending && len(m.buffer) > 0 {
			outMsgChan = m.messagesOut
			nextMsg = m.buffer[0]
		} else {
			outMsgChan = nil
			nextMsg = types.Message{Parts: nil}
		}

		if !m.limitReached() {
			if responseOutPending {
				outResChan = m.responsesOut
				inMsgChan = nil
			} else {
				inMsgChan = m.messagesIn
				outResChan = nil
			}
		} else {
			inMsgChan = nil
			outResChan = nil
		}

		// If we do not have errors to propagate then set the error chan to nil
		if len(errors) == 0 {
			errChan = nil
		} else {
			errChan = m.errorsChan
		}

		select {
		// OUTPUT CHANNELS
		case msg, open := <-inMsgChan:
			// If the messages chan is closed we do not close ourselves as it can replaced.
			if !open {
				m.messagesIn = nil
			} else {
				m.pushMessage(msg)
				responseOutPending = true
			}
		case outResChan <- types.NewSimpleResponse(nil):
			responseOutPending = false

		// INPUT CHANNELS
		case outMsgChan <- nextMsg:
			responseInPending = true
		case res, open := <-m.responsesIn:
			// If the responses chan is closed we do not close ourselves as it can replaced.
			if !open {
				m.responsesIn = nil
			} else if res.Error() != nil {
				errors = append(errors, res.Error())
			} else if _, err := m.shiftMessage(); err != nil {
				errors = append(errors, err)
			}
			responseInPending = false

		// OTHER CHANNELS
		case errChan <- errors:
			errors = []error{}
		case newChan, open := <-m.newMessagesChan:
			if running = open; running {
				m.messagesIn = newChan
			}
		case newChan, open := <-m.newResponsesChan:
			if running = open; running {
				m.responsesIn = newChan
			}
		case _, running = <-m.closeChan:
		}
	}

	close(m.messagesOut)
	close(m.newMessagesChan)
	close(m.newResponsesChan)
	close(m.errorsChan)
	close(m.responsesOut)
	close(m.closedChan)
}

// SetMessageChan - Assigns a new messages channel for the output to read.
func (m *Memory) SetMessageChan(msgs <-chan types.Message) {
	m.newMessagesChan <- msgs
}

// MessageChan - Returns the channel used for consuming messages from this input.
func (m *Memory) MessageChan() <-chan types.Message {
	return m.messagesOut
}

// SetResponseChan - Sets the channel for reading responses.
func (m *Memory) SetResponseChan(responses <-chan types.Response) {
	m.newResponsesChan <- responses
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
	close(m.closeChan)
}

// WaitForClose - Blocks until the Memory output has closed down.
func (m *Memory) WaitForClose(timeout time.Duration) error {
	select {
	case <-m.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
