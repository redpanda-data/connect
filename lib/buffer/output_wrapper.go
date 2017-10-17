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
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/lib/buffer/impl"
	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/metrics"
)

//------------------------------------------------------------------------------

// OutputWrapper wraps a buffer with a Producer/Consumer interface.
type OutputWrapper struct {
	stats metrics.Type

	buffer impl.Buffer

	running int32

	messagesIn   <-chan types.Message
	messagesOut  chan types.Message
	responsesIn  <-chan types.Response
	responsesOut chan types.Response
	errorsChan   chan []error

	closedWG sync.WaitGroup

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewOutputWrapper creates a new Producer/Consumer around a buffer.
func NewOutputWrapper(buffer impl.Buffer, stats metrics.Type) Type {
	m := OutputWrapper{
		stats:        stats,
		buffer:       buffer,
		running:      1,
		messagesOut:  make(chan types.Message),
		responsesOut: make(chan types.Response),
		errorsChan:   make(chan []error),
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),
	}

	return &m
}

//------------------------------------------------------------------------------

// inputLoop is an internal loop that brokers incoming messages to the buffer.
func (m *OutputWrapper) inputLoop() {
	defer func() {
		close(m.responsesOut)
		m.buffer.CloseOnceEmpty()
		m.closedWG.Done()
	}()

	for atomic.LoadInt32(&m.running) == 1 {
		var msg types.Message
		var open bool
		select {
		case msg, open = <-m.messagesIn:
			if !open {
				return
			}
		case <-m.closeChan:
			return
		}
		backlog, err := m.buffer.PushMessage(msg)
		if err == nil {
			m.stats.Incr("buffer.write.count", 1)
			m.stats.Gauge("buffer.backlog", int64(backlog))
		} else {
			m.stats.Incr("buffer.write.error", 1)
		}
		select {
		case m.responsesOut <- types.NewSimpleResponse(err):
		case <-m.closeChan:
			return
		}
	}
}

// outputLoop is an internal loop brokers buffer messages to output pipe.
func (m *OutputWrapper) outputLoop() {
	defer func() {
		m.buffer.Close()
		close(m.messagesOut)
		close(m.errorsChan)
		m.closedWG.Done()
	}()

	errs := []error{}
	errMap := map[error]struct{}{}

	var msg types.Message
	for atomic.LoadInt32(&m.running) == 1 {
		if msg.Parts == nil {
			var err error
			if msg, err = m.buffer.NextMessage(); err != nil {
				if err != types.ErrTypeClosed {
					m.stats.Incr("buffer.read.error", 1)

					// Unconventional errors here should always indicate some
					// sort of corruption. Hopefully the corruption was message
					// specific and not the whole buffer, so we can try shifting
					// and reading again.
					m.buffer.ShiftMessage()
					if _, exists := errMap[err]; !exists {
						errMap[err] = struct{}{}
						errs = append(errs, err)
					}
				} else {
					// If our buffer is closed then we exit.
					return
				}
			} else {
				m.stats.Incr("buffer.read.count", 1)
			}
		}

		if msg.Parts != nil {
			select {
			case m.messagesOut <- msg:
			case <-m.closeChan:
				return
			}
			res, open := <-m.responsesIn
			if !open {
				return
			}
			if res.Error() == nil {
				msg = types.Message{}
				backlog, _ := m.buffer.ShiftMessage()
				m.stats.Incr("buffer.send.success", 1)
				m.stats.Gauge("buffer.backlog", int64(backlog))
			} else {
				m.stats.Incr("buffer.send.error", 1)
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

// StartReceiving assigns a messages channel for the output to read.
func (m *OutputWrapper) StartReceiving(msgs <-chan types.Message) error {
	if m.messagesIn != nil {
		return types.ErrAlreadyStarted
	}
	m.messagesIn = msgs

	if m.responsesIn != nil {
		m.closedWG.Add(2)
		go m.inputLoop()
		go m.outputLoop()
		go func() {
			m.closedWG.Wait()
			close(m.closedChan)
		}()
	}
	return nil
}

// MessageChan returns the channel used for consuming messages from this input.
func (m *OutputWrapper) MessageChan() <-chan types.Message {
	return m.messagesOut
}

// StartListening sets the channel for reading responses.
func (m *OutputWrapper) StartListening(responses <-chan types.Response) error {
	if m.responsesIn != nil {
		return types.ErrAlreadyStarted
	}
	m.responsesIn = responses

	if m.messagesIn != nil {
		m.closedWG.Add(2)
		go m.inputLoop()
		go m.outputLoop()
		go func() {
			m.closedWG.Wait()
			close(m.closedChan)
		}()
	}
	return nil
}

// ResponseChan returns the response channel.
func (m *OutputWrapper) ResponseChan() <-chan types.Response {
	return m.responsesOut
}

// ErrorsChan returns the errors channel.
func (m *OutputWrapper) ErrorsChan() <-chan []error {
	return m.errorsChan
}

// CloseAsync shuts down the OutputWrapper and stops processing messages.
func (m *OutputWrapper) CloseAsync() {
	if atomic.CompareAndSwapInt32(&m.running, 1, 0) {
		close(m.closeChan)
	}
}

// WaitForClose blocks until the OutputWrapper output has closed down.
func (m *OutputWrapper) WaitForClose(timeout time.Duration) error {
	select {
	case <-m.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
