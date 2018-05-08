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

package buffer

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/buffer/parallel"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/Jeffail/benthos/lib/util/throttle"
)

//------------------------------------------------------------------------------

// Parallel represents a method of buffering sequential messages, supporting
// only a single consumer.
type Parallel interface {
	// NextMessage reads the next oldest message, the message is preserved until
	// the returned AckFunc is called.
	NextMessage() (types.Message, parallel.AckFunc, error)

	// PushMessage adds a new message to the stack. Returns the backlog in
	// bytes.
	PushMessage(types.Message) (int, error)

	// CloseOnceEmpty closes the Buffer once the buffer has been emptied, this
	// is a way for a writer to signal to a reader that it is finished writing
	// messages, and therefore the reader can close once it is caught up. This
	// call blocks until the close is completed.
	CloseOnceEmpty()

	// Close closes the Buffer so that blocked readers or writers become
	// unblocked.
	Close()
}

//------------------------------------------------------------------------------

// ParallelWrapper wraps a buffer with a Producer/Consumer interface.
type ParallelWrapper struct {
	stats metrics.Type
	log   log.Modular

	buffer      Parallel
	errThrottle *throttle.Type

	running int32

	messagesIn  <-chan types.Transaction
	messagesOut chan types.Transaction

	closedWG sync.WaitGroup

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewParallelWrapper creates a new Producer/Consumer around a buffer.
func NewParallelWrapper(
	conf Config,
	buffer Parallel,
	log log.Modular,
	stats metrics.Type,
) Type {
	m := ParallelWrapper{
		stats:       stats,
		log:         log,
		buffer:      buffer,
		running:     1,
		messagesOut: make(chan types.Transaction),
		closeChan:   make(chan struct{}),
		closedChan:  make(chan struct{}),
	}
	m.errThrottle = throttle.New(throttle.OptCloseChan(m.closeChan))
	return &m
}

//------------------------------------------------------------------------------

// inputLoop is an internal loop that brokers incoming messages to the buffer.
func (m *ParallelWrapper) inputLoop() {
	defer func() {
		m.buffer.CloseOnceEmpty()
		m.closedWG.Done()
	}()

	for atomic.LoadInt32(&m.running) == 1 {
		var tr types.Transaction
		var open bool
		select {
		case tr, open = <-m.messagesIn:
			if !open {
				return
			}
		case <-m.closeChan:
			return
		}
		backlog, err := m.buffer.PushMessage(tr.Payload)
		if err == nil {
			m.stats.Incr("buffer.write.count", 1)
			m.stats.Gauge("buffer.backlog", int64(backlog))
		} else {
			m.stats.Incr("buffer.write.error", 1)
		}
		select {
		case tr.ResponseChan <- types.NewSimpleResponse(err):
		case <-m.closeChan:
			return
		}
	}
}

// outputLoop is an internal loop brokers buffer messages to output pipe.
func (m *ParallelWrapper) outputLoop() {
	defer func() {
		m.buffer.Close()
		close(m.messagesOut)
		m.closedWG.Done()
	}()

	for atomic.LoadInt32(&m.running) == 1 {
		msg, ackFunc, err := m.buffer.NextMessage()
		if err != nil {
			if err != types.ErrTypeClosed {
				m.stats.Incr("buffer.read.error", 1)
				m.log.Errorf("Failed to read buffer: %v\n", err)
				m.errThrottle.Retry()
			} else {
				// If our buffer is closed then we exit.
				return
			}
			continue
		}

		m.stats.Incr("buffer.read.count", 1)
		m.errThrottle.Reset()

		resChan := make(chan types.Response)
		select {
		case m.messagesOut <- types.NewTransaction(msg, resChan):
		case <-m.closeChan:
			return
		}

		go func(rChan chan types.Response, aFunc parallel.AckFunc) {
			res, open := <-rChan
			doAck := false
			if open && res.Error() == nil {
				m.stats.Incr("buffer.send.success", 1)
				m.stats.Timing("buffer.latency", time.Since(msg.CreatedAt()).Nanoseconds())
				doAck = true
			} else {
				m.stats.Incr("buffer.send.error", 1)
			}
			blog, ackErr := aFunc(doAck)
			if ackErr != nil {
				m.stats.Incr("buffer.ack.error", 1)
				if ackErr != types.ErrTypeClosed {
					m.log.Errorf("Failed to ack buffer message: %v\n", ackErr)
				}
			} else {
				m.stats.Gauge("buffer.backlog", int64(blog))
			}
		}(resChan, ackFunc)
	}
}

// StartReceiving assigns a messages channel for the output to read.
func (m *ParallelWrapper) StartReceiving(msgs <-chan types.Transaction) error {
	if m.messagesIn != nil {
		return types.ErrAlreadyStarted
	}
	m.messagesIn = msgs

	m.closedWG.Add(2)
	go m.inputLoop()
	go m.outputLoop()
	go func() {
		m.closedWG.Wait()
		close(m.closedChan)
	}()
	return nil
}

// TransactionChan returns the channel used for consuming messages from this
// buffer.
func (m *ParallelWrapper) TransactionChan() <-chan types.Transaction {
	return m.messagesOut
}

// CloseAsync shuts down the ParallelWrapper and stops processing messages.
func (m *ParallelWrapper) CloseAsync() {
	if atomic.CompareAndSwapInt32(&m.running, 1, 0) {
		close(m.closeChan)
	}
}

// WaitForClose blocks until the ParallelWrapper output has closed down.
func (m *ParallelWrapper) WaitForClose(timeout time.Duration) error {
	select {
	case <-m.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
