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

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
)

//------------------------------------------------------------------------------

// Single represents a method of buffering sequential messages, supporting
// only a single, sequential consumer.
type Single interface {
	// ShiftMessage removes the oldest message from the stack. Returns the
	// backlog in bytes.
	ShiftMessage() (int, error)

	// NextMessage reads the oldest message, the message is preserved until
	// ShiftMessage is called.
	NextMessage() (types.Message, error)

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

// SingleWrapper wraps a buffer with a Producer/Consumer interface.
type SingleWrapper struct {
	stats metrics.Type
	log   log.Modular
	conf  Config

	buffer      Single
	errThrottle *throttle.Type

	running   int32
	consuming int32

	messagesIn   <-chan types.Transaction
	messagesOut  chan types.Transaction
	responsesOut chan types.Response

	closedWG sync.WaitGroup

	stopConsumingChan chan struct{}
	closeChan         chan struct{}
	closedChan        chan struct{}
}

// NewSingleWrapper creates a new Producer/Consumer around a buffer.
func NewSingleWrapper(
	conf Config,
	buffer Single,
	log log.Modular,
	stats metrics.Type,
) Type {
	m := SingleWrapper{
		stats:             stats,
		log:               log,
		conf:              conf,
		buffer:            buffer,
		running:           1,
		consuming:         1,
		messagesOut:       make(chan types.Transaction),
		responsesOut:      make(chan types.Response),
		stopConsumingChan: make(chan struct{}),
		closeChan:         make(chan struct{}),
		closedChan:        make(chan struct{}),
	}

	m.errThrottle = throttle.New(throttle.OptCloseChan(m.closeChan))
	return &m
}

//------------------------------------------------------------------------------

// inputLoop is an internal loop that brokers incoming messages to the buffer.
func (m *SingleWrapper) inputLoop() {
	defer func() {
		m.buffer.CloseOnceEmpty()
		m.closedWG.Done()
	}()

	var (
		mWriteCount   = m.stats.GetCounter("write.count")
		mWriteErr     = m.stats.GetCounter("write.error")
		mWriteBacklog = m.stats.GetGauge("backlog")
	)

	for atomic.LoadInt32(&m.consuming) == 1 {
		var tr types.Transaction
		var open bool
		select {
		case tr, open = <-m.messagesIn:
			if !open {
				return
			}
		case <-m.stopConsumingChan:
			return
		}
		backlog, err := m.buffer.PushMessage(tracing.WithSiblingSpans("buffer_"+m.conf.Type, tr.Payload))
		if err == nil {
			mWriteCount.Incr(1)
			mWriteBacklog.Set(int64(backlog))
		} else {
			mWriteErr.Incr(1)
		}
		select {
		case tr.ResponseChan <- response.NewError(err):
		case <-m.stopConsumingChan:
			return
		}
	}
}

// outputLoop is an internal loop brokers buffer messages to output pipe.
func (m *SingleWrapper) outputLoop() {
	defer func() {
		m.buffer.Close()
		close(m.messagesOut)
		m.closedWG.Done()
	}()

	var (
		mReadCount   = m.stats.GetCounter("read.count")
		mReadErr     = m.stats.GetCounter("read.error")
		mSendSuccess = m.stats.GetCounter("send.success")
		mSendErr     = m.stats.GetCounter("send.error")
		mLatency     = m.stats.GetTimer("latency")
		mBacklog     = m.stats.GetGauge("backlog")
	)

	var msg types.Message
	for atomic.LoadInt32(&m.running) == 1 {
		if msg == nil {
			var err error
			if msg, err = m.buffer.NextMessage(); err != nil {
				if err != types.ErrTypeClosed {
					mReadErr.Incr(1)
					m.log.Errorf("Failed to read buffer: %v\n", err)

					m.errThrottle.Retry()

					// Unconventional errors here should always indicate some
					// sort of corruption. Hopefully the corruption was message
					// specific and not the whole buffer, so we can try shifting
					// and reading again.
					m.buffer.ShiftMessage()
				} else {
					// If our buffer is closed then we exit.
					return
				}
			} else {
				mReadCount.Incr(1)
				m.errThrottle.Reset()
			}
		}

		if msg != nil {
			// It's possible that the buffer wiped our previous root span.
			tracing.InitSpans("buffer_"+m.conf.Type, msg)

			select {
			case m.messagesOut <- types.NewTransaction(msg, m.responsesOut):
			case <-m.closeChan:
				return
			}
			res, open := <-m.responsesOut
			if !open {
				return
			}
			if res.Error() == nil {
				mLatency.Timing(time.Since(msg.CreatedAt()).Nanoseconds())
				tracing.FinishSpans(msg)
				msg = nil
				backlog, _ := m.buffer.ShiftMessage()
				mBacklog.Set(int64(backlog))
				mSendSuccess.Incr(1)
			} else {
				mSendErr.Incr(1)
			}
		}
	}
}

// Consume assigns a messages channel for the output to read.
func (m *SingleWrapper) Consume(msgs <-chan types.Transaction) error {
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
func (m *SingleWrapper) TransactionChan() <-chan types.Transaction {
	return m.messagesOut
}

// CloseAsync shuts down the SingleWrapper and stops processing messages.
func (m *SingleWrapper) CloseAsync() {
	m.StopConsuming()
	if atomic.CompareAndSwapInt32(&m.running, 1, 0) {
		close(m.closeChan)
	}
}

// StopConsuming instructs the buffer to stop consuming messages and close once
// the buffer is empty.
func (m *SingleWrapper) StopConsuming() {
	if atomic.CompareAndSwapInt32(&m.consuming, 1, 0) {
		close(m.stopConsumingChan)
	}
}

// WaitForClose blocks until the SingleWrapper output has closed down.
func (m *SingleWrapper) WaitForClose(timeout time.Duration) error {
	select {
	case <-m.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
