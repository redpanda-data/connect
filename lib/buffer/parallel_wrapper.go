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

	"github.com/Jeffail/benthos/v3/lib/buffer/parallel"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
)

//------------------------------------------------------------------------------

// Parallel represents a method of buffering messages such that they can be
// consumed by any number of parallel consumers, and can be acknowledged in any
// order.
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

type parallelBatchedWrite interface {
	Parallel

	// PushMessages adds new messages to the stack. Returns the backlog in
	// bytes.
	PushMessages([]types.Message) (int, error)
}

//------------------------------------------------------------------------------

// ParallelWrapper wraps a buffer with a Producer/Consumer interface.
type ParallelWrapper struct {
	stats metrics.Type
	log   log.Modular
	conf  Config

	buffer      Parallel
	errThrottle *throttle.Type

	running   int32
	consuming int32

	messagesIn  <-chan types.Transaction
	messagesOut chan types.Transaction

	closedWG sync.WaitGroup

	stopConsumingChan chan struct{}
	closeChan         chan struct{}
	closedChan        chan struct{}
}

// NewParallelWrapper creates a new Producer/Consumer around a buffer.
func NewParallelWrapper(
	conf Config,
	buffer Parallel,
	log log.Modular,
	stats metrics.Type,
) Type {
	m := ParallelWrapper{
		stats:             stats,
		log:               log,
		conf:              conf,
		buffer:            buffer,
		running:           1,
		consuming:         1,
		messagesOut:       make(chan types.Transaction),
		stopConsumingChan: make(chan struct{}),
		closeChan:         make(chan struct{}),
		closedChan:        make(chan struct{}),
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

func (m *ParallelWrapper) batchedInputLoop() {
	batchWriteBuffer, ok := m.buffer.(parallelBatchedWrite)
	if !ok {
		m.inputLoop()
		return
	}

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
		var transactions []types.Transaction
		var messages []types.Message
		select {
		case tr, open := <-m.messagesIn:
			if !open {
				return
			}
			messages = append(messages, tracing.WithSiblingSpans("buffer_"+m.conf.Type, tr.Payload))
			transactions = append(transactions, tr)
		case <-m.stopConsumingChan:
			return
		}
		// Magic number, assuming a cap of 50 for now.
		for i := 0; i < 50; i++ {
			select {
			case tr, open := <-m.messagesIn:
				if !open {
					return
				}
				messages = append(messages, tracing.WithSiblingSpans("buffer_"+m.conf.Type, tr.Payload))
				transactions = append(transactions, tr)
			default:
				break
			}
		}
		backlog, err := batchWriteBuffer.PushMessages(messages)
		if err == nil {
			mWriteCount.Incr(int64(len(transactions)))
			mWriteBacklog.Set(int64(backlog))
		} else {
			mWriteErr.Incr(1)
		}
		for _, tr := range transactions {
			select {
			case tr.ResponseChan <- response.NewError(err):
			case <-m.stopConsumingChan:
				return
			}
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

	var (
		mReadCount   = m.stats.GetCounter("read.count")
		mReadErr     = m.stats.GetCounter("read.error")
		mSendSuccess = m.stats.GetCounter("send.success")
		mSendErr     = m.stats.GetCounter("send.error")
		mAckErr      = m.stats.GetCounter("ack.error")
		mLatency     = m.stats.GetTimer("latency")
		mBacklog     = m.stats.GetGauge("backlog")
	)

	for atomic.LoadInt32(&m.running) == 1 {
		msg, ackFunc, err := m.buffer.NextMessage()
		if err != nil {
			if err != types.ErrTypeClosed {
				mReadErr.Incr(1)
				m.log.Errorf("Failed to read buffer: %v\n", err)
				m.errThrottle.Retry()
			} else {
				// If our buffer is closed then we exit.
				return
			}
			continue
		}

		// It's possible that the buffer wiped our previous root span.
		tracing.InitSpans("buffer_"+m.conf.Type, msg)

		mReadCount.Incr(1)
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
				mSendSuccess.Incr(1)
				mLatency.Timing(time.Since(msg.CreatedAt()).Nanoseconds())
				tracing.FinishSpans(msg)
				doAck = true
			} else {
				mSendErr.Incr(1)
			}
			blog, ackErr := aFunc(doAck)
			if ackErr != nil {
				mAckErr.Incr(1)
				if ackErr != types.ErrTypeClosed {
					m.log.Errorf("Failed to ack buffer message: %v\n", ackErr)
				}
			} else {
				mBacklog.Set(int64(blog))
			}
		}(resChan, ackFunc)
	}
}

// Consume assigns a messages channel for the output to read.
func (m *ParallelWrapper) Consume(msgs <-chan types.Transaction) error {
	if m.messagesIn != nil {
		return types.ErrAlreadyStarted
	}
	m.messagesIn = msgs

	m.closedWG.Add(2)
	go m.batchedInputLoop()
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
	m.StopConsuming()
	if atomic.CompareAndSwapInt32(&m.running, 1, 0) {
		close(m.closeChan)
	}
}

// StopConsuming instructs the buffer to stop consuming messages and close once
// the buffer is empty.
func (m *ParallelWrapper) StopConsuming() {
	if atomic.CompareAndSwapInt32(&m.consuming, 1, 0) {
		close(m.stopConsumingChan)
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
