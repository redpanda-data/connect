package buffer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
)

// AckFunc is a function used to acknowledge receipt of a message batch from a
// buffer. The provided error indicates whether the message batch was
// successfully delivered. Returns an error if the acknowledge was not
// propagated.
type AckFunc func(context.Context, error) error

// ReaderWriter is a read/write interface implemented by buffers.
type ReaderWriter interface {
	// Read the next oldest message batch. If the buffer has a persisted store
	// the message is preserved until the returned AckFunc is called. Some
	// temporal buffer implementations such as windowers will ignore the ack
	// func.
	Read(context.Context) (*message.Batch, AckFunc, error)

	// Write a new message batch to the stack.
	Write(context.Context, *message.Batch, AckFunc) error

	// EndOfInput indicates to the buffer that the input has ended and that once
	// the buffer is depleted it should return component.ErrTypeClosed from Read in
	// order to gracefully shut down the pipeline.
	//
	// EndOfInput should be idempotent as it may be called more than once.
	EndOfInput()

	// Close the buffer and all resources it has, messages should no longer be
	// written or read by the implementation and it should clean up all
	// resources.
	Close(context.Context) error
}

// Stream wraps a read/write buffer implementation with a channel based
// streaming component that satisfies the internal Benthos Consumer and Producer
// interfaces.
type Stream struct {
	stats   metrics.Type
	log     log.Modular
	typeStr string

	buffer ReaderWriter

	errThrottle *throttle.Type
	shutSig     *shutdown.Signaller

	messagesIn  <-chan message.Transaction
	messagesOut chan message.Transaction

	closedWG sync.WaitGroup
}

// NewStream creates a new Producer/Consumer around a buffer.
func NewStream(typeStr string, buffer ReaderWriter, log log.Modular, stats metrics.Type) Streamed {
	m := Stream{
		typeStr:     typeStr,
		stats:       stats,
		log:         log,
		buffer:      buffer,
		shutSig:     shutdown.NewSignaller(),
		messagesOut: make(chan message.Transaction),
	}
	m.errThrottle = throttle.New(throttle.OptCloseChan(m.shutSig.CloseAtLeisureChan()))
	return &m
}

//------------------------------------------------------------------------------

// inputLoop is an internal loop that brokers incoming messages to the buffer.
func (m *Stream) inputLoop() {
	var ackGroup sync.WaitGroup

	defer func() {
		m.buffer.EndOfInput()
		ackGroup.Wait()
		m.closedWG.Done()
	}()

	var (
		mReceivedCount      = m.stats.GetCounter("buffer_received")
		mReceivedBatchCount = m.stats.GetCounter("buffer_batch_received")
	)

	closeAtLeisureCtx, doneLeisure := m.shutSig.CloseAtLeisureCtx(context.Background())
	defer doneLeisure()

	closeNowCtx, doneNow := m.shutSig.CloseNowCtx(context.Background())
	defer doneNow()

	for {
		var tr message.Transaction
		var open bool
		select {
		case tr, open = <-m.messagesIn:
			if !open {
				return
			}
		case <-m.shutSig.CloseAtLeisureChan():
			return
		}

		ackGroup.Add(1)
		var ackOnce sync.Once
		ackFunc := func(ctx context.Context, ackErr error) (err error) {
			ackOnce.Do(func() {
				select {
				case tr.ResponseChan <- response.NewError(ackErr):
				case <-ctx.Done():
					err = ctx.Err()
				case <-m.shutSig.CloseNowChan():
					err = component.ErrTypeClosed
				}
				ackGroup.Done()
			})
			return
		}

		batchLen := tr.Payload.Len()
		err := m.buffer.Write(closeAtLeisureCtx, tracing.WithSiblingSpans(m.typeStr, tr.Payload), ackFunc)
		if err == nil {
			mReceivedCount.Incr(int64(batchLen))
			mReceivedBatchCount.Incr(1)
		} else {
			_ = ackFunc(closeNowCtx, err)
		}
	}
}

// outputLoop is an internal loop brokers buffer messages to output pipe.
func (m *Stream) outputLoop() {
	var ackGroup sync.WaitGroup

	defer func() {
		ackGroup.Wait()
		_ = m.buffer.Close(context.Background())
		close(m.messagesOut)
		m.closedWG.Done()
	}()

	var (
		mSent      = m.stats.GetCounter("buffer_sent")
		mSentBatch = m.stats.GetCounter("buffer_batch_sent")
		mLatency   = m.stats.GetTimer("buffer_latency_ns")
	)

	closeNowCtx, done := m.shutSig.CloseNowCtx(context.Background())
	defer done()

	for {
		msg, ackFunc, err := m.buffer.Read(closeNowCtx)
		if err != nil {
			if err != component.ErrTypeClosed && !errors.Is(err, context.Canceled) {
				m.log.Errorf("Failed to read buffer: %v\n", err)
				if !m.errThrottle.Retry() {
					return
				}
			} else {
				// If our buffer is closed then we exit.
				return
			}
			continue
		}

		// It's possible that the buffer wiped our previous root span.
		tracing.InitSpans(m.typeStr, msg)

		batchLen := msg.Len()

		m.errThrottle.Reset()
		resChan := make(chan response.Error, 1)
		select {
		case m.messagesOut <- message.NewTransaction(msg, resChan):
		case <-m.shutSig.CloseNowChan():
			return
		}

		startedAt := time.Now()

		mSent.Incr(int64(batchLen))
		mSentBatch.Incr(1)
		ackGroup.Add(1)

		go func() {
			defer ackGroup.Done()
			select {
			case res, open := <-resChan:
				if !open {
					return
				}
				mLatency.Timing(time.Since(startedAt).Nanoseconds())
				tracing.FinishSpans(msg)
				if ackErr := ackFunc(closeNowCtx, res.AckError()); ackErr != nil {
					if ackErr != component.ErrTypeClosed {
						m.log.Errorf("Failed to ack buffer message: %v\n", ackErr)
					}
				}
			case <-m.shutSig.CloseNowChan():
				return
			}
		}()
	}
}

// Consume assigns a messages channel for the output to read.
func (m *Stream) Consume(msgs <-chan message.Transaction) error {
	if m.messagesIn != nil {
		return component.ErrAlreadyStarted
	}
	m.messagesIn = msgs

	m.closedWG.Add(2)
	go m.inputLoop()
	go m.outputLoop()
	go func() {
		m.closedWG.Wait()
		m.shutSig.ShutdownComplete()
	}()
	return nil
}

// TransactionChan returns the channel used for consuming messages from this
// buffer.
func (m *Stream) TransactionChan() <-chan message.Transaction {
	return m.messagesOut
}

// CloseAsync shuts down the Stream and stops processing messages.
func (m *Stream) CloseAsync() {
	m.shutSig.CloseNow()
}

// StopConsuming instructs the buffer to stop consuming messages and close once
// the buffer is empty.
func (m *Stream) StopConsuming() {
	m.shutSig.CloseAtLeisure()
}

// WaitForClose blocks until the Stream output has closed down.
func (m *Stream) WaitForClose(timeout time.Duration) error {
	select {
	case <-m.shutSig.HasClosedChan():
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}
