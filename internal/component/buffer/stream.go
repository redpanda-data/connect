package buffer

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/old/util/throttle"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/internal/tracing"
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
	Read(context.Context) (message.Batch, AckFunc, error)

	// Write a new message batch to the stack.
	Write(context.Context, message.Batch, AckFunc) error

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
	tracer  trace.TracerProvider
	typeStr string

	buffer ReaderWriter

	errThrottle *throttle.Type
	shutSig     *shutdown.Signaller

	messagesIn  <-chan message.Transaction
	messagesOut chan message.Transaction

	closedWG sync.WaitGroup
}

// NewStream creates a new Producer/Consumer around a buffer.
func NewStream(typeStr string, buffer ReaderWriter, mgr component.Observability) Streamed {
	m := Stream{
		typeStr:     typeStr,
		stats:       mgr.Metrics(),
		log:         mgr.Logger(),
		tracer:      mgr.Tracer(),
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
				err = tr.Ack(ctx, ackErr)
				ackGroup.Done()
			})
			return
		}

		batchLen := tr.Payload.Len()

		writeBatch, _ := tracing.WithSiblingSpans(m.tracer, m.typeStr, tr.Payload)
		err := m.buffer.Write(closeAtLeisureCtx, writeBatch, ackFunc)
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

	closeNowCtx, done := m.shutSig.CloseNowCtx(context.Background())
	defer done()

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
		tracing.InitSpans(m.tracer, m.typeStr, msg)

		batchLen := msg.Len()

		m.errThrottle.Reset()
		resChan := make(chan error, 1)
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
				if ackErr := ackFunc(closeNowCtx, res); ackErr != nil {
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

// TriggerStopConsuming instructs the buffer to stop consuming messages and
// close once the buffer is empty.
func (m *Stream) TriggerStopConsuming() {
	m.shutSig.CloseAtLeisure()
}

// TriggerCloseNow shuts down the Stream and stops processing messages.
func (m *Stream) TriggerCloseNow() {
	m.shutSig.CloseNow()
}

// WaitForClose blocks until the Stream output has closed down.
func (m *Stream) WaitForClose(ctx context.Context) error {
	select {
	case <-m.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
