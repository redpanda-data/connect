package output

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

// AsyncSink is a type that writes Benthos messages to a third party sink. If
// the protocol supports a form of acknowledgement then it will be returned by
// the call to Write.
type AsyncSink interface {
	// Connect attempts to establish a connection to the sink, if
	// unsuccessful returns an error. If the attempt is successful (or not
	// necessary) returns nil.
	Connect(ctx context.Context) error

	// WriteBatch should block until either the message is sent (and
	// acknowledged) to a sink, or a transport specific error has occurred, or
	// the Type is closed.
	WriteBatch(ctx context.Context, msg message.Batch) error

	// Close is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	Close(ctx context.Context) error
}

// AsyncWriter is an output type that writes messages to a writer.Type.
type AsyncWriter struct {
	isConnected int32

	typeStr     string
	maxInflight int
	writer      AsyncSink

	log    log.Modular
	stats  metrics.Type
	tracer trace.TracerProvider

	transactions <-chan message.Transaction

	shutSig *shutdown.Signaller
}

// NewAsyncWriter creates a Streamed implementation around an AsyncSink.
func NewAsyncWriter(typeStr string, maxInflight int, w AsyncSink, mgr component.Observability) (Streamed, error) {
	aWriter := &AsyncWriter{
		typeStr:      typeStr,
		maxInflight:  maxInflight,
		writer:       w,
		log:          mgr.Logger(),
		stats:        mgr.Metrics(),
		tracer:       mgr.Tracer(),
		transactions: nil,
		shutSig:      shutdown.NewSignaller(),
	}
	return aWriter, nil
}

//------------------------------------------------------------------------------

func (w *AsyncWriter) latencyMeasuringWrite(ctx context.Context, msg message.Batch) (latencyNs int64, err error) {
	t0 := time.Now()
	err = w.writer.WriteBatch(ctx, msg)
	if latencyNs = time.Since(t0).Nanoseconds(); latencyNs < 1 {
		latencyNs = 1
	}
	return latencyNs, err
}

// loop is an internal loop that brokers incoming messages to output pipe.
func (w *AsyncWriter) loop() {
	// Metrics paths
	var (
		mSent       = w.stats.GetCounter("output_sent")
		mBatchSent  = w.stats.GetCounter("output_batch_sent")
		mError      = w.stats.GetCounter("output_error")
		mLatency    = w.stats.GetTimer("output_latency_ns")
		mConn       = w.stats.GetCounter("output_connection_up")
		mFailedConn = w.stats.GetCounter("output_connection_failed")
		mLostConn   = w.stats.GetCounter("output_connection_lost")

		traceName = "output_" + w.typeStr
	)

	defer func() {
		_ = w.writer.Close(context.Background())

		atomic.StoreInt32(&w.isConnected, 0)
		w.shutSig.ShutdownComplete()
	}()

	connBackoff := backoff.NewExponentialBackOff()
	connBackoff.InitialInterval = time.Millisecond * 500
	connBackoff.MaxInterval = time.Second
	connBackoff.MaxElapsedTime = 0

	closeLeisureCtx, done := w.shutSig.CloseAtLeisureCtx(context.Background())
	defer done()

	initConnection := func() bool {
		for {
			if err := w.writer.Connect(closeLeisureCtx); err != nil {
				if w.shutSig.ShouldCloseAtLeisure() || errors.Is(err, component.ErrTypeClosed) {
					return false
				}
				w.log.Error("Failed to connect to %v: %v\n", w.typeStr, err)
				mFailedConn.Incr(1)

				var nextBoff time.Duration

				var ebo *component.ErrBackOff
				if errors.As(err, &ebo) {
					nextBoff = ebo.Wait
				} else {
					nextBoff = connBackoff.NextBackOff()
				}

				if sleepWithCancellation(closeLeisureCtx, nextBoff) != nil {
					return false
				}
			} else {
				connBackoff.Reset()
				return true
			}
		}
	}
	if !initConnection() {
		return
	}

	w.log.Info("Output type %v is now active", w.typeStr)
	mConn.Incr(1)
	atomic.StoreInt32(&w.isConnected, 1)

	wg := sync.WaitGroup{}
	wg.Add(w.maxInflight)

	connectMut := sync.Mutex{}
	connectLoop := func(msg message.Batch) (latency int64, err error) {
		atomic.StoreInt32(&w.isConnected, 0)

		connectMut.Lock()
		defer connectMut.Unlock()

		// If another goroutine got here first and we're able to send over the
		// connection, then we gracefully accept defeat.
		if atomic.LoadInt32(&w.isConnected) == 1 {
			if latency, err = w.latencyMeasuringWrite(closeLeisureCtx, msg); err != component.ErrNotConnected {
				return
			} else if err != nil {
				mError.Incr(1)
			}
		}
		mLostConn.Incr(1)

		// Continue to try to reconnect while still active.
		for {
			if !initConnection() {
				err = component.ErrTypeClosed
				return
			}
			if latency, err = w.latencyMeasuringWrite(closeLeisureCtx, msg); err != component.ErrNotConnected {
				atomic.StoreInt32(&w.isConnected, 1)
				mConn.Incr(1)
				return
			} else if err != nil {
				mError.Incr(1)
			}
		}
	}

	writerLoop := func() {
		defer wg.Done()

		for {
			var ts message.Transaction
			var open bool
			select {
			case ts, open = <-w.transactions:
				if !open {
					return
				}
			case <-w.shutSig.CloseAtLeisureChan():
				return
			}

			w.log.Trace("Attempting to write %v messages to '%v'.\n", ts.Payload.Len(), w.typeStr)
			_, spans := tracing.WithChildSpans(w.tracer, traceName, ts.Payload)

			latency, err := w.latencyMeasuringWrite(closeLeisureCtx, ts.Payload)

			// If our writer says it is not connected.
			if errors.Is(err, component.ErrNotConnected) {
				latency, err = connectLoop(ts.Payload)
			} else if err != nil {
				mError.Incr(1)
			}

			// Close immediately if our writer is closed.
			if errors.Is(err, component.ErrTypeClosed) {
				return
			}

			if err != nil {
				if w.typeStr != "reject" {
					// TODO: Maybe reintroduce a sleep here if we encounter a
					// busy retry loop.
					w.log.Error("Failed to send message to %v: %v\n", w.typeStr, err)
				} else {
					w.log.Debug("Rejecting message: %v\n", err)
				}
			} else {
				mBatchSent.Incr(1)
				mSent.Incr(int64(batch.MessageCollapsedCount(ts.Payload)))
				mLatency.Timing(latency)
				w.log.Trace("Successfully wrote %v messages to '%v'.\n", ts.Payload.Len(), w.typeStr)
			}

			for _, s := range spans {
				s.Finish()
			}

			_ = ts.Ack(closeLeisureCtx, err)
		}
	}

	for i := 0; i < w.maxInflight; i++ {
		go writerLoop()
	}
	wg.Wait()
}

// Consume assigns a messages channel for the output to read.
func (w *AsyncWriter) Consume(ts <-chan message.Transaction) error {
	if w.transactions != nil {
		return component.ErrAlreadyStarted
	}
	w.transactions = ts
	go w.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (w *AsyncWriter) Connected() bool {
	return atomic.LoadInt32(&w.isConnected) == 1
}

// TriggerCloseNow shuts down the output and stops processing messages.
func (w *AsyncWriter) TriggerCloseNow() {
	w.shutSig.CloseNow()
}

// WaitForClose blocks until the File output has closed down.
func (w *AsyncWriter) WaitForClose(ctx context.Context) error {
	select {
	case <-w.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func sleepWithCancellation(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
