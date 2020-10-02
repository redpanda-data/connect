package output

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
)

//------------------------------------------------------------------------------

// AsyncSink is a type that writes Benthos messages to a third party sink. If
// the protocol supports a form of acknowledgement then it will be returned by
// the call to Write.
type AsyncSink interface {
	// ConnectWithContext attempts to establish a connection to the sink, if
	// unsuccessful returns an error. If the attempt is successful (or not
	// necessary) returns nil.
	ConnectWithContext(ctx context.Context) error

	// WriteWithContext should block until either the message is sent (and
	// acknowledged) to a sink, or a transport specific error has occurred, or
	// the Type is closed.
	WriteWithContext(ctx context.Context, msg types.Message) error

	types.Closable
}

// AsyncWriter is an output type that writes messages to a writer.Type.
type AsyncWriter struct {
	running     int32
	isConnected int32

	typeStr     string
	maxInflight int
	writer      AsyncSink

	log   log.Modular
	stats metrics.Type

	transactions <-chan types.Transaction

	ctx           context.Context
	close         func()
	fullyCloseCtx context.Context
	fullyClose    func()

	closedChan chan struct{}
}

// NewAsyncWriter creates a new AsyncWriter output type.
func NewAsyncWriter(
	typeStr string,
	maxInflight int,
	w AsyncSink,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	aWriter := &AsyncWriter{
		running:      1,
		typeStr:      typeStr,
		maxInflight:  maxInflight,
		writer:       w,
		log:          log,
		stats:        stats,
		transactions: nil,
		closedChan:   make(chan struct{}),
	}

	aWriter.ctx, aWriter.close = context.WithCancel(context.Background())
	aWriter.fullyCloseCtx, aWriter.fullyClose = context.WithCancel(context.Background())

	return aWriter, nil
}

//------------------------------------------------------------------------------

func (w *AsyncWriter) latencyMeasuringWrite(msg types.Message) (latencyNs int64, err error) {
	t0 := time.Now()
	err = w.writer.WriteWithContext(w.ctx, msg)
	latencyNs = time.Since(t0).Nanoseconds()
	return latencyNs, err
}

// loop is an internal loop that brokers incoming messages to output pipe.
func (w *AsyncWriter) loop() {
	// Metrics paths
	var (
		mCount      = w.stats.GetCounter("count")
		mPartsSent  = w.stats.GetCounter("sent")
		mSent       = w.stats.GetCounter("batch.sent")
		mBytesSent  = w.stats.GetCounter("batch.bytes")
		mLatency    = w.stats.GetTimer("batch.latency")
		mConn       = w.stats.GetCounter("connection.up")
		mFailedConn = w.stats.GetCounter("connection.failed")
		mLostConn   = w.stats.GetCounter("connection.lost")
	)

	defer func() {
		err := w.writer.WaitForClose(time.Second)
		for ; err != nil; err = w.writer.WaitForClose(time.Second) {
			w.log.Warnf("Waiting for output to close, blocked by: %v\n", err)
		}
		atomic.StoreInt32(&w.isConnected, 0)
		close(w.closedChan)
	}()

	throt := throttle.New(throttle.OptCloseChan(w.ctx.Done()))

	for {
		if err := w.writer.ConnectWithContext(w.ctx); err != nil {
			// Close immediately if our writer is closed.
			if err == types.ErrTypeClosed {
				return
			}

			w.log.Errorf("Failed to connect to %v: %v\n", w.typeStr, err)
			mFailedConn.Incr(1)
			if !throt.Retry() {
				return
			}
		} else {
			break
		}
	}
	mConn.Incr(1)
	atomic.StoreInt32(&w.isConnected, 1)

	wg := sync.WaitGroup{}
	wg.Add(w.maxInflight)

	connectMut := sync.Mutex{}
	connectLoop := func(msg types.Message) (latency int64, err error) {
		atomic.StoreInt32(&w.isConnected, 0)

		connectMut.Lock()
		defer connectMut.Unlock()

		// If another goroutine got here first and we're able to send over the
		// connection, then we gracefully accept defeat.
		if atomic.LoadInt32(&w.isConnected) == 1 {
			if latency, err = w.latencyMeasuringWrite(msg); err != types.ErrNotConnected {
				return
			}
		}
		mLostConn.Incr(1)

		// Continue to try to reconnect while still active.
		for atomic.LoadInt32(&w.running) == 1 {
			if err = w.writer.ConnectWithContext(w.ctx); err != nil {
				// Close immediately if our writer is closed.
				if err == types.ErrTypeClosed {
					return
				}

				w.log.Errorf("Failed to reconnect to %v: %v\n", w.typeStr, err)
				mFailedConn.Incr(1)
				if !throt.Retry() {
					return
				}
			} else if latency, err = w.latencyMeasuringWrite(msg); err != types.ErrNotConnected {
				atomic.StoreInt32(&w.isConnected, 1)
				mConn.Incr(1)
				break
			} else if !throt.Retry() {
				return
			}
		}
		return
	}

	writerLoop := func() {
		defer wg.Done()

		for atomic.LoadInt32(&w.running) == 1 {
			var ts types.Transaction
			var open bool
			select {
			case ts, open = <-w.transactions:
				if !open {
					return
				}
				mCount.Incr(1)
			case <-w.ctx.Done():
				return
			}

			w.log.Tracef("Attempting to write %v messages to '%v'.\n", ts.Payload.Len(), w.typeStr)
			spans := tracing.CreateChildSpans("output_"+w.typeStr, ts.Payload)
			latency, err := w.latencyMeasuringWrite(ts.Payload)

			// If our writer says it is not connected.
			if err == types.ErrNotConnected {
				latency, err = connectLoop(ts.Payload)
			}

			// Close immediately if our writer is closed.
			if err == types.ErrTypeClosed {
				return
			}

			if err != nil {
				w.log.Errorf("Failed to send message to %v: %v\n", w.typeStr, err)
				if !throt.Retry() {
					return
				}
			} else {
				mSent.Incr(1)
				mPartsSent.Incr(int64(ts.Payload.Len()))
				mBytesSent.Incr(int64(message.GetAllBytesLen(ts.Payload)))
				mLatency.Timing(latency)
				w.log.Tracef("Successfully wrote %v messages to '%v'.\n", ts.Payload.Len(), w.typeStr)
				throt.Reset() // TODO BAD PAYLOAD NAUGHTY RESETS
			}

			for _, s := range spans {
				s.Finish()
			}

			select {
			case ts.ResponseChan <- response.NewError(err):
			case <-w.ctx.Done():
				// The pipeline is terminating but we still want to attempt to
				// propagate an acknowledgement from in-transit messages.
				select {
				case ts.ResponseChan <- response.NewError(err):
				case <-w.fullyCloseCtx.Done():
				}
				return
			}
		}
	}

	for i := 0; i < w.maxInflight; i++ {
		go writerLoop()
	}
	wg.Wait()
}

// Consume assigns a messages channel for the output to read.
func (w *AsyncWriter) Consume(ts <-chan types.Transaction) error {
	if w.transactions != nil {
		return types.ErrAlreadyStarted
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

// CloseAsync shuts down the File output and stops processing messages.
func (w *AsyncWriter) CloseAsync() {
	w.close()
	atomic.StoreInt32(&w.running, 0)
}

// WaitForClose blocks until the File output has closed down.
func (w *AsyncWriter) WaitForClose(timeout time.Duration) error {
	go func() {
		<-time.After(timeout - time.Second)
		w.fullyClose()
	}()
	select {
	case <-w.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
