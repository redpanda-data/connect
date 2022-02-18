package output

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/cenkalti/backoff/v4"
)

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
	WriteWithContext(ctx context.Context, msg *message.Batch) error

	// CloseAsync triggers the shut down of this component but should not block
	// the calling goroutine.
	CloseAsync()

	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(timeout time.Duration) error
}

// AsyncWriter is an output type that writes messages to a writer.Type.
type AsyncWriter struct {
	isConnected int32

	typeStr     string
	maxInflight int
	noCancel    bool
	writer      AsyncSink

	injectTracingMap *mapping.Executor

	mgr   interop.Manager
	log   log.Modular
	stats metrics.Type

	transactions <-chan message.Transaction

	shutSig *shutdown.Signaller
}

// NewAsyncWriter creates a new AsyncWriter output type.
// Deprecated
func NewAsyncWriter(
	typeStr string,
	maxInflight int,
	w AsyncSink,
	log log.Modular,
	stats metrics.Type,
) (output.Streamed, error) {
	return newAsyncWriter(typeStr, maxInflight, w, mock.NewManager(), log, stats)
}

func newAsyncWriter(
	typeStr string,
	maxInflight int,
	w AsyncSink,
	mgr interop.Manager,
	log log.Modular,
	stats metrics.Type,
) (output.Streamed, error) {
	aWriter := &AsyncWriter{
		typeStr:      typeStr,
		maxInflight:  maxInflight,
		writer:       w,
		mgr:          mgr,
		log:          log,
		stats:        stats,
		transactions: nil,
		shutSig:      shutdown.NewSignaller(),
	}
	return aWriter, nil
}

// SetInjectTracingMap sets a mapping to be used for injecting tracing events
// into messages.
func (w *AsyncWriter) SetInjectTracingMap(mapping string) error {
	var err error
	w.injectTracingMap, err = w.mgr.BloblEnvironment().NewMapping(mapping)
	return err
}

// SetNoCancel configures the async writer so that write calls do not use a
// context that gets cancelled on shutdown. This is much more efficient as it
// reduces allocations, goroutines and defers for each write call, but also
// means the write can block graceful termination. Therefore this setting should
// be reserved for outputs that are exceptionally fast.
func (w *AsyncWriter) SetNoCancel() {
	w.noCancel = true
}

//------------------------------------------------------------------------------

func (w *AsyncWriter) latencyMeasuringWrite(msg *message.Batch) (latencyNs int64, err error) {
	t0 := time.Now()
	var ctx context.Context
	if w.noCancel {
		ctx = context.Background()
	} else {
		var done func()
		ctx, done = w.shutSig.CloseAtLeisureCtx(context.Background())
		defer done()
	}
	err = w.writer.WriteWithContext(ctx, msg)
	latencyNs = time.Since(t0).Nanoseconds()
	return latencyNs, err
}

func (w *AsyncWriter) injectSpans(msg *message.Batch, spans []*tracing.Span) *message.Batch {
	if w.injectTracingMap == nil || msg.Len() > len(spans) {
		return msg
	}

	parts := make([]*message.Part, msg.Len())

	for i := 0; i < msg.Len(); i++ {
		parts[i] = msg.Get(i).Copy()

		spanMapGeneric, err := spans[i].TextMap()
		if err != nil {
			w.log.Warnf("Failed to inject span: %v", err)
			continue
		}

		spanPart := message.NewPart(nil)
		if err = spanPart.SetJSON(spanMapGeneric); err != nil {
			w.log.Warnf("Failed to inject span: %v", err)
			continue
		}

		spanMsg := message.QuickBatch(nil)
		spanMsg.Append(spanPart)

		if parts[i], err = w.injectTracingMap.MapOnto(parts[i], i, spanMsg); err != nil {
			w.log.Warnf("Failed to inject span: %v", err)
			parts[i] = msg.Get(i)
		}
	}

	newMsg := message.QuickBatch(nil)
	newMsg.SetAll(parts)
	return newMsg
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
	)

	defer func() {
		w.writer.CloseAsync()
		_ = w.writer.WaitForClose(shutdown.MaximumShutdownWait())

		atomic.StoreInt32(&w.isConnected, 0)
		w.shutSig.ShutdownComplete()
	}()

	connBackoff := backoff.NewExponentialBackOff()
	connBackoff.InitialInterval = time.Millisecond * 500
	connBackoff.MaxInterval = time.Second
	connBackoff.MaxElapsedTime = 0

	initConnection := func() bool {
		initConnCtx, initConnDone := w.shutSig.CloseAtLeisureCtx(context.Background())
		defer initConnDone()
		for {
			if err := w.writer.ConnectWithContext(initConnCtx); err != nil {
				if w.shutSig.ShouldCloseAtLeisure() || err == component.ErrTypeClosed {
					return false
				}
				w.log.Errorf("Failed to connect to %v: %v\n", w.typeStr, err)
				mFailedConn.Incr(1)
				select {
				case <-time.After(connBackoff.NextBackOff()):
				case <-initConnCtx.Done():
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
	mConn.Incr(1)
	atomic.StoreInt32(&w.isConnected, 1)

	wg := sync.WaitGroup{}
	wg.Add(w.maxInflight)

	connectMut := sync.Mutex{}
	connectLoop := func(msg *message.Batch) (latency int64, err error) {
		atomic.StoreInt32(&w.isConnected, 0)

		connectMut.Lock()
		defer connectMut.Unlock()

		// If another goroutine got here first and we're able to send over the
		// connection, then we gracefully accept defeat.
		if atomic.LoadInt32(&w.isConnected) == 1 {
			if latency, err = w.latencyMeasuringWrite(msg); err != component.ErrNotConnected {
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
			if latency, err = w.latencyMeasuringWrite(msg); err != component.ErrNotConnected {
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

			w.log.Tracef("Attempting to write %v messages to '%v'.\n", ts.Payload.Len(), w.typeStr)
			spans := tracing.CreateChildSpans("output_"+w.typeStr, ts.Payload)
			ts.Payload = w.injectSpans(ts.Payload, spans)

			latency, err := w.latencyMeasuringWrite(ts.Payload)

			// If our writer says it is not connected.
			if err == component.ErrNotConnected {
				latency, err = connectLoop(ts.Payload)
			} else if err != nil {
				mError.Incr(1)
			}

			// Close immediately if our writer is closed.
			if err == component.ErrTypeClosed {
				return
			}

			if err != nil {
				if w.typeStr != TypeReject {
					// TODO: Maybe reintroduce a sleep here if we encounter a
					// busy retry loop.
					w.log.Errorf("Failed to send message to %v: %v\n", w.typeStr, err)
				} else {
					w.log.Debugf("Rejecting message: %v\n", err)
				}
			} else {
				mBatchSent.Incr(1)
				mSent.Incr(int64(batch.MessageCollapsedCount(ts.Payload)))
				mLatency.Timing(latency)
				w.log.Tracef("Successfully wrote %v messages to '%v'.\n", ts.Payload.Len(), w.typeStr)
			}

			for _, s := range spans {
				s.Finish()
			}

			select {
			case ts.ResponseChan <- response.NewError(err):
			case <-w.shutSig.CloseAtLeisureChan():
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

// MaxInFlight returns the maximum number of in flight messages permitted by the
// output. This value can be used to determine a sensible value for parent
// outputs, but should not be relied upon as part of dispatcher logic.
func (w *AsyncWriter) MaxInFlight() (int, bool) {
	return w.maxInflight, true
}

// CloseAsync shuts down the File output and stops processing messages.
func (w *AsyncWriter) CloseAsync() {
	w.shutSig.CloseAtLeisure()
}

// WaitForClose blocks until the File output has closed down.
func (w *AsyncWriter) WaitForClose(timeout time.Duration) error {
	select {
	case <-w.shutSig.HasClosedChan():
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}
