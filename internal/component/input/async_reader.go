package input

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

// AsyncReader is an input implementation that reads messages from an
// input.Async component.
type AsyncReader struct {
	connected   int32
	connBackoff backoff.BackOff

	typeStr string
	reader  Async

	mgr component.Observability

	transactions chan message.Transaction
	shutSig      *shutdown.Signaller
}

// NewAsyncReader creates a new AsyncReader input type.
func NewAsyncReader(
	typeStr string,
	r Async,
	mgr component.Observability,
) (Streamed, error) {
	boff := backoff.NewExponentialBackOff()
	boff.InitialInterval = time.Millisecond * 100
	boff.MaxInterval = time.Second
	boff.MaxElapsedTime = 0

	rdr := &AsyncReader{
		connBackoff:  boff,
		typeStr:      typeStr,
		reader:       r,
		mgr:          mgr,
		transactions: make(chan message.Transaction),
		shutSig:      shutdown.NewSignaller(),
	}

	go rdr.loop()
	return rdr, nil
}

//------------------------------------------------------------------------------

func (r *AsyncReader) loop() {
	// Metrics paths
	var (
		mRcvd       = r.mgr.Metrics().GetCounter("input_received")
		mConn       = r.mgr.Metrics().GetCounter("input_connection_up")
		mFailedConn = r.mgr.Metrics().GetCounter("input_connection_failed")
		mLostConn   = r.mgr.Metrics().GetCounter("input_connection_lost")
		mLatency    = r.mgr.Metrics().GetTimer("input_latency_ns")

		traceName = "input_" + r.typeStr
	)

	closeAtLeisureCtx, calDone := r.shutSig.CloseAtLeisureCtx(context.Background())
	defer calDone()

	closeNowCtx, cnDone := r.shutSig.CloseNowCtx(context.Background())
	defer cnDone()

	defer func() {
		_ = r.reader.Close(context.Background())

		atomic.StoreInt32(&r.connected, 0)

		close(r.transactions)
		r.shutSig.ShutdownComplete()
	}()

	pendingAcks := sync.WaitGroup{}
	defer func() {
		r.mgr.Logger().Debugln("Waiting for pending acks to resolve before shutting down.")
		pendingAcks.Wait()
		r.mgr.Logger().Debugln("Pending acks resolved.")
	}()

	initConnection := func() bool {
		for {
			if err := r.reader.Connect(closeAtLeisureCtx); err != nil {
				if r.shutSig.ShouldCloseAtLeisure() || errors.Is(err, component.ErrTypeClosed) {
					return false
				}
				r.mgr.Logger().Errorf("Failed to connect to %v: %v\n", r.typeStr, err)
				mFailedConn.Incr(1)
				select {
				case <-time.After(r.connBackoff.NextBackOff()):
				case <-closeAtLeisureCtx.Done():
					return false
				}
			} else {
				r.connBackoff.Reset()
				return true
			}
		}
	}
	if !initConnection() {
		return
	}
	mConn.Incr(1)
	atomic.StoreInt32(&r.connected, 1)

	for {
		msg, ackFn, err := r.reader.ReadBatch(closeAtLeisureCtx)

		// If our reader says it is not connected.
		if errors.Is(err, component.ErrNotConnected) {
			mLostConn.Incr(1)
			atomic.StoreInt32(&r.connected, 0)

			// Continue to try to reconnect while still active.
			if !initConnection() {
				return
			}
			mConn.Incr(1)
			atomic.StoreInt32(&r.connected, 1)
		}

		// Close immediately if our reader is closed.
		if r.shutSig.ShouldCloseAtLeisure() || errors.Is(err, component.ErrTypeClosed) {
			return
		}

		if err != nil || msg == nil {
			if err != nil && err != component.ErrTimeout && err != component.ErrNotConnected {
				r.mgr.Logger().Errorf("Failed to read message: %v\n", err)
			}
			select {
			case <-time.After(r.connBackoff.NextBackOff()):
			case <-r.shutSig.CloseAtLeisureChan():
				return
			}
			continue
		} else {
			r.connBackoff.Reset()
			mRcvd.Incr(int64(msg.Len()))
			r.mgr.Logger().Tracef("Consumed %v messages from '%v'.\n", msg.Len(), r.typeStr)
		}

		startedAt := time.Now()

		resChan := make(chan error, 1)
		tracing.InitSpans(r.mgr.Tracer(), traceName, msg)
		select {
		case r.transactions <- message.NewTransaction(msg, resChan):
		case <-r.shutSig.CloseAtLeisureChan():
			return
		}

		pendingAcks.Add(1)
		go func(
			m message.Batch,
			aFn AsyncAckFn,
			rChan chan error,
		) {
			defer pendingAcks.Done()

			var res error
			select {
			case res = <-rChan:
			case <-r.shutSig.CloseNowChan():
				// Even if the pipeline is terminating we still want to attempt
				// to propagate an acknowledgement from in-transit messages.
				return
			}

			mLatency.Timing(time.Since(startedAt).Nanoseconds())
			tracing.FinishSpans(m)

			if err = aFn(closeNowCtx, res); err != nil {
				r.mgr.Logger().Errorf("Failed to acknowledge message: %v\n", err)
			}
		}(msg, ackFn, resChan)
	}
}

// TransactionChan returns a transactions channel for consuming messages from
// this input type.
func (r *AsyncReader) TransactionChan() <-chan message.Transaction {
	return r.transactions
}

// Connected returns a boolean indicating whether this input is currently
// connected to its target.
func (r *AsyncReader) Connected() bool {
	return atomic.LoadInt32(&r.connected) == 1
}

// TriggerStopConsuming instructs the input to start shutting down resources
// once all pending messages are delivered and acknowledged. This call does
// not block.
func (r *AsyncReader) TriggerStopConsuming() {
	r.shutSig.CloseAtLeisure()
}

// TriggerCloseNow triggers the shut down of this component but should not block
// the calling goroutine.
func (r *AsyncReader) TriggerCloseNow() {
	r.shutSig.CloseNow()
}

// WaitForClose is a blocking call to wait until the component has finished
// shutting down and cleaning up resources.
func (r *AsyncReader) WaitForClose(ctx context.Context) error {
	select {
	case <-r.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
