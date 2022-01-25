package input

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/cenkalti/backoff/v4"
)

//------------------------------------------------------------------------------

// AsyncReader is an input implementation that reads messages from a
// reader.Async component.
type AsyncReader struct {
	connected   int32
	connBackoff backoff.BackOff

	allowSkipAcks bool

	typeStr string
	reader  reader.Async

	stats metrics.Type
	log   log.Modular

	transactions chan types.Transaction
	shutSig      *shutdown.Signaller
}

// NewAsyncReader creates a new AsyncReader input type.
func NewAsyncReader(
	typeStr string,
	allowSkipAcks bool,
	r reader.Async,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	boff := backoff.NewExponentialBackOff()
	boff.InitialInterval = time.Millisecond * 100
	boff.MaxInterval = time.Second
	boff.MaxElapsedTime = 0

	rdr := &AsyncReader{
		connBackoff:   boff,
		allowSkipAcks: allowSkipAcks,
		typeStr:       typeStr,
		reader:        r,
		log:           log,
		stats:         stats,
		transactions:  make(chan types.Transaction),
		shutSig:       shutdown.NewSignaller(),
	}

	go rdr.loop()
	return rdr, nil
}

//------------------------------------------------------------------------------

func (r *AsyncReader) loop() {
	// Metrics paths
	var (
		mRunning    = r.stats.GetGauge("running")
		mCount      = r.stats.GetCounter("count")
		mRcvd       = r.stats.GetCounter("batch.received")
		mPartsRcvd  = r.stats.GetCounter("received")
		mConn       = r.stats.GetCounter("connection.up")
		mFailedConn = r.stats.GetCounter("connection.failed")
		mLostConn   = r.stats.GetCounter("connection.lost")
		mLatency    = r.stats.GetTimer("latency")
	)

	defer func() {
		r.reader.CloseAsync()
		go func() {
			select {
			case <-r.shutSig.CloseNowChan():
				_ = r.reader.WaitForClose(0)
			case <-r.shutSig.HasClosedChan():
			}
		}()
		_ = r.reader.WaitForClose(shutdown.MaximumShutdownWait())

		mRunning.Decr(1)
		atomic.StoreInt32(&r.connected, 0)

		close(r.transactions)
		r.shutSig.ShutdownComplete()
	}()
	mRunning.Incr(1)

	pendingAcks := sync.WaitGroup{}
	defer func() {
		r.log.Debugln("Waiting for pending acks to resolve before shutting down.")
		pendingAcks.Wait()
		r.log.Debugln("Pending acks resolved.")
	}()

	initConnection := func() bool {
		initConnCtx, initConnDone := r.shutSig.CloseAtLeisureCtx(context.Background())
		defer initConnDone()
		for {
			if err := r.reader.ConnectWithContext(initConnCtx); err != nil {
				if r.shutSig.ShouldCloseAtLeisure() || err == types.ErrTypeClosed {
					return false
				}
				r.log.Errorf("Failed to connect to %v: %v\n", r.typeStr, err)
				mFailedConn.Incr(1)
				select {
				case <-time.After(r.connBackoff.NextBackOff()):
				case <-initConnCtx.Done():
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
		readCtx, readDone := r.shutSig.CloseAtLeisureCtx(context.Background())
		msg, ackFn, err := r.reader.ReadWithContext(readCtx)
		readDone()

		// If our reader says it is not connected.
		if err == types.ErrNotConnected {
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
		if r.shutSig.ShouldCloseAtLeisure() || err == types.ErrTypeClosed {
			return
		}

		if err != nil || msg == nil {
			if err != nil && err != types.ErrTimeout && err != types.ErrNotConnected {
				r.log.Errorf("Failed to read message: %v\n", err)
			}
			select {
			case <-time.After(r.connBackoff.NextBackOff()):
			case <-r.shutSig.CloseAtLeisureChan():
				return
			}
			continue
		} else {
			r.connBackoff.Reset()
			mCount.Incr(1)
			mPartsRcvd.Incr(int64(msg.Len()))
			mRcvd.Incr(1)
			r.log.Tracef("Consumed %v messages from '%v'.\n", msg.Len(), r.typeStr)
		}

		resChan := make(chan types.Response)
		tracing.InitSpans("input_"+r.typeStr, msg)
		select {
		case r.transactions <- types.NewTransaction(msg, resChan):
		case <-r.shutSig.CloseAtLeisureChan():
			return
		}

		pendingAcks.Add(1)
		go func(
			m types.Message,
			aFn reader.AsyncAckFn,
			rChan chan types.Response,
		) {
			defer pendingAcks.Done()

			var res types.Response
			var open bool
			select {
			case res, open = <-rChan:
			case <-r.shutSig.CloseNowChan():
				// Even if the pipeline is terminating we still want to attempt
				// to propagate an acknowledgement from in-transit messages.
				return
			}
			if !open {
				return
			}
			mLatency.Timing(time.Since(m.CreatedAt()).Nanoseconds())
			tracing.FinishSpans(m)

			ackCtx, ackDone := r.shutSig.CloseNowCtx(context.Background())
			if err = aFn(ackCtx, res); err != nil {
				r.log.Errorf("Failed to acknowledge message: %v\n", err)
			}
			ackDone()
		}(msg, ackFn, resChan)
	}
}

// TransactionChan returns a transactions channel for consuming messages from
// this input type.
func (r *AsyncReader) TransactionChan() <-chan types.Transaction {
	return r.transactions
}

// Connected returns a boolean indicating whether this input is currently
// connected to its target.
func (r *AsyncReader) Connected() bool {
	return atomic.LoadInt32(&r.connected) == 1
}

// CloseAsync shuts down the AsyncReader input and stops processing requests.
func (r *AsyncReader) CloseAsync() {
	r.shutSig.CloseAtLeisure()
}

// WaitForClose blocks until the AsyncReader input has closed down.
func (r *AsyncReader) WaitForClose(timeout time.Duration) error {
	go func() {
		<-time.After(timeout - time.Second)
		r.shutSig.CloseNow()
	}()
	select {
	case <-r.shutSig.HasClosedChan():
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
