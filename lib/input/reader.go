package input

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
)

//------------------------------------------------------------------------------

// Reader is an input implementation that reads messages from a reader.Type.
type Reader struct {
	running   int32
	connected int32

	typeStr string
	reader  reader.Type

	stats metrics.Type
	log   log.Modular

	connThrot *throttle.Type

	transactions chan types.Transaction
	responses    chan types.Response

	closeChan      chan struct{}
	fullyCloseChan chan struct{}
	fullyCloseOnce sync.Once

	closedChan chan struct{}
}

// NewReader creates a new Reader input type.
func NewReader(
	typeStr string,
	r reader.Type,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	rdr := &Reader{
		running:        1,
		typeStr:        typeStr,
		reader:         r,
		log:            log,
		stats:          stats,
		transactions:   make(chan types.Transaction),
		responses:      make(chan types.Response),
		closeChan:      make(chan struct{}),
		fullyCloseChan: make(chan struct{}),
		closedChan:     make(chan struct{}),
	}

	rdr.connThrot = throttle.New(throttle.OptCloseChan(rdr.closeChan))

	go rdr.loop()
	return rdr, nil
}

//------------------------------------------------------------------------------

func (r *Reader) loop() {
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
		_ = r.reader.WaitForClose(shutdown.MaximumShutdownWait())
		mRunning.Decr(1)
		atomic.StoreInt32(&r.connected, 0)

		close(r.transactions)
		close(r.closedChan)
	}()
	mRunning.Incr(1)

	for {
		if err := r.reader.Connect(); err != nil {
			if err == types.ErrTypeClosed {
				return
			}
			r.log.Errorf("Failed to connect to %v: %v\n", r.typeStr, err)
			mFailedConn.Incr(1)
			if !r.connThrot.Retry() {
				return
			}
		} else {
			r.connThrot.Reset()
			break
		}
	}
	mConn.Incr(1)
	atomic.StoreInt32(&r.connected, 1)

	for atomic.LoadInt32(&r.running) == 1 {
		msg, err := r.reader.Read()

		// If our reader says it is not connected.
		if err == types.ErrNotConnected {
			mLostConn.Incr(1)
			atomic.StoreInt32(&r.connected, 0)

			// Continue to try to reconnect while still active.
			for atomic.LoadInt32(&r.running) == 1 {
				if err = r.reader.Connect(); err != nil {
					// Close immediately if our reader is closed.
					if err == types.ErrTypeClosed {
						return
					}

					r.log.Errorf("Failed to reconnect to %v: %v\n", r.typeStr, err)
					mFailedConn.Incr(1)
				} else if msg, err = r.reader.Read(); err != types.ErrNotConnected {
					mConn.Incr(1)
					atomic.StoreInt32(&r.connected, 1)
					r.connThrot.Reset()
					break
				}
				if !r.connThrot.Retry() {
					return
				}
			}
		}

		// Close immediately if our reader is closed.
		if err == types.ErrTypeClosed {
			return
		}

		if err != nil || msg == nil {
			if err != types.ErrTimeout && err != types.ErrNotConnected {
				r.log.Errorf("Failed to read message: %v\n", err)
			}
			if !r.connThrot.Retry() {
				return
			}
			continue
		} else {
			r.connThrot.Reset()
			mCount.Incr(1)
			mPartsRcvd.Incr(int64(msg.Len()))
			mRcvd.Incr(1)
			r.log.Tracef("Consumed %v messages from '%v'.\n", msg.Len(), r.typeStr)
		}

		tracing.InitSpans("input_"+r.typeStr, msg)
		select {
		case r.transactions <- types.NewTransaction(msg, r.responses):
		case <-r.closeChan:
			return
		}

		var res types.Response
		var open bool
		select {
		case res, open = <-r.responses:
		case <-r.closeChan:
			// The pipeline is terminating but we still want to attempt to
			// propagate an acknowledgement from in-transit messages.
			select {
			case res, open = <-r.responses:
			case <-r.fullyCloseChan:
				return
			}
		}
		if !open {
			return
		}

		if err = r.reader.Acknowledge(res.Error()); err != nil {
			r.log.Errorf("Failed to acknowledge message: %v\n", err)
		}
		tTaken := time.Since(msg.CreatedAt()).Nanoseconds()
		mLatency.Timing(tTaken)
		tracing.FinishSpans(msg)
	}
}

// TransactionChan returns a transactions channel for consuming messages from
// this input type.
func (r *Reader) TransactionChan() <-chan types.Transaction {
	return r.transactions
}

// Connected returns a boolean indicating whether this input is currently
// connected to its target.
func (r *Reader) Connected() bool {
	return atomic.LoadInt32(&r.connected) == 1
}

// CloseAsync shuts down the Reader input and stops processing requests.
func (r *Reader) CloseAsync() {
	if atomic.CompareAndSwapInt32(&r.running, 1, 0) {
		close(r.closeChan)
		r.reader.CloseAsync()
	}
}

// WaitForClose blocks until the Reader input has closed down.
func (r *Reader) WaitForClose(timeout time.Duration) error {
	go r.fullyCloseOnce.Do(func() {
		<-time.After(timeout - time.Second)
		close(r.fullyCloseChan)
	})
	select {
	case <-r.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
