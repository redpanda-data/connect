// Copyright (c) 2019 Ashley Jeffs
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

package input

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

func sanitiseWithBatch(
	componentConfig interface{},
	batchConfig batch.PolicyConfig,
) (map[string]interface{}, error) {
	batchSanit, err := batch.SanitisePolicyConfig(batchConfig)
	if err != nil {
		return nil, err
	}

	cBytes, err := yaml.Marshal(componentConfig)
	if err != nil {
		return nil, err
	}

	hashMap := map[string]interface{}{}
	if err = yaml.Unmarshal(cBytes, &hashMap); err != nil {
		return nil, err
	}

	hashMap["batching"] = batchSanit
	return hashMap, nil
}

//------------------------------------------------------------------------------

// AsyncReader is an input implementation that reads messages from a
// reader.Async component.
type AsyncReader struct {
	running   int32
	connected int32

	allowSkipAcks bool

	typeStr string
	reader  reader.Async

	stats metrics.Type
	log   log.Modular

	connThrot *throttle.Type

	transactions chan types.Transaction

	ctx           context.Context
	closeFn       func()
	fullyCloseCtx context.Context
	fullyCloseFn  func()

	closedChan chan struct{}
}

// NewAsyncReader creates a new AsyncReader input type.
func NewAsyncReader(
	typeStr string,
	allowSkipAcks bool,
	r reader.Async,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	ctx, cancelFn := context.WithCancel(context.Background())
	fullyCloseCtx, fullyCancelFn := context.WithCancel(context.Background())
	rdr := &AsyncReader{
		running:       1,
		allowSkipAcks: allowSkipAcks,
		typeStr:       typeStr,
		reader:        r,
		log:           log,
		stats:         stats,
		transactions:  make(chan types.Transaction),
		ctx:           ctx,
		closeFn:       cancelFn,
		fullyCloseCtx: fullyCloseCtx,
		fullyCloseFn:  fullyCancelFn,
		closedChan:    make(chan struct{}),
	}

	rdr.connThrot = throttle.New(throttle.OptCloseChan(ctx.Done()))

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
		err := r.reader.WaitForClose(time.Second)
		for ; err != nil; err = r.reader.WaitForClose(time.Second) {
		}
		mRunning.Decr(1)
		atomic.StoreInt32(&r.connected, 0)

		close(r.transactions)
		close(r.closedChan)
	}()
	mRunning.Incr(1)

	pendingAcks := sync.WaitGroup{}
	defer func() {
		r.log.Debugln("Waiting for pending acks to resolve before shutting down.")
		pendingAcks.Wait()
		r.log.Debugln("Pending acks resolved.")
	}()

	for {
		if err := r.reader.ConnectWithContext(r.ctx); err != nil {
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
		msg, ackFn, err := r.reader.ReadWithContext(r.ctx)

		// If our reader says it is not connected.
		if err == types.ErrNotConnected {
			mLostConn.Incr(1)
			atomic.StoreInt32(&r.connected, 0)

			// Continue to try to reconnect while still active.
			for atomic.LoadInt32(&r.running) == 1 {
				if err = r.reader.ConnectWithContext(r.ctx); err != nil {
					// Close immediately if our reader is closed.
					if err == types.ErrTypeClosed {
						return
					}

					r.log.Errorf("Failed to reconnect to %v: %v\n", r.typeStr, err)
					mFailedConn.Incr(1)
				} else if msg, ackFn, err = r.reader.ReadWithContext(r.ctx); err != types.ErrNotConnected {
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
			if err != nil && err != types.ErrTimeout && err != types.ErrNotConnected {
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
		}

		resChan := make(chan types.Response)
		tracing.InitSpans("input_"+r.typeStr, msg)
		select {
		case r.transactions <- types.NewTransaction(msg, resChan):
		case <-r.ctx.Done():
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
			case <-r.ctx.Done():
				// The pipeline is terminating but we still want to attempt to
				// propagate an acknowledgement from in-transit messages.
				select {
				case res, open = <-rChan:
				case <-r.fullyCloseCtx.Done():
					return
				}
			}
			if !open {
				return
			}
			if res.SkipAck() && !r.allowSkipAcks {
				r.log.Errorf("Detected downstream batch processor which is not permitted with this input, please refer to the documentation for more information. This input will now shut down.")
				res = response.NewNoack()
				r.CloseAsync()
			}
			mLatency.Timing(time.Since(m.CreatedAt()).Nanoseconds())
			tracing.FinishSpans(m)
			if err = aFn(r.fullyCloseCtx, res); err != nil {
				r.log.Errorf("Failed to acknowledge message: %v\n", err)
			}
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
	if atomic.CompareAndSwapInt32(&r.running, 1, 0) {
		r.closeFn()
	}
}

// WaitForClose blocks until the AsyncReader input has closed down.
func (r *AsyncReader) WaitForClose(timeout time.Duration) error {
	go func() {
		<-time.After(timeout - time.Second)
		r.fullyCloseFn()
	}()
	select {
	case <-r.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
