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

package input

import (
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/input/reader"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

// Reader is an input type that reads from a Reader instance.
type Reader struct {
	running int32

	typeStr string
	reader  reader.Type

	stats metrics.Type
	log   log.Modular

	transactions chan types.Transaction
	responses    chan types.Response

	closeChan  chan struct{}
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
		running:      1,
		typeStr:      typeStr,
		reader:       r,
		log:          log.NewModule(".input." + typeStr),
		stats:        stats,
		transactions: make(chan types.Transaction),
		responses:    make(chan types.Response),
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),
	}

	go rdr.loop()
	return rdr, nil
}

//------------------------------------------------------------------------------

func (r *Reader) loop() {
	// Metrics paths
	var (
		mRunning      = r.stats.GetCounter("input." + r.typeStr + ".running")
		mRunningF     = r.stats.GetCounter("input.running")
		mCount        = r.stats.GetCounter("input." + r.typeStr + ".count")
		mCountF       = r.stats.GetCounter("input.count")
		mReadSuccess  = r.stats.GetCounter("input." + r.typeStr + ".read.success")
		mReadSuccessF = r.stats.GetCounter("input.read.success")
		mReadError    = r.stats.GetCounter("input." + r.typeStr + ".read.error")
		mReadErrorF   = r.stats.GetCounter("input.read.error")
		mSendSuccess  = r.stats.GetCounter("input." + r.typeStr + ".send.success")
		mSendSuccessF = r.stats.GetCounter("input.send.success")
		mSendError    = r.stats.GetCounter("input." + r.typeStr + ".send.error")
		mSendErrorF   = r.stats.GetCounter("input.send.error")
		mAckSuccess   = r.stats.GetCounter("input." + r.typeStr + ".ack.success")
		mAckSuccessF  = r.stats.GetCounter("input.ack.success")
		mAckError     = r.stats.GetCounter("input." + r.typeStr + ".ack.error")
		mAckErrorF    = r.stats.GetCounter("input.ack.error")
		mConn         = r.stats.GetCounter("input." + r.typeStr + ".connection.up")
		mConnF        = r.stats.GetCounter("input.connection.up")
		mFailedConn   = r.stats.GetCounter("input." + r.typeStr + ".connection.failed")
		mFailedConnF  = r.stats.GetCounter("input.connection.failed")
		mLostConn     = r.stats.GetCounter("input." + r.typeStr + ".connection.lost")
		mLostConnF    = r.stats.GetCounter("input.connection.lost")
		mLatency      = r.stats.GetTimer("input." + r.typeStr + ".latency")
		mLatencyF     = r.stats.GetTimer("input.latency")
	)

	defer func() {
		err := r.reader.WaitForClose(time.Second)
		for ; err != nil; err = r.reader.WaitForClose(time.Second) {
		}
		mRunning.Decr(1)
		mRunningF.Decr(1)

		close(r.transactions)
		close(r.closedChan)
	}()
	mRunning.Incr(1)
	mRunningF.Incr(1)

	for {
		if err := r.reader.Connect(); err != nil {
			if err == types.ErrTypeClosed {
				return
			}
			r.log.Errorf("Failed to connect to %v: %v\n", r.typeStr, err)
			mFailedConn.Incr(1)
			mFailedConnF.Incr(1)
			select {
			case <-time.After(time.Second):
			case <-r.closeChan:
				return
			}
		} else {
			break
		}
	}
	mConn.Incr(1)
	mConnF.Incr(1)

	for atomic.LoadInt32(&r.running) == 1 {
		msg, err := r.reader.Read()

		// If our reader says it is not connected.
		if err == types.ErrNotConnected {
			mLostConn.Incr(1)
			mLostConnF.Incr(1)

			// Continue to try to reconnect while still active.
			for atomic.LoadInt32(&r.running) == 1 {
				if err = r.reader.Connect(); err != nil {
					// Close immediately if our reader is closed.
					if err == types.ErrTypeClosed {
						return
					}

					r.log.Errorf("Failed to reconnect to %v: %v\n", r.typeStr, err)
					mFailedConn.Incr(1)
					mFailedConnF.Incr(1)
					select {
					case <-time.After(time.Second):
					case <-r.closeChan:
						return
					}
				} else if msg, err = r.reader.Read(); err != types.ErrNotConnected {
					mConn.Incr(1)
					mConnF.Incr(1)
					break
				}
			}
		}

		// Close immediately if our reader is closed.
		if err == types.ErrTypeClosed {
			return
		}

		if err != nil || msg == nil {
			if err != types.ErrTimeout && err != types.ErrNotConnected {
				mReadError.Incr(1)
				mReadErrorF.Incr(1)
				r.log.Errorf("Failed to read message: %v\n", err)
			}
			continue
		} else {
			mCount.Incr(1)
			mCountF.Incr(1)
			mReadSuccess.Incr(1)
			mReadSuccessF.Incr(1)
		}

		select {
		case r.transactions <- types.NewTransaction(msg, r.responses):
		case <-r.closeChan:
			return
		}

		select {
		case res, open := <-r.responses:
			if !open {
				return
			}
			if res.Error() != nil {
				mSendError.Incr(1)
				mSendErrorF.Incr(1)
			} else {
				mSendSuccess.Incr(1)
				mSendSuccessF.Incr(1)
			}
			if res.Error() != nil || !res.SkipAck() {
				if err = r.reader.Acknowledge(res.Error()); err != nil {
					mAckError.Incr(1)
					mAckErrorF.Incr(1)
				} else {
					tTaken := time.Since(msg.CreatedAt()).Nanoseconds()
					mLatency.Timing(tTaken)
					mLatencyF.Timing(tTaken)
					mAckSuccess.Incr(1)
					mAckSuccessF.Incr(1)
				}
			}
		case <-r.closeChan:
			return
		}
	}
}

// TransactionChan returns the transactions channel.
func (r *Reader) TransactionChan() <-chan types.Transaction {
	return r.transactions
}

// CloseAsync shuts down the Reader input and stops processing requests.
func (r *Reader) CloseAsync() {
	if atomic.CompareAndSwapInt32(&r.running, 1, 0) {
		r.reader.CloseAsync()
		close(r.closeChan)
	}
}

// WaitForClose blocks until the Reader input has closed down.
func (r *Reader) WaitForClose(timeout time.Duration) error {
	select {
	case <-r.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
