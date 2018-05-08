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
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
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
		runningPath     = [2]string{"input." + r.typeStr + ".running", "input.running"}
		countPath       = [2]string{"input." + r.typeStr + ".count", "input.count"}
		readSuccessPath = [2]string{"input." + r.typeStr + ".read.success", "input.read.success"}
		readErrorPath   = [2]string{"input." + r.typeStr + ".read.error", "input.read.error"}
		sendSuccessPath = [2]string{"input." + r.typeStr + ".send.success", "input.send.success"}
		sendErrorPath   = [2]string{"input." + r.typeStr + ".send.error", "input.send.error"}
		ackSuccessPath  = [2]string{"input." + r.typeStr + ".ack.success", "input.ack.success"}
		ackErrorPath    = [2]string{"input." + r.typeStr + ".ack.error", "input.ack.error"}
		connPath        = [2]string{"input." + r.typeStr + ".connection.up", "input.connection.up"}
		failedConnPath  = [2]string{"input." + r.typeStr + ".connection.failed", "input.connection.failed"}
		lostConnPath    = [2]string{"input." + r.typeStr + ".connection.lost", "input.connection.lost"}
		latencyPath     = [2]string{"input." + r.typeStr + ".latency", "input.latency"}
	)

	defer func() {
		err := r.reader.WaitForClose(time.Second)
		for ; err != nil; err = r.reader.WaitForClose(time.Second) {
		}
		r.stats.Decr(runningPath[0], 1)
		r.stats.Decr(runningPath[1], 1)

		close(r.transactions)
		close(r.closedChan)
	}()
	r.stats.Incr(runningPath[0], 1)
	r.stats.Incr(runningPath[1], 1)

	for {
		if err := r.reader.Connect(); err != nil {
			if err == types.ErrTypeClosed {
				return
			}
			r.log.Errorf("Failed to connect to %v: %v\n", r.typeStr, err)
			r.stats.Incr(failedConnPath[0], 1)
			r.stats.Incr(failedConnPath[1], 1)
			select {
			case <-time.After(time.Second):
			case <-r.closeChan:
				return
			}
		} else {
			break
		}
	}
	r.stats.Incr(connPath[0], 1)
	r.stats.Incr(connPath[1], 1)

	for atomic.LoadInt32(&r.running) == 1 {
		msg, err := r.reader.Read()

		// If our reader says it is not connected.
		if err == types.ErrNotConnected {
			r.stats.Incr(lostConnPath[0], 1)
			r.stats.Incr(lostConnPath[1], 1)

			// Continue to try to reconnect while still active.
			for atomic.LoadInt32(&r.running) == 1 {
				if err = r.reader.Connect(); err != nil {
					// Close immediately if our reader is closed.
					if err == types.ErrTypeClosed {
						return
					}

					r.log.Errorf("Failed to reconnect to %v: %v\n", r.typeStr, err)
					r.stats.Incr(failedConnPath[0], 1)
					r.stats.Incr(failedConnPath[1], 1)
					select {
					case <-time.After(time.Second):
					case <-r.closeChan:
						return
					}
				} else if msg, err = r.reader.Read(); err != types.ErrNotConnected {
					r.stats.Incr(connPath[0], 1)
					r.stats.Incr(connPath[1], 1)
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
				r.stats.Incr(readErrorPath[0], 1)
				r.stats.Incr(readErrorPath[1], 1)
				r.log.Errorf("Failed to read message: %v\n", err)
			}
			continue
		} else {
			r.stats.Incr(countPath[0], 1)
			r.stats.Incr(countPath[1], 1)
			r.stats.Incr(readSuccessPath[0], 1)
			r.stats.Incr(readSuccessPath[1], 1)
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
				r.stats.Incr(sendErrorPath[0], 1)
				r.stats.Incr(sendErrorPath[1], 1)
			} else {
				r.stats.Incr(sendSuccessPath[0], 1)
				r.stats.Incr(sendSuccessPath[1], 1)
			}
			if res.Error() != nil || !res.SkipAck() {
				if err = r.reader.Acknowledge(res.Error()); err != nil {
					r.stats.Incr(ackErrorPath[0], 1)
					r.stats.Incr(ackErrorPath[1], 1)
				} else {
					tTaken := time.Since(msg.CreatedAt()).Nanoseconds()
					r.stats.Timing(latencyPath[0], tTaken)
					r.stats.Timing(latencyPath[1], tTaken)
					r.stats.Incr(ackSuccessPath[0], 1)
					r.stats.Incr(ackSuccessPath[1], 1)
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
