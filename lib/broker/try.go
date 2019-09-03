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

package broker

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Try is a broker that implements types.Consumer and attempts to send each
// message to a single output, but on failure will attempt the next output in
// the list.
type Try struct {
	running int32

	stats metrics.Type

	transactions <-chan types.Transaction

	outputTsChans []chan types.Transaction
	outputs       []types.Output

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewTry creates a new Try type by providing consumers.
func NewTry(outputs []types.Output, stats metrics.Type) (*Try, error) {
	t := &Try{
		running:      1,
		stats:        stats,
		transactions: nil,
		outputs:      outputs,
		closedChan:   make(chan struct{}),
		closeChan:    make(chan struct{}),
	}
	t.outputTsChans = make([]chan types.Transaction, len(t.outputs))
	for i := range t.outputTsChans {
		t.outputTsChans[i] = make(chan types.Transaction)
		if err := t.outputs[i].Consume(t.outputTsChans[i]); err != nil {
			return nil, err
		}
	}
	return t, nil
}

//------------------------------------------------------------------------------

// Consume assigns a new messages channel for the broker to read.
func (t *Try) Consume(ts <-chan types.Transaction) error {
	if t.transactions != nil {
		return types.ErrAlreadyStarted
	}
	t.transactions = ts

	go t.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (t *Try) Connected() bool {
	for _, out := range t.outputs {
		if !out.Connected() {
			return false
		}
	}
	return true
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (t *Try) loop() {
	defer func() {
		for _, c := range t.outputTsChans {
			close(c)
		}
		close(t.closedChan)
	}()

	var (
		mMsgsRcvd = t.stats.GetCounter("messages.received")
		mErrs     = []metrics.StatCounter{}
	)
	for i := range t.outputs {
		mErrs = append(mErrs, t.stats.GetCounter(fmt.Sprintf("broker.outputs.%v.failed", i)))
	}

	var open bool
	resChan := make(chan types.Response)
	for atomic.LoadInt32(&t.running) == 1 {
		var ts types.Transaction
		var res types.Response
		select {
		case ts, open = <-t.transactions:
			if !open {
				return
			}
		case <-t.closeChan:
			return
		}
		mMsgsRcvd.Incr(1)

	triesLoop:
		for i, ot := range t.outputTsChans {
			select {
			case ot <- types.NewTransaction(ts.Payload, resChan):
			case <-t.closeChan:
				return
			}
			select {
			case res, open = <-resChan:
				if !open {
					return
				}
				if res.Error() != nil {
					mErrs[i].Incr(1)
				} else {
					break triesLoop
				}
			case <-t.closeChan:
				return
			}
		}
		select {
		case ts.ResponseChan <- res:
		case <-t.closeChan:
			return
		}
	}
}

// CloseAsync shuts down the Try broker and stops processing requests.
func (t *Try) CloseAsync() {
	if atomic.CompareAndSwapInt32(&t.running, 1, 0) {
		close(t.closeChan)
	}
}

// WaitForClose blocks until the Try broker has closed down.
func (t *Try) WaitForClose(timeout time.Duration) error {
	select {
	case <-t.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
