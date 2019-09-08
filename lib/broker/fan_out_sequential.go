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

package broker

import (
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
)

//------------------------------------------------------------------------------

// FanOutSequential is a broker that implements types.Consumer and broadcasts
// each message out to an array of outputs, but does so sequentially, only
// proceeding onto an output when the preceding output has successfully
// reported message receipt.
type FanOutSequential struct {
	running int32

	logger log.Modular
	stats  metrics.Type

	throt *throttle.Type

	transactions <-chan types.Transaction

	outputTsChans  []chan types.Transaction
	outputResChans []chan types.Response
	outputs        []types.Output
	outputNs       []int

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewFanOutSequential creates a new FanOutSequential type by providing outputs.
func NewFanOutSequential(
	outputs []types.Output, logger log.Modular, stats metrics.Type,
) (*FanOutSequential, error) {
	o := &FanOutSequential{
		running:      1,
		stats:        stats,
		logger:       logger,
		transactions: nil,
		outputs:      outputs,
		outputNs:     []int{},
		closedChan:   make(chan struct{}),
		closeChan:    make(chan struct{}),
	}
	o.throt = throttle.New(throttle.OptCloseChan(o.closeChan))

	o.outputTsChans = make([]chan types.Transaction, len(o.outputs))
	o.outputResChans = make([]chan types.Response, len(o.outputs))
	for i := range o.outputTsChans {
		o.outputNs = append(o.outputNs, i)
		o.outputTsChans[i] = make(chan types.Transaction)
		o.outputResChans[i] = make(chan types.Response)
		if err := o.outputs[i].Consume(o.outputTsChans[i]); err != nil {
			return nil, err
		}
	}
	return o, nil
}

//------------------------------------------------------------------------------

// Consume assigns a new transactions channel for the broker to read.
func (o *FanOutSequential) Consume(transactions <-chan types.Transaction) error {
	if o.transactions != nil {
		return types.ErrAlreadyStarted
	}
	o.transactions = transactions

	go o.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (o *FanOutSequential) Connected() bool {
	for _, out := range o.outputs {
		if !out.Connected() {
			return false
		}
	}
	return true
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (o *FanOutSequential) loop() {
	defer func() {
		for _, c := range o.outputTsChans {
			close(c)
		}
		close(o.closedChan)
	}()

	for atomic.LoadInt32(&o.running) == 1 {
		var ts types.Transaction
		var open bool

		select {
		case ts, open = <-o.transactions:
			if !open {
				return
			}
		case <-o.closeChan:
			return
		}

		for _, i := range o.outputNs {
		retryLoop:
			for {
				msgCopy := ts.Payload.Copy()
				select {
				case o.outputTsChans[i] <- types.NewTransaction(msgCopy, o.outputResChans[i]):
				case <-o.closeChan:
					return
				}
				select {
				case res := <-o.outputResChans[i]:
					if res.Error() != nil {
						o.logger.Errorf("Failed to dispatch fan out message: %v\n", res.Error())
						if !o.throt.Retry() {
							return
						}
					} else {
						o.throt.Reset()
						break retryLoop
					}
				case <-o.closeChan:
					return
				}
			}
		}

		select {
		case ts.ResponseChan <- response.NewAck():
		case <-o.closeChan:
			return
		}
	}
}

// CloseAsync shuts down the FanOutSequential broker and stops processing requests.
func (o *FanOutSequential) CloseAsync() {
	if atomic.CompareAndSwapInt32(&o.running, 1, 0) {
		close(o.closeChan)
	}
}

// WaitForClose blocks until the FanOutSequential broker has closed down.
func (o *FanOutSequential) WaitForClose(timeout time.Duration) error {
	select {
	case <-o.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
