/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package broker

import (
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

// RoundRobin - A broker that implements types.Consumer and sends each message out to a single
// consumer chosen from an array in round-robin fashion. Consumers that apply backpressure will
// block all consumers.
type RoundRobin struct {
	running int32

	stats metrics.Aggregator

	messages     <-chan types.Message
	responseChan chan types.Response

	outputMsgChans []chan types.Message
	outputs        []types.Consumer

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewRoundRobin - Create a new RoundRobin type by providing consumers.
func NewRoundRobin(outputs []types.Consumer, stats metrics.Aggregator) (*RoundRobin, error) {
	o := &RoundRobin{
		running:      1,
		stats:        stats,
		messages:     nil,
		responseChan: make(chan types.Response),
		outputs:      outputs,
		closedChan:   make(chan struct{}),
		closeChan:    make(chan struct{}),
	}
	o.outputMsgChans = make([]chan types.Message, len(o.outputs))
	for i := range o.outputMsgChans {
		o.outputMsgChans[i] = make(chan types.Message)
		if err := o.outputs[i].StartReceiving(o.outputMsgChans[i]); err != nil {
			return nil, err
		}
	}
	return o, nil
}

//--------------------------------------------------------------------------------------------------

// StartReceiving - Assigns a new messages channel for the broker to read.
func (o *RoundRobin) StartReceiving(msgs <-chan types.Message) error {
	if o.messages != nil {
		return types.ErrAlreadyStarted
	}
	o.messages = msgs

	go o.loop()
	return nil
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to many outputs.
func (o *RoundRobin) loop() {
	defer func() {
		for _, c := range o.outputMsgChans {
			close(c)
		}
		close(o.responseChan)
		close(o.closedChan)
	}()

	i := 0
	open := false
	for atomic.LoadInt32(&o.running) == 1 {
		var msg types.Message
		if msg, open = <-o.messages; !open {
			return
		}
		o.stats.Incr("broker.round_robin.messages.received", 1)
		select {
		case o.outputMsgChans[i] <- msg:
		case <-o.closeChan:
			return
		}

		var res types.Response
		select {
		case res, open = <-o.outputs[i].ResponseChan():
			if !open {
				return
			}
		case <-o.closeChan:
			return
		}
		if res.Error() != nil {
			o.stats.Incr("broker.round_robin.output.error", 1)
		} else {
			o.stats.Incr("broker.round_robin.messages.sent", 1)
		}
		o.responseChan <- res

		i++
		if i >= len(o.outputMsgChans) {
			i = 0
		}
	}
}

// ResponseChan - Returns the response channel.
func (o *RoundRobin) ResponseChan() <-chan types.Response {
	return o.responseChan
}

// CloseAsync - Shuts down the RoundRobin broker and stops processing requests.
func (o *RoundRobin) CloseAsync() {
	if atomic.CompareAndSwapInt32(&o.running, 1, 0) {
		close(o.closeChan)
	}
}

// WaitForClose - Blocks until the RoundRobin broker has closed down.
func (o *RoundRobin) WaitForClose(timeout time.Duration) error {
	select {
	case <-o.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
