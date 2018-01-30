// Copyright (c) 2014 Ashley Jeffs
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

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/Jeffail/benthos/lib/util/throttle"
)

//------------------------------------------------------------------------------

// FanOutConfig is config values for the fan out type.
type FanOutConfig struct {
}

// NewFanOutConfig creates a FanOutConfig fully populated with default values.
func NewFanOutConfig() FanOutConfig {
	return FanOutConfig{}
}

//------------------------------------------------------------------------------

// FanOut is a broker that implements types.Consumer and broadcasts each message
// out to an array of outputs.
type FanOut struct {
	running int32

	logger log.Modular
	stats  metrics.Type

	conf FanOutConfig

	throt *throttle.Type

	messages     <-chan types.Message
	responseChan chan types.Response

	outputMsgChans []chan types.Message
	outputs        []types.Consumer
	outputNs       []int

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewFanOut creates a new FanOut type by providing outputs.
func NewFanOut(
	conf FanOutConfig, outputs []types.Consumer, logger log.Modular, stats metrics.Type,
) (*FanOut, error) {
	o := &FanOut{
		running:      1,
		stats:        stats,
		logger:       logger.NewModule(".broker.fan_out"),
		conf:         conf,
		messages:     nil,
		responseChan: make(chan types.Response),
		outputs:      outputs,
		outputNs:     []int{},
		closedChan:   make(chan struct{}),
		closeChan:    make(chan struct{}),
	}
	o.throt = throttle.New(throttle.OptCloseChan(o.closeChan))

	o.outputMsgChans = make([]chan types.Message, len(o.outputs))
	for i := range o.outputMsgChans {
		o.outputNs = append(o.outputNs, i)
		o.outputMsgChans[i] = make(chan types.Message)
		if err := o.outputs[i].StartReceiving(o.outputMsgChans[i]); err != nil {
			return nil, err
		}
	}
	return o, nil
}

//------------------------------------------------------------------------------

// StartReceiving assigns a new messages channel for the broker to read.
func (o *FanOut) StartReceiving(msgs <-chan types.Message) error {
	if o.messages != nil {
		return types.ErrAlreadyStarted
	}
	o.messages = msgs

	go o.loop()
	return nil
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (o *FanOut) loop() {
	defer func() {
		for _, c := range o.outputMsgChans {
			close(c)
		}
		close(o.responseChan)
		close(o.closedChan)
	}()

	for atomic.LoadInt32(&o.running) == 1 {
		var msg types.Message
		var open bool

		select {
		case msg, open = <-o.messages:
			if !open {
				return
			}
		case <-o.closeChan:
			return
		}
		o.stats.Incr("broker.fan_out.messages.received", 1)

		outputTargets := o.outputNs
		for len(outputTargets) > 0 {
			for _, i := range outputTargets {
				// Perform a copy here as it could be dangerous to release the
				// same message to parallel processor pipelines.
				msgCopy := msg.ShallowCopy()
				select {
				case o.outputMsgChans[i] <- msgCopy:
				case <-o.closeChan:
					return
				}
			}
			newTargets := []int{}
			for _, i := range outputTargets {
				var res types.Response
				select {
				case res, open = <-o.outputs[i].ResponseChan():
					if !open {
						// If any of our outputs is closed then we exit
						// completely. We want to avoid silently starving a
						// particular output.
						o.logger.Warnln("Closing fan_out broker due to closed output")
						return
					} else if res.Error() != nil {
						newTargets = append(newTargets, i)
						o.logger.Errorf("Failed to dispatch fan out message: %v\n", res.Error())
						o.stats.Incr("broker.fan_out.output.error", 1)
						if !o.throt.Retry() {
							return
						}
					} else {
						o.throt.Reset()
						o.stats.Incr("broker.fan_out.messages.sent", 1)
					}
				case <-o.closeChan:
					return
				}
			}
			outputTargets = newTargets
		}
		select {
		case o.responseChan <- types.NewSimpleResponse(nil):
		case <-o.closeChan:
			return
		}
	}
}

// ResponseChan returns the response channel.
func (o *FanOut) ResponseChan() <-chan types.Response {
	return o.responseChan
}

// CloseAsync shuts down the FanOut broker and stops processing requests.
func (o *FanOut) CloseAsync() {
	if atomic.CompareAndSwapInt32(&o.running, 1, 0) {
		close(o.closeChan)
	}
}

// WaitForClose blocks until the FanOut broker has closed down.
func (o *FanOut) WaitForClose(timeout time.Duration) error {
	select {
	case <-o.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
