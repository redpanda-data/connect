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
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

// wrappedMsg used to forward an inputs message and res channel to the FanIn
// broker.
type wrappedMsg struct {
	msg     types.Message
	resChan chan<- types.Response
}

//------------------------------------------------------------------------------

// FanIn is a broker that implements types.Producer, takes an array of inputs
// and routes them through a single message channel.
type FanIn struct {
	stats metrics.Type

	messageChan  chan types.Message
	responseChan <-chan types.Response

	closables       []types.Closable
	wrappedMsgsChan chan wrappedMsg
	inputClosedChan chan int
	inputMap        map[int]struct{}

	closedChan chan struct{}
}

// NewFanIn creates a new FanIn type by providing inputs.
func NewFanIn(inputs []types.Producer, stats metrics.Type) (*FanIn, error) {
	i := &FanIn{
		stats: stats,

		messageChan:  make(chan types.Message),
		responseChan: nil,

		wrappedMsgsChan: make(chan wrappedMsg),
		inputClosedChan: make(chan int),
		inputMap:        make(map[int]struct{}),

		closables:  []types.Closable{},
		closedChan: make(chan struct{}),
	}

	for n, input := range inputs {
		if closable, ok := input.(types.Closable); ok {
			i.closables = append(i.closables, closable)
		}

		// Keep track of # open inputs
		i.inputMap[n] = struct{}{}

		// Create unique response channel for each input
		resChan := make(chan types.Response)
		if err := inputs[n].StartListening(resChan); err != nil {
			return nil, err
		}

		// Launch goroutine that async writes input into single channel
		go func(index int) {
			defer func() {
				// If the input closes we need to signal to the broker
				i.inputClosedChan <- index
			}()
			for {
				in, open := <-inputs[index].MessageChan()
				if !open {
					return
				}
				i.wrappedMsgsChan <- wrappedMsg{
					msg:     in,
					resChan: resChan,
				}
			}
		}(n)
	}
	return i, nil
}

//------------------------------------------------------------------------------

// StartListening assigns a new responses channel for the broker to read.
func (i *FanIn) StartListening(responseChan <-chan types.Response) error {
	if i.responseChan != nil {
		return types.ErrAlreadyStarted
	}
	i.responseChan = responseChan

	go i.loop()
	return nil
}

// MessageChan returns the channel used for consuming messages from this broker.
func (i *FanIn) MessageChan() <-chan types.Message {
	return i.messageChan
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (i *FanIn) loop() {
	defer func() {
		close(i.wrappedMsgsChan)
		close(i.inputClosedChan)
		close(i.messageChan)
		close(i.closedChan)
	}()

	for len(i.inputMap) > 0 {
		select {
		case wrap := <-i.wrappedMsgsChan:
			i.stats.Incr("broker.fan_in.messages.received", 1)
			i.messageChan <- wrap.msg
			// TODO: If our output closes it won't be propagated.
			wrap.resChan <- <-i.responseChan
		case index := <-i.inputClosedChan:
			delete(i.inputMap, index)
		}
	}
}

// CloseAsync shuts down the FanIn broker and stops processing requests.
func (i *FanIn) CloseAsync() {
	for _, closable := range i.closables {
		closable.CloseAsync()
	}
}

// WaitForClose blocks until the FanIn broker has closed down.
func (i *FanIn) WaitForClose(timeout time.Duration) error {
	select {
	case <-i.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
