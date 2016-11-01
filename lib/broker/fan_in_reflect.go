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
	"reflect"
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

/*
FanInReflect - A broker that implements types.Producer, takes an array of inputs and routes them
through a single message channel using reflection.
*/
type FanInReflect struct {
	running int32

	stats metrics.Type

	messageChan  chan types.Message
	responseChan <-chan types.Response

	inputs   []types.Producer
	resChans []chan types.Response

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewFanInReflect - Create a new FanInReflect type by providing inputs.
func NewFanInReflect(inputs []types.Producer, stats metrics.Type) (*FanInReflect, error) {
	i := &FanInReflect{
		running:      1,
		stats:        stats,
		messageChan:  make(chan types.Message),
		responseChan: nil,
		inputs:       []types.Producer{},
		resChans:     []chan types.Response{},
		closedChan:   make(chan struct{}),
		closeChan:    make(chan struct{}),
	}

	for _, input := range inputs {
		i.inputs = append(i.inputs, input)
		resChan := make(chan types.Response)
		if err := input.StartListening(resChan); err != nil {
			return nil, err
		}
		i.resChans = append(i.resChans, resChan)
	}
	return i, nil
}

//--------------------------------------------------------------------------------------------------

// StartListening - Assigns a new responses channel for the broker to read.
func (i *FanInReflect) StartListening(responseChan <-chan types.Response) error {
	if i.responseChan != nil {
		return types.ErrAlreadyStarted
	}
	i.responseChan = responseChan

	go i.loop()
	return nil
}

// MessageChan - Returns the channel used for consuming messages from this broker.
func (i *FanInReflect) MessageChan() <-chan types.Message {
	return i.messageChan
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages from many inputs.
func (i *FanInReflect) loop() {
	defer func() {
		atomic.StoreInt32(&i.running, 0)

		for _, input := range i.inputs {
			if closable, ok := input.(types.Closable); ok {
				closable.CloseAsync()
			}
		}

		close(i.messageChan)
		close(i.closedChan)
	}()

	inputSelects := []reflect.SelectCase{}
	for _, input := range i.inputs {
		inputSelects = append(inputSelects, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(input.MessageChan()),
		})
	}
	for atomic.LoadInt32(&i.running) == 1 && len(i.inputs) > 0 {
		chosen, val, open := reflect.Select(inputSelects)
		if !open {
			inputSelects = append(inputSelects[:chosen], inputSelects[chosen+1:]...)
			i.inputs = append(i.inputs[:chosen], i.inputs[chosen+1:]...)
			i.resChans = append(i.resChans[:chosen], i.resChans[chosen+1:]...)
		} else {
			msg := val.Interface().(types.Message)

			i.stats.Incr("broker.fan_in.messages.received", 1)
			select {
			case i.messageChan <- msg:
			case <-i.closeChan:
				return
			}
			i.resChans[chosen] <- <-i.responseChan
		}
	}
}

// CloseAsync - Shuts down the FanInReflect broker and stops processing requests.
func (i *FanInReflect) CloseAsync() {
	for _, input := range i.inputs {
		if closable, ok := input.(types.Closable); ok {
			closable.CloseAsync()
		}
	}
	if atomic.CompareAndSwapInt32(&i.running, 1, 0) {
		close(i.closeChan)
	}
}

// WaitForClose - Blocks until the FanInReflect broker has closed down.
func (i *FanInReflect) WaitForClose(timeout time.Duration) error {
	select {
	case <-i.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
