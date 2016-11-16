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
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

// inputWrapperMsg - Used to forward an inputs message and res channel to the FanIn broker.
type inputWrapperMsg struct {
	msg     types.Message
	resChan chan<- types.Response
}

/*
inputWrapper - Used by the FanIn broker to forward messages from arbitrary inputs into one channel,
responses have to be returned to the correct input, therefore the inputWrapper has to manage its own
state.
*/
type inputWrapper struct {
	input       types.Producer
	res         chan types.Response
	msgOut      chan<- inputWrapperMsg
	closeSigOut chan<- *inputWrapper
}

// loop - Internal loop of the inputWrapper.
func (i *inputWrapper) loop() {
	defer func() {
		i.closeSigOut <- i
	}()
	for {
		in, open := <-i.input.MessageChan()
		if !open {
			return
		}
		i.msgOut <- inputWrapperMsg{
			msg:     in,
			resChan: i.res,
		}
	}
}

//--------------------------------------------------------------------------------------------------

/*
FanIn - A broker that implements types.Producer, takes an array of inputs and routes them through a
single message channel.
*/
type FanIn struct {
	stats metrics.Type

	messageChan  chan types.Message
	responseChan <-chan types.Response

	closables         []types.Closable
	inputWrappersChan chan inputWrapperMsg
	inputClosedChan   chan *inputWrapper
	inputWrappers     map[*inputWrapper]struct{}

	closedChan chan struct{}
}

// NewFanIn - Create a new FanIn type by providing inputs.
func NewFanIn(inputs []types.Producer, stats metrics.Type) (*FanIn, error) {
	i := &FanIn{
		stats:             stats,
		messageChan:       make(chan types.Message),
		responseChan:      nil,
		closables:         []types.Closable{},
		inputWrappersChan: make(chan inputWrapperMsg),
		inputClosedChan:   make(chan *inputWrapper),
		inputWrappers:     make(map[*inputWrapper]struct{}),
		closedChan:        make(chan struct{}),
	}

	for n := range inputs {
		if closable, ok := inputs[n].(types.Closable); ok {
			i.closables = append(i.closables, closable)
		}
		wrapper := &inputWrapper{
			input:       inputs[n],
			res:         make(chan types.Response),
			msgOut:      i.inputWrappersChan,
			closeSigOut: i.inputClosedChan,
		}
		i.inputWrappers[wrapper] = struct{}{}
		if err := inputs[n].StartListening(wrapper.res); err != nil {
			return nil, err
		}
	}
	for in := range i.inputWrappers {
		go in.loop()
	}

	return i, nil
}

//--------------------------------------------------------------------------------------------------

// StartListening - Assigns a new responses channel for the broker to read.
func (i *FanIn) StartListening(responseChan <-chan types.Response) error {
	if i.responseChan != nil {
		return types.ErrAlreadyStarted
	}
	i.responseChan = responseChan

	go i.loop()
	return nil
}

// MessageChan - Returns the channel used for consuming messages from this broker.
func (i *FanIn) MessageChan() <-chan types.Message {
	return i.messageChan
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to many outputs.
func (i *FanIn) loop() {
	defer func() {
		close(i.inputWrappersChan)
		close(i.inputClosedChan)
		close(i.messageChan)
		close(i.closedChan)
	}()

	for len(i.inputWrappers) > 0 {
		select {
		case wrap := <-i.inputWrappersChan:
			i.stats.Incr("broker.fan_in.messages.received", 1)
			i.messageChan <- wrap.msg
			wrap.resChan <- <-i.responseChan
		case ptr := <-i.inputClosedChan:
			delete(i.inputWrappers, ptr)
		}
	}
}

// CloseAsync - Shuts down the FanIn broker and stops processing requests.
func (i *FanIn) CloseAsync() {
	for _, closable := range i.closables {
		closable.CloseAsync()
	}
}

// WaitForClose - Blocks until the FanIn broker has closed down.
func (i *FanIn) WaitForClose(timeout time.Duration) error {
	select {
	case <-i.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
