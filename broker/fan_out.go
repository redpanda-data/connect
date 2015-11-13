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

	"github.com/jeffail/benthos/types"
)

//--------------------------------------------------------------------------------------------------

// FanOut - A broker that implements types.Output and broadcasts each message out to an array of
// outputs.
type FanOut struct {
	messages     <-chan types.Message
	responseChan chan types.Response

	outputMsgChans []chan types.Message
	outputsChan    chan []types.Output
	outputs        []types.Output

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewFanOut - Create a new FanOut type by providing outputs and a messages channel.
func NewFanOut(outputs []types.Output) *FanOut {
	o := &FanOut{
		messages:     nil,
		responseChan: make(chan types.Response),
		outputsChan:  make(chan []types.Output),
		outputs:      outputs,
		closedChan:   make(chan struct{}),
		closeChan:    make(chan struct{}),
	}

	o.createMessageChans()
	return o
}

//--------------------------------------------------------------------------------------------------

func (o *FanOut) createMessageChans() error {
	if len(o.outputMsgChans) > 0 {
		for i := range o.outputMsgChans {
			close(o.outputMsgChans[i])
		}
	}
	o.outputMsgChans = make([]chan types.Message, len(o.outputs))
	for i := range o.outputMsgChans {
		o.outputMsgChans[i] = make(chan types.Message)
		if err := o.outputs[i].StartReceiving(o.outputMsgChans[i]); err != nil {
			return err
		}
	}
	return nil
}

//--------------------------------------------------------------------------------------------------

// StartReceiving - Assigns a new messages channel for the broker to read.
func (o *FanOut) StartReceiving(msgs <-chan types.Message) error {
	if o.messages != nil {
		return types.ErrAlreadyStarted
	}
	o.messages = msgs

	go o.loop()
	return nil
}

// SetOutputs - Set the broker agents.
func (o *FanOut) SetOutputs(outputs []types.Output) {
	o.outputsChan <- outputs
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to many outputs.
func (o *FanOut) loop() {
	running := true
	for running {
		select {
		case msg, open := <-o.messages:
			// If the read channel is closed we can continue and wait for a replacement.
			if !open {
				o.messages = nil
			} else {
				responses := types.NewMappedResponse()
				for i := range o.outputMsgChans {
					o.outputMsgChans[i] <- msg
				}
				for i := range o.outputs {
					if r := <-o.outputs[i].ResponseChan(); r.Error() != nil {
						responses.Errors[i] = r.Error()
					}
				}
				o.responseChan <- responses
			}
		case outputs, open := <-o.outputsChan:
			if running = open; running {
				o.outputs = outputs
				o.createMessageChans()
			}
		case _, running = <-o.closeChan:
		}
	}

	for _, c := range o.outputMsgChans {
		close(c)
	}

	close(o.responseChan)
	close(o.closedChan)
}

// ResponseChan - Returns the response channel.
func (o *FanOut) ResponseChan() <-chan types.Response {
	return o.responseChan
}

// CloseAsync - Shuts down the FanOut broker and stops processing requests.
func (o *FanOut) CloseAsync() {
	close(o.closeChan)
	close(o.outputsChan)
}

// WaitForClose - Blocks until the FanOut broker has closed down.
func (o *FanOut) WaitForClose(timeout time.Duration) error {
	select {
	case <-o.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
