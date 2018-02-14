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
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

// DynamicInput is an interface of input types that must be closable.
type DynamicInput interface {
	types.MessageSender
	types.ResponderListener
	types.Closable
}

// wrappedInput is a struct that wraps a DynamicInput with an identifying name.
type wrappedInput struct {
	Name    string
	Input   DynamicInput
	Timeout time.Duration
	ResChan chan<- error
}

//------------------------------------------------------------------------------

// DynamicFanIn is a broker that implements types.Producer and manages a map of
// inputs to unique string identifiers, routing them through a single message
// channel. Inputs can be added and removed dynamically as the broker runs.
type DynamicFanIn struct {
	running int32

	stats metrics.Type
	log   log.Modular

	messageChan  chan types.Message
	responseChan <-chan types.Response

	wrappedMsgsChan chan wrappedMsg
	newInputChan    chan wrappedInput
	inputs          map[string]DynamicInput

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewDynamicFanIn creates a new DynamicFanIn type by providing an initial map
// map of inputs.
func NewDynamicFanIn(
	inputs map[string]DynamicInput,
	logger log.Modular,
	stats metrics.Type,
) (*DynamicFanIn, error) {
	d := &DynamicFanIn{
		running: 1,
		stats:   stats,
		log:     logger,

		messageChan:  make(chan types.Message),
		responseChan: nil,

		wrappedMsgsChan: make(chan wrappedMsg),
		newInputChan:    make(chan wrappedInput),
		inputs:          make(map[string]DynamicInput),

		closedChan: make(chan struct{}),
		closeChan:  make(chan struct{}),
	}

	for key, input := range inputs {
		d.addInput(key, input)
	}
	go d.managerLoop()
	return d, nil
}

// SetInput attempts to add a new input to the dynamic input broker. If an input
// already exists with the same identifier it will be closed and removed. If
// either action takes longer than the timeout period an error will be returned.
//
// A nil input is safe and will simply remove the previous input under the
// indentifier, if there was one.
func (d *DynamicFanIn) SetInput(ident string, input DynamicInput, timeout time.Duration) error {
	if atomic.LoadInt32(&d.running) != 1 {
		return types.ErrTypeClosed
	}
	resChan := make(chan error)
	select {
	case d.newInputChan <- wrappedInput{
		Name:    ident,
		Input:   input,
		ResChan: resChan,
		Timeout: timeout,
	}:
	case <-d.closeChan:
		return types.ErrTypeClosed
	}
	return <-resChan
}

//------------------------------------------------------------------------------

// StartListening assigns a new responses channel for the broker to read.
func (d *DynamicFanIn) StartListening(responseChan <-chan types.Response) error {
	if d.responseChan != nil {
		return types.ErrAlreadyStarted
	}
	d.responseChan = responseChan

	go d.readLoop()
	return nil
}

// MessageChan returns the channel used for consuming messages from this broker.
func (d *DynamicFanIn) MessageChan() <-chan types.Message {
	return d.messageChan
}

//------------------------------------------------------------------------------

func (d *DynamicFanIn) addInput(ident string, input DynamicInput) error {
	// Create unique response channel for each input
	resChan := make(chan types.Response)
	if err := input.StartListening(resChan); err != nil {
		return err
	}

	// Launch goroutine that async writes input into single channel
	go func(in DynamicInput, rChan chan types.Response) {
		cancelChan := make(chan struct{})
		defer close(cancelChan)
		for {
			in, open := <-input.MessageChan()
			if !open {
				return
			}
			d.wrappedMsgsChan <- wrappedMsg{
				msg:        in,
				cancelChan: cancelChan,
				resChan:    rChan,
			}
		}
	}(input, resChan)

	// Add new input to our map
	d.inputs[ident] = input

	return nil
}

func (d *DynamicFanIn) removeInput(ident string, timeout time.Duration) error {
	input, exists := d.inputs[ident]
	if !exists {
		// Nothing to do
		return nil
	}

	input.CloseAsync()
	if err := input.WaitForClose(timeout); err != nil {
		// Do NOT remove inputs from our map unless we are sure they are
		// closed.
		return err
	}

	delete(d.inputs, ident)
	return nil
}

// managerLoop is an internal loop that monitors new and dead input types.
func (d *DynamicFanIn) managerLoop() {
	defer func() {
		for _, i := range d.inputs {
			i.CloseAsync()
		}
		for _, i := range d.inputs {
			if err := i.WaitForClose(time.Second); err != nil {
				for err != nil {
					err = i.WaitForClose(time.Second)
				}
			}
		}
		d.inputs = make(map[string]DynamicInput)
		close(d.wrappedMsgsChan)
	}()

	for {
		select {
		case wrappedInput, open := <-d.newInputChan:
			if !open {
				return
			}
			d.stats.Incr("broker.dynamic_fan_in.input.count", 1)

			var err error
			if _, exists := d.inputs[wrappedInput.Name]; exists {
				if err = d.removeInput(wrappedInput.Name, wrappedInput.Timeout); err != nil {
					d.stats.Incr("broker.dynamic_fan_in.input.remove.error", 1)
					d.log.Errorf("Failed to stop old copy of dynamic input '%v': %v\n", wrappedInput.Name, err)
				} else {
					d.stats.Incr("broker.dynamic_fan_in.input.remove.success", 1)
				}
			}
			if err == nil && wrappedInput.Input != nil {
				// If the input is nil then we only wanted to remove the input.
				if err = d.addInput(wrappedInput.Name, wrappedInput.Input); err != nil {
					d.stats.Incr("broker.dynamic_fan_in.input.add.error", 1)
					d.log.Errorf("Failed to start new dynamic input '%v': %v\n", wrappedInput.Name, err)
				} else {
					d.stats.Incr("broker.dynamic_fan_in.input.add.success", 1)
				}
			}
			select {
			case wrappedInput.ResChan <- err:
			case <-d.closeChan:
				close(wrappedInput.ResChan)
				return
			}
		case <-d.closeChan:
			return
		}
	}
}

// readLoop is an internal loop that brokers multiple input streams into a
// single channel.
func (d *DynamicFanIn) readLoop() {
	defer func() {
		d.CloseAsync()
		close(d.messageChan)
		close(d.closedChan)
	}()

	for {
		select {
		case wrap, open := <-d.wrappedMsgsChan:
			if !open {
				return
			}

			d.stats.Incr("broker.dynamic_fan_in.messages.received", 1)
			d.messageChan <- wrap.msg

			res, open := <-d.responseChan
			if !open {
				return
			}

			select {
			case wrap.resChan <- res:
			case <-wrap.cancelChan:
			}
		}
	}
}

// CloseAsync shuts down the DynamicFanIn broker and stops processing requests.
func (d *DynamicFanIn) CloseAsync() {
	if atomic.CompareAndSwapInt32(&d.running, 1, 0) {
		close(d.closeChan)
	}
}

// WaitForClose blocks until the DynamicFanIn broker has closed down.
func (d *DynamicFanIn) WaitForClose(timeout time.Duration) error {
	select {
	case <-d.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
