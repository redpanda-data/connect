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

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/Jeffail/benthos/lib/util/throttle"
)

//------------------------------------------------------------------------------

// DynamicOutput is an interface of output types that must be closable.
type DynamicOutput interface {
	types.MessageReceiver
	types.Responder
	types.Closable
}

// wrappedOutput is a struct that wraps a DynamicOutput with an identifying
// name.
type wrappedOutput struct {
	Name    string
	Output  DynamicOutput
	Timeout time.Duration
	ResChan chan<- error
}

// outputWithMsgChan is a struct containing both an output and the message chan
// it reads from.
type outputWithMsgChan struct {
	msgChan chan types.Message
	output  DynamicOutput
}

//------------------------------------------------------------------------------

// DynamicFanOut is a broker that implements types.Consumer and broadcasts each
// message out to a dynamic map of outputs.
type DynamicFanOut struct {
	running int32

	log   log.Modular
	stats metrics.Type

	throt *throttle.Type

	onAdd    func(label string)
	onRemove func(label string)

	messages     <-chan types.Message
	responseChan chan types.Response

	newOutputChan chan wrappedOutput
	outputs       map[string]outputWithMsgChan

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewDynamicFanOut creates a new DynamicFanOut type by providing outputs.
func NewDynamicFanOut(
	outputs map[string]DynamicOutput,
	logger log.Modular,
	stats metrics.Type,
	options ...func(*DynamicFanOut),
) (*DynamicFanOut, error) {
	d := &DynamicFanOut{
		running:       1,
		stats:         stats,
		log:           logger.NewModule(".broker.dynamic_fan_out"),
		onAdd:         func(l string) {},
		onRemove:      func(l string) {},
		messages:      nil,
		responseChan:  make(chan types.Response),
		newOutputChan: make(chan wrappedOutput),
		outputs:       make(map[string]outputWithMsgChan, len(outputs)),
		closedChan:    make(chan struct{}),
		closeChan:     make(chan struct{}),
	}
	for _, opt := range options {
		opt(d)
	}
	d.throt = throttle.New(throttle.OptCloseChan(d.closeChan))

	for k, v := range outputs {
		if err := d.addOutput(k, v); err != nil {
			d.log.Errorf("Failed to initialise dynamic output '%v': %v\n", k, err)
			d.stats.Incr("broker.dynamic_fan_out.output.add.error", 1)
		} else {
			d.onAdd(k)
		}
	}
	return d, nil
}

// SetOutput attempts to add a new output to the dynamic output broker. If an
// output already exists with the same identifier it will be closed and removed.
// If either action takes longer than the timeout period an error will be
// returned.
//
// A nil output argument is safe and will simply remove the previous output
// under the indentifier, if there was one.
func (d *DynamicFanOut) SetOutput(ident string, output DynamicOutput, timeout time.Duration) error {
	if atomic.LoadInt32(&d.running) != 1 {
		return types.ErrTypeClosed
	}
	resChan := make(chan error)
	select {
	case d.newOutputChan <- wrappedOutput{
		Name:    ident,
		Output:  output,
		ResChan: resChan,
		Timeout: timeout,
	}:
	case <-d.closeChan:
		return types.ErrTypeClosed
	}
	return <-resChan
}

//------------------------------------------------------------------------------

// OptDynamicFanOutSetOnAdd sets the function that is called whenever a dynamic
// output is added.
func OptDynamicFanOutSetOnAdd(onAddFunc func(label string)) func(*DynamicFanOut) {
	return func(d *DynamicFanOut) {
		d.onAdd = onAddFunc
	}
}

// OptDynamicFanOutSetOnRemove sets the function that is called whenever a
// dynamic output is removed.
func OptDynamicFanOutSetOnRemove(onRemoveFunc func(label string)) func(*DynamicFanOut) {
	return func(d *DynamicFanOut) {
		d.onRemove = onRemoveFunc
	}
}

//------------------------------------------------------------------------------

// StartReceiving assigns a new messages channel for the broker to read.
func (d *DynamicFanOut) StartReceiving(msgs <-chan types.Message) error {
	if d.messages != nil {
		return types.ErrAlreadyStarted
	}
	d.messages = msgs

	go d.loop()
	return nil
}

//------------------------------------------------------------------------------

func (d *DynamicFanOut) addOutput(ident string, output DynamicOutput) error {
	if _, exists := d.outputs[ident]; exists {
		return fmt.Errorf("output key '%v' already exists", ident)
	}

	ow := outputWithMsgChan{
		msgChan: make(chan types.Message),
		output:  output,
	}

	if err := output.StartReceiving(ow.msgChan); err != nil {
		output.CloseAsync()
		return err
	}

	d.outputs[ident] = ow
	return nil
}

func (d *DynamicFanOut) removeOutput(ident string, timeout time.Duration) error {
	ow, exists := d.outputs[ident]
	if !exists {
		return nil
	}

	ow.output.CloseAsync()
	if err := ow.output.WaitForClose(timeout); err != nil {
		return err
	}

	close(ow.msgChan)
	delete(d.outputs, ident)
	return nil
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (d *DynamicFanOut) loop() {
	defer func() {
		for _, ow := range d.outputs {
			ow.output.CloseAsync()
			close(ow.msgChan)
		}
		for _, ow := range d.outputs {
			if err := ow.output.WaitForClose(time.Second); err != nil {
				for err != nil {
					err = ow.output.WaitForClose(time.Second)
				}
			}
		}
		d.outputs = map[string]outputWithMsgChan{}
		close(d.responseChan)
		close(d.closedChan)
	}()

	for atomic.LoadInt32(&d.running) == 1 {
		var msg types.Message
		var open bool

		// Only actually consume messages if we have at least one output.
		var msgChan <-chan types.Message
		if len(d.outputs) > 0 {
			msgChan = d.messages
		}

		// Either attempt to read the next message or listen for changes to our
		// outputs and apply them.
		select {
		case wrappedOutput, open := <-d.newOutputChan:
			if !open {
				return
			}
			d.stats.Incr("broker.dynamic_fan_out.output.count", 1)

			if _, exists := d.outputs[wrappedOutput.Name]; exists {
				if err := d.removeOutput(wrappedOutput.Name, wrappedOutput.Timeout); err != nil {
					d.stats.Incr("broker.dynamic_fan_out.output.remove.error", 1)
					d.log.Errorf("Failed to stop old copy of dynamic output '%v': %v\n", wrappedOutput.Name, err)
					wrappedOutput.ResChan <- err
					continue
				}
				d.stats.Incr("broker.dynamic_fan_out.output.remove.success", 1)
				d.onRemove(wrappedOutput.Name)
			}
			if wrappedOutput.Output == nil {
				wrappedOutput.ResChan <- nil
			} else {
				err := d.addOutput(wrappedOutput.Name, wrappedOutput.Output)
				if err != nil {
					d.stats.Incr("broker.dynamic_fan_out.output.add.error", 1)
					d.log.Errorf("Failed to start new dynamic output '%v': %v\n", wrappedOutput.Name, err)
				} else {
					d.stats.Incr("broker.dynamic_fan_out.output.add.success", 1)
					d.onAdd(wrappedOutput.Name)
				}
				wrappedOutput.ResChan <- err
			}
			continue
		case msg, open = <-msgChan:
			if !open {
				return
			}
		case <-d.closeChan:
			return
		}
		d.stats.Incr("broker.dynamic_fan_out.messages.received", 1)

		// If we received a message attempt to send it to each output.
		remainingTargets := make(map[string]outputWithMsgChan, len(d.outputs))
		for k, v := range d.outputs {
			remainingTargets[k] = v
		}
		for len(remainingTargets) > 0 {
			for _, v := range remainingTargets {
				// Perform a copy here as it could be dangerous to release the
				// same message to parallel processor pipelines.
				msgCopy := msg.ShallowCopy()
				select {
				case v.msgChan <- msgCopy:
				case <-d.closeChan:
					return
				}
			}
			for k, v := range remainingTargets {
				var res types.Response
				select {
				case res, open = <-v.output.ResponseChan():
					if !open {
						d.log.Warnf("Dynamic output '%v' has closed\n", k)
						d.removeOutput(k, time.Second)
						delete(remainingTargets, k)
					} else if res.Error() != nil {
						d.log.Errorf("Failed to dispatch dynamic fan out message: %v\n", res.Error())
						d.stats.Incr("broker.dynamic_fan_out.output.error", 1)
						if !d.throt.Retry() {
							return
						}
					} else {
						d.throt.Reset()
						d.stats.Incr("broker.dynamic_fan_out.messages.sent", 1)
						delete(remainingTargets, k)
					}
				case <-d.closeChan:
					return
				}
			}
		}
		select {
		case d.responseChan <- types.NewSimpleResponse(nil):
		case <-d.closeChan:
			return
		}
	}
}

// ResponseChan returns the response channel.
func (d *DynamicFanOut) ResponseChan() <-chan types.Response {
	return d.responseChan
}

// CloseAsync shuts down the DynamicFanOut broker and stops processing requests.
func (d *DynamicFanOut) CloseAsync() {
	if atomic.CompareAndSwapInt32(&d.running, 1, 0) {
		close(d.closeChan)
	}
}

// WaitForClose blocks until the DynamicFanOut broker has closed down.
func (d *DynamicFanOut) WaitForClose(timeout time.Duration) error {
	select {
	case <-d.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
