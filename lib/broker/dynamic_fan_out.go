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

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
)

//------------------------------------------------------------------------------

// DynamicOutput is an interface of output types that must be closable.
type DynamicOutput interface {
	types.Consumer
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

// outputWithResChan is a struct containing both an output and the response chan
// it reads from.
type outputWithResChan struct {
	tsChan  chan types.Transaction
	resChan chan types.Response
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

	transactions <-chan types.Transaction

	newOutputChan chan wrappedOutput
	outputs       map[string]outputWithResChan

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
		log:           logger,
		onAdd:         func(l string) {},
		onRemove:      func(l string) {},
		transactions:  nil,
		newOutputChan: make(chan wrappedOutput),
		outputs:       make(map[string]outputWithResChan, len(outputs)),
		closedChan:    make(chan struct{}),
		closeChan:     make(chan struct{}),
	}
	for _, opt := range options {
		opt(d)
	}
	d.throt = throttle.New(throttle.OptCloseChan(d.closeChan))

	mAddErr := d.stats.GetCounter("output.add.error")
	for k, v := range outputs {
		if err := d.addOutput(k, v); err != nil {
			d.log.Errorf("Failed to initialise dynamic output '%v': %v\n", k, err)
			mAddErr.Incr(1)
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

// Consume assigns a new transactions channel for the broker to read.
func (d *DynamicFanOut) Consume(transactions <-chan types.Transaction) error {
	if d.transactions != nil {
		return types.ErrAlreadyStarted
	}
	d.transactions = transactions

	go d.loop()
	return nil
}

//------------------------------------------------------------------------------

func (d *DynamicFanOut) addOutput(ident string, output DynamicOutput) error {
	if _, exists := d.outputs[ident]; exists {
		return fmt.Errorf("output key '%v' already exists", ident)
	}

	ow := outputWithResChan{
		tsChan:  make(chan types.Transaction),
		resChan: make(chan types.Response),
		output:  output,
	}

	if err := output.Consume(ow.tsChan); err != nil {
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

	close(ow.tsChan)
	delete(d.outputs, ident)
	return nil
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (d *DynamicFanOut) loop() {
	defer func() {
		for _, ow := range d.outputs {
			ow.output.CloseAsync()
			close(ow.tsChan)
		}
		for _, ow := range d.outputs {
			if err := ow.output.WaitForClose(time.Second); err != nil {
				for err != nil {
					err = ow.output.WaitForClose(time.Second)
				}
			}
		}
		d.outputs = map[string]outputWithResChan{}
		close(d.closedChan)
	}()

	var (
		mCount      = d.stats.GetCounter("count")
		mRemoveErr  = d.stats.GetCounter("output.remove.error")
		mRemoveSucc = d.stats.GetCounter("output.remove.success")
		mAddErr     = d.stats.GetCounter("output.add.error")
		mAddSucc    = d.stats.GetCounter("output.add.success")
		mMsgsRcd    = d.stats.GetCounter("messages.received")
		mOutputErr  = d.stats.GetCounter("output.error")
		mMsgsSnt    = d.stats.GetCounter("messages.sent")
	)

	for atomic.LoadInt32(&d.running) == 1 {
		var ts types.Transaction
		var open bool

		// Only actually consume messages if we have at least one output.
		var tsChan <-chan types.Transaction
		if len(d.outputs) > 0 {
			tsChan = d.transactions
		}

		// Either attempt to read the next message or listen for changes to our
		// outputs and apply them.
		select {
		case wrappedOutput, open := <-d.newOutputChan:
			if !open {
				return
			}
			mCount.Incr(1)

			if _, exists := d.outputs[wrappedOutput.Name]; exists {
				if err := d.removeOutput(wrappedOutput.Name, wrappedOutput.Timeout); err != nil {
					mRemoveErr.Incr(1)
					d.log.Errorf("Failed to stop old copy of dynamic output '%v': %v\n", wrappedOutput.Name, err)
					wrappedOutput.ResChan <- err
					continue
				}
				mRemoveSucc.Incr(1)
				d.onRemove(wrappedOutput.Name)
			}
			if wrappedOutput.Output == nil {
				wrappedOutput.ResChan <- nil
			} else {
				err := d.addOutput(wrappedOutput.Name, wrappedOutput.Output)
				if err != nil {
					mAddErr.Incr(1)
					d.log.Errorf("Failed to start new dynamic output '%v': %v\n", wrappedOutput.Name, err)
				} else {
					mAddSucc.Incr(1)
					d.onAdd(wrappedOutput.Name)
				}
				wrappedOutput.ResChan <- err
			}
			continue
		case ts, open = <-tsChan:
			if !open {
				return
			}
		case <-d.closeChan:
			return
		}
		mMsgsRcd.Incr(1)

		// If we received a message attempt to send it to each output.
		remainingTargets := make(map[string]outputWithResChan, len(d.outputs))
		for k, v := range d.outputs {
			remainingTargets[k] = v
		}
		for len(remainingTargets) > 0 {
			for _, v := range remainingTargets {
				// Perform a copy here as it could be dangerous to release the
				// same message to parallel processor pipelines.
				msgCopy := ts.Payload.Copy()
				select {
				case v.tsChan <- types.NewTransaction(msgCopy, v.resChan):
				case <-d.closeChan:
					return
				}
			}
			for k, v := range remainingTargets {
				var res types.Response
				select {
				case res, open = <-v.resChan:
					if !open {
						d.log.Warnf("Dynamic output '%v' has closed\n", k)
						d.removeOutput(k, time.Second)
						delete(remainingTargets, k)
					} else if res.Error() != nil {
						d.log.Errorf("Failed to dispatch dynamic fan out message: %v\n", res.Error())
						mOutputErr.Incr(1)
						if !d.throt.Retry() {
							return
						}
					} else {
						d.throt.Reset()
						mMsgsSnt.Incr(1)
						delete(remainingTargets, k)
					}
				case <-d.closeChan:
					return
				}
			}
		}
		select {
		case ts.ResponseChan <- response.NewAck():
		case <-d.closeChan:
			return
		}
	}
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (d *DynamicFanOut) Connected() bool {
	// Always return true as this is fuzzy right now.
	return true
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
