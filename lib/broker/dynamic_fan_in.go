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

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// DynamicInput is an interface of input types that must be closable.
type DynamicInput interface {
	types.Producer
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

	transactionChan chan types.Transaction
	internalTsChan  chan types.Transaction

	onAdd    func(label string)
	onRemove func(label string)

	newInputChan     chan wrappedInput
	inputs           map[string]DynamicInput
	inputClosedChans map[string]chan struct{}

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewDynamicFanIn creates a new DynamicFanIn type by providing an initial map
// map of inputs.
func NewDynamicFanIn(
	inputs map[string]DynamicInput,
	logger log.Modular,
	stats metrics.Type,
	options ...func(*DynamicFanIn),
) (*DynamicFanIn, error) {
	d := &DynamicFanIn{
		running: 1,
		stats:   stats,
		log:     logger,

		transactionChan: make(chan types.Transaction),

		onAdd:    func(l string) {},
		onRemove: func(l string) {},

		newInputChan:     make(chan wrappedInput),
		inputs:           make(map[string]DynamicInput),
		inputClosedChans: make(map[string]chan struct{}),

		closedChan: make(chan struct{}),
		closeChan:  make(chan struct{}),
	}
	for _, opt := range options {
		opt(d)
	}
	mAddErr := d.stats.GetCounter("input.add.error")
	for key, input := range inputs {
		if err := d.addInput(key, input); err != nil {
			mAddErr.Incr(1)
			d.log.Errorf("Failed to start new dynamic input '%v': %v\n", key, err)
		}
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

// TransactionChan returns the channel used for consuming messages from this
// broker.
func (d *DynamicFanIn) TransactionChan() <-chan types.Transaction {
	return d.transactionChan
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (d *DynamicFanIn) Connected() bool {
	// Always return true as this is fuzzy right now.
	return true
}

//------------------------------------------------------------------------------

// OptDynamicFanInSetOnAdd sets the function that is called whenever a dynamic
// input is added.
func OptDynamicFanInSetOnAdd(onAddFunc func(label string)) func(*DynamicFanIn) {
	return func(d *DynamicFanIn) {
		d.onAdd = onAddFunc
	}
}

// OptDynamicFanInSetOnRemove sets the function that is called whenever a
// dynamic input is removed.
func OptDynamicFanInSetOnRemove(onRemoveFunc func(label string)) func(*DynamicFanIn) {
	return func(d *DynamicFanIn) {
		d.onRemove = onRemoveFunc
	}
}

//------------------------------------------------------------------------------

func (d *DynamicFanIn) addInput(ident string, input DynamicInput) error {
	closedChan := make(chan struct{})
	// Launch goroutine that async writes input into single channel
	go func(in DynamicInput, cChan chan struct{}) {
		defer func() {
			d.onRemove(ident)
			close(cChan)
		}()
		d.onAdd(ident)
		for {
			in, open := <-input.TransactionChan()
			if !open {
				// Race condition: This will be called when shutting down.
				return
			}
			d.transactionChan <- in
		}
	}(input, closedChan)

	// Add new input to our map
	d.inputs[ident] = input
	d.inputClosedChans[ident] = closedChan

	return nil
}

func (d *DynamicFanIn) removeInput(ident string, timeout time.Duration) error {
	input, exists := d.inputs[ident]
	if !exists {
		// Nothing to do
		return nil
	}

	input.CloseAsync()
	select {
	case <-d.inputClosedChans[ident]:
	case <-time.After(timeout):
		// Do NOT remove inputs from our map unless we are sure they are
		// closed.
		return types.ErrTimeout
	}

	delete(d.inputs, ident)
	delete(d.inputClosedChans, ident)

	return nil
}

// managerLoop is an internal loop that monitors new and dead input types.
func (d *DynamicFanIn) managerLoop() {
	defer func() {
		for _, i := range d.inputs {
			i.CloseAsync()
		}
		for key := range d.inputs {
			if err := d.removeInput(key, time.Second); err != nil {
				for err != nil {
					err = d.removeInput(key, time.Second)
				}
			}
		}
		close(d.transactionChan)
		close(d.closedChan)
	}()

	var (
		mCount      = d.stats.GetCounter("count")
		mRemoveErr  = d.stats.GetCounter("input.remove.error")
		mRemoveSucc = d.stats.GetCounter("input.remove.success")
		mAddErr     = d.stats.GetCounter("input.add.error")
		mAddSucc    = d.stats.GetCounter("input.add.success")
	)

	for {
		select {
		case wrappedInput, open := <-d.newInputChan:
			if !open {
				return
			}
			mCount.Incr(1)

			var err error
			if _, exists := d.inputs[wrappedInput.Name]; exists {
				if err = d.removeInput(wrappedInput.Name, wrappedInput.Timeout); err != nil {
					mRemoveErr.Incr(1)
					d.log.Errorf("Failed to stop old copy of dynamic input '%v': %v\n", wrappedInput.Name, err)
				} else {
					mRemoveSucc.Incr(1)
				}
			}
			if err == nil && wrappedInput.Input != nil {
				// If the input is nil then we only wanted to remove the input.
				if err = d.addInput(wrappedInput.Name, wrappedInput.Input); err != nil {
					mAddErr.Incr(1)
					d.log.Errorf("Failed to start new dynamic input '%v': %v\n", wrappedInput.Name, err)
				} else {
					mAddSucc.Incr(1)
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
