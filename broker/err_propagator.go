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
	"time"

	"github.com/jeffail/benthos/agent"
	"github.com/jeffail/benthos/types"
)

//--------------------------------------------------------------------------------------------------

// PropagatedErrs - The collected errors sent out by the ErrPropagator.
type PropagatedErrs map[int][]error

// ErrPropagator - Takes an array of error channels from agents and outputs into a single channel.
type ErrPropagator struct {
	agentsChan chan []agent.Type
	agents     []agent.Type

	outputChan chan PropagatedErrs

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewErrPropagator - Create a new ErrPropagator type.
func NewErrPropagator(agents []agent.Type) *ErrPropagator {
	e := ErrPropagator{
		agentsChan: make(chan []agent.Type),
		agents:     agents,
		outputChan: make(chan PropagatedErrs),
		closedChan: make(chan struct{}),
		closeChan:  make(chan struct{}),
	}

	go e.loop()

	return &e
}

//--------------------------------------------------------------------------------------------------

// SetAgents - Set the err readers agents.
func (e *ErrPropagator) SetAgents(agents []agent.Type) {
	e.agentsChan <- agents
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to error channel.
func (e *ErrPropagator) loop() {
	propErrors := PropagatedErrs{}

	var selectCases []reflect.SelectCase
	setSelectCases := func() {
		selectCases = make([]reflect.SelectCase, len(e.agents)+3)

		for i, agent := range e.agents {
			selectCases[i] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(agent.ErrorsChan()),
			}
		}

		selectCases[len(e.agents)] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(e.agentsChan),
		}
		selectCases[len(e.agents)+1] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(e.closeChan),
		}
		selectCases[len(e.agents)+2] = reflect.SelectCase{
			Dir:  reflect.SelectSend,
			Send: reflect.ValueOf(propErrors),
			Chan: reflect.ValueOf(nil),
		}
	}
	setSelectCases()

	running := true
	for running {
		if len(propErrors) == 0 {
			selectCases[len(e.agents)+2] = reflect.SelectCase{
				Dir:  reflect.SelectSend,
				Send: reflect.ValueOf(propErrors),
				Chan: reflect.ValueOf(nil),
			}
		} else {
			selectCases[len(e.agents)+2] = reflect.SelectCase{
				Dir:  reflect.SelectSend,
				Send: reflect.ValueOf(propErrors),
				Chan: reflect.ValueOf(e.outputChan),
			}
		}

		chosen, val, open := reflect.Select(selectCases)
		if chosen < len(e.agents) {
			if !open {
				propErrors[chosen] = append(propErrors[chosen], types.ErrChanClosed)
			} else if errs, ok := val.Interface().([]error); ok {
				propErrors[chosen] = append(propErrors[chosen], errs...)
			}
		} else if chosen == len(e.agents) {
			if running = open; open {
				if agents, ok := val.Interface().([]agent.Type); ok {
					e.agents = agents
					setSelectCases()
				}
			}
		} else if chosen == len(e.agents)+1 {
		} else if chosen == len(e.agents)+2 {
			propErrors = PropagatedErrs{}
		}
	}

	close(e.outputChan)
	close(e.closedChan)
}

//--------------------------------------------------------------------------------------------------

// OutputChan - Returns the channel used to extract propagated errors.
func (e *ErrPropagator) OutputChan() <-chan PropagatedErrs {
	return e.outputChan
}

// CloseAsync - Shuts down the ErrPropagator output and stops processing messages.
func (e *ErrPropagator) CloseAsync() {
	close(e.agentsChan)
	close(e.closeChan)
}

// WaitForClose - Blocks until the ErrPropagator output has closed down.
func (e *ErrPropagator) WaitForClose(timeout time.Duration) error {
	select {
	case <-e.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
