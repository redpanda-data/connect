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

	"github.com/jeffail/benthos/agent"
	"github.com/jeffail/benthos/types"
)

//--------------------------------------------------------------------------------------------------

// OneToMany - A one-to-many broker type.
type OneToMany struct {
	newMessagesChan chan (<-chan types.Message)
	messages        <-chan types.Message
	responseChan    chan types.Response

	agentsChan chan []agent.Type
	agents     []agent.Type

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewOneToMany - Create a new OneToMany type by providing agents and a messages channel.
func NewOneToMany(agents []agent.Type) *OneToMany {
	o := &OneToMany{
		newMessagesChan: make(chan (<-chan types.Message)),
		messages:        nil,
		responseChan:    make(chan types.Response),
		agentsChan:      make(chan []agent.Type),
		agents:          agents,
		closedChan:      make(chan struct{}),
		closeChan:       make(chan struct{}),
	}

	go o.loop()

	return o
}

//--------------------------------------------------------------------------------------------------

// SetMessageChan - Assigns a new messages channel for the broker to read.
func (o *OneToMany) SetMessageChan(msgs <-chan types.Message) {
	o.newMessagesChan <- msgs
}

// SetAgents - Set the broker agents.
func (o *OneToMany) SetAgents(agents []agent.Type) {
	o.agentsChan <- agents
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to many outputs.
func (o *OneToMany) loop() {
	running := true
	for running {
		select {
		case msg, open := <-o.messages:
			// If the read channel is closed we can continue and wait for a replacement.
			if !open {
				o.messages = nil
			} else {
				responses := types.NewMappedResponse()
				for i := range o.agents {
					o.agents[i].MessageChan() <- msg
				}
				for i := range o.agents {
					if r := <-o.agents[i].ResponseChan(); r.Error() != nil {
						responses.Errors[i] = r.Error()
					}
				}
				o.responseChan <- responses
			}
		case newChan, open := <-o.newMessagesChan:
			if running = open; running {
				o.messages = newChan
			}
		case agents, open := <-o.agentsChan:
			if running = open; running {
				o.agents = agents
			}
		case _, running = <-o.closeChan:
		}
	}

	close(o.responseChan)
	close(o.closedChan)
}

// ResponseChan - Returns the response channel.
func (o *OneToMany) ResponseChan() <-chan types.Response {
	return o.responseChan
}

// CloseAsync - Shuts down the OneToMany broker and stops processing requests.
func (o *OneToMany) CloseAsync() {
	close(o.newMessagesChan)
	close(o.closeChan)
	close(o.agentsChan)
}

// WaitForClose - Blocks until the OneToMany broker has closed down.
func (o *OneToMany) WaitForClose(timeout time.Duration) error {
	select {
	case <-o.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
