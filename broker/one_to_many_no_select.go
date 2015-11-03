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
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/agent"
	"github.com/jeffail/benthos/types"
)

//--------------------------------------------------------------------------------------------------

/*
oneToManyNoSelect - A one-to-many broker type which avoids using the select statement. This is used
for benchmarking the impact of adding select statements between channel pipes.
*/
type oneToManyNoSelect struct {
	sync.RWMutex

	running int32
	stopped int32

	messages     <-chan types.Message
	responseChan chan Response

	agents []agent.Type

	closedChan chan struct{}
	closeChan  chan struct{}
}

// newOneToManyNoSelect - Create a new oneToManyNoSelect type by providing agents and a messages channel.
func newOneToManyNoSelect(agents []agent.Type) *oneToManyNoSelect {
	o := &oneToManyNoSelect{
		running:      1,
		stopped:      0,
		messages:     nil,
		responseChan: make(chan Response),
		agents:       agents,
	}

	go o.loop()

	return o
}

//--------------------------------------------------------------------------------------------------

// SetReadChan - Assigns a new messages channel for the broker to read.
func (o *oneToManyNoSelect) SetReadChan(msgs <-chan types.Message) {
	o.Lock()
	defer o.Unlock()
	o.messages = msgs
}

// SetAgents - Set the broker agents.
func (o *oneToManyNoSelect) SetAgents(agents []agent.Type) {
	o.Lock()
	defer o.Unlock()
	o.agents = agents
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to many outputs.
func (o *oneToManyNoSelect) loop() {
	for atomic.LoadInt32(&o.running) == 1 {
		o.RLock()
		msg, open := <-o.messages
		// If the read channel is closed we can continue and wait for a replacement.
		if !open {
			atomic.StoreInt32(&o.running, 0)
		} else {
			responses := Response{}
			for i, a := range o.agents {
				a.MessageChan() <- msg
				if r := <-a.ResponseChan(); r != nil {
					responses[i] = r
				}
			}
			o.responseChan <- responses
		}
		o.RUnlock()
	}

	atomic.StoreInt32(&o.stopped, 0)
}

// ResponseChan - Returns the response channel.
func (o *oneToManyNoSelect) ResponseChan() <-chan Response {
	return o.responseChan
}

// CloseAsync - Shuts down the oneToManyNoSelect broker and stops processing requests.
func (o *oneToManyNoSelect) CloseAsync() {
	atomic.StoreInt32(&o.running, 0)
}

// WaitForClose - Blocks until the oneToManyNoSelect broker has closed down.
func (o *oneToManyNoSelect) WaitForClose(timeout time.Duration) error {
	now := time.Now()

	for atomic.LoadInt32(&o.stopped) == 0 && time.Since(now) < timeout {
		time.Sleep(time.Millisecond * 10)
	}

	if atomic.LoadInt32(&o.stopped) == 0 {
		return types.ErrTimeout
	}

	return nil
}

//--------------------------------------------------------------------------------------------------
