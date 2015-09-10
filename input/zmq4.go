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

package input

import (
	"bufio"
	"github.com/jeffail/benthos/types"
	"github.com/pebbe/zmq4"
	"os"
	"time"
)

//--------------------------------------------------------------------------------------------------

// ZMQ4Config - Configuration for the ZMQ4 input type.
type ZMQ4Config struct {
}

// NewZMQ4Config - Creates a new ZMQ4Config with default values.
func NewZMQ4Config() ZMQ4Config {
	return ZMQ4Config{}
}

//--------------------------------------------------------------------------------------------------

// ZMQ4 - An input type that serves ZMQ4 POST requests.
type ZMQ4 struct {
	conf Config

	messages  chan types.Message
	responses <-chan types.Response

	newResponsesChan chan (<-chan types.Response)

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewZMQ4 - Create a new ZMQ4 input type.
func NewZMQ4(conf Config) *ZMQ4 {
	z := ZMQ4{
		conf:             conf,
		messages:         make(chan types.Message),
		responses:        nil,
		newResponsesChan: make(chan (<-chan types.Response)),
		closedChan:       make(chan struct{}),
		closeChan:        make(chan struct{}),
	}

	go z.loop()

	return &z
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to output pipe.
func (z *ZMQ4) loop() {
	stdin := bufio.NewScanner(os.Stdin)

	var bytes []byte
	var msgChan chan<- types.Message

	running, responsePending := true, false
	for running {
		// If no bytes then read a message
		if bytes == nil {
			// TODO
		}

		// If we have a line to push out
		if bytes != nil && !responsePending {
			msgChan = z.messages
		} else {
			msgChan = nil
		}

		if running {
			select {
			case msgChan <- types.Message{Content: bytes}:
				responsePending = true
			case err, open := <-z.responses:
				if !open {
					s.responses = nil
				} else if err == nil {
					responsePending = false
					bytes = nil
				}
			case newResChan, open := <-z.newResponsesChan:
				if running = open; open {
					z.responses = newResChan
				}
			case _, running = <-z.closeChan:
			}
		}
	}

	close(z.messages)
	close(z.newResponsesChan)
	close(z.closedChan)
}

// SetResponseChan - Sets the channel used by the input to validate message receipt.
func (z *ZMQ4) SetResponseChan(responses <-chan types.Response) {
	z.newResponsesChan <- responses
}

// ConsumerChan - Returns the messages channel.
func (z *ZMQ4) ConsumerChan() <-chan types.Message {
	return z.messages
}

// CloseAsync - Shuts down the ZMQ4 input and stops processing requests.
func (z *ZMQ4) CloseAsync() {
	close(z.closeChan)
}

// WaitForClose - Blocks until the ZMQ4 input has closed down.
func (z *ZMQ4) WaitForClose(timeout time.Duration) error {
	select {
	case <-z.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
