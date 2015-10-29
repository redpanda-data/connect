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
	"strings"
	"time"

	"github.com/jeffail/benthos/types"
	"github.com/pebbe/zmq4"
)

//--------------------------------------------------------------------------------------------------

// ZMQ4Config - Configuration for the ZMQ4 input type.
type ZMQ4Config struct {
	Addresses     []string `json:"addresses"`
	SocketType    string   `json:"socket_type"`
	PollTimeoutMS int      `json:"poll_timeout_ms"`
}

// NewZMQ4Config - Creates a new ZMQ4Config with default values.
func NewZMQ4Config() ZMQ4Config {
	return ZMQ4Config{
		Addresses:     []string{"localhost:1234"},
		SocketType:    "PULL",
		PollTimeoutMS: 5000,
	}
}

//--------------------------------------------------------------------------------------------------

// ZMQ4 - An input type that serves ZMQ4 POST requests.
type ZMQ4 struct {
	conf Config

	socket *zmq4.Socket
	poller *zmq4.Poller

	messages  chan types.Message
	responses <-chan types.Response

	newResponsesChan chan (<-chan types.Response)

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewZMQ4 - Create a new ZMQ4 input type.
func NewZMQ4(conf Config) (*ZMQ4, error) {
	z := ZMQ4{
		conf:             conf,
		messages:         make(chan types.Message),
		responses:        nil,
		newResponsesChan: make(chan (<-chan types.Response)),
		closedChan:       make(chan struct{}),
		closeChan:        make(chan struct{}),
	}

	t, err := getZMQType(conf.ZMQ4.SocketType)
	if nil != err {
		return nil, err
	}

	ctx, err := zmq4.NewContext()
	if nil != err {
		return nil, err
	}

	if z.socket, err = ctx.NewSocket(t); nil != err {
		return nil, err
	}

	for _, address := range conf.ZMQ4.Addresses {
		if strings.Contains(address, "*") {
			err = z.socket.Bind(address)
		} else {
			err = z.socket.Connect(address)
		}
		if err != nil {
			return nil, err
		}
	}

	z.poller = zmq4.NewPoller()
	z.poller.Add(z.socket, zmq4.POLLIN)

	go z.loop()

	return &z, nil
}

//--------------------------------------------------------------------------------------------------

func getZMQType(t string) (zmq4.Type, error) {
	switch t {
	case "REQ":
		return zmq4.REQ, nil
	case "REP":
		return zmq4.REP, nil
	case "DEALER":
		return zmq4.DEALER, nil
	case "ROUTER":
		return zmq4.ROUTER, nil
	case "PUB":
		return zmq4.PUB, nil
	case "SUB":
		return zmq4.SUB, nil
	case "XPUB":
		return zmq4.XPUB, nil
	case "XSUB":
		return zmq4.XSUB, nil
	case "PUSH":
		return zmq4.PUSH, nil
	case "PULL":
		return zmq4.PULL, nil
	case "PAIR":
		return zmq4.PAIR, nil
	case "STREAM":
		return zmq4.STREAM, nil
	}
	return zmq4.PULL, types.ErrInvalidZMQType
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to output pipe.
func (z *ZMQ4) loop() {
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
					z.responses = nil
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
