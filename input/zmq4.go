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
	"sync/atomic"
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
	running int32

	conf Config

	socket *zmq4.Socket

	internalMessages chan [][]byte

	messages  chan types.Message
	responses <-chan types.Response

	newResponsesChan chan (<-chan types.Response)

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewZMQ4 - Create a new ZMQ4 input type.
func NewZMQ4(conf Config) (*ZMQ4, error) {
	z := ZMQ4{
		running:          1,
		conf:             conf,
		internalMessages: make(chan [][]byte),
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

	go z.readerLoop()
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

// readerLoop - Internal loop for polling new messages.
func (z *ZMQ4) readerLoop() {
	pollTimeout := time.Millisecond * time.Duration(z.conf.ZMQ4.PollTimeoutMS)
	poller := zmq4.NewPoller()
	poller.Add(z.socket, zmq4.POLLIN)

	for atomic.LoadInt32(&z.running) == 1 {
		// If no bytes then read a message
		polled, err := poller.Poll(pollTimeout)
		if err == nil && len(polled) == 1 {
			if bytes, err := z.socket.RecvMessageBytes(0); err == nil {
				z.internalMessages <- bytes
			} else {
				_ = err
				// TODO: propagate errors, input type should have error channel.
			}
		}
	}
	close(z.internalMessages)
}

// loop - Internal loop brokers incoming messages to output pipe.
func (z *ZMQ4) loop() {
	var bytes [][]byte

	var msgChan chan<- types.Message
	var internalChan <-chan [][]byte

	running, responsePending := true, false
	for running {
		// If we have a line to push out
		if responsePending {
			msgChan = nil
			internalChan = nil
		} else {
			if bytes == nil {
				msgChan = nil
				internalChan = z.internalMessages
			} else {
				msgChan = z.messages
				internalChan = nil
			}
		}

		select {
		case bytes, running = <-internalChan:
		case msgChan <- types.Message{Parts: bytes}:
			responsePending = true
		case err, open := <-z.responses:
			responsePending = false
			if !open {
				z.responses = nil
			} else if err == nil {
				bytes = nil
			}
		case newResChan, open := <-z.newResponsesChan:
			if running = open; open {
				z.responses = newResChan
			}
		case _, running = <-z.closeChan:
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
	atomic.StoreInt32(&z.running, 0)
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
