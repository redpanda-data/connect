// +build ZMQ4

// Copyright (c) 2014 Ashley Jeffs
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

package input

import (
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
	"github.com/pebbe/zmq4"
)

//------------------------------------------------------------------------------

func init() {
	constructors["zmq4"] = typeSpec{
		constructor: NewZMQ4,
		description: `
ZMQ4 is supported but currently depends on C bindings. Since this is an
annoyance when building or using Benthos it is not compiled by default.

Build it into your project by getting CZMQ installed on your machine, then build
with the tag: 'go install -tags "ZMQ4" github.com/jeffail/benthos/cmd/...'

ZMQ4 input supports PULL and SUB sockets only. If there is demand for other
socket types then they can be added easily.`,
	}
}

//------------------------------------------------------------------------------

// ZMQ4Config is configuration for the ZMQ4 input type.
type ZMQ4Config struct {
	Addresses     []string `json:"addresses" yaml:"addresses"`
	Bind          bool     `json:"bind" yaml:"bind"`
	SocketType    string   `json:"socket_type" yaml:"socket_type"`
	SubFilters    []string `json:"sub_filters" yaml:"sub_filters"`
	HighWaterMark int      `json:"high_water_mark" yaml:"high_water_mark"`
	PollTimeoutMS int      `json:"poll_timeout_ms" yaml:"poll_timeout_ms"`
}

// NewZMQ4Config creates a new ZMQ4Config with default values.
func NewZMQ4Config() *ZMQ4Config {
	return &ZMQ4Config{
		Addresses:     []string{"tcp://localhost:5555"},
		Bind:          false,
		SocketType:    "PULL",
		SubFilters:    []string{},
		HighWaterMark: 0,
		PollTimeoutMS: 5000,
	}
}

//------------------------------------------------------------------------------

// ZMQ4 is an input type that serves ZMQ4 POST requests.
type ZMQ4 struct {
	running int32

	conf  Config
	stats metrics.Type
	log   log.Modular

	socket *zmq4.Socket

	messages  chan types.Message
	responses <-chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewZMQ4 creates a new ZMQ4 input type.
func NewZMQ4(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	z := ZMQ4{
		running:    1,
		conf:       conf,
		stats:      stats,
		log:        log.NewModule(".input.zmq4"),
		messages:   make(chan types.Message),
		responses:  nil,
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
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

	z.socket.SetRcvhwm(conf.ZMQ4.HighWaterMark)

	for _, address := range conf.ZMQ4.Addresses {
		if conf.ZMQ4.Bind {
			err = z.socket.Bind(address)
		} else {
			err = z.socket.Connect(address)
		}
		if err != nil {
			return nil, err
		}
	}

	for _, filter := range conf.ZMQ4.SubFilters {
		if err = z.socket.SetSubscribe(filter); err != nil {
			return nil, err
		}
	}

	return &z, nil
}

//------------------------------------------------------------------------------

func getZMQType(t string) (zmq4.Type, error) {
	switch t {
	case "SUB":
		return zmq4.SUB, nil
	case "PULL":
		return zmq4.PULL, nil
	}
	return zmq4.PULL, types.ErrInvalidZMQType
}

//------------------------------------------------------------------------------

func (z *ZMQ4) loop() {
	defer func() {
		atomic.StoreInt32(&z.running, 0)
		close(z.messages)
		close(z.closedChan)
	}()

	pollTimeout := time.Millisecond * time.Duration(z.conf.ZMQ4.PollTimeoutMS)
	poller := zmq4.NewPoller()
	poller.Add(z.socket, zmq4.POLLIN)

	for _, address := range z.conf.ZMQ4.Addresses {
		if z.conf.ZMQ4.Bind {
			z.log.Infof("Receiving ZMQ4 messages on bound address: %v\n", address)
		} else {
			z.log.Infof("Receiving ZMQ4 messages on connected address: %v\n", address)
		}
	}

	var data [][]byte

	for atomic.LoadInt32(&z.running) == 1 {
		// If no bytes then read a message
		if data == nil {
			var err error
			if data, err = z.socket.RecvMessageBytes(zmq4.DONTWAIT); err != nil {
				var polled []zmq4.Polled
				if polled, err = poller.Poll(pollTimeout); len(polled) == 1 {
					data, err = z.socket.RecvMessageBytes(0)
				}
			}
			if err != nil {
				z.stats.Incr("input.zmq4.receive.error", 1)
				z.log.Errorf("Failed to receive message bytes: %v\n", err)
				data = nil
			} else {
				z.stats.Incr("input.zmq4.count", 1)
			}
			if len(data) == 0 {
				data = nil
			}
		}

		// If bytes are read then try and propagate.
		if data != nil {
			select {
			case z.messages <- types.Message{Parts: data}:
			case <-z.closeChan:
				return
			}
			res, open := <-z.responses
			if !open {
				return
			}
			if resErr := res.Error(); resErr == nil {
				data = nil
			} else {
				z.stats.Incr("input.zmq4.send.error", 1)
			}
		}
	}
}

// StartListening sets the channel used by the input to validate message
// receipt.
func (z *ZMQ4) StartListening(responses <-chan types.Response) error {
	if z.responses != nil {
		return types.ErrAlreadyStarted
	}
	z.responses = responses
	go z.loop()
	return nil
}

// MessageChan returns the messages channel.
func (z *ZMQ4) MessageChan() <-chan types.Message {
	return z.messages
}

// CloseAsync shuts down the ZMQ4 input and stops processing requests.
func (z *ZMQ4) CloseAsync() {
	if atomic.CompareAndSwapInt32(&z.running, 1, 0) {
		close(z.closeChan)
	}
}

// WaitForClose blocks until the ZMQ4 input has closed down.
func (z *ZMQ4) WaitForClose(timeout time.Duration) error {
	select {
	case <-z.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
