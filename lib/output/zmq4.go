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

package output

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/pebbe/zmq4"
)

//------------------------------------------------------------------------------

func init() {
	constructors["zmq4"] = typeSpec{
		constructor: NewZMQ4,
		description: `
The zmq4 output type attempts to send messages to a ZMQ4 port, currently only
PUSH and PUB sockets are supported.`,
	}
}

//------------------------------------------------------------------------------

// ZMQ4Config is configuration for the ZMQ4 output type.
type ZMQ4Config struct {
	URLs          []string `json:"urls" yaml:"urls"`
	Bind          bool     `json:"bind" yaml:"bind"`
	SocketType    string   `json:"socket_type" yaml:"socket_type"`
	HighWaterMark int      `json:"high_water_mark" yaml:"high_water_mark"`
}

// NewZMQ4Config creates a new ZMQ4Config with default values.
func NewZMQ4Config() *ZMQ4Config {
	return &ZMQ4Config{
		URLs:          []string{"tcp://*:5556"},
		Bind:          true,
		SocketType:    "PUSH",
		HighWaterMark: 0,
	}
}

//------------------------------------------------------------------------------

// ZMQ4 is an output type that writes ZMQ4 messages.
type ZMQ4 struct {
	running int32

	log   log.Modular
	stats metrics.Type

	urls []string
	conf Config

	socket *zmq4.Socket

	messages     <-chan types.Message
	responseChan chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewZMQ4 creates a new ZMQ4 output type.
func NewZMQ4(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	z := ZMQ4{
		running:      1,
		log:          log.NewModule(".output.zmq4"),
		stats:        stats,
		conf:         conf,
		messages:     nil,
		responseChan: make(chan types.Response),
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),
	}
	for _, u := range conf.ZMQ4.URLs {
		for _, splitU := range strings.Split(u, ",") {
			if len(splitU) > 0 {
				z.urls = append(z.urls, splitU)
			}
		}
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

	z.socket.SetSndhwm(conf.ZMQ4.HighWaterMark)

	for _, address := range z.urls {
		if conf.ZMQ4.Bind {
			err = z.socket.Bind(address)
		} else {
			err = z.socket.Connect(address)
		}
		if err != nil {
			return nil, err
		}
	}

	return &z, nil
}

//------------------------------------------------------------------------------

func getZMQType(t string) (zmq4.Type, error) {
	switch t {
	case "PUB":
		return zmq4.PUB, nil
	case "PUSH":
		return zmq4.PUSH, nil
	}
	return zmq4.PULL, types.ErrInvalidZMQType
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to output pipe, does
// not use select.
func (z *ZMQ4) loop() {
	defer func() {
		z.socket.Close()

		atomic.StoreInt32(&z.running, 0)
		z.stats.Decr("output.zmq4.running", 1)

		close(z.responseChan)
		close(z.closedChan)
	}()
	z.stats.Incr("output.zmq4.running", 1)

	if z.conf.ZMQ4.Bind {
		z.log.Infof("Sending ZMQ4 messages to bound URLs: %s\n", z.urls)
	} else {
		z.log.Infof("Sending ZMQ4 messages to connected URLs: %s\n", z.urls)
	}

	for atomic.LoadInt32(&z.running) == 1 {
		var err error
		select {
		case msg, open := <-z.messages:
			if !open {
				return
			}
			z.stats.Incr("output.zmq4.count", 1)
			_, err = z.socket.SendMessage(msg.Parts) // Could lock entire service
			if err != nil {
				z.stats.Incr("output.zmq4.send.error", 1)
			} else {
				z.stats.Incr("output.zmq4.send.success", 1)
			}
		case <-z.closeChan:
			return
		}
		select {
		case z.responseChan <- types.NewSimpleResponse(err):
		case <-z.closeChan:
			return
		}
	}
}

// StartReceiving assigns a messages channel for the output to read.
func (z *ZMQ4) StartReceiving(msgs <-chan types.Message) error {
	if z.messages != nil {
		return types.ErrAlreadyStarted
	}
	z.messages = msgs
	go z.loop()
	return nil
}

// ResponseChan returns the errors channel.
func (z *ZMQ4) ResponseChan() <-chan types.Response {
	return z.responseChan
}

// CloseAsync shuts down the ZMQ4 output and stops processing messages.
func (z *ZMQ4) CloseAsync() {
	if atomic.CompareAndSwapInt32(&z.running, 1, 0) {
		close(z.closeChan)
	}
}

// WaitForClose blocks until the ZMQ4 output has closed down.
func (z *ZMQ4) WaitForClose(timeout time.Duration) error {
	select {
	case <-z.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
