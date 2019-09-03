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

// +build ZMQ4

package writer

import (
	"fmt"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/pebbe/zmq4"
)

//------------------------------------------------------------------------------

// ZMQ4Config contains configuration fields for the ZMQ4 output type.
type ZMQ4Config struct {
	URLs          []string `json:"urls" yaml:"urls"`
	Bind          bool     `json:"bind" yaml:"bind"`
	SocketType    string   `json:"socket_type" yaml:"socket_type"`
	HighWaterMark int      `json:"high_water_mark" yaml:"high_water_mark"`
	PollTimeout   string   `json:"poll_timeout" yaml:"poll_timeout"`
}

// NewZMQ4Config creates a new ZMQ4Config with default values.
func NewZMQ4Config() *ZMQ4Config {
	return &ZMQ4Config{
		URLs:          []string{"tcp://*:5556"},
		Bind:          true,
		SocketType:    "PUSH",
		HighWaterMark: 0,
		PollTimeout:   "5s",
	}
}

//------------------------------------------------------------------------------

// ZMQ4 is an output type that writes ZMQ4 messages.
type ZMQ4 struct {
	log   log.Modular
	stats metrics.Type

	urls []string
	conf *ZMQ4Config

	pollTimeout time.Duration
	poller      *zmq4.Poller
	socket      *zmq4.Socket
}

// NewZMQ4 creates a new ZMQ4 output type.
func NewZMQ4(conf *ZMQ4Config, log log.Modular, stats metrics.Type) (*ZMQ4, error) {
	z := ZMQ4{
		log:   log,
		stats: stats,
		conf:  conf,
	}

	_, err := getZMQType(conf.SocketType)
	if nil != err {
		return nil, err
	}

	if tout := conf.PollTimeout; len(tout) > 0 {
		var err error
		if z.pollTimeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse poll timeout string: %v", err)
		}
	}

	for _, u := range conf.URLs {
		for _, splitU := range strings.Split(u, ",") {
			if len(splitU) > 0 {
				z.urls = append(z.urls, splitU)
			}
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

// Connect attempts to establish a connection to a ZMQ4 socket.
func (z *ZMQ4) Connect() error {
	if z.socket != nil {
		return nil
	}

	t, err := getZMQType(z.conf.SocketType)
	if nil != err {
		return err
	}

	ctx, err := zmq4.NewContext()
	if nil != err {
		return err
	}

	var socket *zmq4.Socket
	if socket, err = ctx.NewSocket(t); nil != err {
		return err
	}

	defer func() {
		if err != nil && socket != nil {
			socket.Close()
		}
	}()

	socket.SetSndhwm(z.conf.HighWaterMark)

	for _, address := range z.urls {
		if z.conf.Bind {
			err = socket.Bind(address)
		} else {
			err = socket.Connect(address)
		}
		if err != nil {
			return err
		}
	}

	z.socket = socket
	z.poller = zmq4.NewPoller()
	z.poller.Add(z.socket, zmq4.POLLOUT)

	z.log.Infof("Sending ZMQ4 messages to URLs: %s\n", z.urls)
	return nil
}

// Write will attempt to write a message to the ZMQ4 socket.
func (z *ZMQ4) Write(msg types.Message) error {
	if z.socket == nil {
		return types.ErrNotConnected
	}
	_, err := z.socket.SendMessageDontwait(message.GetAllBytes(msg))
	if err != nil {
		var polled []zmq4.Polled
		if polled, err = z.poller.Poll(z.pollTimeout); len(polled) == 1 {
			_, err = z.socket.SendMessage(message.GetAllBytes(msg))
		} else if err == nil {
			return types.ErrTimeout
		}
	}
	return err
}

// CloseAsync shuts down the ZMQ4 output and stops processing messages.
func (z *ZMQ4) CloseAsync() {
	if z.socket != nil {
		z.socket.Close()
		z.socket = nil
	}
}

// WaitForClose blocks until the ZMQ4 output has closed down.
func (z *ZMQ4) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
