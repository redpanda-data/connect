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

// +build ZMQ4

package reader

import (
	"context"
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

// ZMQ4Config contains configuration fields for the ZMQ4 input type.
type ZMQ4Config struct {
	URLs          []string `json:"urls" yaml:"urls"`
	Bind          bool     `json:"bind" yaml:"bind"`
	SocketType    string   `json:"socket_type" yaml:"socket_type"`
	SubFilters    []string `json:"sub_filters" yaml:"sub_filters"`
	HighWaterMark int      `json:"high_water_mark" yaml:"high_water_mark"`
	PollTimeout   string   `json:"poll_timeout" yaml:"poll_timeout"`
}

// NewZMQ4Config creates a new ZMQ4Config with default values.
func NewZMQ4Config() *ZMQ4Config {
	return &ZMQ4Config{
		URLs:          []string{"tcp://localhost:5555"},
		Bind:          false,
		SocketType:    "PULL",
		SubFilters:    []string{},
		HighWaterMark: 0,
		PollTimeout:   "5s",
	}
}

//------------------------------------------------------------------------------

// ZMQ4 is an input type that consumes ZMQ messages.
type ZMQ4 struct {
	urls  []string
	conf  *ZMQ4Config
	stats metrics.Type
	log   log.Modular

	pollTimeout time.Duration
	poller      *zmq4.Poller
	socket      *zmq4.Socket
}

// NewZMQ4 creates a new ZMQ4 input type.
func NewZMQ4(conf *ZMQ4Config, log log.Modular, stats metrics.Type) (*ZMQ4, error) {
	z := ZMQ4{
		conf:  conf,
		stats: stats,
		log:   log,
	}

	for _, u := range conf.URLs {
		for _, splitU := range strings.Split(u, ",") {
			if len(splitU) > 0 {
				z.urls = append(z.urls, splitU)
			}
		}
	}

	_, err := getZMQType(conf.SocketType)
	if nil != err {
		return nil, err
	}

	if tout := conf.PollTimeout; len(tout) > 0 {
		if z.pollTimeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse poll timeout string: %v", err)
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

// Connect establishes a ZMQ4 socket.
func (z *ZMQ4) Connect() error {
	return z.ConnectWithContext(context.Background())
}

// ConnectWithContext establishes a ZMQ4 socket.
func (z *ZMQ4) ConnectWithContext(ignored context.Context) error {
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

	socket.SetRcvhwm(z.conf.HighWaterMark)

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

	for _, filter := range z.conf.SubFilters {
		if err = socket.SetSubscribe(filter); err != nil {
			return err
		}
	}

	z.socket = socket
	z.poller = zmq4.NewPoller()
	z.poller.Add(z.socket, zmq4.POLLIN)

	if z.conf.Bind {
		z.log.Infof("Receiving ZMQ4 messages on bound URLs: %s\n", z.urls)
	} else {
		z.log.Infof("Receiving ZMQ4 messages on connected URLs: %s\n", z.urls)
	}
	return nil
}

// Read attempts to read a new message from the ZMQ socket.
func (z *ZMQ4) Read() (types.Message, error) {
	msg, _, err := z.ReadWithContext(context.Background())
	return msg, err
}

// ReadWithContext attempts to read a new message from the ZMQ socket.
func (z *ZMQ4) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	if z.socket == nil {
		return nil, nil, types.ErrNotConnected
	}

	data, err := z.socket.RecvMessageBytes(zmq4.DONTWAIT)
	if err != nil {
		var polled []zmq4.Polled
		if polled, err = z.poller.Poll(z.pollTimeout); len(polled) == 1 {
			data, err = z.socket.RecvMessageBytes(0)
		} else if err == nil {
			return nil, nil, types.ErrTimeout
		}
	}
	if err != nil {
		return nil, nil, err
	}

	return message.New(data), noopAsyncAckFn, nil
}

// Acknowledge instructs whether the pending messages were propagated
// successfully.
func (z *ZMQ4) Acknowledge(err error) error {
	return nil
}

// CloseAsync shuts down the ZMQ4 input and stops processing requests.
func (z *ZMQ4) CloseAsync() {
	if z.socket != nil {
		z.socket.Close()
		z.socket = nil
	}
}

// WaitForClose blocks until the ZMQ4 input has closed down.
func (z *ZMQ4) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
