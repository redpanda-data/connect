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
	"sync/atomic"
	"time"

	"github.com/go-mangos/mangos"
	"github.com/go-mangos/mangos/protocol/pub"
	"github.com/go-mangos/mangos/protocol/push"
	"github.com/go-mangos/mangos/transport/ipc"
	"github.com/go-mangos/mangos/transport/tcp"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//------------------------------------------------------------------------------

func init() {
	constructors["scalability_protocols"] = typeSpec{
		constructor: NewScaleProto,
		description: `
The scalability protocols are common communication patterns which will be
familiar to anyone accustomed to service messaging protocols.

This outnput type should be compatible with any implementation of these
protocols, but nanomsg (http://nanomsg.org/index.html) is the specific target of
this type.

Since scale proto messages are only single part we would need a binary format
for sending multi part messages. We can use the benthos binary format for this
purpose. However, this format may appear to be gibberish to other services. If
you want to use the binary format you can set 'benthos_multi' to true.

Currently only PUSH and PUB sockets are supported.`,
	}
}

//------------------------------------------------------------------------------

// ScaleProtoConfig is configuration for the ScaleProto output type.
type ScaleProtoConfig struct {
	Address       string `json:"address" yaml:"address"`
	Bind          bool   `json:"bind_address" yaml:"bind_address"`
	SocketType    string `json:"socket_type" yaml:"socket_type"`
	PollTimeoutMS int    `json:"poll_timeout_ms" yaml:"poll_timeout_ms"`
}

// NewScaleProtoConfig creates a new ScaleProtoConfig with default values.
func NewScaleProtoConfig() ScaleProtoConfig {
	return ScaleProtoConfig{
		Address:       "tcp://localhost:5556",
		Bind:          false,
		SocketType:    "PUSH",
		PollTimeoutMS: 5000,
	}
}

//------------------------------------------------------------------------------

// ScaleProto is an output type that serves ScaleProto messages.
type ScaleProto struct {
	running int32

	log   log.Modular
	stats metrics.Type

	conf Config

	socket mangos.Socket

	messages     <-chan types.Message
	responseChan chan types.Response

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewScaleProto creates a new ScaleProto output type.
func NewScaleProto(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	s := ScaleProto{
		running:      1,
		log:          log.NewModule(".output.scale_proto"),
		stats:        stats,
		conf:         conf,
		messages:     nil,
		responseChan: make(chan types.Response),
		closedChan:   make(chan struct{}),
		closeChan:    make(chan struct{}),
	}

	var err error
	s.socket, err = getSocketFromType(conf.ScaleProto.SocketType)
	if nil != err {
		return nil, err
	}

	// Set timeout to prevent endless lock.
	err = s.socket.SetOption(
		mangos.OptionRecvDeadline,
		time.Millisecond*time.Duration(s.conf.ScaleProto.PollTimeoutMS),
	)
	if nil != err {
		return nil, err
	}
	err = s.socket.SetOption(
		mangos.OptionSendDeadline,
		time.Millisecond*time.Duration(s.conf.ScaleProto.PollTimeoutMS),
	)
	if nil != err {
		return nil, err
	}

	s.socket.AddTransport(ipc.NewTransport())
	s.socket.AddTransport(tcp.NewTransport())

	if s.conf.ScaleProto.Bind {
		err = s.socket.Listen(s.conf.ScaleProto.Address)
	} else {
		err = s.socket.Dial(s.conf.ScaleProto.Address)
	}
	if err != nil {
		return nil, err
	}

	return &s, nil
}

//------------------------------------------------------------------------------

// getSocketFromType returns a socket based on a socket type string.
func getSocketFromType(t string) (mangos.Socket, error) {
	switch t {
	case "PUSH":
		return push.NewSocket()
	case "PUB":
		return pub.NewSocket()
	}
	return nil, types.ErrInvalidScaleProtoType
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to output pipe, does
// not use select.
func (s *ScaleProto) loop() {
	defer func() {
		atomic.StoreInt32(&s.running, 0)

		s.socket.Close()
		close(s.responseChan)
		close(s.closedChan)
	}()

	if s.conf.ScaleProto.Bind {
		s.log.Infof(
			"Sending Scalability Protocols messages to bound address: %s\n",
			s.conf.ScaleProto.Address,
		)
	} else {
		s.log.Infof(
			"Sending Scalability Protocols messages to connected address: %s\n",
			s.conf.ScaleProto.Address,
		)
	}

	var open bool
	for atomic.LoadInt32(&s.running) == 1 {
		var msg types.Message
		if msg, open = <-s.messages; !open {
			return
		}
		s.stats.Incr("output.scale_proto.count", 1)
		var err error
		for _, part := range msg.Parts {
			if err == nil {
				err = s.socket.Send(part)
			}
		}
		if err != nil {
			s.stats.Incr("output.scale_proto.send.error", 1)
		} else {
			s.stats.Incr("output.scale_proto.send.success", 1)
		}
		select {
		case s.responseChan <- types.NewSimpleResponse(err):
		case <-s.closeChan:
			return
		}
	}
}

// StartReceiving assigns a messages channel for the output to read.
func (s *ScaleProto) StartReceiving(msgs <-chan types.Message) error {
	if s.messages != nil {
		return types.ErrAlreadyStarted
	}
	s.messages = msgs
	go s.loop()
	return nil
}

// ResponseChan returns the errors channel.
func (s *ScaleProto) ResponseChan() <-chan types.Response {
	return s.responseChan
}

// CloseAsync shuts down the ScaleProto output and stops processing messages.
func (s *ScaleProto) CloseAsync() {
	if atomic.CompareAndSwapInt32(&s.running, 1, 0) {
		close(s.closeChan)
	}
}

// WaitForClose blocks until the ScaleProto output has closed down.
func (s *ScaleProto) WaitForClose(timeout time.Duration) error {
	select {
	case <-s.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
