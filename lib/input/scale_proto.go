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
	"sync/atomic"
	"time"

	"github.com/go-mangos/mangos"
	"github.com/go-mangos/mangos/protocol/pull"
	"github.com/go-mangos/mangos/protocol/sub"
	"github.com/go-mangos/mangos/transport/ipc"
	"github.com/go-mangos/mangos/transport/tcp"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["scalability_protocols"] = typeSpec{
		constructor: NewScaleProto,
		description: `
The scalability protocols are common communication patterns which will be
familiar to anyone accustomed to service messaging protocols.

This input type should be compatible with any implementation of these protocols,
but nanomsg (http://nanomsg.org/index.html) is the specific target of this type.

Since scale proto messages are only single part we would need a binary format
for interpretting multi part messages. If the input is receiving messages from a
benthos output you can set both to use the benthos binary multipart format with
the 'benthos_multi' flag. Note, however, that this format may appear to be
gibberish to other services, and the input will be unable to read normal
messages with this setting.

Currently only PULL and SUB sockets are supported.`,
	}
}

//--------------------------------------------------------------------------------------------------

// ScaleProtoConfig - Configuration for the ScaleProto input type.
type ScaleProtoConfig struct {
	Address       string   `json:"address" yaml:"address"`
	Bind          bool     `json:"bind_address" yaml:"bind_address"`
	SocketType    string   `json:"socket_type" yaml:"socket_type"`
	SubFilters    []string `json:"sub_filters" yaml:"sub_filters"`
	PollTimeoutMS int      `json:"poll_timeout_ms" yaml:"poll_timeout_ms"`
}

// NewScaleProtoConfig - Creates a new ScaleProtoConfig with default values.
func NewScaleProtoConfig() ScaleProtoConfig {
	return ScaleProtoConfig{
		Address:       "tcp://*:5555",
		Bind:          true,
		SocketType:    "PULL",
		SubFilters:    []string{},
		PollTimeoutMS: 5000,
	}
}

//--------------------------------------------------------------------------------------------------

// ScaleProto - An input type that serves Scalability Protocols messages.
type ScaleProto struct {
	running int32

	socket mangos.Socket

	conf  Config
	stats metrics.Type
	log   log.Modular

	messages  chan types.Message
	responses <-chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewScaleProto - Create a new ScaleProto input type.
func NewScaleProto(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	s := ScaleProto{
		running:    1,
		conf:       conf,
		stats:      stats,
		log:        log.NewModule(".input.scale_proto"),
		messages:   make(chan types.Message),
		responses:  nil,
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
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

	for _, filter := range s.conf.ScaleProto.SubFilters {
		if err = s.socket.SetOption(mangos.OptionSubscribe, []byte(filter)); err != nil {
			return nil, err
		}
	}

	return &s, nil
}

//--------------------------------------------------------------------------------------------------

// getSocketFromType - Returns a socket based on a socket type string.
func getSocketFromType(t string) (mangos.Socket, error) {
	switch t {
	case "PULL":
		return pull.NewSocket()
	case "SUB":
		return sub.NewSocket()
	}
	return nil, types.ErrInvalidScaleProtoType
}

//--------------------------------------------------------------------------------------------------

func (s *ScaleProto) loop() {
	defer func() {
		atomic.StoreInt32(&s.running, 0)

		s.socket.Close()
		close(s.messages)
		close(s.closedChan)
	}()

	if s.conf.ScaleProto.Bind {
		s.log.Infof(
			"Receiving Scalability Protocols messages at bound address: %s\n",
			s.conf.ScaleProto.Address,
		)
	} else {
		s.log.Infof(
			"Receiving Scalability Protocols messages at connected address: %s\n",
			s.conf.ScaleProto.Address,
		)
	}

	var data []byte

	for atomic.LoadInt32(&s.running) == 1 {
		// If no bytes then read a message
		if data == nil {
			var err error
			data, err = s.socket.Recv()
			if err != nil && err != mangos.ErrRecvTimeout {
				s.log.Errorf("ScaleProto Socket recv error: %v\n", err)
				s.stats.Incr("input.scale_proto.socket.recv.error", 1)
			}
		}

		// If bytes are read then try and propagate.
		if data != nil {
			msg := types.Message{Parts: [][]byte{data}}
			start := time.Now()
			select {
			case s.messages <- msg:
			case <-s.closeChan:
				return
			}
			res, open := <-s.responses
			if !open {
				return
			}
			if resErr := res.Error(); resErr == nil {
				s.stats.Timing("input.scale_proto.timing", int64(time.Since(start)))
				s.stats.Incr("input.scale_proto.count", 1)
				data = nil
			} else if resErr == types.ErrMessageTooLarge {
				s.stats.Incr("input.scale_proto.send.rejected", 1)
				s.log.Errorf("ScaleProto message was rejected: %v\n", resErr)
				s.log.Errorf("Message content: %s\n", data)
				data = nil
			} else {
				s.stats.Incr("input.scale_proto.send.error", 1)
			}
		}
	}

}

// StartListening - Sets the channel used by the input to validate message receipt.
func (s *ScaleProto) StartListening(responses <-chan types.Response) error {
	if s.responses != nil {
		return types.ErrAlreadyStarted
	}
	s.responses = responses
	go s.loop()
	return nil
}

// MessageChan - Returns the messages channel.
func (s *ScaleProto) MessageChan() <-chan types.Message {
	return s.messages
}

// CloseAsync - Shuts down the ScaleProto input and stops processing requests.
func (s *ScaleProto) CloseAsync() {
	if atomic.CompareAndSwapInt32(&s.running, 1, 0) {
		close(s.closeChan)
	}
}

// WaitForClose - Blocks until the ScaleProto input has closed down.
func (s *ScaleProto) WaitForClose(timeout time.Duration) error {
	select {
	case <-s.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
