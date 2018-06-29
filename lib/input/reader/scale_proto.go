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

package reader

import (
	"strings"
	"sync"
	"time"

	"nanomsg.org/go-mangos"
	"nanomsg.org/go-mangos/protocol/pull"
	"nanomsg.org/go-mangos/protocol/sub"
	"nanomsg.org/go-mangos/transport/ipc"
	"nanomsg.org/go-mangos/transport/tcp"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

// ScaleProtoConfig is configuration for the ScaleProto input type.
type ScaleProtoConfig struct {
	URLs          []string `json:"urls" yaml:"urls"`
	Bind          bool     `json:"bind" yaml:"bind"`
	SocketType    string   `json:"socket_type" yaml:"socket_type"`
	SubFilters    []string `json:"sub_filters" yaml:"sub_filters"`
	PollTimeoutMS int      `json:"poll_timeout_ms" yaml:"poll_timeout_ms"`
	RepTimeoutMS  int      `json:"reply_timeout_ms" yaml:"reply_timeout_ms"`
}

// NewScaleProtoConfig creates a new ScaleProtoConfig with default values.
func NewScaleProtoConfig() ScaleProtoConfig {
	return ScaleProtoConfig{
		URLs:          []string{"tcp://*:5555"},
		Bind:          true,
		SocketType:    "PULL",
		SubFilters:    []string{},
		PollTimeoutMS: 5000,
		RepTimeoutMS:  5000,
	}
}

//------------------------------------------------------------------------------

// ScaleProto is an input type that serves Scalability Protocols messages.
type ScaleProto struct {
	socket mangos.Socket
	cMut   sync.Mutex

	urls  []string
	conf  ScaleProtoConfig
	stats metrics.Type
	log   log.Modular
}

// NewScaleProto creates a new ScaleProto input type.
func NewScaleProto(conf ScaleProtoConfig, log log.Modular, stats metrics.Type) (Type, error) {
	s := ScaleProto{
		conf:  conf,
		stats: stats,
		log:   log.NewModule(".input.scale_proto"),
	}

	for _, u := range conf.URLs {
		for _, splitU := range strings.Split(u, ",") {
			if len(splitU) > 0 {
				s.urls = append(s.urls, splitU)
			}
		}
	}
	return &s, nil
}

//------------------------------------------------------------------------------

// getSocketFromType returns a socket based on a socket type string.
func getSocketFromType(t string) (mangos.Socket, error) {
	switch t {
	case "PULL":
		return pull.NewSocket()
	case "SUB":
		return sub.NewSocket()
	}
	return nil, types.ErrInvalidScaleProtoType
}

//------------------------------------------------------------------------------

// Connect establishes a nanomsg socket.
func (s *ScaleProto) Connect() error {
	s.cMut.Lock()
	defer s.cMut.Unlock()

	if s.socket != nil {
		return nil
	}

	var socket mangos.Socket
	var err error

	defer func() {
		if err != nil && socket != nil {
			socket.Close()
		}
	}()

	socket, err = getSocketFromType(s.conf.SocketType)
	if nil != err {
		return err
	}

	// Set timeout to prevent endless lock.
	err = socket.SetOption(
		mangos.OptionRecvDeadline,
		time.Millisecond*time.Duration(s.conf.PollTimeoutMS),
	)
	if nil != err {
		return err
	}
	err = socket.SetOption(
		mangos.OptionSendDeadline,
		time.Millisecond*time.Duration(s.conf.RepTimeoutMS),
	)
	if nil != err {
		return err
	}

	socket.AddTransport(ipc.NewTransport())
	socket.AddTransport(tcp.NewTransport())

	if s.conf.Bind {
		for _, addr := range s.urls {
			if err = socket.Listen(addr); err != nil {
				break
			}
		}
	} else {
		for _, addr := range s.urls {
			if err = socket.Dial(addr); err != nil {
				break
			}
		}
	}
	if err != nil {
		return err
	}

	for _, filter := range s.conf.SubFilters {
		if err = socket.SetOption(mangos.OptionSubscribe, []byte(filter)); err != nil {
			return err
		}
	}

	if s.conf.Bind {
		s.log.Infof(
			"Receiving Scalability Protocols messages at bound URLs: %s\n",
			s.urls,
		)
	} else {
		s.log.Infof(
			"Receiving Scalability Protocols messages at connected URLs: %s\n",
			s.urls,
		)
	}

	s.socket = socket
	return nil
}

// Read attempts to read a new message from the nanomsg socket.
func (s *ScaleProto) Read() (types.Message, error) {
	s.cMut.Lock()
	socket := s.socket
	s.cMut.Unlock()

	if socket == nil {
		return nil, types.ErrNotConnected
	}
	data, err := socket.Recv()
	if err != nil {
		if err == mangos.ErrRecvTimeout {
			return nil, types.ErrTimeout
		}
		return nil, err
	}
	return types.NewMessage([][]byte{data}), nil
}

// Acknowledge instructs whether the pending messages were propagated
// successfully.
func (s *ScaleProto) Acknowledge(err error) error {
	return nil
}

// CloseAsync shuts down the ScaleProto input and stops processing requests.
func (s *ScaleProto) CloseAsync() {
	s.cMut.Lock()
	if s.socket != nil {
		s.socket.Close()
		s.socket = nil
	}
	s.cMut.Unlock()
}

// WaitForClose blocks until the ScaleProto input has closed down.
func (s *ScaleProto) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
