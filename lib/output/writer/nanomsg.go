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

package writer

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"nanomsg.org/go-mangos"
	"nanomsg.org/go-mangos/protocol/pub"
	"nanomsg.org/go-mangos/protocol/push"
	"nanomsg.org/go-mangos/transport/ipc"
	"nanomsg.org/go-mangos/transport/tcp"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// NanomsgConfig contains configuration fields for the Nanomsg output type.
type NanomsgConfig struct {
	URLs        []string `json:"urls" yaml:"urls"`
	Bind        bool     `json:"bind" yaml:"bind"`
	SocketType  string   `json:"socket_type" yaml:"socket_type"`
	PollTimeout string   `json:"poll_timeout" yaml:"poll_timeout"`
}

// NewNanomsgConfig creates a new NanomsgConfig with default values.
func NewNanomsgConfig() NanomsgConfig {
	return NanomsgConfig{
		URLs:        []string{"tcp://localhost:5556"},
		Bind:        false,
		SocketType:  "PUSH",
		PollTimeout: "5s",
	}
}

//------------------------------------------------------------------------------

// Nanomsg is an output type that serves Nanomsg messages.
type Nanomsg struct {
	log   log.Modular
	stats metrics.Type

	urls []string
	conf NanomsgConfig

	timeout time.Duration

	socket  mangos.Socket
	sockMut sync.RWMutex
}

// NewNanomsg creates a new Nanomsg output type.
func NewNanomsg(conf NanomsgConfig, log log.Modular, stats metrics.Type) (*Nanomsg, error) {
	s := Nanomsg{
		log:   log,
		stats: stats,
		conf:  conf,
	}
	for _, u := range conf.URLs {
		for _, splitU := range strings.Split(u, ",") {
			if len(splitU) > 0 {
				s.urls = append(s.urls, splitU)
			}
		}
	}

	if tout := conf.PollTimeout; len(tout) > 0 {
		var err error
		if s.timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse poll timeout string: %v", err)
		}
	}

	socket, err := getSocketFromType(conf.SocketType)
	if nil != err {
		return nil, err
	}
	socket.Close()
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

// Connect establishes a connection to a nanomsg socket.
func (s *Nanomsg) Connect() error {
	s.sockMut.Lock()
	defer s.sockMut.Unlock()

	if s.socket != nil {
		return nil
	}

	socket, err := getSocketFromType(s.conf.SocketType)
	if nil != err {
		return err
	}

	// Set timeout to prevent endless lock.
	if err = socket.SetOption(
		mangos.OptionRecvDeadline, s.timeout,
	); nil != err {
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

	if s.conf.Bind {
		s.log.Infof(
			"Sending nanomsg messages to bound URLs: %s\n",
			s.urls,
		)
	} else {
		s.log.Infof(
			"Sending nanomsg messages to connected URLs: %s\n",
			s.urls,
		)
	}
	s.socket = socket
	return nil
}

//------------------------------------------------------------------------------

// Write attempts to write a message by pushing it to a nanomsg socket.
func (s *Nanomsg) Write(msg types.Message) error {
	s.sockMut.RLock()
	socket := s.socket
	s.sockMut.RUnlock()

	if socket == nil {
		return types.ErrNotConnected
	}

	return msg.Iter(func(i int, p types.Part) error {
		return socket.Send(p.Get())
	})
}

// CloseAsync shuts down the Nanomsg output and stops processing messages.
func (s *Nanomsg) CloseAsync() {
	s.sockMut.Lock()
	if s.socket != nil {
		s.socket.Close()
		s.socket = nil
	}
	s.sockMut.Unlock()
}

// WaitForClose blocks until the Nanomsg output has closed down.
func (s *Nanomsg) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
