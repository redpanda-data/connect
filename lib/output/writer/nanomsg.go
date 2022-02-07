package writer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pub"
	"go.nanomsg.org/mangos/v3/protocol/push"

	// Import all transport types
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

//------------------------------------------------------------------------------

// NanomsgConfig contains configuration fields for the Nanomsg output type.
type NanomsgConfig struct {
	URLs        []string `json:"urls" yaml:"urls"`
	Bind        bool     `json:"bind" yaml:"bind"`
	SocketType  string   `json:"socket_type" yaml:"socket_type"`
	PollTimeout string   `json:"poll_timeout" yaml:"poll_timeout"`
	MaxInFlight int      `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewNanomsgConfig creates a new NanomsgConfig with default values.
func NewNanomsgConfig() NanomsgConfig {
	return NanomsgConfig{
		URLs:        []string{},
		Bind:        false,
		SocketType:  "PUSH",
		PollTimeout: "5s",
		MaxInFlight: 1,
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
				s.urls = append(s.urls, strings.Replace(splitU, "//*:", "//0.0.0.0:", 1))
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
	if err != nil {
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
	return nil, errors.New("invalid Scalability Protocols socket type")
}

// ConnectWithContext establishes a connection to a nanomsg socket.
func (s *Nanomsg) ConnectWithContext(ctx context.Context) error {
	return s.Connect()
}

// Connect establishes a connection to a nanomsg socket.
func (s *Nanomsg) Connect() error {
	s.sockMut.Lock()
	defer s.sockMut.Unlock()

	if s.socket != nil {
		return nil
	}

	socket, err := getSocketFromType(s.conf.SocketType)
	if err != nil {
		return err
	}

	// Set timeout to prevent endless lock.
	if s.conf.SocketType == "PUSH" {
		if err := socket.SetOption(
			mangos.OptionSendDeadline, s.timeout,
		); err != nil {
			return err
		}
	}

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

// WriteWithContext attempts to write a message by pushing it to a nanomsg
// socket.
func (s *Nanomsg) WriteWithContext(ctx context.Context, msg types.Message) error {
	return s.Write(msg)
}

// Write attempts to write a message by pushing it to a nanomsg socket.
func (s *Nanomsg) Write(msg types.Message) error {
	s.sockMut.RLock()
	socket := s.socket
	s.sockMut.RUnlock()

	if socket == nil {
		return types.ErrNotConnected
	}

	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		return socket.Send(p.Get())
	})
}

// CloseAsync shuts down the Nanomsg output and stops processing messages.
func (s *Nanomsg) CloseAsync() {
	go func() {
		s.sockMut.Lock()
		if s.socket != nil {
			s.socket.Close()
			s.socket = nil
		}
		s.sockMut.Unlock()
	}()
}

// WaitForClose blocks until the Nanomsg output has closed down.
func (s *Nanomsg) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
