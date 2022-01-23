package reader

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pull"
	"go.nanomsg.org/mangos/v3/protocol/sub"

	// Import all transport types
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

//------------------------------------------------------------------------------

// ScaleProtoConfig contains configuration fields for the ScaleProto input type.
type ScaleProtoConfig struct {
	URLs        []string `json:"urls" yaml:"urls"`
	Bind        bool     `json:"bind" yaml:"bind"`
	SocketType  string   `json:"socket_type" yaml:"socket_type"`
	SubFilters  []string `json:"sub_filters" yaml:"sub_filters"`
	PollTimeout string   `json:"poll_timeout" yaml:"poll_timeout"`
	RepTimeout  string   `json:"reply_timeout" yaml:"reply_timeout"`
}

// NewScaleProtoConfig creates a new ScaleProtoConfig with default values.
func NewScaleProtoConfig() ScaleProtoConfig {
	return ScaleProtoConfig{
		URLs:        []string{"tcp://*:5555"},
		Bind:        true,
		SocketType:  "PULL",
		SubFilters:  []string{},
		PollTimeout: "5s",
		RepTimeout:  "5s",
	}
}

//------------------------------------------------------------------------------

// ScaleProto is an input type that contains Scalability Protocols messages.
type ScaleProto struct {
	socket mangos.Socket
	cMut   sync.Mutex

	pollTimeout time.Duration
	repTimeout  time.Duration

	urls  []string
	conf  ScaleProtoConfig
	stats metrics.Type
	log   log.Modular
}

// NewScaleProto creates a new ScaleProto input type.
func NewScaleProto(conf ScaleProtoConfig, log log.Modular, stats metrics.Type) (*ScaleProto, error) {
	s := ScaleProto{
		conf:  conf,
		stats: stats,
		log:   log,
	}

	for _, u := range conf.URLs {
		for _, splitU := range strings.Split(u, ",") {
			if len(splitU) > 0 {
				s.urls = append(s.urls, splitU)
			}
		}
	}

	if conf.SocketType == "SUB" && len(conf.SubFilters) == 0 {
		return nil, errors.New("must provide at least one sub filter when connecting with a SUB socket, in order to subscribe to all messages add an empty string")
	}

	if tout := conf.PollTimeout; len(tout) > 0 {
		var err error
		if s.pollTimeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse poll timeout string: %v", err)
		}
	}
	if tout := conf.RepTimeout; len(tout) > 0 {
		var err error
		if s.repTimeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse reply timeout string: %v", err)
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
	return s.ConnectWithContext(context.Background())
}

// ConnectWithContext establishes a nanomsg socket.
func (s *ScaleProto) ConnectWithContext(ctx context.Context) error {
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
	if err != nil {
		return err
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

	// TODO: This is only used for request/response sockets, and is invalid with
	// other socket types.
	// err = socket.SetOption(mangos.OptionSendDeadline, s.pollTimeout)
	// if err != nil {
	// 	return err
	// }

	// Set timeout to prevent endless lock.
	err = socket.SetOption(mangos.OptionRecvDeadline, s.repTimeout)
	if err != nil {
		return err
	}

	for _, filter := range s.conf.SubFilters {
		if err := socket.SetOption(mangos.OptionSubscribe, []byte(filter)); err != nil {
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
	msg, _, err := s.ReadWithContext(context.Background())
	return msg, err
}

// ReadWithContext attempts to read a new message from the nanomsg socket.
func (s *ScaleProto) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	s.cMut.Lock()
	socket := s.socket
	s.cMut.Unlock()

	if socket == nil {
		return nil, nil, types.ErrNotConnected
	}
	data, err := socket.Recv()
	if err != nil {
		if err == mangos.ErrRecvTimeout {
			return nil, nil, types.ErrTimeout
		}
		return nil, nil, err
	}
	return message.New([][]byte{data}), noopAsyncAckFn, nil
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
