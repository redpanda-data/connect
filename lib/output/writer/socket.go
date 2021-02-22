package writer

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// SocketConfig contains configuration fields for the Socket output type.
type SocketConfig struct {
	Network string `json:"network" yaml:"network"`
	Address string `json:"address" yaml:"address"`
	Codec   string `json:"codec" yaml:"codec"`
}

// NewSocketConfig creates a new SocketConfig with default values.
func NewSocketConfig() SocketConfig {
	return SocketConfig{
		Network: "unix",
		Address: "/tmp/benthos.sock",
		Codec:   "lines",
	}
}

//------------------------------------------------------------------------------

// Socket is an output type that sends messages as a continuous steam of line
// delimied messages over socket.
type Socket struct {
	connMut sync.Mutex
	conn    net.Conn

	network   string
	address   string
	codec     codec.WriterConstructor
	codecConf codec.WriterConfig

	stats metrics.Type
	log   log.Modular

	handle    codec.Writer
	handleMut sync.Mutex
}

// NewSocket creates a new Socket writer type.
func NewSocket(
	conf SocketConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*Socket, error) {
	switch conf.Network {
	case "tcp", "udp", "unix":
	default:
		return nil, fmt.Errorf("socket network '%v' is not supported by this output", conf.Network)
	}
	codec, codecConf, err := codec.GetWriter(conf.Codec)
	if err != nil {
		return nil, err
	}
	t := Socket{
		network:   conf.Network,
		address:   conf.Address,
		codec:     codec,
		codecConf: codecConf,
		stats:     stats,
		log:       log,
	}
	return &t, nil
}

//------------------------------------------------------------------------------

// Connect does nothing.
func (s *Socket) Connect() error {
	return s.ConnectWithContext(context.Background())
}

func (s *Socket) ConnectWithContext(ctx context.Context) error {
	s.connMut.Lock()
	defer s.connMut.Unlock()
	if s.conn != nil {
		return nil
	}

	var err error
	if s.conn, err = net.Dial(s.network, s.address); err != nil {
		return err
	}

	s.log.Infof("Sending messages over socket to: %s\n", s.address)
	return nil
}

// Write attempts to write a message.
func (s *Socket) Write(msg types.Message) error {
	return s.WriteWithContext(context.Background(), msg)
}

func (s *Socket) WriteWithContext(ctx context.Context, msg types.Message) error {
	s.connMut.Lock()
	conn := s.conn
	s.connMut.Unlock()

	if conn == nil {
		return types.ErrNotConnected
	}

	err := msg.Iter(func(i int, part types.Part) error {
		s.handleMut.Lock()
		defer s.handleMut.Unlock()

		if s.handle != nil {
			return s.handle.Write(ctx, part)
		}

		handle, err := s.codec(conn)
		if err != nil {
			return err
		}

		if err = handle.Write(ctx, part); err != nil {
			handle.Close(ctx)
			return err
		}

		if !s.codecConf.CloseAfter {
			s.handle = handle
		} else {
			handle.Close(ctx)
		}
		return nil
	})
	if err == nil && msg.Len() > 1 {
		s.handleMut.Lock()
		if s.handle != nil {
			s.handle.EndBatch()
		}
		s.handleMut.Unlock()
	}
	if err != nil {
		s.connMut.Lock()
		s.handleMut.Lock()
		s.handle.Close(ctx)
		s.conn.Close()
		s.handle = nil
		s.conn = nil
		s.handleMut.Unlock()
		s.connMut.Unlock()
	}
	return err
}

// CloseAsync shuts down the socket output and stops processing messages.
func (s *Socket) CloseAsync() {
	s.handleMut.Lock()
	if s.handle != nil {
		s.handle.Close(context.Background())
		s.handle = nil
	}
	s.handleMut.Unlock()
	s.connMut.Lock()
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
	s.connMut.Unlock()
}

// WaitForClose blocks until the socket output has closed down.
func (s *Socket) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
