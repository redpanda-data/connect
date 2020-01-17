package writer

import (
	"net"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// SocketConfig contains configuration fields for the Socket output type.
type SocketConfig struct {
	Network string `json:"network" yaml:"network"`
	Address string `json:"address" yaml:"address"`
}

// NewSocketConfig creates a new SocketConfig with default values.
func NewSocketConfig() SocketConfig {
	return SocketConfig{
		Network: "unix",
		Address: "/tmp/benthos.sock",
	}
}

//------------------------------------------------------------------------------

// Socket is an output type that sends messages as a continuous steam of line
// delimied messages over socket.
type Socket struct {
	connMut sync.Mutex
	conn    net.Conn

	network string
	address string

	stats metrics.Type
	log   log.Modular
}

// NewSocket creates a new Socket writer type.
func NewSocket(
	conf SocketConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*Socket, error) {
	t := Socket{
		network: conf.Network,
		address: conf.Address,
		stats:   stats,
		log:     log,
	}
	return &t, nil
}

//------------------------------------------------------------------------------

// Connect does nothing.
func (s *Socket) Connect() error {
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
	s.connMut.Lock()
	conn := s.conn
	s.connMut.Unlock()

	if conn == nil {
		return types.ErrNotConnected
	}

	err := msg.Iter(func(i int, part types.Part) error {
		partBytes := part.Get()
		if partBytes[len(partBytes)-1] != '\n' {
			partBytes = append(partBytes[:len(partBytes):len(partBytes)], []byte("\n")...)
		}
		_, werr := conn.Write(partBytes)
		return werr
	})
	if err == nil && msg.Len() > 1 {
		_, err = conn.Write([]byte("\n"))
	}
	if err != nil {
		s.connMut.Lock()
		s.conn.Close()
		s.conn = nil
		s.connMut.Unlock()
	}
	return err
}

// CloseAsync shuts down the socket output and stops processing messages.
func (s *Socket) CloseAsync() {
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
