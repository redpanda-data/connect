package writer

import (
	"net"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// UDPConfig contains configuration fields for the UDP output type.
type UDPConfig struct {
	Address string `json:"address" yaml:"address"`
}

// NewUDPConfig creates a new UDPConfig with default values.
func NewUDPConfig() UDPConfig {
	return UDPConfig{
		Address: "localhost:4194",
	}
}

//------------------------------------------------------------------------------

// UDP is an output type that sends messages as a continuous steam of line
// delimied messages over UDP.
type UDP struct {
	connMut sync.Mutex
	conn    net.Conn

	address string

	stats metrics.Type
	log   log.Modular
}

// NewUDP creates a new UDP writer type.
func NewUDP(
	conf UDPConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*UDP, error) {
	t := UDP{
		address: conf.Address,
		stats:   stats,
		log:     log,
	}
	return &t, nil
}

//------------------------------------------------------------------------------

// Connect does nothing.
func (t *UDP) Connect() error {
	t.connMut.Lock()
	defer t.connMut.Unlock()
	if t.conn != nil {
		return nil
	}

	var err error
	if t.conn, err = net.Dial("udp", t.address); err != nil {
		return err
	}

	t.log.Infof("Sending messages over UDP to: %s\n", t.address)
	return nil
}

// Write attempts to write a message.
func (t *UDP) Write(msg *message.Batch) error {
	t.connMut.Lock()
	conn := t.conn
	t.connMut.Unlock()

	if conn == nil {
		return types.ErrNotConnected
	}

	err := msg.Iter(func(i int, part *message.Part) error {
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
		t.connMut.Lock()
		t.conn.Close()
		t.conn = nil
		t.connMut.Unlock()
	}
	return err
}

// CloseAsync shuts down the UDP output and stops processing messages.
func (t *UDP) CloseAsync() {
	t.connMut.Lock()
	if t.conn != nil {
		t.conn.Close()
		t.conn = nil
	}
	t.connMut.Unlock()
}

// WaitForClose blocks until the UDP output has closed down.
func (t *UDP) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
