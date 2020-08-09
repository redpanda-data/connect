package writer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/nats-io/nats.go"
)

//------------------------------------------------------------------------------

// NATSConfig contains configuration fields for the NATS output type.
type NATSConfig struct {
	URLs        []string `json:"urls" yaml:"urls"`
	Subject     string   `json:"subject" yaml:"subject"`
	MaxInFlight int      `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewNATSConfig creates a new NATSConfig with default values.
func NewNATSConfig() NATSConfig {
	return NATSConfig{
		URLs:        []string{nats.DefaultURL},
		Subject:     "benthos_messages",
		MaxInFlight: 1,
	}
}

//------------------------------------------------------------------------------

// NATS is an output type that serves NATS messages.
type NATS struct {
	log log.Modular

	natsConn *nats.Conn
	connMut  sync.RWMutex

	urls       string
	conf       NATSConfig
	subjectStr field.Expression
}

// NewNATS creates a new NATS output type.
func NewNATS(conf NATSConfig, log log.Modular, stats metrics.Type) (*NATS, error) {
	n := NATS{
		log:  log,
		conf: conf,
	}
	var err error
	if n.subjectStr, err = bloblang.NewField(conf.Subject); err != nil {
		return nil, fmt.Errorf("failed to parse subject expression: %v", err)
	}
	n.urls = strings.Join(conf.URLs, ",")

	return &n, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext attempts to establish a connection to NATS servers.
func (n *NATS) ConnectWithContext(ctx context.Context) error {
	return n.Connect()
}

// Connect attempts to establish a connection to NATS servers.
func (n *NATS) Connect() error {
	n.connMut.Lock()
	defer n.connMut.Unlock()

	if n.natsConn != nil {
		return nil
	}

	var err error
	n.natsConn, err = nats.Connect(n.urls)
	if err == nil {
		n.log.Infof("Sending NATS messages to subject: %v\n", n.conf.Subject)
	}
	return err
}

// WriteWithContext attempts to write a message.
func (n *NATS) WriteWithContext(ctx context.Context, msg types.Message) error {
	return n.Write(msg)
}

// Write attempts to write a message.
func (n *NATS) Write(msg types.Message) error {
	n.connMut.RLock()
	conn := n.natsConn
	n.connMut.RUnlock()

	if conn == nil {
		return types.ErrNotConnected
	}

	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		subject := n.subjectStr.String(i, msg)
		n.log.Debugf("Writing NATS message to topic %s", subject)
		err := conn.Publish(subject, p.Get())
		if err == nats.ErrConnectionClosed {
			conn.Close()
			n.connMut.Lock()
			n.natsConn = nil
			n.connMut.Unlock()
			return types.ErrNotConnected
		}
		return err
	})
}

// CloseAsync shuts down the MQTT output and stops processing messages.
func (n *NATS) CloseAsync() {
	go func() {
		n.connMut.Lock()
		if n.natsConn != nil {
			n.natsConn.Close()
			n.natsConn = nil
		}
		n.connMut.Unlock()
	}()
}

// WaitForClose blocks until the NATS output has closed down.
func (n *NATS) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
