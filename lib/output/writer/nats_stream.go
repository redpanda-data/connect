package writer

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/impl/nats/auth"
	"github.com/Jeffail/benthos/v3/lib/message"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/nats-io/nats.go"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/nats-io/stan.go"
)

//------------------------------------------------------------------------------

// NATSStreamConfig contains configuration fields for the NATSStream output
// type.
type NATSStreamConfig struct {
	URLs        []string    `json:"urls" yaml:"urls"`
	ClusterID   string      `json:"cluster_id" yaml:"cluster_id"`
	ClientID    string      `json:"client_id" yaml:"client_id"`
	Subject     string      `json:"subject" yaml:"subject"`
	MaxInFlight int         `json:"max_in_flight" yaml:"max_in_flight"`
	TLS         btls.Config `json:"tls" yaml:"tls"`
	Auth        auth.Config `json:"auth" yaml:"auth"`
}

// NewNATSStreamConfig creates a new NATSStreamConfig with default values.
func NewNATSStreamConfig() NATSStreamConfig {
	return NATSStreamConfig{
		URLs:        []string{stan.DefaultNatsURL},
		ClusterID:   "test-cluster",
		ClientID:    "benthos_client",
		Subject:     "benthos_messages",
		MaxInFlight: 1,
		TLS:         btls.NewConfig(),
		Auth:        auth.New(),
	}
}

//------------------------------------------------------------------------------

// NATSStream is an output type that serves NATS messages.
type NATSStream struct {
	log log.Modular

	stanConn stan.Conn
	natsConn *nats.Conn
	connMut  sync.RWMutex

	urls    string
	conf    NATSStreamConfig
	tlsConf *tls.Config
}

// NewNATSStream creates a new NATS Stream output type.
func NewNATSStream(conf NATSStreamConfig, log log.Modular, stats metrics.Type) (*NATSStream, error) {
	if conf.ClientID == "" {
		rgen := rand.New(rand.NewSource(time.Now().UnixNano()))

		// Generate random client id if one wasn't supplied.
		b := make([]byte, 16)
		rgen.Read(b)
		conf.ClientID = fmt.Sprintf("client-%x", b)
	}

	n := NATSStream{
		log:  log,
		conf: conf,
	}
	n.urls = strings.Join(conf.URLs, ",")
	var err error
	if conf.TLS.Enabled {
		if n.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}

	return &n, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext attempts to establish a connection to NATS servers.
func (n *NATSStream) ConnectWithContext(ctx context.Context) error {
	return n.Connect()
}

// Connect attempts to establish a connection to NATS servers.
func (n *NATSStream) Connect() error {
	n.connMut.Lock()
	defer n.connMut.Unlock()

	if n.natsConn != nil {
		return nil
	}

	var opts []nats.Option
	if n.tlsConf != nil {
		opts = append(opts, nats.Secure(n.tlsConf))
	}

	opts = append(opts, auth.GetOptions(n.conf.Auth)...)

	natsConn, err := nats.Connect(n.urls, opts...)
	if err != nil {
		return err
	}

	stanConn, err := stan.Connect(
		n.conf.ClusterID,
		n.conf.ClientID,
		stan.NatsConn(natsConn),
	)
	if err != nil {
		natsConn.Close()
		return err
	}

	n.stanConn = stanConn
	n.natsConn = natsConn
	n.log.Infof("Sending NATS messages to subject: %v\n", n.conf.Subject)
	return nil
}

// WriteWithContext attempts to write a message.
func (n *NATSStream) WriteWithContext(ctx context.Context, msg *message.Batch) error {
	return n.Write(msg)
}

// Write attempts to write a message.
func (n *NATSStream) Write(msg *message.Batch) error {
	n.connMut.RLock()
	conn := n.stanConn
	n.connMut.RUnlock()

	if conn == nil {
		return component.ErrNotConnected
	}

	return IterateBatchedSend(msg, func(i int, p *message.Part) error {
		err := conn.Publish(n.conf.Subject, p.Get())
		if err == stan.ErrConnectionClosed {
			conn.Close()
			n.connMut.Lock()
			n.stanConn = nil
			n.natsConn.Close()
			n.natsConn = nil
			n.connMut.Unlock()
			return component.ErrNotConnected
		}
		return err
	})
}

// CloseAsync shuts down the MQTT output and stops processing messages.
func (n *NATSStream) CloseAsync() {
	n.connMut.Lock()
	if n.natsConn != nil {
		n.natsConn.Close()
		n.natsConn = nil
	}
	if n.stanConn != nil {
		n.stanConn.Close()
		n.stanConn = nil
	}
	n.connMut.Unlock()
}

// WaitForClose blocks until the NATS output has closed down.
func (n *NATSStream) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
