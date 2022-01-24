package writer

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/impl/nats/auth"
	"github.com/Jeffail/benthos/v3/internal/interop"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/nats-io/nats.go"
)

//------------------------------------------------------------------------------

// NATSConfig contains configuration fields for the NATS output type.
type NATSConfig struct {
	URLs        []string          `json:"urls" yaml:"urls"`
	Subject     string            `json:"subject" yaml:"subject"`
	Headers     map[string]string `json:"headers" yaml:"headers"`
	MaxInFlight int               `json:"max_in_flight" yaml:"max_in_flight"`
	TLS         btls.Config       `json:"tls" yaml:"tls"`
	Auth        auth.Config       `json:"auth" yaml:"auth"`
}

// NewNATSConfig creates a new NATSConfig with default values.
func NewNATSConfig() NATSConfig {
	return NATSConfig{
		URLs:        []string{nats.DefaultURL},
		Subject:     "benthos_messages",
		MaxInFlight: 1,
		TLS:         btls.NewConfig(),
		Auth:        auth.New(),
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
	headers    map[string]*field.Expression
	subjectStr *field.Expression
	tlsConf    *tls.Config
}

// NewNATSV2 creates a new NATS output type.
func NewNATSV2(conf NATSConfig, mgr types.Manager, log log.Modular, stats metrics.Type) (*NATS, error) {
	n := NATS{
		log:     log,
		conf:    conf,
		headers: make(map[string]*field.Expression),
	}
	var err error
	if n.subjectStr, err = interop.NewBloblangField(mgr, conf.Subject); err != nil {
		return nil, fmt.Errorf("failed to parse subject expression: %v", err)
	}
	for k, v := range conf.Headers {
		if n.headers[k], err = interop.NewBloblangField(mgr, v); err != nil {
			return nil, fmt.Errorf("failed to parse header '%s' expresion: %v", k, err)
		}
	}
	n.urls = strings.Join(conf.URLs, ",")

	if conf.TLS.Enabled {
		if n.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}

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
	var opts []nats.Option

	if n.tlsConf != nil {
		opts = append(opts, nats.Secure(n.tlsConf))
	}

	opts = append(opts, auth.GetOptions(n.conf.Auth)...)

	if n.natsConn, err = nats.Connect(n.urls, opts...); err != nil {
		return err
	}

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
		// fill message data
		nMsg := nats.NewMsg(subject)
		nMsg.Data = p.Get()
		if conn.HeadersSupported() {
			// fill bloblang headers
			for k, v := range n.headers {
				nMsg.Header.Add(k, v.String(i, msg))
			}
		}
		err := conn.PublishMsg(nMsg)
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
