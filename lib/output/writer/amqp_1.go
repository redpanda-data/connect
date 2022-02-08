package writer

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/metadata"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/util/amqp/sasl"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
)

//------------------------------------------------------------------------------

// AMQP1Config contains configuration fields for the AMQP1 output type.
type AMQP1Config struct {
	URL           string                       `json:"url" yaml:"url"`
	TargetAddress string                       `json:"target_address" yaml:"target_address"`
	MaxInFlight   int                          `json:"max_in_flight" yaml:"max_in_flight"`
	TLS           btls.Config                  `json:"tls" yaml:"tls"`
	SASL          sasl.Config                  `json:"sasl" yaml:"sasl"`
	Metadata      metadata.ExcludeFilterConfig `json:"metadata" yaml:"metadata"`
}

// NewAMQP1Config creates a new AMQP1Config with default values.
func NewAMQP1Config() AMQP1Config {
	return AMQP1Config{
		URL:           "",
		TargetAddress: "",
		MaxInFlight:   1,
		TLS:           btls.NewConfig(),
		SASL:          sasl.NewConfig(),
		Metadata:      metadata.NewExcludeFilterConfig(),
	}
}

//------------------------------------------------------------------------------

// AMQP1 is an output type that serves AMQP1 messages.
type AMQP1 struct {
	client  *amqp.Client
	session *amqp.Session
	sender  *amqp.Sender

	metaFilter *metadata.ExcludeFilter

	log   log.Modular
	stats metrics.Type

	conf    AMQP1Config
	tlsConf *tls.Config

	connLock sync.RWMutex
}

// NewAMQP1 creates a new AMQP1 writer type.
func NewAMQP1(conf AMQP1Config, log log.Modular, stats metrics.Type) (*AMQP1, error) {
	a := AMQP1{
		log:   log,
		stats: stats,
		conf:  conf,
	}
	var err error
	if conf.TLS.Enabled {
		if a.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}
	if a.metaFilter, err = conf.Metadata.Filter(); err != nil {
		return nil, fmt.Errorf("failed to construct metadata filter: %w", err)
	}
	return &a, nil
}

//------------------------------------------------------------------------------

// Connect establishes a connection to an AMQP1 server.
func (a *AMQP1) Connect() error {
	return a.ConnectWithContext(context.Background())
}

// ConnectWithContext establishes a connection to an AMQP1 server.
func (a *AMQP1) ConnectWithContext(ctx context.Context) error {
	a.connLock.Lock()
	defer a.connLock.Unlock()

	if a.client != nil {
		return nil
	}

	var (
		client  *amqp.Client
		session *amqp.Session
		sender  *amqp.Sender
		err     error
	)

	opts, err := a.conf.SASL.ToOptFns()
	if err != nil {
		return err
	}
	if a.conf.TLS.Enabled {
		opts = append(opts, amqp.ConnTLS(true), amqp.ConnTLSConfig(a.tlsConf))
	}

	// Create client
	if client, err = amqp.Dial(a.conf.URL, opts...); err != nil {
		return err
	}

	// Open a session
	if session, err = client.NewSession(); err != nil {
		client.Close()
		return err
	}

	// Create a sender
	if sender, err = session.NewSender(
		amqp.LinkTargetAddress(a.conf.TargetAddress),
	); err != nil {
		session.Close(context.Background())
		client.Close()
		return err
	}

	a.client = client
	a.session = session
	a.sender = sender

	a.log.Infof("Sending AMQP 1.0 messages to target: %v\n", a.conf.TargetAddress)
	return nil
}

// disconnect safely closes a connection to an AMQP1 server.
func (a *AMQP1) disconnect(ctx context.Context) error {
	a.connLock.Lock()
	defer a.connLock.Unlock()

	if a.client == nil {
		return nil
	}

	if err := a.sender.Close(ctx); err != nil {
		a.log.Errorf("Failed to cleanly close sender: %v\n", err)
	}
	if err := a.session.Close(ctx); err != nil {
		a.log.Errorf("Failed to cleanly close session: %v\n", err)
	}
	if err := a.client.Close(); err != nil {
		a.log.Errorf("Failed to cleanly close client: %v\n", err)
	}
	a.client = nil
	a.session = nil
	a.sender = nil

	return nil
}

//------------------------------------------------------------------------------

// Write will attempt to write a message over AMQP1, wait for acknowledgement,
// and returns an error if applicable.
func (a *AMQP1) Write(msg *message.Batch) error {
	return a.WriteWithContext(context.Background(), msg)
}

// WriteWithContext will attempt to write a message over AMQP1, wait for
// acknowledgement, and returns an error if applicable.
func (a *AMQP1) WriteWithContext(ctx context.Context, msg *message.Batch) error {
	var s *amqp.Sender
	a.connLock.RLock()
	if a.sender != nil {
		s = a.sender
	}
	a.connLock.RUnlock()

	if s == nil {
		return component.ErrNotConnected
	}

	return IterateBatchedSend(msg, func(i int, p *message.Part) error {
		m := amqp.NewMessage(p.Get())
		a.metaFilter.Iter(p, func(k, v string) error {
			if m.Annotations == nil {
				m.Annotations = amqp.Annotations{}
			}
			m.Annotations[k] = v
			return nil
		})
		err := s.Send(ctx, m)
		if err != nil {
			if err == amqp.ErrTimeout {
				err = component.ErrTimeout
			} else {
				if dErr, isDetachError := err.(*amqp.DetachError); isDetachError && dErr.RemoteError != nil {
					a.log.Errorf("Lost connection due to: %v\n", dErr.RemoteError)
				} else {
					a.log.Errorf("Lost connection due to: %v\n", err)
				}
				a.disconnect(ctx)
				err = component.ErrNotConnected
			}
		}
		return err
	})
}

// CloseAsync shuts down the AMQP1 output and stops processing messages.
func (a *AMQP1) CloseAsync() {
	a.disconnect(context.Background())
}

// WaitForClose blocks until the AMQP1 output has closed down.
func (a *AMQP1) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
