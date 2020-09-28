package reader

import (
	"context"
	"crypto/tls"
	"sync"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/amqp/sasl"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
)

//------------------------------------------------------------------------------

// AMQP1Config contains configuration for the AMQP1 input type.
type AMQP1Config struct {
	URL           string      `json:"url" yaml:"url"`
	SourceAddress string      `json:"source_address" yaml:"source_address"`
	TLS           btls.Config `json:"tls" yaml:"tls"`
	SASL          sasl.Config `json:"sasl" yaml:"sasl"`
}

// NewAMQP1Config creates a new AMQP1Config with default values.
func NewAMQP1Config() AMQP1Config {
	return AMQP1Config{
		URL:           "",
		SourceAddress: "",
		TLS:           btls.NewConfig(),
		SASL:          sasl.NewConfig(),
	}
}

//------------------------------------------------------------------------------

// AMQP1 is an input type that reads messages via the AMQP 1.0 protocol.
type AMQP1 struct {
	client   *amqp.Client
	session  *amqp.Session
	receiver *amqp.Receiver

	tlsConf *tls.Config

	conf  AMQP1Config
	stats metrics.Type
	log   log.Modular

	m sync.RWMutex
}

// NewAMQP1 creates a new AMQP1 input type.
func NewAMQP1(conf AMQP1Config, log log.Modular, stats metrics.Type) (*AMQP1, error) {
	a := AMQP1{
		conf:  conf,
		stats: stats,
		log:   log,
	}
	if conf.TLS.Enabled {
		var err error
		if a.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}
	return &a, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to an AMQP1 server.
func (a *AMQP1) ConnectWithContext(ctx context.Context) error {
	a.m.Lock()
	defer a.m.Unlock()

	if a.client != nil {
		return nil
	}

	var (
		client   *amqp.Client
		session  *amqp.Session
		receiver *amqp.Receiver
		err      error
	)

	opts, err := a.conf.SASL.ToOptFns()
	if err != nil {
		return err
	}
	if a.conf.TLS.Enabled {
		opts = append(opts, amqp.ConnTLS(true))
		opts = append(opts, amqp.ConnTLSConfig(a.tlsConf))
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

	// Create a receiver
	if receiver, err = session.NewReceiver(
		amqp.LinkSourceAddress(a.conf.SourceAddress),
		amqp.LinkCredit(10),
	); err != nil {
		session.Close(context.Background())
		client.Close()
		return err
	}

	a.client = client
	a.session = session
	a.receiver = receiver

	a.log.Infof("Receiving AMQP 1.0 messages from source: %v\n", a.conf.SourceAddress)
	return nil
}

// disconnect safely closes a connection to an AMQP1 server.
func (a *AMQP1) disconnect(ctx context.Context) error {
	a.m.Lock()
	defer a.m.Unlock()

	if a.client == nil {
		return nil
	}

	if err := a.receiver.Close(ctx); err != nil {
		a.log.Errorf("Failed to cleanly close receiver: %v\n", err)
	}
	if err := a.session.Close(ctx); err != nil {
		a.log.Errorf("Failed to cleanly close session: %v\n", err)
	}
	if err := a.client.Close(); err != nil {
		a.log.Errorf("Failed to cleanly close client: %v\n", err)
	}
	a.client = nil
	a.session = nil
	a.receiver = nil

	return nil
}

//------------------------------------------------------------------------------

// ReadWithContext a new AMQP1 message.
func (a *AMQP1) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	var r *amqp.Receiver
	a.m.RLock()
	if a.receiver != nil {
		r = a.receiver
	}
	a.m.RUnlock()

	if r == nil {
		return nil, nil, types.ErrNotConnected
	}

	// Receive next message
	amqpMsg, err := r.Receive(ctx)
	if err != nil {
		if err == amqp.ErrTimeout {
			err = types.ErrTimeout
		} else {
			if dErr, isDetachError := err.(*amqp.DetachError); isDetachError && dErr.RemoteError != nil {
				a.log.Errorf("Lost connection due to: %v\n", dErr.RemoteError)
			} else {
				a.log.Errorf("Lost connection due to: %v\n", err)
			}
			a.disconnect(ctx)
			err = types.ErrNotConnected
		}
		return nil, nil, err
	}

	msg := message.New(nil)

	part := message.NewPart(amqpMsg.GetData())

	if amqpMsg.Properties != nil {
		setMetadata(part, "amqp_content_type", amqpMsg.Properties.ContentType)
		setMetadata(part, "amqp_content_encoding", amqpMsg.Properties.ContentEncoding)
		setMetadata(part, "amqp_creation_time", amqpMsg.Properties.CreationTime)
	}

	msg.Append(part)

	return msg, func(ctx context.Context, res types.Response) error {
		if res.Error() != nil {
			return amqpMsg.Modify(ctx, true, false, amqpMsg.Annotations)
		}
		return amqpMsg.Accept(ctx)
	}, nil
}

// CloseAsync shuts down the AMQP1 input and stops processing requests.
func (a *AMQP1) CloseAsync() {
	a.disconnect(context.Background())
}

// WaitForClose blocks until the AMQP1 input has closed down.
func (a *AMQP1) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
