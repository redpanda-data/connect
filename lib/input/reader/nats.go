package reader

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/impl/nats/auth"
	"github.com/Jeffail/benthos/v3/lib/response"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/nats-io/nats.go"
)

//------------------------------------------------------------------------------

// NATSConfig contains configuration fields for the NATS input type.
type NATSConfig struct {
	URLs          []string    `json:"urls" yaml:"urls"`
	Subject       string      `json:"subject" yaml:"subject"`
	QueueID       string      `json:"queue" yaml:"queue"`
	PrefetchCount int         `json:"prefetch_count" yaml:"prefetch_count"`
	TLS           btls.Config `json:"tls" yaml:"tls"`
	Auth          auth.Config `json:"auth" yaml:"auth"`
}

// NewNATSConfig creates a new NATSConfig with default values.
func NewNATSConfig() NATSConfig {
	return NATSConfig{
		URLs:          []string{nats.DefaultURL},
		Subject:       "benthos_messages",
		QueueID:       "benthos_queue",
		PrefetchCount: 32,
		TLS:           btls.NewConfig(),
		Auth:          auth.New(),
	}
}

//------------------------------------------------------------------------------

// NATS is an input type that receives NATS messages.
type NATS struct {
	urls  string
	conf  NATSConfig
	stats metrics.Type
	log   log.Modular

	cMut sync.Mutex

	natsConn      *nats.Conn
	natsSub       *nats.Subscription
	natsChan      chan *nats.Msg
	interruptChan chan struct{}
	tlsConf       *tls.Config
}

// NewNATS creates a new NATS input type.
func NewNATS(conf NATSConfig, log log.Modular, stats metrics.Type) (*NATS, error) {
	n := NATS{
		conf:          conf,
		stats:         stats,
		log:           log,
		interruptChan: make(chan struct{}),
	}
	n.urls = strings.Join(conf.URLs, ",")
	if conf.PrefetchCount < 0 {
		return nil, errors.New("prefetch count must be greater than or equal to zero")
	}
	var err error
	if conf.TLS.Enabled {
		if n.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}

	return &n, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to a NATS server.
func (n *NATS) ConnectWithContext(ctx context.Context) error {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.natsConn != nil {
		return nil
	}

	var natsConn *nats.Conn
	var natsSub *nats.Subscription
	var err error
	var opts []nats.Option

	if n.tlsConf != nil {
		opts = append(opts, nats.Secure(n.tlsConf))
	}

	opts = append(opts, auth.GetOptions(n.conf.Auth)...)

	if natsConn, err = nats.Connect(n.urls, opts...); err != nil {
		return err
	}
	natsChan := make(chan *nats.Msg, n.conf.PrefetchCount)

	if len(n.conf.QueueID) > 0 {
		natsSub, err = natsConn.ChanQueueSubscribe(n.conf.Subject, n.conf.QueueID, natsChan)
	} else {
		natsSub, err = natsConn.ChanSubscribe(n.conf.Subject, natsChan)
	}

	if err != nil {
		return err
	}

	n.log.Infof("Receiving NATS messages from subject: %v\n", n.conf.Subject)

	n.natsConn = natsConn
	n.natsSub = natsSub
	n.natsChan = natsChan
	return nil
}

func (n *NATS) disconnect() {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.natsSub != nil {
		n.natsSub.Unsubscribe()
		n.natsSub = nil
	}
	if n.natsConn != nil {
		n.natsConn.Close()
		n.natsConn = nil
	}
	n.natsChan = nil
}

// ReadWithContext attempts to read a new message from the NATS subject.
func (n *NATS) ReadWithContext(ctx context.Context) (*message.Batch, AsyncAckFn, error) {
	n.cMut.Lock()
	natsChan := n.natsChan
	natsConn := n.natsConn
	n.cMut.Unlock()

	var msg *nats.Msg
	var open bool
	select {
	case msg, open = <-natsChan:
	case <-ctx.Done():
		return nil, nil, component.ErrTimeout
	case _, open = <-n.interruptChan:
	}
	if !open {
		n.disconnect()
		return nil, nil, component.ErrNotConnected
	}

	bmsg := message.QuickBatch([][]byte{msg.Data})
	part := bmsg.Get(0)
	part.MetaSet("nats_subject", msg.Subject)
	// process message headers if server supports the feature
	if natsConn.HeadersSupported() {
		for key := range msg.Header {
			value := msg.Header.Get(key)
			part.MetaSet(key, value)
		}
	}

	return bmsg, func(ctx context.Context, res response.Error) error {
		var ackErr error
		if res.AckError() != nil {
			ackErr = msg.Nak()
		} else {
			ackErr = msg.Ack()
		}
		if errors.Is(ackErr, nats.ErrMsgNoReply) {
			ackErr = nil
		}
		return ackErr
	}, nil
}

// CloseAsync shuts down the NATS input and stops processing requests.
func (n *NATS) CloseAsync() {
	close(n.interruptChan)
}

// WaitForClose blocks until the NATS input has closed down.
func (n *NATS) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
