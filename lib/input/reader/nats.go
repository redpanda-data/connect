package reader

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/nats-io/nats.go"
)

//------------------------------------------------------------------------------

// NATSConfig contains configuration fields for the NATS input type.
type NATSConfig struct {
	URLs          []string `json:"urls" yaml:"urls"`
	Username      string   `json:"username" yaml:"username"`
	Password      string   `json:"password" yaml:"password"`
	Subject       string   `json:"subject" yaml:"subject"`
	QueueID       string   `json:"queue" yaml:"queue"`
	PrefetchCount int      `json:"prefetch_count" yaml:"prefetch_count"`
}

// NewNATSConfig creates a new NATSConfig with default values.
func NewNATSConfig() NATSConfig {
	return NATSConfig{
		URLs:          []string{nats.DefaultURL},
		Subject:       "benthos_messages",
		QueueID:       "benthos_queue",
		PrefetchCount: 32,
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

	return &n, nil
}

//------------------------------------------------------------------------------

// Connect establishes a connection to a NATS server.
func (n *NATS) Connect() error {
	return n.ConnectWithContext(context.Background())
}

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

	opts := []nats.Option{}
	if n.conf.UserName != "" || n.conf.Password != "" {
		opts = append(opts, nats.UserInfo(n.conf.UserName, n.conf.Password))
	}

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

// Read attempts to read a new message from the NATS subject.
func (n *NATS) Read() (types.Message, error) {
	msg, _, err := n.ReadWithContext(context.Background())
	return msg, err
}

// ReadWithContext attempts to read a new message from the NATS subject.
func (n *NATS) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	n.cMut.Lock()
	natsChan := n.natsChan
	n.cMut.Unlock()

	var msg *nats.Msg
	var open bool
	select {
	case msg, open = <-natsChan:
	case <-ctx.Done():
		return nil, nil, types.ErrTimeout
	case _, open = <-n.interruptChan:
	}
	if !open {
		n.disconnect()
		return nil, nil, types.ErrNotConnected
	}

	bmsg := message.New([][]byte{msg.Data})
	bmsg.Get(0).Metadata().Set("nats_subject", msg.Subject)

	return bmsg, noopAsyncAckFn, nil
}

// Acknowledge is a noop since NATS messages do not support acknowledgments.
func (n *NATS) Acknowledge(err error) error {
	return nil
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
