package reader

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/impl/nats/auth"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/nats-io/nats.go"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/gofrs/uuid"
	"github.com/nats-io/stan.go"
)

//------------------------------------------------------------------------------

// NATSStreamConfig contains configuration fields for the NATSStream input type.
type NATSStreamConfig struct {
	URLs            []string    `json:"urls" yaml:"urls"`
	ClusterID       string      `json:"cluster_id" yaml:"cluster_id"`
	ClientID        string      `json:"client_id" yaml:"client_id"`
	QueueID         string      `json:"queue" yaml:"queue"`
	DurableName     string      `json:"durable_name" yaml:"durable_name"`
	UnsubOnClose    bool        `json:"unsubscribe_on_close" yaml:"unsubscribe_on_close"`
	StartFromOldest bool        `json:"start_from_oldest" yaml:"start_from_oldest"`
	Subject         string      `json:"subject" yaml:"subject"`
	MaxInflight     int         `json:"max_inflight" yaml:"max_inflight"`
	AckWait         string      `json:"ack_wait" yaml:"ack_wait"`
	TLS             btls.Config `json:"tls" yaml:"tls"`
	Auth            auth.Config `json:"auth" yaml:"auth"`
}

// NewNATSStreamConfig creates a new NATSStreamConfig with default values.
func NewNATSStreamConfig() NATSStreamConfig {
	return NATSStreamConfig{
		URLs:            []string{stan.DefaultNatsURL},
		ClusterID:       "test-cluster",
		ClientID:        "benthos_client",
		QueueID:         "benthos_queue",
		DurableName:     "benthos_offset",
		UnsubOnClose:    false,
		StartFromOldest: true,
		Subject:         "benthos_messages",
		MaxInflight:     1024,
		AckWait:         "30s",
		TLS:             btls.NewConfig(),
		Auth:            auth.New(),
	}
}

//------------------------------------------------------------------------------

// NATSStream is an input type that receives NATSStream messages.
type NATSStream struct {
	urls    string
	conf    NATSStreamConfig
	ackWait time.Duration

	stats metrics.Type
	log   log.Modular

	unAckMsgs []*stan.Msg

	stanConn stan.Conn
	natsConn *nats.Conn
	natsSub  stan.Subscription
	cMut     sync.Mutex

	msgChan       chan *stan.Msg
	interruptChan chan struct{}
	tlsConf       *tls.Config
}

// NewNATSStream creates a new NATSStream input type.
func NewNATSStream(conf NATSStreamConfig, log log.Modular, stats metrics.Type) (*NATSStream, error) {
	if conf.ClientID == "" {
		u4, err := uuid.NewV4()
		if err != nil {
			return nil, err
		}
		conf.ClientID = u4.String()
	}

	var ackWait time.Duration
	if tout := conf.AckWait; len(tout) > 0 {
		var err error
		if ackWait, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse ack_wait string: %v", err)
		}
	}

	n := NATSStream{
		conf:          conf,
		ackWait:       ackWait,
		stats:         stats,
		log:           log,
		msgChan:       make(chan *stan.Msg),
		interruptChan: make(chan struct{}),
	}

	close(n.msgChan)
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

func (n *NATSStream) disconnect() {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.natsSub != nil {
		if n.conf.UnsubOnClose {
			n.natsSub.Unsubscribe()
		}
		n.natsConn.Close()
		n.stanConn.Close()

		n.natsSub = nil
		n.natsConn = nil
		n.stanConn = nil
	}
}

// Connect attempts to establish a connection to a NATS streaming server.
func (n *NATSStream) Connect() error {
	return n.ConnectWithContext(context.Background())
}

// ConnectWithContext attempts to establish a connection to a NATS streaming
// server.
func (n *NATSStream) ConnectWithContext(ctx context.Context) error {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.natsSub != nil {
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

	newMsgChan := make(chan *stan.Msg)
	handler := func(m *stan.Msg) {
		select {
		case newMsgChan <- m:
		case <-n.interruptChan:
			n.disconnect()
		}
	}
	dcHandler := func() {
		if newMsgChan == nil {
			return
		}
		close(newMsgChan)
		newMsgChan = nil
		n.disconnect()
	}

	stanConn, err := stan.Connect(
		n.conf.ClusterID,
		n.conf.ClientID,
		stan.NatsConn(natsConn),
		stan.SetConnectionLostHandler(func(_ stan.Conn, reason error) {
			n.log.Errorf("Connection lost: %v", reason)
			dcHandler()
		}),
	)
	if err != nil {
		return err
	}

	options := []stan.SubscriptionOption{
		stan.SetManualAckMode(),
	}
	if len(n.conf.DurableName) > 0 {
		options = append(options, stan.DurableName(n.conf.DurableName))
	}
	if n.conf.StartFromOldest {
		options = append(options, stan.DeliverAllAvailable())
	} else {
		options = append(options, stan.StartWithLastReceived())
	}
	if n.conf.MaxInflight != 0 {
		options = append(options, stan.MaxInflight(n.conf.MaxInflight))
	}
	if n.ackWait > 0 {
		options = append(options, stan.AckWait(n.ackWait))
	}

	var natsSub stan.Subscription
	if len(n.conf.QueueID) > 0 {
		natsSub, err = stanConn.QueueSubscribe(
			n.conf.Subject,
			n.conf.QueueID,
			handler,
			options...,
		)
	} else {
		natsSub, err = stanConn.Subscribe(
			n.conf.Subject,
			handler,
			options...,
		)
	}
	if err != nil {
		natsConn.Close()
		return err
	}

	n.natsConn = natsConn
	n.stanConn = stanConn
	n.natsSub = natsSub
	n.msgChan = newMsgChan
	n.log.Infof("Receiving NATS Streaming messages from subject: %v\n", n.conf.Subject)
	return nil
}

func (n *NATSStream) read(ctx context.Context) (*stan.Msg, error) {
	var msg *stan.Msg
	var open bool
	select {
	case msg, open = <-n.msgChan:
		if !open {
			return nil, types.ErrNotConnected
		}
	case <-ctx.Done():
		return nil, types.ErrTimeout
	case <-n.interruptChan:
		n.unAckMsgs = nil
		n.disconnect()
		return nil, types.ErrTypeClosed
	}
	return msg, nil
}

// ReadWithContext attempts to read a new message from the NATS streaming
// server.
func (n *NATSStream) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	msg, err := n.read(ctx)
	if err != nil {
		return nil, nil, err
	}

	bmsg := message.New([][]byte{msg.Data})
	bmsg.Get(0).Metadata().Set("nats_stream_subject", msg.Subject)
	bmsg.Get(0).Metadata().Set("nats_stream_sequence", strconv.FormatUint(msg.Sequence, 10))

	return bmsg, func(rctx context.Context, res types.Response) error {
		if res.Error() == nil {
			return msg.Ack()
		}
		return nil
	}, nil
}

// Read attempts to read a new message from the NATS streaming server.
func (n *NATSStream) Read() (types.Message, error) {
	msg, err := n.read(context.Background())
	if err != nil {
		return nil, err
	}
	n.unAckMsgs = append(n.unAckMsgs, msg)

	bmsg := message.New([][]byte{msg.Data})
	bmsg.Get(0).Metadata().Set("nats_stream_subject", msg.Subject)
	bmsg.Get(0).Metadata().Set("nats_stream_sequence", strconv.FormatUint(msg.Sequence, 10))

	return bmsg, nil
}

// Acknowledge instructs whether unacknowledged messages have been successfully
// propagated.
func (n *NATSStream) Acknowledge(err error) error {
	if err == nil {
		for _, m := range n.unAckMsgs {
			m.Ack()
		}
	}
	n.unAckMsgs = nil
	return nil
}

// CloseAsync shuts down the NATSStream input and stops processing requests.
func (n *NATSStream) CloseAsync() {
	close(n.interruptChan)
}

// WaitForClose blocks until the NATSStream input has closed down.
func (n *NATSStream) WaitForClose(timeout time.Duration) error {
	n.disconnect()
	return nil
}

//------------------------------------------------------------------------------
