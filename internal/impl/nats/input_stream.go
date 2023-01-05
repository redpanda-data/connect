package nats

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(newNATSStreamInput), docs.ComponentSpec{
		Name:    "nats_stream",
		Summary: `Subscribe to a NATS Stream subject. Joining a queue is optional and allows multiple clients of a subject to consume using queue semantics.`,
		Description: `
Tracking and persisting offsets through a durable name is also optional and works with or without a queue. If a durable name is not provided then subjects are consumed from the most recently published message.

When a consumer closes its connection it unsubscribes, when all consumers of a durable queue do this the offsets are deleted. In order to avoid this you can stop the consumers from unsubscribing by setting the field ` + "`unsubscribe_on_close` to `false`" + `.

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- nats_stream_subject
- nats_stream_sequence
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).

` + auth.Description(),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString(
				"urls",
				"A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.",
				[]string{"nats://127.0.0.1:4222"},
				[]string{"nats://username:password@127.0.0.1:4222"},
			).Array(),
			docs.FieldString("cluster_id", "The ID of the cluster to consume from."),
			docs.FieldString("client_id", "A client ID to connect as."),
			docs.FieldString("queue", "The queue to consume from."),
			docs.FieldString("subject", "A subject to consume from."),
			docs.FieldString("durable_name", "Preserve the state of your consumer under a durable name."),
			docs.FieldBool("unsubscribe_on_close", "Whether the subscription should be destroyed when this client disconnects."),
			docs.FieldBool("start_from_oldest", "If a position is not found for a queue, determines whether to consume from the oldest available message, otherwise messages are consumed from the latest.").Advanced(),
			docs.FieldInt("max_inflight", "The maximum number of unprocessed messages to fetch at a given time.").Advanced(),
			docs.FieldString("ack_wait", "An optional duration to specify at which a message that is yet to be acked will be automatically retried.").Advanced(),
			btls.FieldSpec(),
			auth.FieldSpec(),
		).ChildDefaultAndTypesFromStruct(input.NewNATSStreamConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newNATSStreamInput(conf input.Config, mgr bundle.NewManagement) (input.Streamed, error) {
	var c input.Async
	var err error
	if c, err = newNATSStreamReader(conf.NATSStream, mgr); err != nil {
		return nil, err
	}
	return input.NewAsyncReader("nats_stream", c, mgr)
}

type natsStreamReader struct {
	urls    string
	conf    input.NATSStreamConfig
	ackWait time.Duration

	log log.Modular

	unAckMsgs []*stan.Msg

	stanConn stan.Conn
	natsConn *nats.Conn
	natsSub  stan.Subscription
	cMut     sync.Mutex

	msgChan       chan *stan.Msg
	interruptChan chan struct{}
	interruptOnce sync.Once
	tlsConf       *tls.Config
}

func newNATSStreamReader(conf input.NATSStreamConfig, mgr bundle.NewManagement) (*natsStreamReader, error) {
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

	n := natsStreamReader{
		conf:          conf,
		ackWait:       ackWait,
		log:           mgr.Logger(),
		msgChan:       make(chan *stan.Msg),
		interruptChan: make(chan struct{}),
	}

	close(n.msgChan)
	n.urls = strings.Join(conf.URLs, ",")
	var err error
	if conf.TLS.Enabled {
		if n.tlsConf, err = conf.TLS.Get(mgr.FS()); err != nil {
			return nil, err
		}
	}

	return &n, nil
}

func (n *natsStreamReader) disconnect() {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.natsSub != nil {
		if n.conf.UnsubOnClose {
			_ = n.natsSub.Unsubscribe()
		}
		n.natsConn.Close()
		n.stanConn.Close()

		n.natsSub = nil
		n.natsConn = nil
		n.stanConn = nil
	}
}

func (n *natsStreamReader) Connect(ctx context.Context) error {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.natsSub != nil {
		return nil
	}

	var opts []nats.Option
	if n.tlsConf != nil {
		opts = append(opts, nats.Secure(n.tlsConf))
	}

	opts = append(opts, authConfToOptions(n.conf.Auth)...)

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

func (n *natsStreamReader) read(ctx context.Context) (*stan.Msg, error) {
	var msg *stan.Msg
	var open bool
	select {
	case msg, open = <-n.msgChan:
		if !open {
			return nil, component.ErrNotConnected
		}
	case <-ctx.Done():
		return nil, component.ErrTimeout
	case <-n.interruptChan:
		n.unAckMsgs = nil
		n.disconnect()
		return nil, component.ErrTypeClosed
	}
	return msg, nil
}

func (n *natsStreamReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	msg, err := n.read(ctx)
	if err != nil {
		return nil, nil, err
	}

	bmsg := message.QuickBatch([][]byte{msg.Data})
	part := bmsg.Get(0)
	part.MetaSetMut("nats_stream_subject", msg.Subject)
	part.MetaSetMut("nats_stream_sequence", strconv.FormatUint(msg.Sequence, 10))

	return bmsg, func(rctx context.Context, res error) error {
		if res == nil {
			return msg.Ack()
		}
		return nil
	}, nil
}

func (n *natsStreamReader) Close(ctx context.Context) (err error) {
	n.interruptOnce.Do(func() {
		close(n.interruptChan)
	})
	return
}
