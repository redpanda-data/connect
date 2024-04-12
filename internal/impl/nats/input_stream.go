package nats

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input/span"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	// Stream Input Fields
	siFieldURLs            = "urls"
	siFieldClusterID       = "cluster_id"
	siFieldClientID        = "client_id"
	siFieldQueueID         = "queue"
	siFieldDurableName     = "durable_name"
	siFieldUnsubOnClose    = "unsubscribe_on_close"
	siFieldStartFromOldest = "start_from_oldest"
	siFieldSubject         = "subject"
	siFieldMaxInflight     = "max_inflight"
	siFieldAckWait         = "ack_wait"
	siFieldTLS             = "tls"
	siFieldAuth            = "auth"
)

type siConfig struct {
	connDetails     connectionDetails
	ClusterID       string
	ClientID        string
	QueueID         string
	DurableName     string
	UnsubOnClose    bool
	StartFromOldest bool
	Subject         string
	MaxInflight     int
	AckWait         time.Duration
}

func siConfigFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (conf siConfig, err error) {
	if conf.connDetails, err = connectionDetailsFromParsed(pConf, mgr); err != nil {
		return
	}
	if conf.ClusterID, err = pConf.FieldString(siFieldClusterID); err != nil {
		return
	}
	if conf.ClientID, err = pConf.FieldString(siFieldClientID); err != nil {
		return
	}
	if conf.QueueID, err = pConf.FieldString(siFieldQueueID); err != nil {
		return
	}
	if conf.DurableName, err = pConf.FieldString(siFieldDurableName); err != nil {
		return
	}
	if conf.UnsubOnClose, err = pConf.FieldBool(siFieldUnsubOnClose); err != nil {
		return
	}
	if conf.StartFromOldest, err = pConf.FieldBool(siFieldStartFromOldest); err != nil {
		return
	}
	if conf.Subject, err = pConf.FieldString(siFieldSubject); err != nil {
		return
	}
	if conf.MaxInflight, err = pConf.FieldInt(siFieldMaxInflight); err != nil {
		return
	}
	if conf.AckWait, err = pConf.FieldDuration(siFieldAckWait); err != nil {
		return
	}
	return
}

func siSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary(`Subscribe to a NATS Stream subject. Joining a queue is optional and allows multiple clients of a subject to consume using queue semantics.`).
		Description(`
:::caution Deprecation Notice
The NATS Streaming Server is being deprecated. Critical bug fixes and security fixes will be applied until June of 2023. NATS-enabled applications requiring persistence should use [JetStream](https://docs.nats.io/nats-concepts/jetstream).
:::

Tracking and persisting offsets through a durable name is also optional and works with or without a queue. If a durable name is not provided then subjects are consumed from the most recently published message.

When a consumer closes its connection it unsubscribes, when all consumers of a durable queue do this the offsets are deleted. In order to avoid this you can stop the consumers from unsubscribing by setting the field `+"`unsubscribe_on_close` to `false`"+`.

### Metadata

This input adds the following metadata fields to each message:

`+"``` text"+`
- nats_stream_subject
- nats_stream_sequence
`+"```"+`

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).

`+authDescription()).
		Fields(connectionHeadFields()...).
		Fields(
			service.NewStringField(siFieldClusterID).
				Description("The ID of the cluster to consume from."),
			service.NewStringField(siFieldClientID).
				Description("A client ID to connect as.").
				Default(""),
			service.NewStringField(siFieldQueueID).
				Description("The queue to consume from.").
				Default(""),
			service.NewStringField(siFieldSubject).
				Description("A subject to consume from.").
				Default(""),
			service.NewStringField(siFieldDurableName).
				Description("Preserve the state of your consumer under a durable name.").
				Default(""),
			service.NewBoolField(siFieldUnsubOnClose).
				Description("Whether the subscription should be destroyed when this client disconnects.").
				Default(false),
			service.NewBoolField(siFieldStartFromOldest).
				Description("If a position is not found for a queue, determines whether to consume from the oldest available message, otherwise messages are consumed from the latest.").
				Advanced().
				Default(true),
			service.NewIntField(siFieldMaxInflight).
				Description("The maximum number of unprocessed messages to fetch at a given time.").
				Advanced().
				Default(1024),
			service.NewDurationField(siFieldAckWait).
				Description("An optional duration to specify at which a message that is yet to be acked will be automatically retried.").
				Advanced().
				Default("30s"),
		).
		Fields(connectionTailFields()...).
		Field(inputTracingDocs())
}

func init() {
	err := service.RegisterInput(
		"nats_stream", siSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			pConf, err := siConfigFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			input, err := newNATSStreamReader(pConf, mgr)
			if err != nil {
				return nil, err
			}
			return span.NewInput("nats_stream", conf, input, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type natsStreamReader struct {
	conf siConfig
	log  *service.Logger

	unAckMsgs []*stan.Msg

	stanConn stan.Conn
	natsConn *nats.Conn
	natsSub  stan.Subscription
	cMut     sync.Mutex

	msgChan       chan *stan.Msg
	interruptChan chan struct{}
	interruptOnce sync.Once
}

func newNATSStreamReader(conf siConfig, mgr *service.Resources) (*natsStreamReader, error) {
	if conf.ClientID == "" {
		u4, err := uuid.NewV4()
		if err != nil {
			return nil, err
		}
		conf.ClientID = u4.String()
	}

	n := natsStreamReader{
		conf:          conf,
		log:           mgr.Logger(),
		msgChan:       make(chan *stan.Msg),
		interruptChan: make(chan struct{}),
	}

	close(n.msgChan)
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

	natsConn, err := n.conf.connDetails.get(ctx)
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
	if n.conf.DurableName != "" {
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
	if n.conf.AckWait > 0 {
		options = append(options, stan.AckWait(n.conf.AckWait))
	}

	var natsSub stan.Subscription
	if n.conf.QueueID != "" {
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
	return nil
}

func (n *natsStreamReader) read(ctx context.Context) (*stan.Msg, error) {
	var msg *stan.Msg
	var open bool
	select {
	case msg, open = <-n.msgChan:
		if !open {
			return nil, service.ErrNotConnected
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

func (n *natsStreamReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	msg, err := n.read(ctx)
	if err != nil {
		return nil, nil, err
	}

	part := service.NewMessage(msg.Data)
	part.MetaSetMut("nats_stream_subject", msg.Subject)
	part.MetaSetMut("nats_stream_sequence", strconv.FormatUint(msg.Sequence, 10))

	return part, func(rctx context.Context, res error) error {
		if res == nil {
			return msg.Ack()
		}
		return nil
	}, nil
}

func (n *natsStreamReader) Close(ctx context.Context) (err error) {
	go func() {
		n.disconnect()
	}()
	n.interruptOnce.Do(func() {
		close(n.interruptChan)
	})
	return
}
