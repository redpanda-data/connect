package nats

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nats-io/nats.go"

	"github.com/benthosdev/benthos/v4/internal/component/input/span"
	"github.com/benthosdev/benthos/v4/public/service"
)

func natsInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary(`Subscribe to a NATS subject.`).
		Description(`
### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- nats_subject
- nats_reply_subject
- All message headers (when supported by the connection)
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).

` + connectionNameDescription() + authDescription()).
		Fields(connectionHeadFields()...).
		Field(service.NewStringField("subject").
			Description("A subject to consume from. Supports wildcards for consuming multiple subjects. Either a subject or stream must be specified.").
			Example("foo.bar.baz").Example("foo.*.baz").Example("foo.bar.*").Example("foo.>")).
		Field(service.NewStringField("queue").
			Description("An optional queue group to consume as.").
			Optional()).
		Field(service.NewAutoRetryNacksToggleField()).
		Field(service.NewDurationField("nak_delay").
			Description("An optional delay duration on redelivering a message when negatively acknowledged.").
			Example("1m").
			Advanced().
			Optional()).
		Field(service.NewIntField("prefetch_count").
			Description("The maximum number of messages to pull at a time.").
			Advanced().
			Default(nats.DefaultSubPendingMsgsLimit).
			LintRule(`root = if this < 0 { ["prefetch count must be greater than or equal to zero"] }`)).
		Fields(connectionTailFields()...).
		Field(inputTracingDocs())
}

func init() {
	err := service.RegisterInput(
		"nats", natsInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			input, err := newNATSReader(conf, mgr)
			if err != nil {
				return nil, err
			}

			r, err := service.AutoRetryNacksToggled(conf, input)
			if err != nil {
				return nil, err
			}
			return span.NewInput("nats", conf, r, mgr)
		},
	)
	if err != nil {
		panic(err)
	}
}

type natsReader struct {
	connDetails   connectionDetails
	subject       string
	queue         string
	prefetchCount int
	nakDelay      time.Duration

	log *service.Logger

	cMut sync.Mutex

	natsConn      *nats.Conn
	natsSub       *nats.Subscription
	natsChan      chan *nats.Msg
	interruptChan chan struct{}
	interruptOnce sync.Once

	// The pool caller id. This is a unique identifier we will provide when calling methods on the pool. This is used by
	// the pool to do reference counting and ensure that connections are only closed when they are no longer in use.
	pcid string
}

func newNATSReader(conf *service.ParsedConfig, mgr *service.Resources) (*natsReader, error) {
	n := natsReader{
		log:           mgr.Logger(),
		interruptChan: make(chan struct{}),
		pcid:          uuid.New().String(),
	}

	var err error
	if n.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	if n.subject, err = conf.FieldString("subject"); err != nil {
		return nil, err
	}

	if n.prefetchCount, err = conf.FieldInt("prefetch_count"); err != nil {
		return nil, err
	}

	if n.prefetchCount < 0 {
		return nil, errors.New("prefetch count must be greater than or equal to zero")
	}

	if conf.Contains("nak_delay") {
		if n.nakDelay, err = conf.FieldDuration("nak_delay"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("queue") {
		if n.queue, err = conf.FieldString("queue"); err != nil {
			return nil, err
		}
	}
	return &n, nil
}

func (n *natsReader) Connect(ctx context.Context) error {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.natsConn != nil {
		return nil
	}

	var natsConn *nats.Conn
	var natsSub *nats.Subscription
	var err error

	if natsConn, err = pool.Get(ctx, n.pcid, n.connDetails); err != nil {
		return err
	}

	natsChan := make(chan *nats.Msg, n.prefetchCount)

	if n.queue != "" {
		natsSub, err = natsConn.ChanQueueSubscribe(n.subject, n.queue, natsChan)
	} else {
		natsSub, err = natsConn.ChanSubscribe(n.subject, natsChan)
	}

	if err != nil {
		return err
	}

	n.natsConn = natsConn
	n.natsSub = natsSub
	n.natsChan = natsChan
	return nil
}

func (n *natsReader) disconnect() {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.natsSub != nil {
		_ = n.natsSub.Unsubscribe()
		n.natsSub = nil
	}
	if n.natsConn != nil {
		_ = pool.Release(n.pcid, n.connDetails)
		n.natsConn = nil
	}
	n.natsChan = nil
}

func (n *natsReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	n.cMut.Lock()
	natsChan := n.natsChan
	natsConn := n.natsConn
	n.cMut.Unlock()

	var msg *nats.Msg
	var open bool
	select {
	case msg, open = <-natsChan:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case _, open = <-n.interruptChan:
	}
	if !open {
		n.disconnect()
		return nil, nil, service.ErrNotConnected
	}

	bmsg := service.NewMessage(msg.Data)
	bmsg.MetaSetMut("nats_subject", msg.Subject)
	bmsg.MetaSetMut("nats_reply_subject", msg.Reply)
	// process message headers if server supports the feature
	if natsConn.HeadersSupported() {
		for key := range msg.Header {
			value := msg.Header.Get(key)
			bmsg.MetaSetMut(key, value)
		}
	}

	return bmsg, func(_ context.Context, res error) error {
		var ackErr error
		if res != nil {
			if n.nakDelay > 0 {
				ackErr = msg.NakWithDelay(n.nakDelay)
			} else {
				ackErr = msg.Nak()
			}
		} else {
			ackErr = msg.Ack()
		}
		if errors.Is(ackErr, nats.ErrMsgNoReply) {
			ackErr = nil
		}
		return ackErr
	}, nil
}

func (n *natsReader) Close(ctx context.Context) (err error) {
	go func() {
		n.disconnect()
	}()
	n.interruptOnce.Do(func() {
		close(n.interruptChan)
	})
	return
}
