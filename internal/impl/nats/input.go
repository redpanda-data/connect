package nats

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
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
- All message headers (when supported by the connection)
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).

` + auth.Description()).
		Field(service.NewStringListField("urls").
			Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"nats://127.0.0.1:4222"}).
			Example([]string{"nats://username:password@127.0.0.1:4222"})).
		Field(service.NewStringField("subject").
			Description("A subject to consume from. Supports wildcards for consuming multiple subjects. Either a subject or stream must be specified.").
			Example("foo.bar.baz").Example("foo.*.baz").Example("foo.bar.*").Example("foo.>")).
		Field(service.NewStringField("queue").
			Description("An optional queue group to consume as.").
			Optional()).
		Field(service.NewIntField("prefetch_count").
			Description("The maximum number of messages to pull at a time.").
			Advanced().
			Default(32).
			LintRule(`root = if this < 0 { ["prefetch count must be greater than or equal to zero"] }`)).
		Field(service.NewTLSToggledField("tls")).
		Field(service.NewInternalField(auth.FieldSpec()))
}

func init() {
	err := service.RegisterInput(
		"nats", natsInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			input, err := newNATSReader(conf, mgr)
			return service.AutoRetryNacks(input), err
		},
	)
	if err != nil {
		panic(err)
	}
}

type natsReader struct {
	urls          string
	subject       string
	queue         string
	prefetchCount int
	authConf      auth.Config
	tlsConf       *tls.Config

	log *service.Logger
	fs  *service.FS

	cMut sync.Mutex

	natsConn      *nats.Conn
	natsSub       *nats.Subscription
	natsChan      chan *nats.Msg
	interruptChan chan struct{}
	interruptOnce sync.Once
}

func newNATSReader(conf *service.ParsedConfig, mgr *service.Resources) (*natsReader, error) {
	n := natsReader{
		log:           mgr.Logger(),
		fs:            mgr.FS(),
		interruptChan: make(chan struct{}),
	}

	urlList, err := conf.FieldStringList("urls")
	if err != nil {
		return nil, err
	}
	n.urls = strings.Join(urlList, ",")

	if n.subject, err = conf.FieldString("subject"); err != nil {
		return nil, err
	}

	if n.prefetchCount, err = conf.FieldInt("prefetch_count"); err != nil {
		return nil, err
	}

	if n.prefetchCount < 0 {
		return nil, errors.New("prefetch count must be greater than or equal to zero")
	}

	if conf.Contains("queue") {
		if n.queue, err = conf.FieldString("queue"); err != nil {
			return nil, err
		}
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		n.tlsConf = tlsConf
	}

	if n.authConf, err = AuthFromParsedConfig(conf.Namespace("auth")); err != nil {
		return nil, err
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
	var opts []nats.Option

	if n.tlsConf != nil {
		opts = append(opts, nats.Secure(n.tlsConf))
	}

	opts = append(opts, authConfToOptions(n.authConf, n.fs)...)
	opts = append(opts, errorHandlerOption(n.log))

	if natsConn, err = nats.Connect(n.urls, opts...); err != nil {
		return err
	}
	natsChan := make(chan *nats.Msg, n.prefetchCount)

	if len(n.queue) > 0 {
		natsSub, err = natsConn.ChanQueueSubscribe(n.subject, n.queue, natsChan)
	} else {
		natsSub, err = natsConn.ChanSubscribe(n.subject, natsChan)
	}

	if err != nil {
		return err
	}

	n.log.Infof("Receiving NATS messages from subject: %v\n", n.subject)

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
		n.natsConn.Close()
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

func (n *natsReader) Close(ctx context.Context) (err error) {
	n.interruptOnce.Do(func() {
		close(n.interruptChan)
	})
	return
}
