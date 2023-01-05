package nsq

import (
	"context"
	"crypto/tls"
	"io"
	llog "log"
	"strconv"
	"strings"
	"sync"

	"github.com/nsqio/go-nsq"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(newNSQInput), docs.ComponentSpec{
		Name:    "nsq",
		Summary: `Subscribe to an NSQ instance topic and channel.`,
		Description: `
### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- nsq_attempts
- nsq_id
- nsq_nsqd_address
- nsq_timestamp
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).
`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("nsqd_tcp_addresses", "A list of nsqd addresses to connect to.").Array(),
			docs.FieldString("lookupd_http_addresses", "A list of nsqlookupd addresses to connect to.").Array(),
			btls.FieldSpec(),
			docs.FieldString("topic", "The topic to consume from."),
			docs.FieldString("channel", "The channel to consume from."),
			docs.FieldString("user_agent", "A user agent to assume when connecting."),
			docs.FieldInt("max_in_flight", "The maximum number of pending messages to consume at any given time."),
			docs.FieldInt("max_attempts", "The maximum number of attempts to successfully consume a messages."),
		).ChildDefaultAndTypesFromStruct(input.NewNSQConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newNSQInput(conf input.Config, mgr bundle.NewManagement) (input.Streamed, error) {
	var n input.Async
	var err error
	if n, err = newNSQReader(conf.NSQ, mgr); err != nil {
		return nil, err
	}
	return input.NewAsyncReader("nsq", n, mgr)
}

type nsqReader struct {
	consumer *nsq.Consumer
	cMut     sync.Mutex

	unAckMsgs []*nsq.Message

	tlsConf         *tls.Config
	addresses       []string
	lookupAddresses []string
	conf            input.NSQConfig
	log             log.Modular

	internalMessages chan *nsq.Message
	interruptChan    chan struct{}
	interruptOnce    sync.Once
}

func newNSQReader(conf input.NSQConfig, mgr bundle.NewManagement) (*nsqReader, error) {
	n := nsqReader{
		conf:             conf,
		log:              mgr.Logger(),
		internalMessages: make(chan *nsq.Message),
		interruptChan:    make(chan struct{}),
	}
	for _, addr := range conf.Addresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if len(splitAddr) > 0 {
				n.addresses = append(n.addresses, splitAddr)
			}
		}
	}
	for _, addr := range conf.LookupAddresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if len(splitAddr) > 0 {
				n.lookupAddresses = append(n.lookupAddresses, splitAddr)
			}
		}
	}
	if conf.TLS.Enabled {
		var err error
		if n.tlsConf, err = conf.TLS.Get(mgr.FS()); err != nil {
			return nil, err
		}
	}
	return &n, nil
}

func (n *nsqReader) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	select {
	case n.internalMessages <- message:
	case <-n.interruptChan:
		message.Requeue(-1)
		message.Finish()
	}
	return nil
}

func (n *nsqReader) Connect(ctx context.Context) (err error) {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.consumer != nil {
		return nil
	}

	cfg := nsq.NewConfig()
	cfg.UserAgent = n.conf.UserAgent
	cfg.MaxInFlight = n.conf.MaxInFlight
	cfg.MaxAttempts = n.conf.MaxAttempts
	if n.tlsConf != nil {
		cfg.TlsV1 = true
		cfg.TlsConfig = n.tlsConf
	}

	var consumer *nsq.Consumer
	if consumer, err = nsq.NewConsumer(n.conf.Topic, n.conf.Channel, cfg); err != nil {
		return
	}

	consumer.SetLogger(llog.New(io.Discard, "", llog.Flags()), nsq.LogLevelError)
	consumer.AddHandler(n)

	if err = consumer.ConnectToNSQDs(n.addresses); err != nil {
		consumer.Stop()
		return
	}
	if err = consumer.ConnectToNSQLookupds(n.lookupAddresses); err != nil {
		consumer.Stop()
		return
	}

	n.consumer = consumer
	n.log.Infof("Receiving NSQ messages from addresses: %s\n", n.addresses)
	return
}

func (n *nsqReader) disconnect() error {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.consumer != nil {
		n.consumer.Stop()
		n.consumer = nil
	}
	return nil
}

func (n *nsqReader) read(ctx context.Context) (*nsq.Message, error) {
	var msg *nsq.Message
	select {
	case msg = <-n.internalMessages:
		return msg, nil
	case <-ctx.Done():
	case <-n.interruptChan:
		for _, m := range n.unAckMsgs {
			m.Requeue(-1)
			m.Finish()
		}
		n.unAckMsgs = nil
		_ = n.disconnect()
		return nil, component.ErrTypeClosed
	}
	return nil, component.ErrTimeout
}

func (n *nsqReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	msg, err := n.read(ctx)
	if err != nil {
		return nil, nil, err
	}
	n.unAckMsgs = append(n.unAckMsgs, msg)

	bmsg := message.QuickBatch([][]byte{msg.Body})
	part := bmsg.Get(0)
	part.MetaSetMut("nsq_attempts", strconv.Itoa(int(msg.Attempts)))
	part.MetaSetMut("nsq_id", string(msg.ID[:]))
	part.MetaSetMut("nsq_timestamp", strconv.FormatInt(msg.Timestamp, 10))
	part.MetaSetMut("nsq_nsqd_address", msg.NSQDAddress)

	return bmsg, func(rctx context.Context, res error) error {
		if res != nil {
			msg.Requeue(-1)
		}
		msg.Finish()
		return nil
	}, nil
}

func (n *nsqReader) Close(ctx context.Context) (err error) {
	n.interruptOnce.Do(func() {
		close(n.interruptChan)
	})
	err = n.disconnect()
	return
}
