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

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	niFieldNSQDAddrs    = "nsqd_tcp_addresses"
	niFieldLookupDAddrs = "lookupd_http_addresses"
	niFieldTLS          = "tls"
	niFieldMaxInFlight  = "max_in_flight"
	niFieldTopic        = "topic"
	niFieldChannel      = "channel"
	niFieldUserAgent    = "user_agent"
	niFieldMaxAttempts  = "max_attempts"
)

func inputConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary(`Subscribe to an NSQ instance topic and channel.`).
		Description(`
### Metadata

This input adds the following metadata fields to each message:

`+"``` text"+`
- nsq_attempts
- nsq_id
- nsq_nsqd_address
- nsq_timestamp
`+"```"+`

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).
`).
		Fields(
			service.NewStringListField(niFieldNSQDAddrs).
				Description("A list of nsqd addresses to connect to."),
			service.NewStringListField(niFieldLookupDAddrs).
				Description("A list of nsqlookupd addresses to connect to."),
			service.NewTLSToggledField(niFieldTLS),
			service.NewStringField(niFieldTopic).
				Description("The topic to consume from."),
			service.NewStringField(niFieldChannel).
				Description("The channel to consume from."),
			service.NewStringField(niFieldUserAgent).
				Description("A user agent to assume when connecting.").
				Optional(),
			service.NewIntField(niFieldMaxInFlight).
				Description("The maximum number of pending messages to consume at any given time.").
				Default(100),
			service.NewIntField(niFieldMaxAttempts).
				Description("The maximum number of attempts to successfully consume a messages.").
				Default(5),
		)
}

func init() {
	err := service.RegisterInput("nsq", inputConfigSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
		return newNSQReaderFromParsed(conf, mgr)
	})
	if err != nil {
		panic(err)
	}
}

type nsqReader struct {
	consumer *nsq.Consumer
	cMut     sync.Mutex

	unAckMsgs []*nsq.Message

	tlsConf         *tls.Config
	addresses       []string
	lookupAddresses []string
	topic           string
	channel         string
	userAgent       string
	maxInFlight     int
	maxAttempts     uint16
	log             *service.Logger

	internalMessages chan *nsq.Message
	interruptChan    chan struct{}
	interruptOnce    sync.Once
}

func newNSQReaderFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (n *nsqReader, err error) {
	n = &nsqReader{
		log:              mgr.Logger(),
		internalMessages: make(chan *nsq.Message),
		interruptChan:    make(chan struct{}),
	}

	var addresses []string
	if addresses, err = conf.FieldStringList(niFieldNSQDAddrs); err != nil {
		return
	}
	for _, addr := range addresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if splitAddr != "" {
				n.addresses = append(n.addresses, splitAddr)
			}
		}
	}

	if addresses, err = conf.FieldStringList(niFieldLookupDAddrs); err != nil {
		return
	}
	for _, addr := range addresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if splitAddr != "" {
				n.lookupAddresses = append(n.lookupAddresses, splitAddr)
			}
		}
	}

	if n.tlsConf, _, err = conf.FieldTLSToggled(niFieldTLS); err != nil {
		return
	}

	if n.topic, err = conf.FieldString(niFieldTopic); err != nil {
		return
	}
	if n.channel, err = conf.FieldString(niFieldChannel); err != nil {
		return
	}
	n.userAgent, _ = conf.FieldString(niFieldUserAgent)
	if n.maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
		return
	}
	var tmpMA int
	if tmpMA, err = conf.FieldInt(niFieldMaxAttempts); err != nil {
		return
	}
	n.maxAttempts = uint16(tmpMA)
	return
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
	cfg.UserAgent = n.userAgent
	cfg.MaxInFlight = n.maxInFlight
	cfg.MaxAttempts = n.maxAttempts
	if n.tlsConf != nil {
		cfg.TlsV1 = true
		cfg.TlsConfig = n.tlsConf
	}

	var consumer *nsq.Consumer
	if consumer, err = nsq.NewConsumer(n.topic, n.channel, cfg); err != nil {
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
		return nil, ctx.Err()
	case <-n.interruptChan:
		for _, m := range n.unAckMsgs {
			m.Requeue(-1)
			m.Finish()
		}
		n.unAckMsgs = nil
		_ = n.disconnect()
		return nil, service.ErrEndOfInput
	}
}

func (n *nsqReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	msg, err := n.read(ctx)
	if err != nil {
		return nil, nil, err
	}
	n.unAckMsgs = append(n.unAckMsgs, msg)

	part := service.NewMessage(msg.Body)
	part.MetaSetMut("nsq_attempts", strconv.Itoa(int(msg.Attempts)))
	part.MetaSetMut("nsq_id", string(msg.ID[:]))
	part.MetaSetMut("nsq_timestamp", strconv.FormatInt(msg.Timestamp, 10))
	part.MetaSetMut("nsq_nsqd_address", msg.NSQDAddress)

	return part, func(rctx context.Context, res error) error {
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
