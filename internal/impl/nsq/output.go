package nsq

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	llog "log"
	"sync"

	nsq "github.com/nsqio/go-nsq"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	noFieldNSQDAddr  = "nsqd_tcp_address"
	noFieldTLS       = "tls"
	noFieldTopic     = "topic"
	noFieldUserAgent = "user_agent"
)

func outputConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary(`Publish to an NSQ topic.`).
		Description(output.Description(true, false, `The `+"`topic`"+` field can be dynamically set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries). When sending batched messages these interpolations are performed per message part.`)).
		Fields(
			service.NewStringField(noFieldNSQDAddr).
				Description("The address of the target NSQD server."),
			service.NewInterpolatedStringField(noFieldTopic).
				Description("The topic to publish to."),
			service.NewStringField(noFieldUserAgent).
				Description("A user agent to assume when connecting.").
				Optional(),
			service.NewTLSToggledField(noFieldTLS),
			service.NewOutputMaxInFlightField(),
		)
}

func init() {
	err := service.RegisterOutput("nsq", outputConfigSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
		wtr, err := newNSQWriterFromParsed(conf, mgr)
		if err != nil {
			return nil, 0, err
		}
		mIF, err := conf.FieldMaxInFlight()
		if err != nil {
			return nil, 0, err
		}
		return wtr, mIF, nil
	})
	if err != nil {
		panic(err)
	}
}

type nsqWriter struct {
	log *service.Logger

	address   string
	topicStr  *service.InterpolatedString
	tlsConf   *tls.Config
	userAgent string

	connMut  sync.RWMutex
	producer *nsq.Producer
}

func newNSQWriterFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (n *nsqWriter, err error) {
	n = &nsqWriter{
		log: mgr.Logger(),
	}

	if n.address, err = conf.FieldString(noFieldNSQDAddr); err != nil {
		return
	}
	if n.topicStr, err = conf.FieldInterpolatedString(noFieldTopic); err != nil {
		return nil, err
	}
	if n.tlsConf, _, err = conf.FieldTLSToggled(noFieldTLS); err != nil {
		return
	}
	n.userAgent, _ = conf.FieldString(noFieldUserAgent)
	return
}

func (n *nsqWriter) Connect(ctx context.Context) error {
	n.connMut.Lock()
	defer n.connMut.Unlock()

	cfg := nsq.NewConfig()
	cfg.UserAgent = n.userAgent
	if n.tlsConf != nil {
		cfg.TlsV1 = true
		cfg.TlsConfig = n.tlsConf
	}

	producer, err := nsq.NewProducer(n.address, cfg)
	if err != nil {
		return err
	}

	producer.SetLogger(llog.New(io.Discard, "", llog.Flags()), nsq.LogLevelError)

	if err := producer.Ping(); err != nil {
		return err
	}
	n.producer = producer
	return nil
}

func (n *nsqWriter) Write(ctx context.Context, msg *service.Message) error {
	n.connMut.RLock()
	prod := n.producer
	n.connMut.RUnlock()

	if prod == nil {
		return service.ErrNotConnected
	}

	topicStr, err := n.topicStr.TryString(msg)
	if err != nil {
		return fmt.Errorf("topic interpolation error: %w", err)
	}

	mBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}
	return prod.Publish(topicStr, mBytes)
}

func (n *nsqWriter) Close(context.Context) error {
	n.connMut.Lock()
	defer n.connMut.Unlock()

	if n.producer != nil {
		n.producer.Stop()
		n.producer = nil
	}
	return nil
}
