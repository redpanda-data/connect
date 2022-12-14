package nsq

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	llog "log"
	"sync"

	nsq "github.com/nsqio/go-nsq"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(newNSQOutput), docs.ComponentSpec{
		Name:        "nsq",
		Summary:     `Publish to an NSQ topic.`,
		Description: output.Description(true, false, `The `+"`topic`"+` field can be dynamically set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries). When sending batched messages these interpolations are performed per message part.`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("nsqd_tcp_address", "The address of the target NSQD server."),
			docs.FieldString("topic", "The topic to publish to.").IsInterpolated(),
			docs.FieldString("user_agent", "A user agent string to connect with."),
			btls.FieldSpec(),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		).ChildDefaultAndTypesFromStruct(output.NewNSQConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newNSQOutput(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
	w, err := newNSQWriter(conf.NSQ, mgr)
	if err != nil {
		return nil, err
	}
	return output.NewAsyncWriter("nsq", conf.NSQ.MaxInFlight, w, mgr)
}

type nsqWriter struct {
	log log.Modular

	topicStr *field.Expression

	tlsConf  *tls.Config
	connMut  sync.RWMutex
	producer *nsq.Producer

	conf output.NSQConfig
}

func newNSQWriter(conf output.NSQConfig, mgr bundle.NewManagement) (*nsqWriter, error) {
	n := nsqWriter{
		log:  mgr.Logger(),
		conf: conf,
	}
	var err error
	if n.topicStr, err = mgr.BloblEnvironment().NewField(conf.Topic); err != nil {
		return nil, fmt.Errorf("failed to parse topic expression: %v", err)
	}
	if conf.TLS.Enabled {
		if n.tlsConf, err = conf.TLS.Get(mgr.FS()); err != nil {
			return nil, err
		}
	}
	return &n, nil
}

func (n *nsqWriter) Connect(ctx context.Context) error {
	n.connMut.Lock()
	defer n.connMut.Unlock()

	cfg := nsq.NewConfig()
	cfg.UserAgent = n.conf.UserAgent
	if n.tlsConf != nil {
		cfg.TlsV1 = true
		cfg.TlsConfig = n.tlsConf
	}

	producer, err := nsq.NewProducer(n.conf.Address, cfg)
	if err != nil {
		return err
	}

	producer.SetLogger(llog.New(io.Discard, "", llog.Flags()), nsq.LogLevelError)

	if err := producer.Ping(); err != nil {
		return err
	}
	n.producer = producer
	n.log.Infof("Sending NSQ messages to address: %s\n", n.conf.Address)
	return nil
}

func (n *nsqWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	n.connMut.RLock()
	prod := n.producer
	n.connMut.RUnlock()

	if prod == nil {
		return component.ErrNotConnected
	}

	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		topicStr, err := n.topicStr.String(i, msg)
		if err != nil {
			return fmt.Errorf("topic interpolation error: %w", err)
		}
		return prod.Publish(topicStr, p.AsBytes())
	})
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
