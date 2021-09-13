package nats

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/impl/nats/auth"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/nats-io/nats.go"
)

func init() {
	bundle.AllInputs.Add(bundle.InputConstructorFromSimple(func(c input.Config, nm bundle.NewManagement) (input.Type, error) {
		var a reader.Async
		var err error
		if a, err = newJetStreamReader(c.NATSJetStream, nm.Logger(), nm.Metrics()); err != nil {
			return nil, err
		}
		return input.NewAsyncReader(input.TypeNATSStream, false, a, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:    input.TypeNATSJetStream,
		Type:    docs.TypeInput,
		Status:  docs.StatusExperimental,
		Version: "3.46.0",
		Summary: `Reads messages from NATS JetStream subjects.`,
		Description: `
### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- nats_subject
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).

` + auth.Description(),
		Categories: []string{
			string(input.CategoryServices),
		},
		Config: docs.FieldComponent().WithChildren(
			docs.FieldCommon(
				"urls",
				"A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.",
				[]string{"nats://127.0.0.1:4222"},
				[]string{"nats://username:password@127.0.0.1:4222"},
			).Array(),
			docs.FieldCommon("queue", "The queue group to consume as."),
			docs.FieldCommon(
				"subject", "A subject to consume from. Supports wildcards for consuming multiple subjects.",
				"foo.bar.baz", "foo.*.baz", "foo.bar.*", "foo.>",
			),
			docs.FieldCommon("durable", "Preserve the state of your consumer under a durable name."),
			docs.FieldCommon(
				"deliver", "Determines which messages to deliver when consuming without a durable subscriber.",
			).HasAnnotatedOptions(
				"all", "Deliver all available messages.",
				"last", "Deliver starting with the last published messages.",
			),
			docs.FieldAdvanced("max_ack_pending", "The maximum number of outstanding acks to be allowed before consuming is halted."),
			btls.FieldSpec(),
			auth.FieldSpec(),
		).ChildDefaultAndTypesFromStruct(input.NewNATSJetStreamConfig()),
	})
}

//------------------------------------------------------------------------------

type jetStreamReader struct {
	urls       string
	conf       input.NATSJetStreamConfig
	deliverOpt nats.SubOpt
	tlsConf    *tls.Config

	stats metrics.Type
	log   log.Modular

	connMut  sync.Mutex
	natsConn *nats.Conn
	natsSub  *nats.Subscription

	shutSig *shutdown.Signaller
}

func newJetStreamReader(conf input.NATSJetStreamConfig, log log.Modular, stats metrics.Type) (*jetStreamReader, error) {
	j := jetStreamReader{
		conf:    conf,
		stats:   stats,
		log:     log,
		shutSig: shutdown.NewSignaller(),
	}
	j.urls = strings.Join(conf.URLs, ",")
	var err error
	if conf.TLS.Enabled {
		if j.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}
	switch conf.Deliver {
	case "all":
		j.deliverOpt = nats.DeliverAll()
	case "last":
		j.deliverOpt = nats.DeliverLast()
	default:
		return nil, fmt.Errorf("deliver option %v was not recognised", conf.Deliver)
	}
	return &j, nil
}

//------------------------------------------------------------------------------

func (j *jetStreamReader) ConnectWithContext(ctx context.Context) error {
	j.connMut.Lock()
	defer j.connMut.Unlock()

	if j.natsConn != nil {
		return nil
	}

	var natsConn *nats.Conn
	var natsSub *nats.Subscription
	var err error

	defer func() {
		if err != nil {
			if natsSub != nil {
				_ = natsSub.Drain()
			}
			if natsConn != nil {
				natsConn.Close()
			}
		}
	}()

	var opts []nats.Option
	if j.tlsConf != nil {
		opts = append(opts, nats.Secure(j.tlsConf))
	}
	opts = append(opts, auth.GetOptions(j.conf.Auth)...)
	if natsConn, err = nats.Connect(j.urls, opts...); err != nil {
		return err
	}

	jCtx, err := natsConn.JetStream()
	if err != nil {
		return err
	}

	options := []nats.SubOpt{
		nats.ManualAck(),
	}
	if j.conf.Durable != "" {
		options = append(options, nats.Durable(j.conf.Durable))
	}
	options = append(options, j.deliverOpt)
	if j.conf.MaxAckPending != 0 {
		options = append(options, nats.MaxAckPending(j.conf.MaxAckPending))
	}

	if j.conf.Queue == "" {
		natsSub, err = jCtx.SubscribeSync(j.conf.Subject, options...)
	} else {
		natsSub, err = jCtx.QueueSubscribeSync(j.conf.Subject, j.conf.Queue, options...)
	}
	if err != nil {
		return err
	}

	j.log.Infof("Receiving NATS messages from JetStream subject: %v\n", j.conf.Subject)

	j.natsConn = natsConn
	j.natsSub = natsSub
	return nil
}

func (j *jetStreamReader) disconnect() {
	j.connMut.Lock()
	defer j.connMut.Unlock()

	if j.natsSub != nil {
		_ = j.natsSub.Drain()
		j.natsSub = nil
	}
	if j.natsConn != nil {
		j.natsConn.Close()
		j.natsConn = nil
	}
}

func (j *jetStreamReader) ReadWithContext(ctx context.Context) (types.Message, reader.AsyncAckFn, error) {
	j.connMut.Lock()
	natsSub := j.natsSub
	j.connMut.Unlock()
	if natsSub == nil {
		return nil, nil, types.ErrNotConnected
	}

	nmsg, err := natsSub.NextMsgWithContext(ctx)
	if err != nil {
		// TODO: Any errors need capturing here to signal a lost connection?
		return nil, nil, err
	}

	msg := message.New([][]byte{nmsg.Data})
	msg.Get(0).Metadata().Set("nats_subject", nmsg.Subject)

	return msg, func(ctx context.Context, res types.Response) error {
		if res.Error() == nil {
			return nmsg.Ack()
		}
		return nmsg.Nak()
	}, nil
}

func (j *jetStreamReader) CloseAsync() {
	go func() {
		j.disconnect()
		j.shutSig.ShutdownComplete()
	}()
}

func (j *jetStreamReader) WaitForClose(timeout time.Duration) error {
	select {
	case <-j.shutSig.HasClosedChan():
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}
