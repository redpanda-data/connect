package nats

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

func natsJetStreamInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Services").
		Version("3.46.0").
		Summary("Reads messages from NATS JetStream subjects.").
		Description(`
### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- nats_subject
- nats_sequence_stream
- nats_sequence_consumer
- nats_num_delivered
- nats_num_pending
- nats_domain
- nats_timestamp_unix_nano
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).

` + auth.Description()).
		Field(service.NewStringListField("urls").
			Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"nats://127.0.0.1:4222"}).
			Example([]string{"nats://username:password@127.0.0.1:4222"})).
		Field(service.NewStringField("queue").
			Description("An optional queue group to consume as.").
			Optional()).
		Field(service.NewStringField("subject").
			Description("A subject to consume from. Supports wildcards for consuming multiple subjects. Either a subject or stream must be specified.").
			Optional().
			Example("foo.bar.baz").Example("foo.*.baz").Example("foo.bar.*").Example("foo.>")).
		Field(service.NewStringField("durable").
			Description("Preserve the state of your consumer under a durable name.").
			Optional()).
		Field(service.NewStringField("stream").
			Description("A stream to consume from. Either a subject or stream must be specified.").
			Optional()).
		Field(service.NewBoolField("bind").
			Description("Indicates that the subscription should use an existing consumer.").
			Optional()).
		Field(service.NewStringAnnotatedEnumField("deliver", map[string]string{
			"all":  "Deliver all available messages.",
			"last": "Deliver starting with the last published messages.",
		}).
			Description("Determines which messages to deliver when consuming without a durable subscriber.").
			Default("all")).
		Field(service.NewStringField("ack_wait").
			Description("The maximum amount of time NATS server should wait for an ack from consumer.").
			Advanced().
			Default("30s").
			Example("100ms").
			Example("5m")).
		Field(service.NewIntField("max_ack_pending").
			Description("The maximum number of outstanding acks to be allowed before consuming is halted.").
			Advanced().
			Default(1024)).
		Field(service.NewTLSToggledField("tls")).
		Field(service.NewInternalField(auth.FieldSpec()))
}

func init() {
	err := service.RegisterInput(
		"nats_jetstream", natsJetStreamInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newJetStreamReaderFromConfig(conf, mgr.Logger(), mgr.FS())
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type jetStreamReader struct {
	urls          string
	deliverOpt    nats.SubOpt
	subject       string
	queue         string
	stream        string
	bind          bool
	pull          bool
	durable       string
	ackWait       time.Duration
	maxAckPending int
	authConf      auth.Config
	tlsConf       *tls.Config

	log *service.Logger
	fs  *service.FS

	connMut  sync.Mutex
	natsConn *nats.Conn
	natsSub  *nats.Subscription

	shutSig *shutdown.Signaller
}

func newJetStreamReaderFromConfig(conf *service.ParsedConfig, log *service.Logger, fs *service.FS) (*jetStreamReader, error) {
	j := jetStreamReader{
		log:     log,
		fs:      fs,
		shutSig: shutdown.NewSignaller(),
	}

	urlList, err := conf.FieldStringList("urls")
	if err != nil {
		return nil, err
	}
	j.urls = strings.Join(urlList, ",")

	deliver, err := conf.FieldString("deliver")
	if err != nil {
		return nil, err
	}
	switch deliver {
	case "all":
		j.deliverOpt = nats.DeliverAll()
	case "last":
		j.deliverOpt = nats.DeliverLast()
	default:
		return nil, fmt.Errorf("deliver option %v was not recognised", deliver)
	}

	if conf.Contains("subject") {
		if j.subject, err = conf.FieldString("subject"); err != nil {
			return nil, err
		}
	}
	if conf.Contains("queue") {
		if j.queue, err = conf.FieldString("queue"); err != nil {
			return nil, err
		}
	}
	if conf.Contains("durable") {
		if j.durable, err = conf.FieldString("durable"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("stream") {
		if j.stream, err = conf.FieldString("stream"); err != nil {
			return nil, err
		}
	}
	if conf.Contains("bind") {
		if j.bind, err = conf.FieldBool("bind"); err != nil {
			return nil, err
		}
	}
	if j.bind {
		if j.stream == "" || j.durable == "" {
			return nil, fmt.Errorf("stream or durable is required, when bind is true")
		}
	} else {
		if j.subject == "" {
			return nil, fmt.Errorf("subject is empty")
		}
	}

	ackWaitStr, err := conf.FieldString("ack_wait")
	if err != nil {
		return nil, err
	}
	if ackWaitStr != "" {
		j.ackWait, err = time.ParseDuration(ackWaitStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse ack wait duration: %v", err)
		}
	}

	if j.maxAckPending, err = conf.FieldInt("max_ack_pending"); err != nil {
		return nil, err
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		j.tlsConf = tlsConf
	}

	if j.authConf, err = AuthFromParsedConfig(conf.Namespace("auth")); err != nil {
		return nil, err
	}

	return &j, nil
}

//------------------------------------------------------------------------------

func (j *jetStreamReader) Connect(ctx context.Context) error {
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
	opts = append(opts, authConfToOptions(j.authConf, j.fs)...)
	opts = append(opts, errorHandlerOption(j.log))
	if natsConn, err = nats.Connect(j.urls, opts...); err != nil {
		return err
	}

	jCtx, err := natsConn.JetStream()
	if err != nil {
		return err
	}

	if j.bind && j.stream != "" && j.durable != "" {
		info, err := jCtx.ConsumerInfo(j.stream, j.durable)
		if err != nil {
			return err
		}

		if j.subject == "" {
			if info.Config.DeliverSubject != "" {
				j.subject = info.Config.DeliverSubject
			} else if info.Config.FilterSubject != "" {
				j.subject = info.Config.FilterSubject
			}
		}

		j.pull = info.Config.DeliverSubject == ""
	}

	options := []nats.SubOpt{
		nats.ManualAck(),
	}

	if j.pull {
		options = append(options, nats.Bind(j.stream, j.durable))

		natsSub, err = jCtx.PullSubscribe(j.subject, j.durable, options...)
	} else {
		if j.durable != "" {
			options = append(options, nats.Durable(j.durable))
		}
		options = append(options, j.deliverOpt)
		if j.ackWait > 0 {
			options = append(options, nats.AckWait(j.ackWait))
		}
		if j.maxAckPending != 0 {
			options = append(options, nats.MaxAckPending(j.maxAckPending))
		}

		if j.bind {
			if j.stream != "" && j.durable != "" {
				options = append(options, nats.Bind(j.stream, j.durable))
			} else if j.stream != "" {
				options = append(options, nats.BindStream(j.stream))
			}
		}

		if j.queue == "" {
			natsSub, err = jCtx.SubscribeSync(j.subject, options...)
		} else {
			natsSub, err = jCtx.QueueSubscribeSync(j.subject, j.queue, options...)
		}
	}
	if err != nil {
		return err
	}

	j.log.Infof("Receiving NATS messages from JetStream subject: %v", j.subject)

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

func (j *jetStreamReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	j.connMut.Lock()
	natsSub := j.natsSub
	j.connMut.Unlock()
	if natsSub == nil {
		return nil, nil, service.ErrNotConnected
	}

	if !j.pull {
		nmsg, err := natsSub.NextMsgWithContext(ctx)
		if err != nil {
			// TODO: Any errors need capturing here to signal a lost connection?
			return nil, nil, err
		}
		return convertMessage(nmsg)
	}

	for {
		msgs, err := natsSub.Fetch(1, nats.Context(ctx))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
				// NATS enforces its own context that might time out faster than the original context
				// Let's check if it was the original context that timed out
				select {
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				default:
					continue
				}
			}
			return nil, nil, err
		}
		if len(msgs) == 0 {
			continue
		}
		return convertMessage(msgs[0])
	}
}

func (j *jetStreamReader) Close(ctx context.Context) error {
	go func() {
		j.disconnect()
		j.shutSig.ShutdownComplete()
	}()
	select {
	case <-j.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func convertMessage(m *nats.Msg) (*service.Message, service.AckFunc, error) {
	msg := service.NewMessage(m.Data)
	msg.MetaSet("nats_subject", m.Subject)

	metadata, err := m.Metadata()
	if err == nil {
		msg.MetaSet("nats_sequence_stream", strconv.Itoa(int(metadata.Sequence.Stream)))
		msg.MetaSet("nats_sequence_consumer", strconv.Itoa(int(metadata.Sequence.Consumer)))
		msg.MetaSet("nats_num_delivered", strconv.Itoa(int(metadata.NumDelivered)))
		msg.MetaSet("nats_num_pending", strconv.Itoa(int(metadata.NumPending)))
		msg.MetaSet("nats_domain", metadata.Domain)
		msg.MetaSet("nats_timestamp_unix_nano", strconv.Itoa(int(metadata.Timestamp.UnixNano())))
	}

	for k := range m.Header {
		v := m.Header.Get(k)
		if v != "" {
			msg.MetaSet(k, v)
		}
	}

	return msg, func(ctx context.Context, res error) error {
		if res == nil {
			return m.Ack()
		}
		return m.Nak()
	}, nil
}
