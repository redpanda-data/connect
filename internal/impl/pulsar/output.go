package pulsar

import (
	"context"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterOutput(
		"pulsar",
		outputConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			w, err := newPulsarWriterFromParsed(conf, mgr.Logger())
			if err != nil {
				return nil, 0, err
			}
			n, err := conf.FieldInt("max_in_flight")
			if err != nil {
				return nil, 0, err
			}
			return w, n, err
		})
	if err != nil {
		panic(err)
	}
}

func outputConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("3.43.0").
		Categories("Services").
		Summary("Write messages to an Apache Pulsar server.").
		Field(service.NewURLField("url").
			Description("A URL to connect to.").
			Example("pulsar://localhost:6650").
			Example("pulsar://pulsar.us-west.example.com:6650").
			Example("pulsar+ssl://pulsar.us-west.example.com:6651")).
		Field(service.NewStringField("topic").
			Description("The topic to publish to.")).
		Field(service.NewObjectField("tls",
			service.NewStringField("root_cas_file").
				Description("An optional path of a root certificate authority file to use. This is a file, often with a .pem extension, containing a certificate chain from the parent trusted root certificate, to possible intermediate signing certificates, to the host certificate.").
				Default("").
				Example("./root_cas.pem")).
			Description("Specify the path to a custom CA certificate to trust broker TLS service.")).
		Field(service.NewInterpolatedStringField("key").
			Description("The key to publish messages with.").
			Default("")).
		Field(service.NewInterpolatedStringField("ordering_key").
			Description("The ordering key to publish messages with.").
			Default("")).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of messages to have in flight at a given time. Increase this to improve throughput.").
			Default(64)).
		Field(authField())
}

//------------------------------------------------------------------------------

type pulsarWriter struct {
	client   pulsar.Client
	producer pulsar.Producer
	m        sync.RWMutex

	log *service.Logger

	authConf    authConfig
	url         string
	topic       string
	rootCasFile string
	key         *service.InterpolatedString
	orderingKey *service.InterpolatedString
}

func newPulsarWriterFromParsed(conf *service.ParsedConfig, log *service.Logger) (p *pulsarWriter, err error) {
	p = &pulsarWriter{
		log: log,
	}

	if p.authConf, err = authFromParsed(conf); err != nil {
		return
	}

	if p.url, err = conf.FieldString("url"); err != nil {
		return
	}
	if p.topic, err = conf.FieldString("topic"); err != nil {
		return
	}
	if p.rootCasFile, err = conf.FieldString("tls", "root_cas_file"); err != nil {
		return
	}
	if p.key, err = conf.FieldInterpolatedString("key"); err != nil {
		return
	}
	if p.orderingKey, err = conf.FieldInterpolatedString("ordering_key"); err != nil {
		return
	}
	return
}

//------------------------------------------------------------------------------

func (p *pulsarWriter) Connect(ctx context.Context) error {
	p.m.Lock()
	defer p.m.Unlock()

	if p.client != nil {
		return nil
	}

	var (
		client   pulsar.Client
		producer pulsar.Producer
		err      error
	)

	opts := pulsar.ClientOptions{
		Logger:                createDefaultLogger(p.log),
		ConnectionTimeout:     time.Second * 3,
		URL:                   p.url,
		TLSTrustCertsFilePath: p.rootCasFile,
	}

	if p.authConf.OAuth2.Enabled {
		opts.Authentication = pulsar.NewAuthenticationOAuth2(p.authConf.OAuth2.ToMap())
	} else if p.authConf.Token.Enabled {
		opts.Authentication = pulsar.NewAuthenticationToken(p.authConf.Token.Token)
	}

	if client, err = pulsar.NewClient(opts); err != nil {
		return err
	}

	if producer, err = client.CreateProducer(pulsar.ProducerOptions{
		Topic: p.topic,
	}); err != nil {
		client.Close()
		return err
	}

	p.client = client
	p.producer = producer
	return nil
}

// disconnect safely closes a connection to an Pulsar server.
func (p *pulsarWriter) disconnect(ctx context.Context) error {
	p.m.Lock()
	defer p.m.Unlock()

	if p.client == nil {
		return nil
	}

	p.producer.Close()
	p.client.Close()

	p.producer = nil
	p.client = nil
	return nil
}

//------------------------------------------------------------------------------

func (p *pulsarWriter) Write(ctx context.Context, msg *service.Message) error {
	var r pulsar.Producer
	p.m.RLock()
	if p.producer != nil {
		r = p.producer
	}
	p.m.RUnlock()

	if r == nil {
		return component.ErrNotConnected
	}

	b, err := msg.AsBytes()
	if err != nil {
		return err
	}

	m := &pulsar.ProducerMessage{
		Payload: b,
	}

	key, err := p.key.TryBytes(msg)
	if err != nil {
		return err
	}

	if len(key) > 0 {
		m.Key = string(key)
	}

	orderingKey, err := p.orderingKey.TryBytes(msg)
	if err != nil {
		return err
	}

	if len(orderingKey) > 0 {
		m.OrderingKey = string(orderingKey)
	}

	_, err = r.Send(context.Background(), m)
	return err
}

func (p *pulsarWriter) Close(ctx context.Context) error {
	return p.disconnect(ctx)
}
