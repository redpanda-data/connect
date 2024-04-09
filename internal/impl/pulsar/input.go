package pulsar

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	defaultSubscriptionType = "shared"
)

func init() {
	err := service.RegisterInput(
		"pulsar",
		inputConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newPulsarReaderFromParsed(conf, mgr.Logger())
		})
	if err != nil {
		panic(err)
	}
}

func inputConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("3.43.0").
		Categories("Services").
		Summary("Reads messages from an Apache Pulsar server.").
		Description(`
### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- pulsar_message_id
- pulsar_key
- pulsar_ordering_key
- pulsar_event_time_unix
- pulsar_publish_time_unix
- pulsar_topic
- pulsar_producer_name
- pulsar_redelivery_count
- All properties of the message
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).
`).
		Field(service.NewURLField("url").
			Description("A URL to connect to.").
			Example("pulsar://localhost:6650").
			Example("pulsar://pulsar.us-west.example.com:6650").
			Example("pulsar+ssl://pulsar.us-west.example.com:6651")).
		Field(service.NewStringListField("topics").
			Description("A list of topics to subscribe to. This or topics_pattern must be set.").
			Optional()).
		Field(service.NewStringField("topics_pattern").
			Description("A regular expression matching the topics to subscribe to. This or topics must be set.").
			Optional()).
		Field(service.NewStringField("subscription_name").
			Description("Specify the subscription name for this consumer.")).
		Field(service.NewStringEnumField("subscription_type", "shared", "key_shared", "failover", "exclusive").
			Description("Specify the subscription type for this consumer.\n\n> NOTE: Using a `key_shared` subscription type will __allow out-of-order delivery__ since nack-ing messages sets non-zero nack delivery delay - this can potentially cause consumers to stall. See [Pulsar documentation](https://pulsar.apache.org/docs/en/2.8.1/concepts-messaging/#negative-acknowledgement) and [this Github issue](https://github.com/apache/pulsar/issues/12208) for more details.").
			Default(defaultSubscriptionType)).
		Field(service.NewObjectField("tls",
			service.NewStringField("root_cas_file").
				Description("An optional path of a root certificate authority file to use. This is a file, often with a .pem extension, containing a certificate chain from the parent trusted root certificate, to possible intermediate signing certificates, to the host certificate.").
				Default("").
				Example("./root_cas.pem")).
			Description("Specify the path to a custom CA certificate to trust broker TLS service.")).
		Field(authField())
}

//------------------------------------------------------------------------------

type pulsarReader struct {
	client   pulsar.Client
	consumer pulsar.Consumer
	m        sync.RWMutex

	log *service.Logger

	authConf      authConfig
	url           string
	topics        []string
	topicsPattern string
	subName       string
	subType       string
	rootCasFile   string
}

func newPulsarReaderFromParsed(conf *service.ParsedConfig, log *service.Logger) (p *pulsarReader, err error) {
	p = &pulsarReader{
		log: log,
	}

	if p.authConf, err = authFromParsed(conf); err != nil {
		return
	}

	if p.url, err = conf.FieldString("url"); err != nil {
		return
	}

	p.topics, _ = conf.FieldStringList("topics")

	p.topicsPattern, _ = conf.FieldString("topics_pattern")

	if p.subName, err = conf.FieldString("subscription_name"); err != nil {
		return
	}
	if p.subType, err = conf.FieldString("subscription_type"); err != nil {
		return
	}
	if p.rootCasFile, err = conf.FieldString("tls", "root_cas_file"); err != nil {
		return
	}

	if p.url == "" {
		err = errors.New("field url must not be empty")
		return
	}
	if (len(p.topics) == 0 && p.topicsPattern == "") ||
		(len(p.topics) > 0 && p.topicsPattern != "") {
		err = errors.New("exactly one of fields topics and topics_pattern must be set")
		return
	}
	if p.subName == "" {
		err = errors.New("field subscription_name must not be empty")
		return
	}
	if p.subType == "" {
		p.subType = defaultSubscriptionType // set default subscription type if empty
	}
	if _, err = parseSubscriptionType(p.subType); err != nil {
		err = fmt.Errorf("field subscription_type is invalid: %v", err)
		return
	}
	if err = p.authConf.Validate(); err != nil {
		err = fmt.Errorf("field auth is invalid: %v", err)
	}
	return
}

func parseSubscriptionType(subType string) (pulsar.SubscriptionType, error) {
	// Pulsar docs: https://pulsar.apache.org/docs/en/2.8.0/concepts-messaging/#subscriptions
	switch subType {
	case "shared":
		return pulsar.Shared, nil
	case "key_shared":
		return pulsar.KeyShared, nil
	case "failover":
		return pulsar.Failover, nil
	case "exclusive":
		return pulsar.Exclusive, nil
	}
	return pulsar.Shared, fmt.Errorf("could not parse subscription type: %s", subType)
}

//------------------------------------------------------------------------------

func (p *pulsarReader) Connect(ctx context.Context) error {
	p.m.Lock()
	defer p.m.Unlock()

	if p.client != nil {
		return nil
	}

	var (
		client   pulsar.Client
		consumer pulsar.Consumer
		subType  pulsar.SubscriptionType
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

	if subType, err = parseSubscriptionType(p.subType); err != nil {
		return err
	}

	options := pulsar.ConsumerOptions{
		Topics:           p.topics,
		TopicsPattern:    p.topicsPattern,
		SubscriptionName: p.subName,
		Type:             subType,
		KeySharedPolicy: &pulsar.KeySharedPolicy{
			AllowOutOfOrderDelivery: true,
		},
	}
	if consumer, err = client.Subscribe(options); err != nil {
		client.Close()
		return err
	}

	p.client = client
	p.consumer = consumer
	return nil
}

func (p *pulsarReader) disconnect(ctx context.Context) error {
	p.m.Lock()
	defer p.m.Unlock()

	if p.client == nil {
		return nil
	}

	p.consumer.Close()
	p.client.Close()

	p.consumer = nil
	p.client = nil
	return nil
}

func (p *pulsarReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	var r pulsar.Consumer
	p.m.RLock()
	if p.consumer != nil {
		r = p.consumer
	}
	p.m.RUnlock()

	if r == nil {
		return nil, nil, component.ErrNotConnected
	}

	// Receive next message
	pulMsg, err := r.Receive(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			err = component.ErrTimeout
		} else {
			p.log.Errorf("Lost connection due to: %v\n", err)
			_ = p.disconnect(ctx)
			err = component.ErrNotConnected
		}
		return nil, nil, err
	}

	msg := service.NewMessage(pulMsg.Payload())

	msg.MetaSet("pulsar_message_id", string(pulMsg.ID().Serialize()))
	msg.MetaSet("pulsar_topic", pulMsg.Topic())
	msg.MetaSet("pulsar_publish_time_unix", strconv.FormatInt(pulMsg.PublishTime().Unix(), 10))
	msg.MetaSet("pulsar_redelivery_count", strconv.FormatInt(int64(pulMsg.RedeliveryCount()), 10))
	if key := pulMsg.Key(); key != "" {
		msg.MetaSet("pulsar_key", key)
	}
	if orderingKey := pulMsg.OrderingKey(); orderingKey != "" {
		msg.MetaSet("pulsar_ordering_key", orderingKey)
	}
	if !pulMsg.EventTime().IsZero() {
		msg.MetaSet("pulsar_event_time_unix", strconv.FormatInt(pulMsg.EventTime().Unix(), 10))
	}
	if producerName := pulMsg.ProducerName(); producerName != "" {
		msg.MetaSet("pulsar_producer_name", producerName)
	}
	for k, v := range pulMsg.Properties() {
		msg.MetaSet(k, v)
	}

	return msg, func(ctx context.Context, res error) error {
		var r pulsar.Consumer
		p.m.RLock()
		if p.consumer != nil {
			r = p.consumer
		}
		p.m.RUnlock()
		if r != nil {
			if res != nil {
				r.Nack(pulMsg)
			} else {
				return r.Ack(pulMsg)
			}
		}
		return nil
	}, nil
}

func (p *pulsarReader) Close(ctx context.Context) error {
	return p.disconnect(ctx)
}
