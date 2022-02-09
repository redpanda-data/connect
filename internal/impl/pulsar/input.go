package pulsar

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/impl/pulsar/auth"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/apache/pulsar-client-go/pulsar"
)

const (
	defaultSubscriptionType = "shared"
)

func init() {
	bundle.AllInputs.Add(bundle.InputConstructorFromSimple(func(c input.Config, nm bundle.NewManagement) (input.Type, error) {
		var a reader.Async
		var err error
		if a, err = newPulsarReader(c.Pulsar, nm.Logger(), nm.Metrics()); err != nil {
			return nil, err
		}
		return input.NewAsyncReader(input.TypePulsar, false, a, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:    input.TypePulsar,
		Type:    docs.TypeInput,
		Status:  docs.StatusExperimental,
		Version: "3.43.0",
		Summary: `Reads messages from an Apache Pulsar server.`,
		Description: `
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
[function interpolation](/docs/configuration/interpolation#metadata).`,
		Categories: []string{
			string(input.CategoryServices),
		},
		Config: docs.FieldComponent().WithChildren(
			docs.FieldCommon("url",
				"A URL to connect to.",
				"pulsar://localhost:6650",
				"pulsar://pulsar.us-west.example.com:6650",
				"pulsar+ssl://pulsar.us-west.example.com:6651",
			),
			docs.FieldString("topics", "A list of topics to subscribe to.").Array(),
			docs.FieldCommon("subscription_name", "Specify the subscription name for this consumer."),
			docs.FieldCommon("subscription_type", "Specify the subscription type for this consumer.\n\n> NOTE: Using a `key_shared` subscription type will __allow out-of-order delivery__ since nack-ing messages sets non-zero nack delivery delay - this can potentially cause consumers to stall. See [Pulsar documentation](https://pulsar.apache.org/docs/en/2.8.1/concepts-messaging/#negative-acknowledgement) and [this Github issue](https://github.com/apache/pulsar/issues/12208) for more details.").
				HasOptions("shared", "key_shared", "failover", "exclusive").
				HasDefault(defaultSubscriptionType),
			auth.FieldSpec(),
		).ChildDefaultAndTypesFromStruct(input.NewPulsarConfig()),
	})
}

//------------------------------------------------------------------------------

type pulsarReader struct {
	client   pulsar.Client
	consumer pulsar.Consumer

	conf  input.PulsarConfig
	stats metrics.Type
	log   log.Modular

	m       sync.RWMutex
	shutSig *shutdown.Signaller
}

func newPulsarReader(conf input.PulsarConfig, log log.Modular, stats metrics.Type) (*pulsarReader, error) {
	if conf.URL == "" {
		return nil, errors.New("field url must not be empty")
	}
	if len(conf.Topics) == 0 {
		return nil, errors.New("field topics must not be empty")
	}
	if conf.SubscriptionName == "" {
		return nil, errors.New("field subscription_name must not be empty")
	}
	if conf.SubscriptionType == "" {
		conf.SubscriptionType = defaultSubscriptionType // set default subscription type if empty
	}
	if _, err := parseSubscriptionType(conf.SubscriptionType); err != nil {
		return nil, fmt.Errorf("field subscription_type is invalid: %v", err)
	}
	if err := conf.Auth.Validate(); err != nil {
		return nil, fmt.Errorf("field auth is invalid: %v", err)
	}

	p := pulsarReader{
		conf:    conf,
		stats:   stats,
		log:     log,
		shutSig: shutdown.NewSignaller(),
	}
	return &p, nil
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

// ConnectWithContext establishes a connection to an Pulsar server.
func (p *pulsarReader) ConnectWithContext(ctx context.Context) error {
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
		Logger:            DefaultLogger(p.log),
		ConnectionTimeout: time.Second * 3,
		URL:               p.conf.URL,
	}

	if p.conf.Auth.OAuth2.Enabled {
		opts.Authentication = pulsar.NewAuthenticationOAuth2(p.conf.Auth.OAuth2.ToMap())
	} else if p.conf.Auth.Token.Enabled {
		opts.Authentication = pulsar.NewAuthenticationToken(p.conf.Auth.Token.Token)
	}

	if client, err = pulsar.NewClient(opts); err != nil {
		return err
	}

	if subType, err = parseSubscriptionType(p.conf.SubscriptionType); err != nil {
		return err
	}

	if consumer, err = client.Subscribe(pulsar.ConsumerOptions{
		Topics:           p.conf.Topics,
		SubscriptionName: p.conf.SubscriptionName,
		Type:             subType,
		KeySharedPolicy: &pulsar.KeySharedPolicy{
			AllowOutOfOrderDelivery: true,
		},
	}); err != nil {
		client.Close()
		return err
	}

	p.client = client
	p.consumer = consumer

	p.log.Infof("Receiving Pulsar messages to URL: %v\n", p.conf.URL)
	return nil
}

// disconnect safely closes a connection to an Pulsar server.
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

	if p.shutSig.ShouldCloseAtLeisure() {
		p.shutSig.ShutdownComplete()
	}
	return nil
}

//------------------------------------------------------------------------------

// ReadWithContext a new Pulsar message.
func (p *pulsarReader) ReadWithContext(ctx context.Context) (*message.Batch, reader.AsyncAckFn, error) {
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
			p.disconnect(ctx)
			err = component.ErrNotConnected
		}
		return nil, nil, err
	}

	msg := message.QuickBatch(nil)

	part := message.NewPart(pulMsg.Payload())

	part.MetaSet("pulsar_message_id", string(pulMsg.ID().Serialize()))
	part.MetaSet("pulsar_topic", pulMsg.Topic())
	part.MetaSet("pulsar_publish_time_unix", strconv.FormatInt(pulMsg.PublishTime().Unix(), 10))
	part.MetaSet("pulsar_redelivery_count", strconv.FormatInt(int64(pulMsg.RedeliveryCount()), 10))
	if key := pulMsg.Key(); len(key) > 0 {
		part.MetaSet("pulsar_key", key)
	}
	if orderingKey := pulMsg.OrderingKey(); len(orderingKey) > 0 {
		part.MetaSet("pulsar_ordering_key", orderingKey)
	}
	if !pulMsg.EventTime().IsZero() {
		part.MetaSet("pulsar_event_time_unix", strconv.FormatInt(pulMsg.EventTime().Unix(), 10))
	}
	if producerName := pulMsg.ProducerName(); producerName != "" {
		part.MetaSet("pulsar_producer_name", producerName)
	}
	for k, v := range pulMsg.Properties() {
		part.MetaSet(k, v)
	}

	msg.Append(part)

	return msg, func(ctx context.Context, res response.Error) error {
		var r pulsar.Consumer
		p.m.RLock()
		if p.consumer != nil {
			r = p.consumer
		}
		p.m.RUnlock()
		if r != nil {
			if res.AckError() != nil {
				r.Nack(pulMsg)
			} else {
				r.Ack(pulMsg)
			}
		}
		return nil
	}, nil
}

// CloseAsync shuts down the Pulsar input and stops processing requests.
func (p *pulsarReader) CloseAsync() {
	p.shutSig.CloseAtLeisure()
	go p.disconnect(context.Background())
}

// WaitForClose blocks until the Pulsar input has closed down.
func (p *pulsarReader) WaitForClose(timeout time.Duration) error {
	select {
	case <-p.shutSig.HasClosedChan():
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
