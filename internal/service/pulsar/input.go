package pulsar

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/apache/pulsar-client-go/pulsar"
)

func init() {
	bundle.AllInputs.Add(bundle.InputConstructorFromSimple(func(c input.Config, nm bundle.NewManagement) (input.Type, error) {
		var a reader.Async
		var err error
		if a, err = newPulsarReader(c.Pulsar, nm.Logger(), nm.Metrics()); err != nil {
			return nil, err
		}
		a = reader.NewAsyncBundleUnacks(a)
		return input.NewAsyncReader(input.TypePulsar, true, a, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:   input.TypePulsar,
		Type:   docs.TypeInput,
		Status: docs.StatusExperimental,
		Summary: `
Reads messages from an Apache Pulsar server.`,
		Description: `
### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- pulsar_key
- pulsar_topic
- All properties of the message
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
		Categories: []string{
			string(input.CategoryServices),
		},
		Fields: docs.FieldSpecs{
			docs.FieldCommon("url",
				"A URL to connect to.",
				"pulsar://localhost:6650",
				"pulsar://pulsar.us-west.example.com:6650",
				"pulsar+ssl://pulsar.us-west.example.com:6651",
			),
			docs.FieldCommon("topics", "A list of topics to subscribe to."),
			docs.FieldCommon("subscription_name", "Specify the subscription name for this consumer."),
		},
	})
}

//------------------------------------------------------------------------------

type pulsarReader struct {
	client   pulsar.Client
	consumer pulsar.Consumer

	conf  input.PulsarConfig
	stats metrics.Type
	log   log.Modular

	m sync.RWMutex
}

func newPulsarReader(conf input.PulsarConfig, log log.Modular, stats metrics.Type) (*pulsarReader, error) {
	if len(conf.URL) == 0 {
		return nil, errors.New("field url must not be empty")
	}
	if len(conf.Topics) == 0 {
		return nil, errors.New("field topics must not be empty")
	}
	if len(conf.SubscriptionName) == 0 {
		return nil, errors.New("field subscription_name must not be empty")
	}
	p := pulsarReader{
		conf:  conf,
		stats: stats,
		log:   log,
	}
	return &p, nil
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
		err      error
	)

	if client, err = pulsar.NewClient(pulsar.ClientOptions{
		Logger:            NoopLogger(),
		ConnectionTimeout: time.Second * 3,
		URL:               p.conf.URL,
	}); err != nil {
		return err
	}

	if consumer, err = client.Subscribe(pulsar.ConsumerOptions{
		Topics:           p.conf.Topics,
		SubscriptionName: p.conf.SubscriptionName,
		Type:             pulsar.Shared,
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

	return nil
}

//------------------------------------------------------------------------------

// ReadWithContext a new Pulsar message.
func (p *pulsarReader) ReadWithContext(ctx context.Context) (types.Message, reader.AsyncAckFn, error) {
	var r pulsar.Consumer
	p.m.RLock()
	if p.consumer != nil {
		r = p.consumer
	}
	p.m.RUnlock()

	if r == nil {
		return nil, nil, types.ErrNotConnected
	}

	// Receive next message
	pulMsg, err := r.Receive(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			err = types.ErrTimeout
		} else {
			p.log.Errorf("Lost connection due to: %v\n", err)
			p.disconnect(ctx)
			err = types.ErrNotConnected
		}
		return nil, nil, err
	}

	msg := message.New(nil)

	part := message.NewPart(pulMsg.Payload())

	if key := pulMsg.Key(); len(key) > 0 {
		part.Metadata().Set("pulsar_key", key)
	}
	part.Metadata().Set("pulsar_topic", pulMsg.Topic())
	for k, v := range pulMsg.Properties() {
		part.Metadata().Set(k, v)
	}

	msg.Append(part)

	return msg, func(ctx context.Context, res types.Response) error {
		var r pulsar.Consumer
		p.m.RLock()
		if p.consumer != nil {
			r = p.consumer
		}
		p.m.RUnlock()
		if r != nil {
			if res.Error() != nil {
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
	p.disconnect(context.Background())
}

// WaitForClose blocks until the Pulsar input has closed down.
func (p *pulsarReader) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
