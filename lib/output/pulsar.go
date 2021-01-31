package output

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	bpulsar "github.com/Jeffail/benthos/v3/internal/service/pulsar"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/apache/pulsar-client-go/pulsar"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypePulsar] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			w, err := newPulsar(conf.Pulsar, log, stats)
			if err != nil {
				return nil, err
			}
			return NewAsyncWriter(TypePulsar, conf.Pulsar.MaxInFlight, w, log, stats)
		}),
		Status: docs.StatusBeta,
		Async:  true,
		Summary: `
Write messages to an Apache Pulsar server.`,
		Categories: []Category{
			CategoryServices,
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url",
				"A URL to connect to.",
				"pulsar://localhost:6650",
				"pulsar://pulsar.us-west.example.com:6650",
				"pulsar+ssl://pulsar.us-west.example.com:6651",
			),
			docs.FieldCommon("topic", "A topic to publish to."),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		},
	}
}

//------------------------------------------------------------------------------

// PulsarConfig contains configuration for the Pulsar input type.
type PulsarConfig struct {
	URL         string `json:"url" yaml:"url"`
	Topic       string `json:"topic" yaml:"topic"`
	MaxInFlight int    `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewPulsarConfig creates a new PulsarConfig with default values.
func NewPulsarConfig() PulsarConfig {
	return PulsarConfig{
		URL:         "",
		Topic:       "",
		MaxInFlight: 1,
	}
}

//------------------------------------------------------------------------------

type pulsarWriter struct {
	client   pulsar.Client
	producer pulsar.Producer

	conf  PulsarConfig
	stats metrics.Type
	log   log.Modular

	m sync.RWMutex
}

func newPulsar(conf PulsarConfig, log log.Modular, stats metrics.Type) (*pulsarWriter, error) {
	if len(conf.URL) == 0 {
		return nil, errors.New("field url must not be empty")
	}
	if len(conf.Topic) == 0 {
		return nil, errors.New("field topic must not be empty")
	}
	p := pulsarWriter{
		conf:  conf,
		stats: stats,
		log:   log,
	}
	return &p, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to an Pulsar server.
func (p *pulsarWriter) ConnectWithContext(ctx context.Context) error {
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

	if client, err = pulsar.NewClient(pulsar.ClientOptions{
		URL:    p.conf.URL,
		Logger: bpulsar.NoopLogger(),
	}); err != nil {
		return err
	}

	if producer, err = client.CreateProducer(pulsar.ProducerOptions{
		Topic: p.conf.Topic,
	}); err != nil {
		client.Close()
		return err
	}

	p.client = client
	p.producer = producer

	p.log.Infof("Writing Pulsar messages to URL: %v\n", p.conf.URL)
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

// WriteWithContext will attempt to write a message over Pulsar, wait for
// acknowledgement, and returns an error if applicable.
func (p *pulsarWriter) WriteWithContext(ctx context.Context, msg types.Message) error {
	var r pulsar.Producer
	p.m.RLock()
	if p.producer != nil {
		r = p.producer
	}
	p.m.RUnlock()

	if r == nil {
		return types.ErrNotConnected
	}

	return writer.IterateBatchedSend(msg, func(i int, p types.Part) error {
		m := &pulsar.ProducerMessage{
			Payload: p.Get(),
		}
		_, err := r.Send(context.Background(), m)
		return err
	})
}

// CloseAsync shuts down the Pulsar input and stops processing requests.
func (p *pulsarWriter) CloseAsync() {
	p.disconnect(context.Background())
}

// WaitForClose blocks until the Pulsar input has closed down.
func (p *pulsarWriter) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
