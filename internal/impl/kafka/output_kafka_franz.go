package kafka

import (
	"context"
	"crypto/tls"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

func franzKafkaOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Services").
		Version("3.61.0").
		Summary("An alternative Kafka output using the [Franz Kafka client library](https://github.com/twmb/franz-go).").
		Description(`
Consumes one or more topics by balancing the partitions across any other connected clients with the same consumer group.

This input is new and experimental, and the existing ` + "`kafka`" + ` input is not going anywhere, but here's some reasons why it might be worth trying this one out:

- You like shiny new stuff
- You are exeriencing issues with the existing ` + "`kafka`" + ` input
- Someone told you to
`).
		Field(service.NewStringListField("seed_brokers").
			Description("A list of broker addresses to connect to in order to establish connections. If an item of the list contains commas it will be expanded into multiple addresses.").
			Example([]string{"localhost:9092"}).
			Example([]string{"foo:9092", "bar:9092"}).
			Example([]string{"foo:9092,bar:9092"})).
		Field(service.NewInterpolatedStringField("topic").
			Description("A topic to write messages to.")).
		Field(service.NewInterpolatedStringField("key").
			Description("An optional key to populate for each message.").Optional()).
		Field(service.NewMetadataFilterField("metadata").
			Description("Determine which (if any) metadata values should be added to messages as headers.").
			Optional()).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of batches to be sending in parallel at any given time.").
			Default(10)).
		Field(service.NewBatchPolicyField("batching")).
		Field(service.NewTLSToggledField("tls"))
}

func init() {
	err := service.RegisterBatchOutput("kafka_franz", franzKafkaOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			output, err = newFranzKafkaWriterFromConfig(conf, mgr.Logger())
			return
		})

	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type franzKafkaWriter struct {
	seedBrokers []string
	topicStr    string
	topic       *service.InterpolatedString
	key         *service.InterpolatedString
	tlsConf     *tls.Config
	metaFilter  *service.MetadataFilter

	client *kgo.Client

	log     *service.Logger
	shutSig *shutdown.Signaller
}

func newFranzKafkaWriterFromConfig(conf *service.ParsedConfig, log *service.Logger) (*franzKafkaWriter, error) {
	f := franzKafkaWriter{
		log:     log,
		shutSig: shutdown.NewSignaller(),
	}

	brokerList, err := conf.FieldStringList("seed_brokers")
	if err != nil {
		return nil, err
	}
	for _, b := range brokerList {
		f.seedBrokers = append(f.seedBrokers, strings.Split(b, ",")...)
	}

	if f.topic, err = conf.FieldInterpolatedString("topic"); err != nil {
		return nil, err
	}
	f.topicStr, _ = conf.FieldString("topic")

	if conf.Contains("key") {
		if f.key, err = conf.FieldInterpolatedString("key"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("metadata") {
		if f.metaFilter, err = conf.FieldMetadataFilter("metadata"); err != nil {
			return nil, err
		}
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		f.tlsConf = tlsConf
	}

	return &f, nil
}

//------------------------------------------------------------------------------

func (f *franzKafkaWriter) Connect(ctx context.Context) error {
	if f.client != nil {
		return nil
	}

	clientOpts := []kgo.Opt{
		kgo.SeedBrokers(f.seedBrokers...),
		kgo.AllowAutoTopicCreation(), // TODO: Configure this
	}
	if f.tlsConf != nil {
		clientOpts = append(clientOpts, kgo.DialTLSConfig(f.tlsConf))
	}

	cl, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return err
	}

	f.client = cl
	f.log.Infof("Writing messages to Kafka topic: %v", f.topicStr)
	return nil
}

func (f *franzKafkaWriter) WriteBatch(ctx context.Context, b service.MessageBatch) (err error) {
	if f.client == nil {
		return service.ErrNotConnected
	}

	records := make([]*kgo.Record, 0, len(b))
	for i, msg := range b {
		record := &kgo.Record{Topic: b.InterpolatedString(i, f.topic)}
		if record.Value, err = msg.AsBytes(); err != nil {
			return
		}
		if f.key != nil {
			record.Key = b.InterpolatedBytes(i, f.key)
		}
		_ = f.metaFilter.Walk(msg, func(key, value string) error {
			record.Headers = append(record.Headers, kgo.RecordHeader{
				Key:   key,
				Value: []byte(value),
			})
			return nil
		})
		records = append(records, record)
	}

	// TODO: This is very cool and allows us to easily return granular errors,
	// so we should honor travis by doing it.
	err = f.client.ProduceSync(ctx, records...).FirstErr()
	return
}

func (f *franzKafkaWriter) disconnect() {
	if f.client == nil {
		return
	}
	f.client.Close()
	f.client = nil
}

func (f *franzKafkaWriter) Close(ctx context.Context) error {
	f.disconnect()
	return nil
}
