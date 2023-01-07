package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/benthosdev/benthos/v4/public/service"
)

func franzKafkaOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Services").
		Version("3.61.0").
		Summary("An alternative Kafka output using the [Franz Kafka client library](https://github.com/twmb/franz-go).").
		Description(`
Writes a batch of messages to Kafka brokers and waits for acknowledgement before propagating it back to the input.

This output is new and experimental, and the existing ` + "`kafka`" + ` input is not going anywhere, but here's some reasons why it might be worth trying this one out:

- You like shiny new stuff
- You are experiencing issues with the existing ` + "`kafka`" + ` output
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
		Field(service.NewStringAnnotatedEnumField("partitioner", map[string]string{
			"murmur2_hash": "Kafka's default hash algorithm that uses a 32-bit murmur2 hash of the key to compute which partition the record will be on.",
			"round_robin":  "Round-robin's messages through all available partitions. This algorithm has lower throughput and causes higher CPU load on brokers, but can be useful if you want to ensure an even distribution of records to partitions.",
			"least_backup": "Chooses the least backed up partition (the partition with the fewest amount of buffered records). Partitions are selected per batch.",
		}).
			Description("Override the default murmur2 hashing partitioner.").
			Advanced().Optional()).
		Field(service.NewMetadataFilterField("metadata").
			Description("Determine which (if any) metadata values should be added to messages as headers.").
			Optional()).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of batches to be sending in parallel at any given time.").
			Default(10)).
		Field(service.NewDurationField("timeout").
			Description("The maximum period of time to wait for message sends before abandoning the request and retrying").
			Default("10s").
			Advanced()).
		Field(service.NewBatchPolicyField("batching")).
		Field(service.NewStringField("max_message_bytes").
			Description("The maximum space in bytes than an individual message may take, messages larger than this value will be rejected. This field corresponds to Kafka's `max.message.bytes`.").
			Advanced().
			Default("1MB").
			Example("100MB").
			Example("50mib")).
		Field(service.NewStringEnumField("compression", "lz4", "snappy", "gzip", "none", "zstd").
			Description("Optionally set an explicit compression type. The default preference is to use snappy when the broker supports it, and fall back to none if not.").
			Optional().
			Advanced()).
		Field(service.NewTLSToggledField("tls")).
		Field(saslField())
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
	seedBrokers      []string
	topicStr         string
	topic            *service.InterpolatedString
	key              *service.InterpolatedString
	tlsConf          *tls.Config
	saslConfs        []sasl.Mechanism
	metaFilter       *service.MetadataFilter
	partitioner      kgo.Partitioner
	timeout          time.Duration
	produceMaxBytes  int32
	compressionPrefs []kgo.CompressionCodec

	client *kgo.Client

	log *service.Logger
}

func newFranzKafkaWriterFromConfig(conf *service.ParsedConfig, log *service.Logger) (*franzKafkaWriter, error) {
	f := franzKafkaWriter{
		log: log,
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

	if f.timeout, err = conf.FieldDuration("timeout"); err != nil {
		return nil, err
	}

	maxBytesStr, err := conf.FieldString("max_message_bytes")
	if err != nil {
		return nil, err
	}
	maxBytes, err := humanize.ParseBytes(maxBytesStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse max_message_bytes: %w", err)
	}
	if maxBytes > uint64(math.MaxInt32) {
		return nil, fmt.Errorf("invalid max_message_bytes, must not exceed %v", math.MaxInt32)
	}
	f.produceMaxBytes = int32(maxBytes)

	if conf.Contains("compression") {
		cStr, err := conf.FieldString("compression")
		if err != nil {
			return nil, err
		}

		var c kgo.CompressionCodec
		switch cStr {
		case "lz4":
			c = kgo.Lz4Compression()
		case "gzip":
			c = kgo.GzipCompression()
		case "snappy":
			c = kgo.SnappyCompression()
		case "zstd":
			c = kgo.ZstdCompression()
		case "none":
			c = kgo.NoCompression()
		default:
			return nil, fmt.Errorf("compression codec %v not recognised", cStr)
		}
		f.compressionPrefs = append(f.compressionPrefs, c)
	}

	f.partitioner = kgo.StickyKeyPartitioner(nil)
	if conf.Contains("partitioner") {
		partStr, err := conf.FieldString("partitioner")
		if err != nil {
			return nil, err
		}
		switch partStr {
		case "murmur2_hash":
			f.partitioner = kgo.StickyKeyPartitioner(nil)
		case "round_robin":
			f.partitioner = kgo.RoundRobinPartitioner()
		case "least_backup":
			f.partitioner = kgo.LeastBackupPartitioner()
		default:
			return nil, fmt.Errorf("unknown partitioner: %v", partStr)
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
	if f.saslConfs, err = saslMechanismsFromConfig(conf); err != nil {
		return nil, err
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
		kgo.SASL(f.saslConfs...),
		kgo.AllowAutoTopicCreation(), // TODO: Configure this
		kgo.ProducerBatchMaxBytes(f.produceMaxBytes),
		kgo.ProduceRequestTimeout(f.timeout),
		kgo.WithLogger(&kgoLogger{f.log}),
	}
	if f.tlsConf != nil {
		clientOpts = append(clientOpts, kgo.DialTLSConfig(f.tlsConf))
	}
	if f.partitioner != nil {
		clientOpts = append(clientOpts, kgo.RecordPartitioner(f.partitioner))
	}
	if len(f.compressionPrefs) > 0 {
		clientOpts = append(clientOpts, kgo.ProducerBatchCompression(f.compressionPrefs...))
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
		var topic string
		if topic, err = b.TryInterpolatedString(i, f.topic); err != nil {
			return fmt.Errorf("topic interpolation error: %w", err)
		}

		record := &kgo.Record{Topic: topic}
		if record.Value, err = msg.AsBytes(); err != nil {
			return
		}
		if f.key != nil {
			if record.Key, err = b.TryInterpolatedBytes(i, f.key); err != nil {
				return fmt.Errorf("key interpolation error: %w", err)
			}
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
