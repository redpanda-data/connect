// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
)

const (
	rproDefaultLabel = "kafka_migrator_output"
)

func kafkaMigratorOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.35.0").
		Summary("A Kafka Migrator output using the https://github.com/twmb/franz-go[Franz Kafka client library^].").
		Description(`
Writes a batch of messages to a Kafka broker and waits for acknowledgement before propagating it back to the input.

This output should be used in combination with a `+"`kafka_migrator`"+` input which it can query for topic and ACL configurations.

If the configured broker does not contain the current message `+"topic"+`, it attempts to create it along with the topic
ACLs which are read automatically from the `+"`kafka_migrator`"+` input identified by the label specified in
`+"`input_resource`"+`.
`).
		Fields(KafkaMigratorOutputConfigFields()...).
		LintRule(`
root = if this.partitioner == "manual" {
if this.partition.or("") == "" {
"a partition must be specified when the partitioner is set to manual"
}
} else if this.partition.or("") != "" {
"a partition cannot be specified unless the partitioner is set to manual"
}`).Example("Transfer data", "Writes messages to the configured broker and creates topics and topic ACLs if they don't exist. It also ensures that the message order is preserved.", `
output:
  kafka_migrator:
    seed_brokers: [ "127.0.0.1:9093" ]
    topic: ${! metadata("kafka_topic").or(throw("missing kafka_topic metadata")) }
    key: ${! metadata("kafka_key") }
    partitioner: manual
    partition: ${! metadata("kafka_partition").or(throw("missing kafka_partition metadata")) }
    timestamp: ${! metadata("kafka_timestamp_unix").or(timestamp_unix()) }
    input_resource: kafka_migrator_input
    max_in_flight: 1
`)
}

// KafkaMigratorOutputConfigFields returns the full suite of config fields for a `kafka_migrator` output using
// the franz-go client library.
func KafkaMigratorOutputConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField("seed_brokers").
			Description("A list of broker addresses to connect to in order to establish connections. If an item of the list contains commas it will be expanded into multiple addresses.").
			Example([]string{"localhost:9092"}).
			Example([]string{"foo:9092", "bar:9092"}).
			Example([]string{"foo:9092,bar:9092"}),
		service.NewInterpolatedStringField("topic").
			Description("A topic to write messages to."),
		service.NewInterpolatedStringField("key").
			Description("An optional key to populate for each message.").Optional(),
		service.NewStringAnnotatedEnumField("partitioner", map[string]string{
			"murmur2_hash": "Kafka's default hash algorithm that uses a 32-bit murmur2 hash of the key to compute which partition the record will be on.",
			"round_robin":  "Round-robin's messages through all available partitions. This algorithm has lower throughput and causes higher CPU load on brokers, but can be useful if you want to ensure an even distribution of records to partitions.",
			"least_backup": "Chooses the least backed up partition (the partition with the fewest amount of buffered records). Partitions are selected per batch.",
			"manual":       "Manually select a partition for each message, requires the field `partition` to be specified.",
		}).
			Description("Override the default murmur2 hashing partitioner.").
			Advanced().Optional(),
		service.NewInterpolatedStringField("partition").
			Description("An optional explicit partition to set for each message. This field is only relevant when the `partitioner` is set to `manual`. The provided interpolation string must be a valid integer.").
			Example(`${! meta("partition") }`).
			Optional(),
		service.NewStringField("client_id").
			Description("An identifier for the client connection.").
			Default("benthos").
			Advanced(),
		service.NewStringField("rack_id").
			Description("A rack identifier for this client.").
			Default("").
			Advanced(),
		service.NewBoolField("idempotent_write").
			Description("Enable the idempotent write producer option. This requires the `IDEMPOTENT_WRITE` permission on `CLUSTER` and can be disabled if this permission is not available.").
			Default(true).
			Advanced(),
		service.NewMetadataFilterField("metadata").
			Description("Determine which (if any) metadata values should be added to messages as headers.").
			Optional(),
		service.NewIntField("max_in_flight").
			Description("The maximum number of batches to be sending in parallel at any given time.").
			Default(10),
		service.NewDurationField("timeout").
			Description("The maximum period of time to wait for message sends before abandoning the request and retrying").
			Default("10s").
			Advanced(),
		service.NewBatchPolicyField("batching"),
		service.NewStringField("max_message_bytes").
			Description("The maximum space in bytes than an individual message may take, messages larger than this value will be rejected. This field corresponds to Kafka's `max.message.bytes`.").
			Advanced().
			Default("1MB").
			Example("100MB").
			Example("50mib"),
		service.NewStringField("broker_write_max_bytes").
			Description("The upper bound for the number of bytes written to a broker connection in a single write. This field corresponds to Kafka's `socket.request.max.bytes`.").
			Advanced().
			Default("100MB").
			Example("128MB").
			Example("50mib"),
		service.NewStringEnumField("compression", "lz4", "snappy", "gzip", "none", "zstd").
			Description("Optionally set an explicit compression type. The default preference is to use snappy when the broker supports it, and fall back to none if not.").
			Optional().
			Advanced(),
		service.NewTLSToggledField("tls"),
		kafka.SASLFields(),
		service.NewInterpolatedStringField("timestamp").
			Description("An optional timestamp to set for each message. When left empty, the current timestamp is used.").
			Example(`${! timestamp_unix() }`).
			Example(`${! metadata("kafka_timestamp_unix") }`).
			Optional().
			Advanced(),
		service.NewStringField("input_resource").
			Description("The label of the kafka_migrator input from which to read the configurations for topics and ACLs which need to be created.").
			Default(rpriDefaultLabel).
			Advanced(),
	}
}

func init() {
	err := service.RegisterBatchOutput("kafka_migrator", kafkaMigratorOutputConfig(),
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
			output, err = NewKafkaMigratorWriterFromConfig(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// KafkaMigratorWriter implements a Kafka writer using the franz-go library.
type KafkaMigratorWriter struct {
	SeedBrokers         []string
	topic               *service.InterpolatedString
	key                 *service.InterpolatedString
	partition           *service.InterpolatedString
	timestamp           *service.InterpolatedString
	clientID            string
	rackID              string
	idempotentWrite     bool
	TLSConf             *tls.Config
	saslConfs           []sasl.Mechanism
	metaFilter          *service.MetadataFilter
	partitioner         kgo.Partitioner
	timeout             time.Duration
	produceMaxBytes     int32
	brokerWriteMaxBytes int32
	compressionPrefs    []kgo.CompressionCodec
	inputResource       string

	connMut    sync.Mutex
	client     *kgo.Client
	topicCache sync.Map

	mgr *service.Resources
}

// NewKafkaMigratorWriterFromConfig attempts to instantiate a KafkaMigratorWriter from a parsed config.
func NewKafkaMigratorWriterFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*KafkaMigratorWriter, error) {
	w := KafkaMigratorWriter{
		mgr: mgr,
	}

	brokerList, err := conf.FieldStringList("seed_brokers")
	if err != nil {
		return nil, err
	}
	for _, b := range brokerList {
		w.SeedBrokers = append(w.SeedBrokers, strings.Split(b, ",")...)
	}

	if w.topic, err = conf.FieldInterpolatedString("topic"); err != nil {
		return nil, err
	}

	if conf.Contains("key") {
		if w.key, err = conf.FieldInterpolatedString("key"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("partition") {
		if rawStr, _ := conf.FieldString("partition"); rawStr != "" {
			if w.partition, err = conf.FieldInterpolatedString("partition"); err != nil {
				return nil, err
			}
		}
	}

	if w.timeout, err = conf.FieldDuration("timeout"); err != nil {
		return nil, err
	}

	maxMessageBytesStr, err := conf.FieldString("max_message_bytes")
	if err != nil {
		return nil, err
	}
	maxMessageBytes, err := humanize.ParseBytes(maxMessageBytesStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse max_message_bytes: %w", err)
	}
	if maxMessageBytes > uint64(math.MaxInt32) {
		return nil, fmt.Errorf("invalid max_message_bytes, must not exceed %v", math.MaxInt32)
	}
	w.produceMaxBytes = int32(maxMessageBytes)
	brokerWriteMaxBytesStr, err := conf.FieldString("broker_write_max_bytes")
	if err != nil {
		return nil, err
	}
	brokerWriteMaxBytes, err := humanize.ParseBytes(brokerWriteMaxBytesStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse broker_write_max_bytes: %w", err)
	}
	if brokerWriteMaxBytes > 1<<30 {
		return nil, fmt.Errorf("invalid broker_write_max_bytes, must not exceed %v", 1<<30)
	}
	w.brokerWriteMaxBytes = int32(brokerWriteMaxBytes)

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
		w.compressionPrefs = append(w.compressionPrefs, c)
	}

	w.partitioner = kgo.StickyKeyPartitioner(nil)
	if conf.Contains("partitioner") {
		partStr, err := conf.FieldString("partitioner")
		if err != nil {
			return nil, err
		}
		switch partStr {
		case "murmur2_hash":
			w.partitioner = kgo.StickyKeyPartitioner(nil)
		case "round_robin":
			w.partitioner = kgo.RoundRobinPartitioner()
		case "least_backup":
			w.partitioner = kgo.LeastBackupPartitioner()
		case "manual":
			w.partitioner = kgo.ManualPartitioner()
		default:
			return nil, fmt.Errorf("unknown partitioner: %v", partStr)
		}
	}

	if w.clientID, err = conf.FieldString("client_id"); err != nil {
		return nil, err
	}

	if w.rackID, err = conf.FieldString("rack_id"); err != nil {
		return nil, err
	}

	if w.idempotentWrite, err = conf.FieldBool("idempotent_write"); err != nil {
		return nil, err
	}

	if conf.Contains("metadata") {
		if w.metaFilter, err = conf.FieldMetadataFilter("metadata"); err != nil {
			return nil, err
		}
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		w.TLSConf = tlsConf
	}
	if w.saslConfs, err = kafka.SASLMechanismsFromConfig(conf); err != nil {
		return nil, err
	}

	if conf.Contains("timestamp") {
		if w.timestamp, err = conf.FieldInterpolatedString("timestamp"); err != nil {
			return nil, err
		}
	}

	if w.inputResource, err = conf.FieldString("input_resource"); err != nil {
		return nil, err
	}

	if label := mgr.Label(); label != "" {
		mgr.SetGeneric(mgr.Label(), &w)
	} else {
		mgr.SetGeneric(rproDefaultLabel, &w)
	}

	return &w, nil
}

//------------------------------------------------------------------------------

// Connect to the target seed brokers.
func (w *KafkaMigratorWriter) Connect(ctx context.Context) error {
	w.connMut.Lock()
	defer w.connMut.Unlock()

	if w.client != nil {
		return nil
	}

	clientOpts := []kgo.Opt{
		kgo.SeedBrokers(w.SeedBrokers...),
		kgo.SASL(w.saslConfs...),
		// TODO: Do we want to allow this option in some cases and make it configurable somehow?
		// kgo.AllowAutoTopicCreation(),
		kgo.ProducerBatchMaxBytes(w.produceMaxBytes),
		kgo.BrokerMaxWriteBytes(w.brokerWriteMaxBytes),
		kgo.ProduceRequestTimeout(w.timeout),
		kgo.ClientID(w.clientID),
		kgo.Rack(w.rackID),
		kgo.WithLogger(&kafka.KGoLogger{L: w.mgr.Logger()}),
	}
	if w.TLSConf != nil {
		clientOpts = append(clientOpts, kgo.DialTLSConfig(w.TLSConf))
	}
	if w.partitioner != nil {
		clientOpts = append(clientOpts, kgo.RecordPartitioner(w.partitioner))
	}
	if !w.idempotentWrite {
		clientOpts = append(clientOpts, kgo.DisableIdempotentWrite())
	}
	if len(w.compressionPrefs) > 0 {
		clientOpts = append(clientOpts, kgo.ProducerBatchCompression(w.compressionPrefs...))
	}

	var err error
	if w.client, err = kgo.NewClient(clientOpts...); err != nil {
		return err
	}

	// Check connectivity to cluster
	if err := w.client.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to cluster: %s", err)
	}

	return nil
}

// WriteBatch attempts to write a batch of messages to the target topics.
func (w *KafkaMigratorWriter) WriteBatch(ctx context.Context, b service.MessageBatch) (err error) {
	w.connMut.Lock()
	defer w.connMut.Unlock()

	if w.client == nil {
		return service.ErrNotConnected
	}

	records := make([]*kgo.Record, 0, len(b))
	for i, msg := range b {
		var topic string
		if topic, err = b.TryInterpolatedString(i, w.topic); err != nil {
			return fmt.Errorf("topic interpolation error: %w", err)
		}

		var input *KafkaMigratorReader
		if res, ok := w.mgr.GetGeneric(w.inputResource); ok {
			input = res.(*KafkaMigratorReader)
		} else {
			w.mgr.Logger().Debugf("Input resource %q not found", w.inputResource)
		}

		if input != nil {
			if _, ok := w.topicCache.Load(topic); !ok {
				w.mgr.Logger().Infof("Creating topic %q", topic)

				if err := createTopic(ctx, topic, input.client, w.client); err != nil && err != errTopicAlreadyExists {
					return fmt.Errorf("failed to create topic %q: %s", topic, err)
				} else {
					if err == errTopicAlreadyExists {
						w.mgr.Logger().Infof("Topic %q already exists", topic)
					}
					if err := createACLs(ctx, topic, input.client, w.client); err != nil {
						w.mgr.Logger().Errorf("Failed to create ACLs for topic %q: %s", topic, err)
					}
				}
			}
		}

		record := &kgo.Record{Topic: topic}
		if record.Value, err = msg.AsBytes(); err != nil {
			return
		}
		if w.key != nil {
			if record.Key, err = b.TryInterpolatedBytes(i, w.key); err != nil {
				return fmt.Errorf("key interpolation error: %w", err)
			}
		}
		if w.partition != nil {
			partStr, err := b.TryInterpolatedString(i, w.partition)
			if err != nil {
				return fmt.Errorf("partition interpolation error: %w", err)
			}
			partInt, err := strconv.Atoi(partStr)
			if err != nil {
				return fmt.Errorf("partition parse error: %w", err)
			}
			record.Partition = int32(partInt)
		}
		_ = w.metaFilter.Walk(msg, func(key, value string) error {
			record.Headers = append(record.Headers, kgo.RecordHeader{
				Key:   key,
				Value: []byte(value),
			})
			return nil
		})
		if w.timestamp != nil {
			if tsStr, err := b.TryInterpolatedString(i, w.timestamp); err != nil {
				return fmt.Errorf("timestamp interpolation error: %w", err)
			} else {
				if ts, err := strconv.ParseInt(tsStr, 10, 64); err != nil {
					return fmt.Errorf("failed to parse timestamp: %w", err)
				} else {
					record.Timestamp = time.Unix(ts, 0)
				}
			}
		}
		records = append(records, record)
	}

	// TODO: This is very cool and allows us to easily return granular errors,
	// so we should honor travis by doing it.
	err = w.client.ProduceSync(ctx, records...).FirstErr()
	return
}

func (w *KafkaMigratorWriter) disconnect() {
	if w.client == nil {
		return
	}
	w.client.Close()
	w.client = nil
}

// Close underlying connections.
func (w *KafkaMigratorWriter) Close(ctx context.Context) error {
	w.disconnect()
	return nil
}
