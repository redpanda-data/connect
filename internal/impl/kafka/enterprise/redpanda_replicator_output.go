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
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
)

func franzKafkaOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("3.61.0").
		Summary("A Kafka output using the https://github.com/twmb/franz-go[Franz Kafka client library^].").
		Description(`
Writes a batch of messages to Kafka brokers and waits for acknowledgement before propagating it back to the input.

If the configured broker does not contain the current message `+"topic"+`, it attempts to create it along with the topic
ACLs which are read automatically from the `+"`redpanda_replicator`"+` input identified by the label specified in
`+"`topic_source`"+`.
`).
		Fields(FranzKafkaOutputConfigFields()...).
		LintRule(`
root = if this.partitioner == "manual" {
if this.partition.or("") == "" {
"a partition must be specified when the partitioner is set to manual"
}
} else if this.partition.or("") != "" {
"a partition cannot be specified unless the partitioner is set to manual"
}`).Example("Transfer data", "Writes messages to the configured broker and creates topics and topic ACLs if they don't exist. It also ensures that the message order is preserved.", `
output:
  redpanda_replicator:
    seed_brokers: [ "127.0.0.1:9093" ]
    topic: ${! metadata("kafka_topic").or(throw("missing kafka_topic metadata")) }
    key: ${! metadata("kafka_key") }
    partitioner: manual
    partition: ${! metadata("kafka_partition").or(throw("missing kafka_partition metadata")) }
    timestamp: ${! metadata("kafka_timestamp_unix").or(timestamp_unix()) }
    topic_source: redpanda_replicator_input
    max_in_flight: 1
`)
}

// FranzKafkaOutputConfigFields returns the full suite of config fields for a
// kafka output using the franz-go client library.
func FranzKafkaOutputConfigFields() []*service.ConfigField {
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
		service.NewStringField("topic_source").
			Description("The label of a redpanda_replicator input from which to read the list of topics which need to be created.").
			Default(rpriDefaultLabel).
			Advanced(),
	}
}

func init() {
	err := service.RegisterBatchOutput("redpanda_replicator", franzKafkaOutputConfig(),
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
			output, err = NewFranzKafkaWriterFromConfig(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// FranzKafkaWriter implements a kafka writer using the franz-go library.
type FranzKafkaWriter struct {
	SeedBrokers      []string
	topic            *service.InterpolatedString
	key              *service.InterpolatedString
	partition        *service.InterpolatedString
	timestamp        *service.InterpolatedString
	clientID         string
	rackID           string
	idempotentWrite  bool
	TLSConf          *tls.Config
	saslConfs        []sasl.Mechanism
	metaFilter       *service.MetadataFilter
	partitioner      kgo.Partitioner
	timeout          time.Duration
	produceMaxBytes  int32
	compressionPrefs []kgo.CompressionCodec
	topicSource      string

	client     *kgo.Client
	topicCache sync.Map

	mgr *service.Resources
}

// NewFranzKafkaWriterFromConfig attempts to instantiate a FranzKafkaWriter from
// a parsed config.
func NewFranzKafkaWriterFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*FranzKafkaWriter, error) {
	f := FranzKafkaWriter{
		mgr: mgr,
	}

	brokerList, err := conf.FieldStringList("seed_brokers")
	if err != nil {
		return nil, err
	}
	for _, b := range brokerList {
		f.SeedBrokers = append(f.SeedBrokers, strings.Split(b, ",")...)
	}

	if f.topic, err = conf.FieldInterpolatedString("topic"); err != nil {
		return nil, err
	}

	if conf.Contains("key") {
		if f.key, err = conf.FieldInterpolatedString("key"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("partition") {
		if rawStr, _ := conf.FieldString("partition"); rawStr != "" {
			if f.partition, err = conf.FieldInterpolatedString("partition"); err != nil {
				return nil, err
			}
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
		case "manual":
			f.partitioner = kgo.ManualPartitioner()
		default:
			return nil, fmt.Errorf("unknown partitioner: %v", partStr)
		}
	}

	if f.clientID, err = conf.FieldString("client_id"); err != nil {
		return nil, err
	}

	if f.rackID, err = conf.FieldString("rack_id"); err != nil {
		return nil, err
	}

	if f.idempotentWrite, err = conf.FieldBool("idempotent_write"); err != nil {
		return nil, err
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
		f.TLSConf = tlsConf
	}
	if f.saslConfs, err = kafka.SASLMechanismsFromConfig(conf); err != nil {
		return nil, err
	}

	if conf.Contains("timestamp") {
		if f.timestamp, err = conf.FieldInterpolatedString("timestamp"); err != nil {
			return nil, err
		}
	}

	if f.topicSource, err = conf.FieldString("topic_source"); err != nil {
		return nil, err
	}

	return &f, nil
}

//------------------------------------------------------------------------------

// Connect to the target seed brokers.
func (f *FranzKafkaWriter) Connect(ctx context.Context) error {
	if f.client != nil {
		return nil
	}

	clientOpts := []kgo.Opt{
		kgo.SeedBrokers(f.SeedBrokers...),
		kgo.SASL(f.saslConfs...),
		// TODO: Do we want to allow this option in some cases and make it configurable somehow?
		// kgo.AllowAutoTopicCreation(),
		kgo.ProducerBatchMaxBytes(f.produceMaxBytes),
		kgo.ProduceRequestTimeout(f.timeout),
		kgo.ClientID(f.clientID),
		kgo.Rack(f.rackID),
		kgo.WithLogger(&kafka.KGoLogger{L: f.mgr.Logger()}),
	}
	if f.TLSConf != nil {
		clientOpts = append(clientOpts, kgo.DialTLSConfig(f.TLSConf))
	}
	if f.partitioner != nil {
		clientOpts = append(clientOpts, kgo.RecordPartitioner(f.partitioner))
	}
	if !f.idempotentWrite {
		clientOpts = append(clientOpts, kgo.DisableIdempotentWrite())
	}
	if len(f.compressionPrefs) > 0 {
		clientOpts = append(clientOpts, kgo.ProducerBatchCompression(f.compressionPrefs...))
	}

	cl, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return err
	}

	f.client = cl

	return nil
}

func (f *FranzKafkaWriter) createTopicAndACLs(ctx context.Context, topic string, topicSourceReader *FranzKafkaReader) error {
	outputAdminClient := kadm.NewClient(f.client)

	outputTopicExists := false
	if topics, err := outputAdminClient.ListTopics(ctx, topic); err != nil {
		return fmt.Errorf("failed to fetch topic %q from output broker: %s", topic, err)
	} else {
		if topics.Has(topic) {
			outputTopicExists = true
		}
	}

	if !outputTopicExists {
		f.mgr.Logger().Infof("Creating topic %q", topic)

		inputAdminClient := kadm.NewClient(topicSourceReader.client)
		var inputTopic kadm.TopicDetail
		if topics, err := inputAdminClient.ListTopics(ctx, topic); err != nil {
			return fmt.Errorf("failed to fetch topic %q from source broker: %s", topic, err)
		} else {
			inputTopic = topics[topic]
		}

		partitions := int32(len(inputTopic.Partitions))
		if partitions == 0 {
			partitions = -1
		}
		replicationFactor := int16(inputTopic.Partitions.NumReplicas())
		if replicationFactor == 0 {
			replicationFactor = -1
		}

		if _, err := outputAdminClient.CreateTopic(ctx, partitions, replicationFactor, nil, topic); err != nil {
			if !errors.Is(err, kerr.TopicAlreadyExists) {
				return fmt.Errorf("failed to create topic %q: %s", topic, err)
			}
		}

		// Only topic ACLs are migrated, group ACLs are not migrated.
		// Users are not migrated because we can't read passwords.

		aclBuilder := kadm.NewACLs().Topics(topic).
			ResourcePatternType(kadm.ACLPatternLiteral).Operations().Allow().Deny().AllowHosts().DenyHosts()
		var inputACLResults kadm.DescribeACLsResults
		var err error
		if inputACLResults, err = inputAdminClient.DescribeACLs(ctx, aclBuilder); err != nil {
			return fmt.Errorf("failed to fetch ACLs for topic %q: %s", topic, err)
		}

		if len(inputACLResults) > 1 {
			return fmt.Errorf("received unexpected number of ACL results for topic %q: %d", topic, len(inputACLResults))
		}

		for _, acl := range inputACLResults[0].Described {
			builder := kadm.NewACLs()

			if acl.Permission == kmsg.ACLPermissionTypeAllow && acl.Operation == kmsg.ACLOperationWrite {
				// ALLOW WRITE ACLs for topics are not migrated.
				continue
			}

			op := acl.Operation
			if op == kmsg.ACLOperationAll {
				// ALLOW ALL ACLs for topics are downgraded to ALLOW READ.
				op = kmsg.ACLOperationRead
			}
			switch acl.Permission {
			case kmsg.ACLPermissionTypeAllow:
				builder = builder.Allow(acl.Principal).AllowHosts(acl.Host).Topics(acl.Name).ResourcePatternType(acl.Pattern).Operations(op)
			case kmsg.ACLPermissionTypeDeny:
				builder = builder.Deny(acl.Principal).DenyHosts(acl.Host).Topics(acl.Name).ResourcePatternType(acl.Pattern).Operations(op)
			}

			// Attempting to overwrite existing ACLs is idempotent and doesn't seem to raise an error.
			if _, err := outputAdminClient.CreateACLs(ctx, builder); err != nil {
				return fmt.Errorf("failed to create ACLs for topic %q: %s", topic, err)
			}
		}
	} else {
		f.mgr.Logger().Infof("Topic %q already exists", topic)
	}

	f.topicCache.Store(topic, struct{}{})

	return nil
}

// WriteBatch attempts to write a batch of messages to the target topics.
func (f *FranzKafkaWriter) WriteBatch(ctx context.Context, b service.MessageBatch) (err error) {
	if f.client == nil {
		return service.ErrNotConnected
	}

	records := make([]*kgo.Record, 0, len(b))
	for i, msg := range b {
		var topic string
		if topic, err = b.TryInterpolatedString(i, f.topic); err != nil {
			return fmt.Errorf("topic interpolation error: %w", err)
		}

		var topicSourceReader *FranzKafkaReader
		if res, ok := f.mgr.GetGeneric(f.topicSource); ok {
			topicSourceReader = res.(*FranzKafkaReader)
		} else {
			f.mgr.Logger().Debugf("Reader for topic source %q not found", f.topicSource)
		}

		if topicSourceReader != nil {
			if _, ok := f.topicCache.Load(topic); !ok {
				if err := f.createTopicAndACLs(ctx, topic, topicSourceReader); err != nil {
					return err
				}
			}
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
		if f.partition != nil {
			partStr, err := b.TryInterpolatedString(i, f.partition)
			if err != nil {
				return fmt.Errorf("partition interpolation error: %w", err)
			}
			partInt, err := strconv.Atoi(partStr)
			if err != nil {
				return fmt.Errorf("partition parse error: %w", err)
			}
			record.Partition = int32(partInt)
		}
		_ = f.metaFilter.Walk(msg, func(key, value string) error {
			record.Headers = append(record.Headers, kgo.RecordHeader{
				Key:   key,
				Value: []byte(value),
			})
			return nil
		})
		if f.timestamp != nil {
			if tsStr, err := b.TryInterpolatedString(i, f.timestamp); err != nil {
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
	err = f.client.ProduceSync(ctx, records...).FirstErr()
	return
}

func (f *FranzKafkaWriter) disconnect() {
	if f.client == nil {
		return
	}
	f.client.Close()
	f.client = nil
}

// Close underlying connections.
func (f *FranzKafkaWriter) Close(ctx context.Context) error {
	f.disconnect()
	return nil
}
