// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/dispatch"
)

const (
	// Producer fields
	kfwFieldPartitioner         = "partitioner"
	kfwFieldIdempotentWrite     = "idempotent_write"
	kfwFieldCompression         = "compression"
	kfwFieldTimeout             = "timeout"
	kfwFieldMaxMessageBytes     = "max_message_bytes"
	kfwFieldBrokerWriteMaxBytes = "broker_write_max_bytes"
)

// FranzProducerFields returns a slice of fields specifically for customising
// producer behaviour via the franz-go library.
func FranzProducerFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringAnnotatedEnumField(kfwFieldPartitioner, map[string]string{
			"murmur2_hash": "Kafka's default hash algorithm that uses a 32-bit murmur2 hash of the key to compute which partition the record will be on.",
			"round_robin":  "Round-robin's messages through all available partitions. This algorithm has lower throughput and causes higher CPU load on brokers, but can be useful if you want to ensure an even distribution of records to partitions.",
			"least_backup": "Chooses the least backed up partition (the partition with the fewest amount of buffered records). Partitions are selected per batch.",
			"manual":       "Manually select a partition for each message, requires the field `partition` to be specified.",
		}).
			Description("Override the default murmur2 hashing partitioner.").
			Advanced().Optional(),
		service.NewBoolField(kfwFieldIdempotentWrite).
			Description("Enable the idempotent write producer option. This requires the `IDEMPOTENT_WRITE` permission on `CLUSTER` and can be disabled if this permission is not available.").
			Default(true).
			Advanced(),
		service.NewStringEnumField(kfwFieldCompression, "lz4", "snappy", "gzip", "none", "zstd").
			Description("Optionally set an explicit compression type. The default preference is to use snappy when the broker supports it, and fall back to none if not.").
			Optional().
			Advanced(),
		service.NewDurationField(kfwFieldTimeout).
			Description("The maximum period of time to wait for message sends before abandoning the request and retrying").
			Default("10s").
			Advanced(),
		service.NewStringField(kfwFieldMaxMessageBytes).
			Description("The maximum space in bytes than an individual message may take, messages larger than this value will be rejected. This field corresponds to Kafka's `max.message.bytes`.").
			Advanced().
			Default("1MB").
			Example("100MB").
			Example("50mib"),
		service.NewStringField(kfwFieldBrokerWriteMaxBytes).
			Description("The upper bound for the number of bytes written to a broker connection in a single write. This field corresponds to Kafka's `socket.request.max.bytes`.").
			Advanced().
			Default("100MB").
			Example("128MB").
			Example("50mib"),
	}
}

// FranzProducerOptsFromConfig returns a slice of franz-go client opts from a
// parsed config.
func FranzProducerOptsFromConfig(conf *service.ParsedConfig) ([]kgo.Opt, error) {
	var opts []kgo.Opt

	maxMessageBytesStr, err := conf.FieldString(kfwFieldMaxMessageBytes)
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
	opts = append(opts, kgo.ProducerBatchMaxBytes(int32(maxMessageBytes)))

	brokerWriteMaxBytesStr, err := conf.FieldString(kfwFieldBrokerWriteMaxBytes)
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
	opts = append(opts, kgo.BrokerMaxWriteBytes(int32(brokerWriteMaxBytes)))

	var compressionPrefs []kgo.CompressionCodec
	if conf.Contains(kfwFieldCompression) {
		cStr, err := conf.FieldString(kfwFieldCompression)
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
		compressionPrefs = append(compressionPrefs, c)
	}
	if len(compressionPrefs) > 0 {
		opts = append(opts, kgo.ProducerBatchCompression(compressionPrefs...))
	}

	partitioner := kgo.StickyKeyPartitioner(nil)
	if conf.Contains(kfwFieldPartitioner) {
		partStr, err := conf.FieldString(kfwFieldPartitioner)
		if err != nil {
			return nil, err
		}
		switch partStr {
		case "murmur2_hash":
			partitioner = kgo.StickyKeyPartitioner(nil)
		case "round_robin":
			partitioner = kgo.RoundRobinPartitioner()
		case "least_backup":
			partitioner = kgo.LeastBackupPartitioner()
		case "manual":
			partitioner = kgo.ManualPartitioner()
		default:
			return nil, fmt.Errorf("unknown partitioner: %v", partStr)
		}
	}
	if partitioner != nil {
		opts = append(opts, kgo.RecordPartitioner(partitioner))
	}

	idempotentWrite, err := conf.FieldBool(kfwFieldIdempotentWrite)
	if err != nil {
		return nil, err
	}
	if !idempotentWrite {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	timeout, err := conf.FieldDuration(kfwFieldTimeout)
	if err != nil {
		return nil, err
	}
	opts = append(opts, kgo.ProduceRequestTimeout(timeout))

	return opts, nil
}

//------------------------------------------------------------------------------

const (
	kfwFieldTopic     = "topic"
	kfwFieldKey       = "key"
	kfwFieldPartition = "partition"
	kfwFieldMetadata  = "metadata"
	kfwFieldTimestamp = "timestamp"
)

// FranzWriterConfigFields returns a slice of config fields specifically for
// customising data written to a Kafka broker.
func FranzWriterConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewInterpolatedStringField(kfwFieldTopic).
			Description("A topic to write messages to."),
		service.NewInterpolatedStringField(kfwFieldKey).
			Description("An optional key to populate for each message.").Optional(),
		service.NewInterpolatedStringField(kfwFieldPartition).
			Description("An optional explicit partition to set for each message. This field is only relevant when the `partitioner` is set to `manual`. The provided interpolation string must be a valid integer.").
			Example(`${! meta("partition") }`).
			Optional(),
		service.NewMetadataFilterField(kfwFieldMetadata).
			Description("Determine which (if any) metadata values should be added to messages as headers.").
			Optional(),
		service.NewInterpolatedStringField(kfwFieldTimestamp).
			Description("An optional timestamp to set for each message. When left empty, the current timestamp is used.").
			Example(`${! timestamp_unix() }`).
			Example(`${! metadata("kafka_timestamp_unix") }`).
			Optional().
			Advanced(),
	}
}

// FranzWriter implements a Kafka writer using the franz-go library.
type FranzWriter struct {
	Topic      *service.InterpolatedString
	Key        *service.InterpolatedString
	Partition  *service.InterpolatedString
	Timestamp  *service.InterpolatedString
	MetaFilter *service.MetadataFilter

	accessClientFn func(FranzSharedClientUseFn) error
	yieldClientFn  func(context.Context) error
}

// NewFranzWriterFromConfig uses a parsed config to extract customisation for
// writing data to a Kafka broker. A closure function must be provided that is
// responsible for granting access to a connected client.
func NewFranzWriterFromConfig(conf *service.ParsedConfig, accessClientFn func(FranzSharedClientUseFn) error, yieldClientFn func(context.Context) error) (*FranzWriter, error) {
	w := FranzWriter{
		accessClientFn: accessClientFn,
		yieldClientFn:  yieldClientFn,
	}

	var err error
	if w.Topic, err = conf.FieldInterpolatedString(kfwFieldTopic); err != nil {
		return nil, err
	}

	if conf.Contains(kfwFieldKey) {
		if w.Key, err = conf.FieldInterpolatedString(kfwFieldKey); err != nil {
			return nil, err
		}
	}

	if rawStr, _ := conf.FieldString(kfwFieldPartition); rawStr != "" {
		if w.Partition, err = conf.FieldInterpolatedString(kfwFieldPartition); err != nil {
			return nil, err
		}
	}

	if conf.Contains(kfwFieldMetadata) {
		if w.MetaFilter, err = conf.FieldMetadataFilter(kfwFieldMetadata); err != nil {
			return nil, err
		}
	}

	if conf.Contains(kfwFieldTimestamp) {
		if w.Timestamp, err = conf.FieldInterpolatedString(kfwFieldTimestamp); err != nil {
			return nil, err
		}
	}
	return &w, nil
}

//------------------------------------------------------------------------------

// BatchToRecords converts a batch of messages into a slice of records ready to
// send via the franz-go library.
func (w *FranzWriter) BatchToRecords(ctx context.Context, b service.MessageBatch) ([]*kgo.Record, error) {
	topicExecutor := b.InterpolationExecutor(w.Topic)
	var keyExecutor *service.MessageBatchInterpolationExecutor
	if w.Key != nil {
		keyExecutor = b.InterpolationExecutor(w.Key)
	}
	var partitionExecutor *service.MessageBatchInterpolationExecutor
	if w.Partition != nil {
		partitionExecutor = b.InterpolationExecutor(w.Partition)
	}
	var timestampExecutor *service.MessageBatchInterpolationExecutor
	if w.Timestamp != nil {
		timestampExecutor = b.InterpolationExecutor(w.Timestamp)
	}

	records := make([]*kgo.Record, 0, len(b))
	for i, msg := range b {
		topic, err := topicExecutor.TryString(i)
		if err != nil {
			return nil, fmt.Errorf("topic interpolation error: %w", err)
		}

		record := &kgo.Record{Topic: topic}
		if record.Value, err = msg.AsBytes(); err != nil {
			return nil, err
		}
		if keyExecutor != nil {
			if record.Key, err = keyExecutor.TryBytes(i); err != nil {
				return nil, fmt.Errorf("key interpolation error: %w", err)
			}
		}
		if partitionExecutor != nil {
			partStr, err := partitionExecutor.TryString(i)
			if err != nil {
				return nil, fmt.Errorf("partition interpolation error: %w", err)
			}
			partInt, err := strconv.Atoi(partStr)
			if err != nil {
				return nil, fmt.Errorf("partition parse error: %w", err)
			}
			record.Partition = int32(partInt)
		}
		_ = w.MetaFilter.Walk(msg, func(key, value string) error {
			record.Headers = append(record.Headers, kgo.RecordHeader{
				Key:   key,
				Value: []byte(value),
			})
			return nil
		})
		if timestampExecutor != nil {
			if tsStr, err := timestampExecutor.TryString(i); err != nil {
				return nil, fmt.Errorf("timestamp interpolation error: %w", err)
			} else {
				if ts, err := strconv.ParseInt(tsStr, 10, 64); err != nil {
					return nil, fmt.Errorf("failed to parse timestamp: %w", err)
				} else {
					record.Timestamp = time.Unix(ts, 0)
				}
			}
		}
		records = append(records, record)
	}

	return records, nil
}

// Connect to the target seed brokers.
func (w *FranzWriter) Connect(ctx context.Context) error {
	return w.accessClientFn(func(details *FranzSharedClientInfo) error {
		// Check connectivity to cluster
		if err := details.Client.Ping(ctx); err != nil {
			return fmt.Errorf("failed to connect to cluster: %s", err)
		}
		return nil
	})
}

// WriteBatch attempts to write a batch of messages to the target topics.
func (w *FranzWriter) WriteBatch(ctx context.Context, b service.MessageBatch) error {
	if len(b) == 0 {
		return nil
	}
	return w.accessClientFn(func(details *FranzSharedClientInfo) error {
		records, err := w.BatchToRecords(ctx, b)
		if err != nil {
			return err
		}

		var (
			wg      sync.WaitGroup
			results = make(kgo.ProduceResults, 0, len(records))
			promise = func(r *kgo.Record, err error) {
				results = append(results, kgo.ProduceResult{Record: r, Err: err})
				wg.Done()
			}
		)

		wg.Add(len(records))
		for i, r := range records {
			details.Client.Produce(ctx, r, promise)
			dispatch.TriggerSignal(b[i].Context())
		}
		wg.Wait()

		// TODO: This is very cool and allows us to easily return granular errors,
		// so we should honor travis by doing it.
		return results.FirstErr()
	})
}

// Close calls into the provided yield client func.
func (w *FranzWriter) Close(ctx context.Context) error {
	return w.yieldClientFn(ctx)
}
