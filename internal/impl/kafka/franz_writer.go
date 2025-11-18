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
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/dispatch"
)

const (
	// Producer fields
	kfwFieldPartitioner            = "partitioner"
	kfwFieldIdempotentWrite        = "idempotent_write"
	kfwFieldCompression            = "compression"
	kfwFieldAllowAutoTopicCreation = "allow_auto_topic_creation"
	kfwFieldTimeout                = "timeout"
	kfwFieldMaxMessageBytes        = "max_message_bytes"
	kfwFieldBrokerWriteMaxBytes    = "broker_write_max_bytes"
)

// FranzProducerLimitsFields returns a slice of fields specifically for
// customising producer limits via the franz-go library.
func FranzProducerLimitsFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewDurationField(kfwFieldTimeout).
			Description("The maximum period of time to wait for message sends before abandoning the request and retrying").
			Default("10s").
			Advanced(),
		service.NewStringField(kfwFieldMaxMessageBytes).
			Description("The maximum size of a produced record batch in bytes. " +
				"A `MESSAGE_TOO_LARGE` error is returned if a batch exceeds this limit. " +
				"This field maps to the `max.message.bytes` Kafka property. " +
				"Ensure the Redpanda broker's `kafka_batch_max_bytes` property is at least as large as this value, " +
				"see https://docs.redpanda.com/current/reference/properties/cluster-properties/#kafka_batch_max_bytes.").
			Advanced().
			Default("1MiB").
			Example("100MB").
			Example("50mib"),
		service.NewStringField(kfwFieldBrokerWriteMaxBytes).
			Description("The upper bound for the number of bytes written to a broker connection in a single write. This field corresponds to Kafka's `socket.request.max.bytes`.").
			Advanced().
			Default("100MiB").
			Example("128MB").
			Example("50mib"),
	}
}

// FranzProducerFields returns a slice of fields specifically for customising
// producer behaviour via the franz-go library.
func FranzProducerFields() []*service.ConfigField {
	return slices.Concat(
		[]*service.ConfigField{
			service.NewStringAnnotatedEnumField(kfwFieldPartitioner, map[string]string{
				"murmur2_hash": "Kafka's default hash algorithm that uses a 32-bit murmur2 hash of the key to compute which partition the record will be on.",
				"round_robin":  "Round-robin's messages through all available partitions. This algorithm has lower throughput and causes higher CPU load on brokers, but can be useful if you want to ensure an even distribution of records to partitions.",
				"least_backup": "Chooses the least backed up partition (the partition with the fewest amount of buffered records). Partitions are selected per batch.",
				"manual":       "Manually select a partition for each message, requires the field `partition` to be specified.",
			}).
				Description("Override the default murmur2 hashing partitioner.").
				Advanced().Optional(),
			service.NewBoolField(kfwFieldIdempotentWrite).
				Description("Enable the idempotent write producer option. " +
					"When enabled, the producer initializes a producer ID and uses it to guarantee exactly-once semantics per partition (no duplicates on retries). " +
					"This requires the `IDEMPOTENT_WRITE` permission on the `CLUSTER` resource. " +
					"If your cluster does not grant this permission or uses ACLs restrictively, disable this option. " +
					"Note: Idempotent writes are strictly a win for data integrity but may be unavailable in restricted environments " +
					"(e.g., some managed Kafka services, Redpanda with strict ACLs). " +
					"Disabling this option is safe and only affects retry behavior—duplicates may occur on producer retries, but the pipeline will continue to function normally.").
				Default(true).
				Advanced(),
			service.NewStringEnumField(kfwFieldCompression, "lz4", "snappy", "gzip", "none", "zstd").
				Description("Optionally set an explicit compression type. The default preference is to use snappy when the broker supports it, and fall back to none if not.").
				Optional().
				Advanced(),
			service.NewBoolField(kfwFieldAllowAutoTopicCreation).
				Description("Enables topics to be auto created if they do not exist when fetching their metadata.").
				Default(true).
				Advanced(),
		},
		FranzProducerLimitsFields(),
	)
}

// FranzProducerLimitsOptsFromConfig returns a slice of franz-go client opts for
// customising producer limits from a parsed config.
func FranzProducerLimitsOptsFromConfig(conf *service.ParsedConfig) ([]kgo.Opt, error) {
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

	timeout, err := conf.FieldDuration(kfwFieldTimeout)
	if err != nil {
		return nil, err
	}
	opts = append(opts, kgo.ProduceRequestTimeout(timeout))

	return opts, nil
}

// FranzProducerOptsFromConfig returns a slice of franz-go client opts from a
// parsed config.
func FranzProducerOptsFromConfig(conf *service.ParsedConfig) ([]kgo.Opt, error) {
	var opts []kgo.Opt
	var err error
	if opts, err = FranzProducerLimitsOptsFromConfig(conf); err != nil {
		return nil, err
	}

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

	allowAutoTopicCreation, err := conf.FieldBool(kfwFieldAllowAutoTopicCreation)
	if err != nil {
		return nil, err
	}

	if allowAutoTopicCreation {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	return opts, nil
}

//------------------------------------------------------------------------------

const (
	kfwFieldTopic       = "topic"
	kfwFieldKey         = "key"
	kfwFieldPartition   = "partition"
	kfwFieldMetadata    = "metadata"
	kfwFieldTimestamp   = "timestamp"
	kfwFieldTimestampMs = "timestamp_ms"
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
			Advanced().
			Deprecated(),
		service.NewInterpolatedStringField(kfwFieldTimestampMs).
			Description("An optional timestamp to set for each message expressed in milliseconds. When left empty, the current timestamp is used.").
			Example(`${! timestamp_unix_milli() }`).
			Example(`${! metadata("kafka_timestamp_ms") }`).
			Optional().
			Advanced(),
	}
}

// FranzWriterConfigLints returns the linter rules for a the writer config.
func FranzWriterConfigLints() string {
	return `root = match {
  this.partitioner == "manual" && this.partition.or("") == "" => "a partition must be specified when the partitioner is set to manual"
  this.partitioner != "manual" && this.partition.or("") != "" => "a partition cannot be specified unless the partitioner is set to manual"
  this.timestamp.or("") != "" && this.timestamp_ms.or("") != "" => "both timestamp and timestamp_ms cannot be specified simultaneously"
}`
}

type franzWriterHooks struct {
	accessClientFn func(context.Context, FranzSharedClientUseFn) error
	yieldClientFn  func(context.Context) error
}

// NewFranzWriterHooks creates a new franzWriterHooks instance with a hook function that's executed to fetch the client.
func NewFranzWriterHooks(fn func(context.Context, FranzSharedClientUseFn) error) franzWriterHooks {
	return franzWriterHooks{accessClientFn: fn}
}

// WithYieldClientFn adds a hook function that's executed during close to yield the client.
func (h franzWriterHooks) WithYieldClientFn(fn func(context.Context) error) franzWriterHooks {
	h.yieldClientFn = fn
	return h
}

// FranzWriter implements a Kafka writer using the franz-go library.
type FranzWriter struct {
	Topic         *service.InterpolatedString
	Key           *service.InterpolatedString
	Partition     *service.InterpolatedString
	Timestamp     *service.InterpolatedString
	IsTimestampMs bool
	MetaFilter    *service.MetadataFilter
	hooks         franzWriterHooks

	// MessageBatchToFranzRecords is a custom batch record constructor for
	// specialized cases like migrator.
	//
	// Contract:
	// - Must return exactly one record per input message (same slice length)
	// - Use SkipRecord sentinel value for messages that should not be written
	// - Returned records are validated for count match before processing
	//
	// When nil, the default messageBatchToFranzRecords implementation is used.
	MessageBatchToFranzRecords func(batch service.MessageBatch) ([]kgo.Record, error)

	// DecorateRecord is executed for each record before it is written to the
	// broker.
	//
	// DEPRECATED: Use [MessageBatchToFranzRecords] instead.
	DecorateRecord func(r *kgo.Record) error
}

// NewFranzWriterFromConfig uses a parsed config to extract customisation for writing data to a Kafka broker. A closure
// function must be provided that is responsible for granting access to a connected client.
func NewFranzWriterFromConfig(conf *service.ParsedConfig, hooks franzWriterHooks) (*FranzWriter, error) {
	w := FranzWriter{
		hooks: hooks,
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

	if conf.Contains(kfwFieldTimestamp) && conf.Contains(kfwFieldTimestampMs) {
		return nil, errors.New("cannot specify both timestamp and timestamp_ms fields")
	}

	if conf.Contains(kfwFieldTimestamp) {
		if w.Timestamp, err = conf.FieldInterpolatedString(kfwFieldTimestamp); err != nil {
			return nil, err
		}
	}

	if conf.Contains(kfwFieldTimestampMs) {
		if w.Timestamp, err = conf.FieldInterpolatedString(kfwFieldTimestampMs); err != nil {
			return nil, err
		}
		w.IsTimestampMs = true
	}

	return &w, nil
}

//------------------------------------------------------------------------------

// SkipRecord is a sentinel value that can be returned by custom
// MessageBatchToFranzRecords implementations to indicate a record should be
// skipped and not written to Kafka.
var SkipRecord = kgo.Record{}

// messageBatchToFranzRecords is the default implementation that converts
// messages to records using configured interpolation and metadata filters.
func (w *FranzWriter) messageBatchToFranzRecords(batch service.MessageBatch) ([]kgo.Record, error) {
	records := make([]kgo.Record, 0, len(batch))

	for _, msg := range batch {
		r := kgo.Record{
			Context: msg.Context(),
		}

		var err error

		// Required: Value
		r.Value, err = msg.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("message to bytes: %w", err)
		}

		// Required: Topic
		r.Topic, err = w.Topic.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("topic interpolation: %w", err)
		}

		// Optional: Key
		if w.Key != nil {
			r.Key, err = w.Key.TryBytes(msg)
			if err != nil {
				return nil, fmt.Errorf("key interpolation: %w", err)
			}
		}

		// Optional: Headers
		if w.MetaFilter != nil {
			_ = w.MetaFilter.Walk(msg, func(key, value string) error {
				r.Headers = append(r.Headers, kgo.RecordHeader{
					Key:   key,
					Value: []byte(value),
				})
				return nil
			})
		}

		// Optional: Timestamp
		if w.Timestamp != nil {
			tsStr, err := w.Timestamp.TryString(msg)
			if err != nil {
				return nil, fmt.Errorf("timestamp interpolation: %w", err)
			}

			ts, err := strconv.ParseInt(tsStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse timestamp: %w", err)
			}

			if w.IsTimestampMs {
				r.Timestamp = time.UnixMilli(ts)
			} else {
				r.Timestamp = time.Unix(ts, 0)
			}
		}

		// Optional: Partition
		if w.Partition != nil {
			partStr, err := w.Partition.TryString(msg)
			if err != nil {
				return nil, fmt.Errorf("partition interpolation: %w", err)
			}
			partInt, err := strconv.ParseInt(partStr, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("parse partition: %w", err)
			}
			r.Partition = int32(partInt)
		}

		records = append(records, r)
	}

	return records, nil
}

// Connect to the target seed brokers.
func (w *FranzWriter) Connect(ctx context.Context) error {
	return w.hooks.accessClientFn(ctx, func(_ *FranzSharedClientInfo) error {
		// Simply accessing the client is enough to establish that it is
		// successfully connected.
		return nil
	})
}

// WriteBatch attempts to write a batch of messages to the target topics.
func (w *FranzWriter) WriteBatch(ctx context.Context, b service.MessageBatch) error {
	if len(b) == 0 {
		return nil
	}
	return w.hooks.accessClientFn(ctx, w.newBatchWriter(ctx, b).writeBatch)
}

// batchWriter handles concurrent writes of a message batch to Kafka.
type batchWriter struct {
	*FranzWriter
	ctx   context.Context
	batch service.MessageBatch

	wg  sync.WaitGroup
	err atomic.Value
}

func (w *FranzWriter) newBatchWriter(ctx context.Context, batch service.MessageBatch) *batchWriter {
	return &batchWriter{
		FranzWriter: w,
		ctx:         ctx,
		batch:       batch,
	}
}

func (w *batchWriter) writeBatch(details *FranzSharedClientInfo) error {
	conv := w.MessageBatchToFranzRecords
	if conv == nil {
		conv = w.messageBatchToFranzRecords
	}
	records, err := conv(w.batch)
	if err != nil {
		return fmt.Errorf("failed to create records: %w", err)
	}
	if len(records) != len(w.batch) {
		return fmt.Errorf("record count mismatch: got %d records for %d messages", len(records), len(w.batch))
	}
	for i := range records {
		r := &records[i]

		// Skip records that match the SkipRecord sentinel
		if r.Topic == "" && r.Value == nil && r.Key == nil {
			dispatch.TriggerSignal(w.batch[i].Context())
			continue
		}

		if r.Context == nil {
			r.Context = w.ctx
		}
		if w.DecorateRecord != nil {
			if err := w.DecorateRecord(r); err != nil {
				return fmt.Errorf("decorate record: %w", err)
			}
		}

		w.wg.Add(1)
		details.Client.Produce(w.ctx, r, w.onRecordProduced)
	}
	return w.wait()
}

func (w *batchWriter) onRecordProduced(r *kgo.Record, err error) {
	// Note: the order of these operations is important, first set the error if
	// there is one, then signal completion.
	if err != nil {
		w.err.CompareAndSwap(nil, err)
	}
	w.wg.Done()

	if r.Context != nil {
		dispatch.TriggerSignal(r.Context)
	}
}

func (w *batchWriter) wait() error {
	w.wg.Wait()

	if err := w.err.Load(); err != nil {
		return err.(error)
	}

	return nil
}

// Close calls into the provided yield client func.
func (w *FranzWriter) Close(ctx context.Context) error {
	if w.hooks.yieldClientFn != nil {
		return w.hooks.yieldClientFn(ctx)
	}

	return nil
}
