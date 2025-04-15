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
	"errors"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func bytesFromStrField(name string, pConf *service.ParsedConfig) (uint64, error) {
	fieldAsStr, err := pConf.FieldString(name)
	if err != nil {
		return 0, err
	}

	fieldAsBytes, err := humanize.ParseBytes(fieldAsStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse %v bytes: %w", name, err)
	}
	return fieldAsBytes, nil
}

// BytesFromStrFieldAsInt32 attempts to parse string field containing a human-readable byte size
func BytesFromStrFieldAsInt32(name string, pConf *service.ParsedConfig) (int32, error) {
	ui64, err := bytesFromStrField(name, pConf)
	if err != nil {
		return 0, err
	}
	return int32(ui64), nil
}

const (
	// Consumer fields
	kfrFieldInstanceID             = "instance_id"
	kfrFieldRackID                 = "rack_id"
	kfrFieldTopics                 = "topics"
	kfrFieldRegexpTopics           = "regexp_topics"
	kfrFieldStartFromOldest        = "start_from_oldest"
	kfrFieldStartOffset            = "start_offset"
	kfrFieldFetchMaxBytes          = "fetch_max_bytes"
	kfrFieldFetchMinBytes          = "fetch_min_bytes"
	kfrFieldFetchMaxPartitionBytes = "fetch_max_partition_bytes"
	kfrFieldFetchMaxWait           = "fetch_max_wait"
	kfrFieldSessionTimeout         = "session_timeout"
	kfrFieldRebalanceTimeout       = "rebalance_timeout"
	kfrFieldHeartbeatInterval      = "heartbeat_interval"
	kfrFieldTransactionIsolation   = "transaction_isolation_level"
)

// TransactionIsolationLevel is a type that represents the transaction isolation level when reading from kafka.
type TransactionIsolationLevel string

const (
	// TransactionIsolationLevelReadUncommitted is a transaction isolation level that allows reading uncommitted records.
	TransactionIsolationLevelReadUncommitted TransactionIsolationLevel = "read_uncommitted"
	// TransactionIsolationLevelReadCommitted is a transaction isolation level that only allows reading committed records.
	TransactionIsolationLevelReadCommitted TransactionIsolationLevel = "read_committed"
)

// startOffsetType describes the offset to start consuming from, or if OffsetOutOfRange is seen while fetching,
// to restart consuming from.
type startOffsetType string

const (
	// startOffsetEarliest corresponds to auto.offset.reset "earliest"
	startOffsetEarliest startOffsetType = "earliest"
	// startOffsetLatest corresponds to auto.offset.reset "latest"
	startOffsetLatest startOffsetType = "latest"
	// startOffsetCommitted corresponds to auto.offset.reset "none"
	startOffsetCommitted startOffsetType = "committed"
)

const (
	// FranzConsumerFieldLintRules contains the lint rules for the consumer fields.
	FranzConsumerFieldLintRules = `
let has_topic_partitions = this.topics.any(t -> t.contains(":"))

root = [
  if $has_topic_partitions {
    if this.consumer_group.or("") != "" {
      "this input does not support both a consumer group and explicit topic partitions"
    } else if this.regexp_topics {
      "this input does not support both regular expression topics and explicit topic partitions"
    }
  } else {
    if this.consumer_group.or("") == "" {
      "a consumer group is mandatory when not using explicit topic partitions"
    }
  },
  # We don't have any way to distinguish between start_from_oldest set explicitly to true and not set at all, so we
  # assume users will be OK if start_offset overwrites it silently
  if this.start_from_oldest == false && this.start_offset == "earliest" {
    "start_from_oldest cannot be set to false when start_offset is set to earliest"
  }
]
`
)

// FranzConsumerFields returns a slice of fields specifically for customising
// consumer behaviour via the franz-go library.
func FranzConsumerFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField(kfrFieldTopics).
			Description(`
A list of topics to consume from. Multiple comma separated topics can be listed in a single element. When a ` + "`consumer_group`" + ` is specified partitions are automatically distributed across consumers of a topic, otherwise all partitions are consumed.

Alternatively, it's possible to specify explicit partitions to consume from with a colon after the topic name, e.g. ` + "`foo:0`" + ` would consume the partition 0 of the topic foo. This syntax supports ranges, e.g. ` + "`foo:0-10`" + ` would consume partitions 0 through to 10 inclusive.

Finally, it's also possible to specify an explicit offset to consume from by adding another colon after the partition, e.g. ` + "`foo:0:10`" + ` would consume the partition 0 of the topic foo starting from the offset 10. If the offset is not present (or remains unspecified) then the field ` + "`start_from_oldest`" + ` determines which offset to start from.`).
			Example([]string{"foo", "bar"}).
			Example([]string{"things.*"}).
			Example([]string{"foo,bar"}).
			Example([]string{"foo:0", "bar:1", "bar:3"}).
			Example([]string{"foo:0,bar:1,bar:3"}).
			Example([]string{"foo:0-5"}),
		service.NewBoolField(kfrFieldRegexpTopics).
			Description("Whether listed topics should be interpreted as regular expression patterns for matching multiple topics. When topics are specified with explicit partitions this field must remain set to `false`.").
			Default(false),
		service.NewStringField(kfrFieldRackID).
			Description("A rack specifies where the client is physically located and changes fetch requests to consume from the closest replica as opposed to the leader replica.").
			Default("").
			Advanced(),
		service.NewStringField(kfrFieldInstanceID).
			Description("When using a consumer group, an instance ID specifies the groups static membership, which can prevent rebalances during reconnects. When using a instance ID the client does NOT leave the group when closing. To actually leave the group one must use an external admin command to leave the group on behalf of this instance ID. This ID must be unique per consumer within the group.").
			Default("").
			Advanced(),
		service.NewDurationField(kfrFieldRebalanceTimeout).
			Description("When using a consumer group, `rebalance_timeout` sets how long group members are allowed to take when a rebalance has begun. This timeout is how long all members are allowed to complete work and commit offsets, minus the time it took to detect the rebalance (from a heartbeat).").
			Default("45s").
			Advanced(),
		service.NewDurationField(kfrFieldSessionTimeout).
			Description("When using a consumer group, `session_timeout` sets how long a member in hte group can go between heartbeats. If a member does not heartbeat in this timeout, the broker will remove the member from the group and initiate a rebalance.").
			Default("1m").
			Advanced(),
		service.NewDurationField(kfrFieldHeartbeatInterval).
			Description("When using a consumer group, `heartbeat_interval` sets how long a group member goes between heartbeats to Kafka. Kafka uses heartbeats to ensure that a group member's sesion stays active. This value should be no higher than 1/3rd of the `session_timeout`. This is equivalent to the Java heartbeat.interval.ms setting.").
			Default("3s").
			Advanced(),
		service.NewBoolField(kfrFieldStartFromOldest).
			Description("Determines whether to consume from the oldest available offset, otherwise messages are consumed from the latest offset. The setting is applied when creating a new consumer group or the saved offset no longer exists.").
			Default(true).
			Advanced().
			Deprecated(),
		service.NewStringAnnotatedEnumField(kfrFieldStartOffset, map[string]string{
			string(startOffsetEarliest):  "Start from the earliest offset. Corresponds to Kafka's `auto.offset.reset=earliest` option.",
			string(startOffsetLatest):    "Start from the latest offset. Corresponds to Kafka's `auto.offset.reset=latest` option.",
			string(startOffsetCommitted): "Prevents consuming a partition in a group if the partition has no prior commits. Corresponds to Kafka's `auto.offset.reset=none` option",
		}).Description("Sets the offset to start consuming from, or if OffsetOutOfRange is seen while fetching, to restart consuming from.").
			Default(string(startOffsetEarliest)).
			Advanced(),
		service.NewStringField(kfrFieldFetchMaxBytes).
			Description("Sets the maximum amount of bytes a broker will try to send during a fetch. Note that brokers may not obey this limit if it has records larger than this limit. This is the equivalent to the Java fetch.max.bytes setting.").
			Advanced().
			Default("50MiB"),
		service.NewDurationField(kfrFieldFetchMaxWait).
			Description("Sets the maximum amount of time a broker will wait for a fetch response to hit the minimum number of required bytes. This is the equivalent to the Java fetch.max.wait.ms setting.").
			Advanced().
			Default("5s"),
		service.NewStringField(kfrFieldFetchMinBytes).
			Description("Sets the minimum amount of bytes a broker will try to send during a fetch. This is the equivalent to the Java fetch.min.bytes setting.").
			Advanced().
			Default("1B"),
		service.NewStringField(kfrFieldFetchMaxPartitionBytes).
			Description("Sets the maximum amount of bytes that will be consumed for a single partition in a fetch request. Note that if a single batch is larger than this number, that batch will still be returned so the client can make progress. This is the equivalent to the Java fetch.max.partition.bytes setting.").
			Advanced().
			Default("1MiB"),
		service.NewStringAnnotatedEnumField(kfrFieldTransactionIsolation, map[string]string{
			string(TransactionIsolationLevelReadUncommitted): "If set, then uncommitted records are processed.",
			string(TransactionIsolationLevelReadCommitted):   "If set, only committed transactional records are processed.",
		}).
			Description("The transaction isolation level").
			Default(string(TransactionIsolationLevelReadUncommitted)),
	}
}

// FranzConsumerDetails describes information required to create a kafka
// consumer.
type FranzConsumerDetails struct {
	RackID                 string
	InstanceID             string
	IsolationLevel         kgo.IsolationLevel
	SessionTimeout         time.Duration
	RebalanceTimeout       time.Duration
	HeartbeatInterval      time.Duration
	StartOffset            kgo.Offset
	Topics                 []string
	TopicPartitions        map[string]map[int32]kgo.Offset
	RegexPattern           bool
	FetchMinBytes          int32
	FetchMaxBytes          int32
	FetchMaxPartitionBytes int32
	FetchMaxWait           time.Duration
}

// FranzConsumerDetailsFromConfig returns a summary of kafka consumer
// information, which can be used in order to create a consuming client.
func FranzConsumerDetailsFromConfig(conf *service.ParsedConfig) (*FranzConsumerDetails, error) {
	d := FranzConsumerDetails{}

	var err error
	if d.RackID, err = conf.FieldString(kfrFieldRackID); err != nil {
		return nil, err
	}
	if d.InstanceID, err = conf.FieldString(kfrFieldInstanceID); err != nil {
		return nil, err
	}
	if d.SessionTimeout, err = conf.FieldDuration(kfrFieldSessionTimeout); err != nil {
		return nil, err
	}
	if d.RebalanceTimeout, err = conf.FieldDuration(kfrFieldRebalanceTimeout); err != nil {
		return nil, err
	}
	if d.HeartbeatInterval, err = conf.FieldDuration(kfrFieldHeartbeatInterval); err != nil {
		return nil, err
	}
	if d.InstanceID, err = conf.FieldString(kfrFieldInstanceID); err != nil {
		return nil, err
	}
	isolationLevelStr, err := conf.FieldString(kfrFieldTransactionIsolation)
	if err != nil {
		return nil, err
	}
	isolationLevel := TransactionIsolationLevel(isolationLevelStr)
	switch isolationLevel {
	case TransactionIsolationLevelReadCommitted:
		d.IsolationLevel = kgo.ReadCommitted()
	case TransactionIsolationLevelReadUncommitted:
		d.IsolationLevel = kgo.ReadUncommitted()
	default:
		return nil, fmt.Errorf("invalid transaction isolation level: %v", isolationLevelStr)
	}

	startOffset, err := conf.FieldString(kfrFieldStartOffset)
	if err != nil {
		return nil, err
	}

	switch startOffsetType(startOffset) {
	case startOffsetEarliest:
		d.StartOffset = kgo.NewOffset().AtStart()
	case startOffsetLatest:
		d.StartOffset = kgo.NewOffset().AtEnd()
	case startOffsetCommitted:
		d.StartOffset = kgo.NewOffset().AtCommitted()
	default:
		return nil, fmt.Errorf("invalid start offset type: %s", startOffset)
	}

	startFromOldest, err := conf.FieldBool(kfrFieldStartFromOldest)
	if err != nil {
		return nil, err
	}

	if !startFromOldest && d.StartOffset == kgo.NewOffset().AtStart() {
		return nil, errors.New("start_from_oldest cannot be set to false when start_offset is set to earliest")
	}

	topicList, err := conf.FieldStringList(kfrFieldTopics)
	if err != nil {
		return nil, err
	}

	var topicPartitionsInts map[string]map[int32]int64
	if d.Topics, topicPartitionsInts, err = ParseTopics(topicList, d.StartOffset.EpochOffset().Offset, true); err != nil {
		return nil, err
	}

	if len(topicPartitionsInts) > 0 {
		d.TopicPartitions = map[string]map[int32]kgo.Offset{}
		for topic, partitions := range topicPartitionsInts {
			partMap := map[int32]kgo.Offset{}
			for part, offset := range partitions {
				partMap[part] = kgo.NewOffset().At(offset)
			}
			d.TopicPartitions[topic] = partMap
		}
	}

	if d.RegexPattern, err = conf.FieldBool(kfrFieldRegexpTopics); err != nil {
		return nil, err
	}

	if d.FetchMaxBytes, err = BytesFromStrFieldAsInt32(kfrFieldFetchMaxBytes, conf); err != nil {
		return nil, err
	}
	if d.FetchMinBytes, err = BytesFromStrFieldAsInt32(kfrFieldFetchMinBytes, conf); err != nil {
		return nil, err
	}
	if d.FetchMaxPartitionBytes, err = BytesFromStrFieldAsInt32(kfrFieldFetchMaxPartitionBytes, conf); err != nil {
		return nil, err
	}

	if d.FetchMaxWait, err = conf.FieldDuration(kfrFieldFetchMaxWait); err != nil {
		return nil, err
	}

	return &d, nil
}

// FranzOpts returns a slice of franz-go opts that establish a consumer
// described in the consumer details.
func (d *FranzConsumerDetails) FranzOpts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.Rack(d.RackID),
		kgo.ConsumeTopics(d.Topics...),
		kgo.ConsumePartitions(d.TopicPartitions),
		kgo.ConsumeResetOffset(d.StartOffset),
		kgo.FetchMaxBytes(d.FetchMaxBytes),
		kgo.FetchMinBytes(d.FetchMinBytes),
		kgo.FetchMaxPartitionBytes(d.FetchMaxPartitionBytes),
		kgo.FetchMaxWait(d.FetchMaxWait),
		kgo.SessionTimeout(d.SessionTimeout),
		kgo.RebalanceTimeout(d.RebalanceTimeout),
		kgo.HeartbeatInterval(d.HeartbeatInterval),
		kgo.FetchIsolationLevel(d.IsolationLevel),
	}

	if d.RegexPattern {
		opts = append(opts, kgo.ConsumeRegex())
	}

	if d.InstanceID != "" {
		opts = append(opts, kgo.InstanceID(d.InstanceID))
	}

	return opts
}

// FranzConsumerOptsFromConfig returns a slice of franz-go client opts from a
// parsed config.
func FranzConsumerOptsFromConfig(conf *service.ParsedConfig) ([]kgo.Opt, error) {
	details, err := FranzConsumerDetailsFromConfig(conf)
	if err != nil {
		return nil, err
	}
	return details.FranzOpts(), nil
}

// FranzRecordToMessageV0 converts a record into a service.Message, adding
// metadata and other relevant information.
func FranzRecordToMessageV0(record *kgo.Record, multiHeader bool) *service.Message {
	msg := service.NewMessage(record.Value)
	msg.MetaSetMut("kafka_key", string(record.Key))
	msg.MetaSetMut("kafka_topic", record.Topic)
	msg.MetaSetMut("kafka_partition", int(record.Partition))
	msg.MetaSetMut("kafka_offset", int(record.Offset))
	msg.MetaSetMut("kafka_timestamp_unix", record.Timestamp.Unix())
	msg.MetaSetMut("kafka_timestamp_ms", record.Timestamp.UnixMilli())
	msg.MetaSetMut("kafka_tombstone_message", record.Value == nil)
	if multiHeader {
		// in multi header mode we gather headers so we can encode them as lists
		headers := map[string][]any{}

		for _, hdr := range record.Headers {
			headers[hdr.Key] = append(headers[hdr.Key], string(hdr.Value))
		}

		for key, values := range headers {
			msg.MetaSetMut(key, values)
		}
	} else {
		for _, hdr := range record.Headers {
			msg.MetaSetMut(hdr.Key, string(hdr.Value))
		}
	}

	return msg
}

// FranzRecordToMessageV1 converts a record into a service.Message, adding
// metadata and other relevant information.
func FranzRecordToMessageV1(record *kgo.Record) *service.Message {
	msg := service.NewMessage(record.Value)
	msg.MetaSetMut("kafka_key", record.Key)
	msg.MetaSetMut("kafka_topic", record.Topic)
	msg.MetaSetMut("kafka_partition", int(record.Partition))
	msg.MetaSetMut("kafka_offset", int(record.Offset))
	msg.MetaSetMut("kafka_timestamp_unix", record.Timestamp.Unix())
	msg.MetaSetMut("kafka_timestamp_ms", record.Timestamp.UnixMilli())
	msg.MetaSetMut("kafka_tombstone_message", record.Value == nil)

	headers := map[string][]any{}

	for _, hdr := range record.Headers {
		headers[hdr.Key] = append(headers[hdr.Key], string(hdr.Value))
	}

	for key, values := range headers {
		if len(values) == 1 {
			msg.MetaSetMut(key, values[0])
		} else {
			msg.MetaSetMut(key, values)
		}
	}

	return msg
}
