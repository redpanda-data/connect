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
		return 0, fmt.Errorf("parsing %v bytes: %w", name, err)
	}
	return fieldAsBytes, nil
}

// BytesFromStrFieldAsInt32 attempts to parse string field containing a human-readable byte size.
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
	kfrFieldRegexpTopicsInclude    = "regexp_topics_include"
	kfrFieldRegexpTopicsExclude    = "regexp_topics_exclude"
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

// Descriptions of the start_from_oldest field, shared by the franz-go and
// Sarama based Kafka inputs.
const (
	startFromOldestDescription      = "Determines whether to consume from the oldest available offset, otherwise messages are consumed from the latest offset. The setting is applied when creating a new consumer group or the saved offset no longer exists."
	startFromOldestShortDescription = "Consume from the oldest available offset rather than the latest. Applied when the consumer group is new."
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
let has_topics = this.topics.length() > 0
let has_regexp_topics_include = this.regexp_topics_include.length() > 0 
let is_regex_mode = this.regexp_topics || $has_regexp_topics_include

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
  if !$has_topics && !$has_regexp_topics_include {
    "either topics or regexp_topics_include must be specified"
  },
  if $has_topics && $has_regexp_topics_include {
    "cannot specify both topics and regexp_topics_include, use one or the other"
  },
  if this.regexp_topics_exclude.length() > 0 && !$is_regex_mode {
    "regexp_topics_exclude can only be used when regexp_topics is set to true or regexp_topics_include is specified"
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
			Description(`A list of topics to consume from. You can list multiple comma-separated topics in a single element.

If you specify a ` + "`" + `consumer_group` + "`" + `, partitions are automatically distributed across consumers of a topic. Otherwise, all partitions are consumed.

Alternatively, add a colon after the topic name to set the explicit partitions to consume. For example, ` + "`" + `foo:0` + "`" + ` consumes the partition ` + "`" + `0` + "`" + ` of the topic ` + "`" + `foo` + "`" + `. This syntax also supports ranges. For example, ` + "`" + `foo:0-10` + "`" + ` consumes all partitions from ` + "`" + `0` + "`" + ` through to ` + "`" + `10` + "`" + ` inclusive.

Finally, add another colon after the partition to set an explicit offset to consume from. For example, ` + "`" + `foo:0:10` + "`" + ` consumes the partition ` + "`" + `0` + "`" + ` of the topic ` + "`" + `foo` + "`" + ` starting from the offset ` + "`" + `10` + "`" + `. If the offset is not present (or remains unspecified) then the field ` + "`" + `start_offset` + "`" + ` determines which offset to start from.`).
			ShortDescription("A list of topics to consume from. Multiple comma-separated topics may share one element.").
			Example([]string{"foo", "bar"}).
			Example([]string{"things.*"}).
			Example([]string{"foo,bar"}).
			Example([]string{"foo:0", "bar:1", "bar:3"}).
			Example([]string{"foo:0,bar:1,bar:3"}).
			Example([]string{"foo:0-5"}).
			Optional(),
		service.NewBoolField(kfrFieldRegexpTopics).
			Description("Whether listed topics should be interpreted as regular expression patterns for matching multiple topics. When enabled, the client periodically refreshes the list of matching topics based on the `metadata_max_age` interval. If topics are specified with explicit partitions, this field must remain set to `false`.\n\nDEPRECATED: This field is deprecated in favor of `regexp_topics_include` and `regexp_topics_exclude`, which provide more explicit control over topic matching. To migrate, replace `regexp_topics: true` with `regexp_topics_include` containing your topic patterns, and optionally add `regexp_topics_exclude` to filter out specific topics.").
			ShortDescription("Whether listed topics should be treated as regular expression patterns matching multiple topics.").
			Default(false).
			Deprecated(),
		service.NewStringListField(kfrFieldRegexpTopicsInclude).
			Description("A list of regular expression patterns for matching topics to consume from. When specified, the client will periodically refresh the list of matching topics based on the `metadata_max_age` interval.\n\nEach pattern is a full regular expression evaluated against the complete topic name. Patterns are not anchored by default, so `logs_.*` matches `my-logs_events` and `logs_errors`. Use `^logs_.*$` to match only topics starting with `logs_`.\n\nThis field enables regex mode (replacing the deprecated `regexp_topics` boolean) and cannot be used together with explicit `topics` lists. Use `regexp_topics_exclude` to filter out specific patterns from the matched topics.\n\nExample: `regexp_topics_include: [\"events_.*\", \"logs_.*\"]` consumes from all topics starting with `events_` or `logs_`.").
			ShortDescription("Regular expression patterns matching topics to consume from, refreshed periodically to discover new topics.").
			Example([]string{"logs_.*", "metrics_.*"}).
			Example([]string{"events_[0-9]+"}).
			Optional(),
		service.NewStringListField(kfrFieldRegexpTopicsExclude).
			Description("A list of regular expression patterns for excluding topics when regex mode is enabled (using `regexp_topics_include` or the deprecated `regexp_topics` boolean). Topics matching any of these patterns will be excluded from consumption, even if they match include patterns.\n\nEach pattern is a full regular expression evaluated against the complete topic name. Patterns are not anchored by default, so use `^` and `$` for exact matching. Exclude patterns are applied after include patterns, providing fine-grained control over topic selection.\n\nExample: `regexp_topics_exclude: [\"^_\", \".*-temp$\", \".*-test.*\"]` excludes topics starting with underscore, ending with `-temp`, or containing `-test`.").
			ShortDescription("Regular expression patterns for topics to exclude when regex mode is enabled.").
			Optional(),
		service.NewStringField(kfrFieldRackID).
			Description("A rack specifies where the client is physically located, and changes fetch requests to consume from the closest replica as opposed to the leader replica.").
			ShortDescription("Where the client is physically located, so fetches consume from the closest replica rather than the leader.").
			Default("").
			Advanced(),
		service.NewStringField(kfrFieldInstanceID).
			Description("When you specify a `consumer_group`, assign a unique value to `instance_id` to define the group's static membership, which can prevent unnecessary rebalances during reconnections.\n\nWhen you assign an instance ID, the client does not automatically leave the consumer group when it disconnects. To remove the client, you must use an external admin command on behalf of the instance ID.").
			ShortDescription("Static consumer group membership ID, which prevents rebalances on reconnect. The client does not leave the group on close.").
			Default("").
			Advanced(),
		service.NewDurationField(kfrFieldRebalanceTimeout).
			Description("When you specify a `consumer_group`, `rebalance_timeout` sets a time limit for all consumer group members to complete their work and commit offsets after a rebalance has begun. The timeout excludes the time taken to detect a failed or late heartbeat, which indicates a rebalance is required. This field accepts Go duration format strings such as `100ms`, `1s`, or `5s`.").
			ShortDescription("How long consumer group members may take to complete work and commit offsets during a rebalance.").
			Default("45s").
			Advanced(),
		service.NewDurationField(kfrFieldSessionTimeout).
			Description("When you specify a `consumer_group`, `session_timeout` sets the maximum interval between heartbeats sent by a consumer group member to the broker. If a broker doesn't receive a heartbeat from a group member before the timeout expires, it removes the member from the consumer group and initiates a rebalance. This field accepts Go duration format strings such as `100ms`, `1s`, or `5s`.").
			ShortDescription("How long a consumer group member may go between heartbeats before the broker removes it.").
			Default("1m").
			Advanced(),
		service.NewDurationField(kfrFieldHeartbeatInterval).
			Description("When you specify a `consumer_group`, `heartbeat_interval` sets how frequently a consumer group member should send heartbeats to Apache Kafka. Apache Kafka uses heartbeats to make sure that a group member's session is active.\n\nYou must set `heartbeat_interval` to less than one-third of `session_timeout`.\n\nThis field is equivalent to the Java `heartbeat.interval.ms` setting and accepts Go duration format strings such as `10s` or `2m`.").
			ShortDescription("How long a consumer group member waits between heartbeats to Kafka.").
			Default("3s").
			Advanced(),
		service.NewBoolField(kfrFieldStartFromOldest).
			Description(startFromOldestDescription).
			ShortDescription(startFromOldestShortDescription).
			Default(true).
			Advanced().
			Deprecated(),
		service.NewStringAnnotatedEnumField(kfrFieldStartOffset, map[string]string{
			string(startOffsetEarliest):  "Start from the earliest offset. Corresponds to Kafka's `auto.offset.reset=earliest` option.",
			string(startOffsetLatest):    "Start from the latest offset. Corresponds to Kafka's `auto.offset.reset=latest` option.",
			string(startOffsetCommitted): "Prevents consuming a partition in a group if the partition has no prior commits. Corresponds to Kafka's `auto.offset.reset=none` option",
		}).Description("Specify the offset from which this input starts or restarts consuming messages. Restarts occur when the `OffsetOutOfRange` error is seen during a fetch.").
			Default(string(startOffsetEarliest)).
			Advanced(),
		service.NewStringField(kfrFieldFetchMaxBytes).
			Description(`The maximum number of bytes that a broker tries to send during a fetch.

If individual records are larger than the ` + "`" + `fetch_max_bytes` + "`" + ` value, brokers still send them.

This field is equivalent to the Java setting ` + "`" + `fetch.max.bytes` + "`" + `.`).
			ShortDescription("Maximum bytes a broker will try to send during a fetch. Equivalent to the Java fetch.max.bytes setting.").
			Advanced().
			Default("50MiB"),
		service.NewDurationField(kfrFieldFetchMaxWait).
			Description("The maximum period of time a broker can wait for a fetch response to reach the required minimum number of bytes (`fetch_min_bytes`). This field is equivalent to the Java setting `fetch.max.wait.ms`.").
			ShortDescription("Maximum time a broker waits for a fetch to reach the minimum required bytes.").
			Advanced().
			Default("5s"),
		service.NewStringField(kfrFieldFetchMinBytes).
			Description("The minimum number of bytes that a broker tries to send during a fetch. This field is equivalent to the Java setting `fetch.min.bytes`.").
			ShortDescription("Minimum bytes a broker will try to send during a fetch. Equivalent to the Java fetch.min.bytes setting.").
			Advanced().
			Default("1B"),
		service.NewStringField(kfrFieldFetchMaxPartitionBytes).
			Description("The maximum number of bytes that are consumed from a single partition in a fetch request. This field is equivalent to the Java setting `fetch.max.partition.bytes`.\n\nIf a single batch is larger than the `fetch_max_partition_bytes` value, the batch is still sent so that the client can make progress.").
			ShortDescription("Maximum bytes consumed for a single partition in a fetch request.").
			Advanced().
			Default("1MiB"),
		service.NewStringAnnotatedEnumField(kfrFieldTransactionIsolation, map[string]string{
			string(TransactionIsolationLevelReadUncommitted): "If set, then uncommitted records are processed.",
			string(TransactionIsolationLevelReadCommitted):   "If set, only committed transactional records are processed.",
		}).
			Description("The isolation level for handling transactional messages. This setting determines how transactions are processed and affects data consistency guarantees.").
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
	ExcludeTopics          []string
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

	regexpTopics, err := conf.FieldBool(kfrFieldRegexpTopics)
	if err != nil {
		return nil, err
	}
	regexpIncludeTopics, err := conf.FieldStringList(kfrFieldRegexpTopicsInclude)
	if err != nil {
		return nil, err
	}
	d.RegexPattern = regexpTopics || len(regexpIncludeTopics) > 0

	// Update topic list based on regex mode
	if len(regexpIncludeTopics) != 0 {
		topicList = regexpIncludeTopics
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

	if d.ExcludeTopics, err = conf.FieldStringList(kfrFieldRegexpTopicsExclude); err != nil {
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
		if len(d.ExcludeTopics) > 0 {
			opts = append(opts, kgo.ConsumeExcludeTopics(d.ExcludeTopics...))
		}
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

	AddHeaders(msg, record.Headers)

	return msg
}
