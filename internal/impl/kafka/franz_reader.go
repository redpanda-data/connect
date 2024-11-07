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

func bytesFromStrFieldAsInt32(name string, pConf *service.ParsedConfig) (int32, error) {
	ui64, err := bytesFromStrField(name, pConf)
	if err != nil {
		return 0, err
	}
	return int32(ui64), nil
}

const (
	// Consumer fields
	kfrFieldRackID                 = "rack_id"
	kfrFieldTopics                 = "topics"
	kfrFieldRegexpTopics           = "regexp_topics"
	kfrFieldStartFromOldest        = "start_from_oldest"
	kfrFieldFetchMaxBytes          = "fetch_max_bytes"
	kfrFieldFetchMinBytes          = "fetch_min_bytes"
	kfrFieldFetchMaxPartitionBytes = "fetch_max_partition_bytes"
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
		service.NewBoolField(kfrFieldStartFromOldest).
			Description("Determines whether to consume from the oldest available offset, otherwise messages are consumed from the latest offset. The setting is applied when creating a new consumer group or the saved offset no longer exists.").
			Default(true).
			Advanced(),
		service.NewStringField(kfrFieldFetchMaxBytes).
			Description("Sets the maximum amount of bytes a broker will try to send during a fetch. Note that brokers may not obey this limit if it has records larger than this limit. This is the equivalent to the Java fetch.max.bytes setting.").
			Advanced().
			Default("50MiB"),
		service.NewStringField(kfrFieldFetchMinBytes).
			Description("Sets the minimum amount of bytes a broker will try to send during a fetch. This is the equivalent to the Java fetch.min.bytes setting.").
			Advanced().
			Default("1B"),
		service.NewStringField(kfrFieldFetchMaxPartitionBytes).
			Description("Sets the maximum amount of bytes that will be consumed for a single partition in a fetch request. Note that if a single batch is larger than this number, that batch will still be returned so the client can make progress. This is the equivalent to the Java fetch.max.partition.bytes setting.").
			Advanced().
			Default("1MiB"),
	}
}

// FranzConsumerDetails describes information required to create a kafka
// consumer.
type FranzConsumerDetails struct {
	RackID                 string
	InitialOffset          kgo.Offset
	Topics                 []string
	TopicPartitions        map[string]map[int32]kgo.Offset
	RegexPattern           bool
	FetchMinBytes          int32
	FetchMaxBytes          int32
	FetchMaxPartitionBytes int32
}

// FranzConsumerDetailsFromConfig returns a summary of kafka consumer
// information, which can be used in order to create a consuming client.
func FranzConsumerDetailsFromConfig(conf *service.ParsedConfig) (*FranzConsumerDetails, error) {
	d := FranzConsumerDetails{}

	var err error
	if d.RackID, err = conf.FieldString(kfrFieldRackID); err != nil {
		return nil, err
	}

	startFromOldest, err := conf.FieldBool(kfrFieldStartFromOldest)
	if err != nil {
		return nil, err
	}
	var defaultOffset int64 = -1
	if startFromOldest {
		defaultOffset = -2
	}

	if startFromOldest {
		d.InitialOffset = kgo.NewOffset().AtStart()
	} else {
		d.InitialOffset = kgo.NewOffset().AtEnd()
	}

	topicList, err := conf.FieldStringList(kfrFieldTopics)
	if err != nil {
		return nil, err
	}

	var topicPartitionsInts map[string]map[int32]int64
	if d.Topics, topicPartitionsInts, err = ParseTopics(topicList, defaultOffset, true); err != nil {
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

	if d.FetchMaxBytes, err = bytesFromStrFieldAsInt32(kfrFieldFetchMaxBytes, conf); err != nil {
		return nil, err
	}
	if d.FetchMinBytes, err = bytesFromStrFieldAsInt32(kfrFieldFetchMinBytes, conf); err != nil {
		return nil, err
	}
	if d.FetchMaxPartitionBytes, err = bytesFromStrFieldAsInt32(kfrFieldFetchMaxPartitionBytes, conf); err != nil {
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
		kgo.ConsumeResetOffset(d.InitialOffset),
		kgo.FetchMaxBytes(d.FetchMaxBytes),
		kgo.FetchMinBytes(d.FetchMinBytes),
		kgo.FetchMaxPartitionBytes(d.FetchMaxPartitionBytes),
	}

	if d.RegexPattern {
		opts = append(opts, kgo.ConsumeRegex())
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
	msg.MetaSetMut("kafka_timestamp", record.Timestamp.Format(time.RFC3339Nano))
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
	msg.MetaSetMut("kafka_timestamp", record.Timestamp.Format(time.RFC3339Nano))
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
