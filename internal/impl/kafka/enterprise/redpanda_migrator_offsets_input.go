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
	"errors"
	"fmt"
	"regexp"
	"slices"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
)

const (
	// Consumer fields
	rmoiFieldTopics       = "topics"
	rmoiFieldRegexpTopics = "regexp_topics"
	rmoiFieldRackID       = "rack_id"
)

func redpandaMigratorOffsetsInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.45.0").
		Summary(`Redpanda Migrator consumer group offsets input using the https://github.com/twmb/franz-go[Franz Kafka client library^].`).
		Description(`
TODO: Description

== Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- kafka_key
- kafka_topic
- kafka_partition
- kafka_offset
- kafka_timestamp_unix
- kafka_timestamp_ms
- kafka_tombstone_message
- kafka_offset_topic
- kafka_offset_group
- kafka_offset_partition
- kafka_offset_commit_timestamp
- kafka_offset_metadata
- kafka_is_end_offset
` + "```" + `
`).
		Fields(redpandaMigratorOffsetsInputConfigFields()...)
}

func redpandaMigratorOffsetsInputConfigFields() []*service.ConfigField {
	return slices.Concat(
		kafka.FranzConnectionFields(),
		[]*service.ConfigField{
			service.NewStringListField(rmoiFieldTopics).
				Description(`
A list of topics to consume from. Multiple comma separated topics can be listed in a single element. When a ` + "`consumer_group`" + ` is specified partitions are automatically distributed across consumers of a topic, otherwise all partitions are consumed.`).
				Example([]string{"foo", "bar"}).
				Example([]string{"things.*"}).
				Example([]string{"foo,bar"}).
				LintRule(`if this.length() == 0 { ["at least one topic must be specified"] }`),
			service.NewBoolField(rmoiFieldRegexpTopics).
				Description("Whether listed topics should be interpreted as regular expression patterns for matching multiple topics.").
				Default(false),
			service.NewStringField(rmoiFieldRackID).
				Description("A rack specifies where the client is physically located and changes fetch requests to consume from the closest replica as opposed to the leader replica.").
				Default("").
				Advanced(),
		},
		kafka.FranzReaderOrderedConfigFields(),
		[]*service.ConfigField{
			service.NewAutoRetryNacksToggleField(),
		},
	)
}

func init() {
	err := service.RegisterBatchInput("redpanda_migrator_offsets", redpandaMigratorOffsetsInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			clientOpts, err := kafka.FranzConnectionOptsFromConfig(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}

			var rackID string
			if rackID, err = conf.FieldString(rmoiFieldRackID); err != nil {
				return nil, err
			}
			clientOpts = append(clientOpts, kgo.Rack(rackID))

			i := redpandaMigratorOffsetsInput{
				mgr:        mgr,
				clientOpts: clientOpts,
			}

			if topicList, err := conf.FieldStringList(rmoiFieldTopics); err != nil {
				return nil, err
			} else {
				i.topics, _, err = kafka.ParseTopics(topicList, -1, false)
				if err != nil {
					return nil, err
				}
				if len(i.topics) == 0 {
					return nil, errors.New("at least one topic must be specified")
				}
			}

			if regexpTopics, err := conf.FieldBool(rmoiFieldRegexpTopics); err != nil {
				return nil, err
			} else if regexpTopics {
				i.topicPatterns = make([]*regexp.Regexp, 0, len(i.topics))
				for _, topic := range i.topics {
					tp, err := regexp.Compile(topic)
					if err != nil {
						return nil, fmt.Errorf("failed to compile topic regex %q: %s", topic, err)
					}
					i.topicPatterns = append(i.topicPatterns, tp)
				}
			}

			i.FranzReaderOrdered, err = kafka.NewFranzReaderOrderedFromConfig(conf, mgr, func() ([]kgo.Opt, error) {
				// Consume messages from the `__consumer_offsets` topic and configure `start_from_oldest: true`
				return append(clientOpts, kgo.ConsumeTopics("__consumer_offsets"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())), nil
			})
			if err != nil {
				return nil, err
			}

			return service.AutoRetryNacksBatchedToggled(conf, &i)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type redpandaMigratorOffsetsInput struct {
	*kafka.FranzReaderOrdered

	topicPatterns []*regexp.Regexp
	topics        []string
	clientOpts    []kgo.Opt

	mgr *service.Resources
}

func (rmoi *redpandaMigratorOffsetsInput) matchesTopic(topic string) bool {
	if len(rmoi.topicPatterns) > 0 {
		return slices.ContainsFunc(rmoi.topicPatterns, func(tp *regexp.Regexp) bool {
			return tp.MatchString(topic)
		})
	}
	return slices.ContainsFunc(rmoi.topics, func(t string) bool {
		return t == topic
	})
}

func (rmoi *redpandaMigratorOffsetsInput) getKeyAndOffset(msg *service.Message) (key kmsg.OffsetCommitKey, offset kmsg.OffsetCommitValue, ok bool) {
	var recordKey []byte
	if k, exists := msg.MetaGetMut("kafka_key"); !exists {
		return
	} else {
		recordKey = k.([]byte)
	}

	// Check the version to ensure that we process only offset commit keys
	key = kmsg.NewOffsetCommitKey()
	if err := key.ReadFrom(recordKey); err != nil || (key.Version != 0 && key.Version != 1) {
		rmoi.mgr.Logger().Debugf("Failed to decode record key: %s", err)
		return
	}

	isExpectedTopic := rmoi.matchesTopic(key.Topic)
	if !isExpectedTopic {
		rmoi.mgr.Logger().Tracef("Skipping updates for topic %q", key.Topic)
		return
	}

	recordValue, err := msg.AsBytes()
	if err != nil {
		rmoi.mgr.Logger().Debugf("Failed to fetch record value: %s", err)
		return
	}

	offset = kmsg.NewOffsetCommitValue()
	if err := offset.ReadFrom(recordValue); err != nil {
		rmoi.mgr.Logger().Debugf("Failed to decode offset commit value: %s", err)
		return
	}

	return key, offset, true
}

func (rmoi *redpandaMigratorOffsetsInput) getEndTimestamp(ctx context.Context, topic string, partition int32, offset int64) (timestamp int64, isEndOffset bool, err error) {
	client, err := kgo.NewClient(rmoi.clientOpts...)
	if err != nil {
		return 0, false, fmt.Errorf("failed to create Kafka client: %s", err)
	}
	defer client.Close()

	// The default kadm client timeout is 15s. Do we need to make this configurable?
	offsets, err := kadm.NewClient(client).ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, false, fmt.Errorf("failed to read the end offset for topic %q and partition %q: %s", topic, partition, err)
	}

	endOffset, ok := offsets.Lookup(topic, partition)
	if !ok {
		return 0, false, fmt.Errorf("failed to find the end offset for topic %q and partition %q: %s", topic, partition, err)
	}

	// If the end offset on the topic matches the offset we received via `__consumer_offsets`, then we must read the
	// last record from the topic because the end offset does not have a corresponding record yet.
	var recordOffset kgo.Offset
	if endOffset.Offset == offset {
		// The default offset begins at the end.
		recordOffset = kgo.NewOffset().Relative(-1)
	} else if endOffset.Offset > offset {
		recordOffset = kgo.NewOffset().At(offset)
	} else {
		return 0, false, fmt.Errorf(
			"the newest committed offset %d for topic %q partition %q should never be smaller than the received offset %d",
			endOffset.Offset, topic, partition, offset,
		)
	}

	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {
			partition: recordOffset,
		},
	})

	fetches := client.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return 0, false, fmt.Errorf("failed to read record with offset %d for topic %q partition %q: client closed", offset, topic, partition)
	}

	if err := fetches.Err(); err != nil {
		return 0, false, fmt.Errorf("failed to read record with offset %d for topic %q partition %q: %s", offset, topic, partition, err)
	}

	it := fetches.RecordIter()
	if it.Done() {
		return 0, false, fmt.Errorf("couldn't find record with offset %d for topic %q partition %q: %s", offset, topic, partition, err)
	}

	rec := it.Next()

	return rec.Timestamp.UnixMilli(), endOffset.Offset == offset, nil
}

func (rmoi *redpandaMigratorOffsetsInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	for {
		batch, ack, err := rmoi.FranzReaderOrdered.ReadBatch(ctx)
		if err != nil {
			return batch, ack, err
		}

		// Skip records where `getKeyAndOffset()` returns false. This logic is similar to `slices.DeleteFunc()`, but we
		// need to return errors if we can't connect to the Kafka cluster to read data.
		i := 0
		for _, msg := range batch {
			key, offset, ok := rmoi.getKeyAndOffset(msg)
			if !ok {
				continue
			}
			batch[i] = msg
			i++

			ts, isEndOffset, err := rmoi.getEndTimestamp(ctx, key.Topic, key.Partition, offset.Offset)
			if err != nil {
				return nil, nil, err
			}

			msg.MetaSetMut("kafka_offset_topic", key.Topic)
			msg.MetaSetMut("kafka_offset_group", key.Group)
			msg.MetaSetMut("kafka_offset_partition", key.Partition)
			msg.MetaSetMut("kafka_offset_commit_timestamp", ts)
			msg.MetaSetMut("kafka_offset_metadata", offset.Metadata)
			msg.MetaSetMut("kafka_is_end_offset", isEndOffset)
		}

		// Delete the records that we skipped
		batch = slices.Delete(batch, i, len(batch))

		if len(batch) == 0 {
			_ = ack(ctx, nil) // TODO: Log this error?
			continue
		}

		return batch, ack, nil
	}
}
