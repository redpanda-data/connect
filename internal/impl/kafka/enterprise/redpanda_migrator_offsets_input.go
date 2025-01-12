// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"errors"
	"fmt"
	"regexp"
	"slices"

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

			var topics []string
			if topicList, err := conf.FieldStringList(rmoiFieldTopics); err != nil {
				return nil, err
			} else {
				topics, _, err = kafka.ParseTopics(topicList, -1, false)
				if err != nil {
					return nil, err
				}
				if len(topics) == 0 {
					return nil, errors.New("at least one topic must be specified")
				}
			}

			var topicPatterns []*regexp.Regexp
			if regexpTopics, err := conf.FieldBool(rmoiFieldRegexpTopics); err != nil {
				return nil, err
			} else if regexpTopics {
				topicPatterns = make([]*regexp.Regexp, 0, len(topics))
				for _, topic := range topics {
					tp, err := regexp.Compile(topic)
					if err != nil {
						return nil, fmt.Errorf("failed to compile topic regex %q: %s", topic, err)
					}
					topicPatterns = append(topicPatterns, tp)
				}
			}

			var rackID string
			if rackID, err = conf.FieldString(rmoiFieldRackID); err != nil {
				return nil, err
			}
			clientOpts = append(clientOpts, kgo.Rack(rackID))

			// Configure `start_from_oldest: true`
			clientOpts = append(clientOpts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))

			// Consume messages from the `__consumer_offsets` topic
			clientOpts = append(clientOpts, kgo.ConsumeTopics("__consumer_offsets"))

			clientOpts = append(clientOpts, kgo.WithLogger(&kafka.KGoLogger{L: mgr.Logger()}))

			matchesTopic := func(topic string) bool {
				if len(topicPatterns) > 0 {
					return slices.ContainsFunc(topicPatterns, func(tp *regexp.Regexp) bool {
						return tp.MatchString(topic)
					})
				}
				return slices.ContainsFunc(topics, func(t string) bool {
					return t == topic
				})
			}

			rdr, err := kafka.NewFranzReaderOrderedFromConfig(conf, mgr, func() ([]kgo.Opt, error) {
				return clientOpts, nil
			}, func(record *kgo.Record) (*service.Message, error) {
				msg := kafka.FranzRecordToMessageV1(record)

				// Check the version to ensure that we process only offset commit keys
				key := kmsg.NewOffsetCommitKey()
				if err := key.ReadFrom(record.Key); err != nil || (key.Version != 0 && key.Version != 1) {
					return nil, fmt.Errorf("failed to decode record key: %s", err)
				}

				isExpectedTopic := matchesTopic(key.Topic)
				if !isExpectedTopic {
					return nil, fmt.Errorf("skipping updates for topic %q", key.Topic)
				}

				offsetCommitValue := kmsg.NewOffsetCommitValue()
				if err = offsetCommitValue.ReadFrom(record.Value); err != nil {
					return nil, fmt.Errorf("failed to decode offset commit value: %s", err)
				}

				msg.MetaSetMut("kafka_offset_topic", key.Topic)
				msg.MetaSetMut("kafka_offset_group", key.Group)
				msg.MetaSetMut("kafka_offset_partition", key.Partition)
				msg.MetaSetMut("kafka_offset_commit_timestamp", offsetCommitValue.CommitTimestamp)
				msg.MetaSetMut("kafka_offset_metadata", offsetCommitValue.Metadata)

				return msg, nil
			}, nil, nil)
			if err != nil {
				return nil, err
			}

			return service.AutoRetryNacksBatchedToggled(conf, rdr)
		})
	if err != nil {
		panic(err)
	}
}
