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
	"regexp"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
)

const (
	// Consumer fields
	rmoiFieldTopics       = "topics"
	rmoiFieldRegexpTopics = "regexp_topics"
	rmoiFieldRackID       = "rack_id"
	rmoiFieldPollInterval = "poll_interval"

	// Deprecated
	// `consumer_group`, `commit_period`, `partition_buffer_bytes`, `topic_lag_refresh_period`, and `max_yield_batch_bytes`
	rmoFieldConsumerGroup         = "consumer_group"
	rmoFieldCommitPeriod          = "commit_period"
	rmoFieldPartitionBufferBytes  = "partition_buffer_bytes"
	rmoFieldTopicLagRefreshPeriod = "topic_lag_refresh_period"
	rmoFieldMaxYieldBatchBytes    = "max_yield_batch_bytes"
)

func redpandaMigratorOffsetsInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.45.0").
		Summary(`Redpanda Migrator consumer group offsets input using the https://github.com/twmb/franz-go[Franz Kafka client library^].`).
		Description(`
This input reads consumer group updates via the ` + "`OffsetFetch`" + ` API and should be used in combination with the ` + "`redpanda_migrator_offsets`" + ` output.

== Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- kafka_offset_topic
- kafka_offset_group
- kafka_offset_partition
- kafka_offset_commit_timestamp
- kafka_offset_metadata
- kafka_is_high_watermark
` + "```" + `
`).
		Fields(redpandaMigratorOffsetsInputConfigFields()...)
}

func redpandaMigratorOffsetsInputConfigFields() []*service.ConfigField {
	return slices.Concat(
		FranzConnectionFields(),
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
			service.NewDurationField(rmoiFieldPollInterval).
				Description("Duration between OffsetFetch polling attempts.").
				Default("15s").
				Advanced(),
			service.NewAutoRetryNacksToggleField(),

			// Deprecated
			service.NewStringField(rmoFieldConsumerGroup).Deprecated(),
			service.NewDurationField(rmoFieldCommitPeriod).Deprecated(),
			service.NewStringField(rmoFieldPartitionBufferBytes).Deprecated(),
			service.NewDurationField(rmoFieldTopicLagRefreshPeriod).Deprecated(),
			service.NewStringField(rmoFieldMaxYieldBatchBytes).Deprecated(),
		},
	)
}

func init() {
	service.MustRegisterInput("redpanda_migrator_offsets", redpandaMigratorOffsetsInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			clientOpts, err := FranzConnectionOptsFromConfig(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}

			var rackID string
			if rackID, err = conf.FieldString(rmoiFieldRackID); err != nil {
				return nil, err
			}
			clientOpts = append(clientOpts, kgo.Rack(rackID))

			i := redpandaMigratorOffsetsInput{
				clientOpts: clientOpts,
				msgChan:    make(chan *service.Message),
				log:        mgr.Logger(),
			}

			if topicList, err := conf.FieldStringList(rmoiFieldTopics); err != nil {
				return nil, err
			} else {
				i.topics, _, err = ParseTopics(topicList, -1, false)
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

			if i.pollInterval, err = conf.FieldDuration(rmoiFieldPollInterval); err != nil {
				return nil, err
			}

			return service.AutoRetryNacksToggled(conf, &i)
		})
}

//------------------------------------------------------------------------------

type redpandaMigratorOffsetsInput struct {
	topicPatterns []*regexp.Regexp
	topics        []string
	pollInterval  time.Duration
	clientOpts    []kgo.Opt

	client  *kgo.Client
	poller  *asyncroutine.Periodic
	msgChan chan *service.Message

	log *service.Logger
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

func (rmoi *redpandaMigratorOffsetsInput) getTimestampForCommittedOffset(ctx context.Context, topic string, partition int32, offset int64) (timestamp int64, isHighWatermark bool, err error) {
	client, err := NewFranzClient(ctx, rmoi.clientOpts...)
	if err != nil {
		return 0, false, fmt.Errorf("failed to create Kafka client: %s", err)
	}
	defer client.Close()

	// The default kadm client timeout is 15s. Do we need to make this configurable?
	offsets, err := kadm.NewClient(client).ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, false, fmt.Errorf("failed to read the high watermark for topic %q and partition %d: %s", topic, partition, err)
	}

	highWatermark, ok := offsets.Lookup(topic, partition)
	if !ok {
		return 0, false, fmt.Errorf("failed to find the high watermark for topic %q and partition %d: %s", topic, partition, err)
	}

	// If the high watermark on the topic matches the offset we received via `__consumer_offsets`, then we must read the
	// last record from the topic because the high watermark does not have a corresponding record yet.
	var recordOffset kgo.Offset
	if highWatermark.Offset == offset {
		// The default offset begins at the end.
		recordOffset = kgo.NewOffset().Relative(-1)
	} else if highWatermark.Offset > offset {
		recordOffset = kgo.NewOffset().At(offset)
	} else {
		return 0, false, fmt.Errorf(
			"the newest committed offset %d for topic %q partition %d should never be smaller than the received offset %d",
			highWatermark.Offset, topic, partition, offset,
		)
	}

	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {
			partition: recordOffset,
		},
	})

	fetches := client.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return 0, false, fmt.Errorf("failed to read record with offset %d for topic %q partition %d: client closed", offset, topic, partition)
	}

	if err := fetches.Err(); err != nil {
		return 0, false, fmt.Errorf("failed to read record with offset %d for topic %q partition %d: %s", offset, topic, partition, err)
	}

	it := fetches.RecordIter()
	if it.Done() {
		return 0, false, fmt.Errorf("couldn't find record with offset %d for topic %q partition %d: %s", offset, topic, partition, err)
	}

	rec := it.Next()

	return rec.Timestamp.UnixMilli(), highWatermark.Offset == offset, nil
}

func (rmoi *redpandaMigratorOffsetsInput) Connect(ctx context.Context) error {
	if rmoi.poller != nil {
		return nil
	}

	var err error
	rmoi.client, err = NewFranzClient(ctx, rmoi.clientOpts...)
	if err != nil {
		return fmt.Errorf("failed to connect: %s", err)
	}

	type cacheKey struct {
		group     string
		topic     string
		partition int32
	}
	offsetCache := make(map[cacheKey]int64)
	adm := kadm.NewClient(rmoi.client)
	rmoi.poller = asyncroutine.NewPeriodicWithContext(rmoi.pollInterval, func(ctx context.Context) {
		describedGroups, err := adm.DescribeGroups(ctx)
		if err != nil {
			rmoi.log.Errorf("failed to list groups: %s", err)
			return
		}

		var groups []string
		for _, group := range describedGroups {
			if group.Err == nil {
				groups = append(groups, group.Group)
			}
		}

		resp := adm.FetchManyOffsets(ctx, groups...)
		if err := resp.Error(); err != nil {
			rmoi.log.Errorf("failed to fetch group offsets: %s", err)
			return
		}

		for group, offsetResp := range resp {
			offsetResp.Fetched.Each(func(offset kadm.OffsetResponse) {
				if !rmoi.matchesTopic(offset.Topic) {
					// Skip if the topic does not match
					return
				}

				key := cacheKey{
					group:     group,
					topic:     offset.Topic,
					partition: offset.Partition,
				}
				if val, ok := offsetCache[key]; ok && val == offset.At {
					// Skip if the offset hasn't changed
					return
				}

				ts, isHWMCommit, err := rmoi.getTimestampForCommittedOffset(ctx, offset.Topic, offset.Partition, offset.At)
				if err != nil {
					rmoi.log.Errorf("failed to get timestamp for committed offset: %s", err)
					return
				}

				offsetCache[key] = offset.At

				msg := service.NewMessage(nil)
				msg.MetaSetMut("kafka_offset_topic", offset.Topic)
				msg.MetaSetMut("kafka_offset_group", group)
				msg.MetaSetMut("kafka_offset_partition", offset.Partition)
				msg.MetaSetMut("kafka_offset_commit_timestamp", ts)
				msg.MetaSetMut("kafka_offset_metadata", offset.Metadata)
				msg.MetaSetMut("kafka_is_high_watermark", isHWMCommit)

				select {
				case rmoi.msgChan <- msg:
				case <-ctx.Done():
					return
				}
			})
		}
	})

	rmoi.poller.Start()

	return nil
}

func (rmoi *redpandaMigratorOffsetsInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if rmoi.poller == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case msg := <-rmoi.msgChan:
		return msg, func(context.Context, error) error { return nil }, nil
	}
}

func (rmoi *redpandaMigratorOffsetsInput) Close(context.Context) error {
	if rmoi.poller != nil {
		rmoi.poller.Stop()
		rmoi.client.Close()
		rmoi.poller = nil
	}

	return nil
}
