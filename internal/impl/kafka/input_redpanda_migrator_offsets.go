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
	"maps"
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

type timestampRequests map[string]map[int32]int64

type timestampResult struct {
	timestamp       int64
	isHighWatermark bool
}

type timestampResults map[string]map[int32]timestampResult

func (rmoi *redpandaMigratorOffsetsInput) getTimestampsForCommittedOffsets(ctx context.Context, tsRequests timestampRequests) (timestampResults, error) {
	client, err := kgo.NewClient(rmoi.clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %s", err)
	}
	defer client.Close()

	topics := slices.Collect(maps.Keys(tsRequests))
	rmoi.log.Debugf("Fetching timestamps for topics: %s", topics)

	// The default kadm client timeout is 15s. Do we need to make this configurable?
	endOffsets, err := kadm.NewClient(client).ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("failed to read the high watermark for topics %s: %s", topics, err)
	}

	partitions := make(map[string]map[int32]kgo.Offset)
	highWatermarks := make(map[string]map[int32]int64)
	for topic, partitionOffsets := range tsRequests {
		for partition, offset := range partitionOffsets {
			highWatermark, ok := endOffsets.Lookup(topic, partition)
			if !ok {
				return nil, fmt.Errorf("failed to find the high watermark for topic %q and partition %d", topic, partition)
			}

			// If the high watermark on the topic matches the consumer group offset, then we must read the last record from
			// the topic because the high watermark does not have a corresponding record yet.
			var recordOffset kgo.Offset
			if highWatermark.Offset == offset {
				// The default offset begins at the end.
				recordOffset = kgo.NewOffset().Relative(-1)
			} else if highWatermark.Offset > offset {
				recordOffset = kgo.NewOffset().At(offset)
			} else {
				return nil, fmt.Errorf(
					"the high watermark %d for topic %q partition %d should not be smaller than the received offset %d",
					highWatermark.Offset, topic, partition, offset,
				)
			}

			if _, ok := partitions[topic]; !ok {
				partitions[topic] = make(map[int32]kgo.Offset)
			}
			partitions[topic][partition] = recordOffset

			if _, ok := highWatermarks[topic]; !ok {
				highWatermarks[topic] = make(map[int32]int64)
			}
			highWatermarks[topic][partition] = highWatermark.Offset
		}
	}

	client.AddConsumePartitions(partitions)

	// ctx, done := context.WithTimeout(ctx, 5*time.Second)
	// defer done()
	fetches := client.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return nil, errors.New("failed to read topic records: client closed")
	}

	if err := fetches.Err(); err != nil {
		return nil, fmt.Errorf("failed to read topic records: %s", err)
	}

	result := make(timestampResults)
	fetches.EachRecord(func(rec *kgo.Record) {
		if _, ok := result[rec.Topic]; !ok {
			result[rec.Topic] = make(map[int32]timestampResult)
		}
		result[rec.Topic][rec.Partition] = timestampResult{
			timestamp:       rec.Timestamp.UnixMilli(),
			isHighWatermark: highWatermarks[rec.Topic][rec.Partition] == tsRequests[rec.Topic][rec.Partition],
		}
	})

	return result, nil
}

func (rmoi *redpandaMigratorOffsetsInput) Connect(ctx context.Context) error {
	if rmoi.poller != nil {
		return nil
	}

	var err error
	rmoi.client, err = kgo.NewClient(rmoi.clientOpts...)
	if err != nil {
		return fmt.Errorf("failed to connect: %s", err)
	}

	if err := rmoi.client.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping broker: %s", err)
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

		groups := describedGroups.Names()
		rmoi.log.Debugf("Discovered groups: %s", groups)

		resp := adm.FetchManyOffsets(ctx, groups...)
		if err := resp.Error(); err != nil {
			rmoi.log.Errorf("failed to fetch group offsets: %s", err)
			return
		}

		pendingCacheKeys := make(map[string]map[int32]cacheKey)
		groupOffsetMetadata := make(map[string]string)
		tsRequests := make(timestampRequests)
		for group, offsetResp := range resp {
			offsetResp.Fetched.Each(func(offset kadm.OffsetResponse) {
				if !rmoi.matchesTopic(offset.Topic) {
					// Skip if the topic does not match the specified input filters.
					return
				}

				key := cacheKey{
					group:     group,
					topic:     offset.Topic,
					partition: offset.Partition,
				}
				if val, ok := offsetCache[key]; ok && val == offset.At {
					// Skip if the offset hasn't changed.
					return
				}

				if _, ok := pendingCacheKeys[offset.Topic]; !ok {
					pendingCacheKeys[offset.Topic] = make(map[int32]cacheKey)
				}
				pendingCacheKeys[offset.Topic][offset.Partition] = key

				groupOffsetMetadata[group] = offset.Metadata

				if _, ok := tsRequests[offset.Topic]; !ok {
					tsRequests[offset.Topic] = make(map[int32]int64)
				}
				tsRequests[offset.Topic][offset.Partition] = offset.At
			})
		}

		tsResults, err := rmoi.getTimestampsForCommittedOffsets(ctx, tsRequests)
		if err != nil {
			rmoi.log.Errorf("failed to get timestamps for committed offsets: %s", err)
			return
		}

		for topic, partitionTsResult := range tsResults {
			for partition, tsResult := range partitionTsResult {
				cacheKey := pendingCacheKeys[topic][partition]
				offsetCache[cacheKey] = tsRequests[topic][partition]

				msg := service.NewMessage(nil)
				msg.MetaSetMut("kafka_offset_topic", topic)
				msg.MetaSetMut("kafka_offset_group", cacheKey.group)
				msg.MetaSetMut("kafka_offset_partition", partition)
				msg.MetaSetMut("kafka_offset_commit_timestamp", tsResult.timestamp)
				msg.MetaSetMut("kafka_offset_metadata", groupOffsetMetadata[cacheKey.group])
				msg.MetaSetMut("kafka_is_high_watermark", tsResult.isHighWatermark)

				select {
				case rmoi.msgChan <- msg:
				case <-ctx.Done():
					return
				}
			}
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
