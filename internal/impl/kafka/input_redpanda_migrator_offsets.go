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
	"sync"
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

	client    *kgo.Client
	admClient *kadm.Client
	poller    *asyncroutine.Periodic
	msgChan   chan *service.Message
	stateMut  sync.Mutex

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
	topics := slices.Collect(maps.Keys(tsRequests))
	rmoi.log.Debugf("Fetching timestamps for topics: %v", topics)

	defer rmoi.client.PurgeTopicsFromClient(topics...)

	highWatermarks, err := rmoi.admClient.ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("failed to read high watermarks: %s", err)
	}

	// Clone the tsRequests map so we can remove entries from it as we make progress.
	pendingTsRequests := make(timestampRequests, len(tsRequests))
	for t, po := range tsRequests {
		for p, o := range po {
			if _, ok := pendingTsRequests[t]; !ok {
				pendingTsRequests[t] = make(map[int32]int64, len(po))
			}
			pendingTsRequests[t][p] = o
		}
	}

	var pendingTsRequestsLock sync.Mutex
	pollFetchesCtx, cancelPollFetches := context.WithCancel(ctx)

	// The partitionsPoller runs periodically to determine which records have not been fetched yet. If all of them have
	// been fetched, then it will cancel the pollFetchesCtx to stop the fetch loop.
	// TODO: Consider making the duration configurable.
	partitionsPoller := asyncroutine.NewPeriodic(5*time.Second, func() {
		consumePartitions := make(map[string]map[int32]kgo.Offset)
		pendingTsRequestsLock.Lock()

		// Read the start offsets at every poll cycle because the topic may get truncated while we still need to read
		// records.
		startOffsets, err := rmoi.admClient.ListStartOffsets(ctx, slices.Collect(maps.Keys(pendingTsRequests))...)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				rmoi.log.Errorf("failed to read start offsets: %s", err)
			}
			pendingTsRequestsLock.Unlock()
			return
		}

		for t, po := range pendingTsRequests {
			for p, o := range po {
				highWatermark, ok := highWatermarks.Lookup(t, p)
				if !ok {
					rmoi.log.Errorf("Failed to find the high watermark for topic %q and partition %d", t, p)
					continue
				}

				startOffset, ok := startOffsets.Lookup(t, p)
				if !ok {
					rmoi.log.Errorf("Failed to find the start offset for topic %q and partition %d", t, p)
					continue
				}

				// This ensures we skip tsRequests which correspond to truncated topics which either have no more
				// records or no longer contain the requested offset.
				if startOffset.Offset == highWatermark.Offset || o < startOffset.Offset {
					rmoi.log.Debugf("Skipping truncated topic %q partition %d", t, p)
					continue
				}

				// If the high watermark on the topic matches the consumer group offset, then we must read the last record from
				// the topic because the high watermark does not have a corresponding record yet.
				var recordOffset kgo.Offset
				if highWatermark.Offset == o {
					// The default offset begins at the end.
					recordOffset = kgo.NewOffset().Relative(-1)
				} else if highWatermark.Offset > o {
					recordOffset = kgo.NewOffset().At(o)
				} else {
					rmoi.log.Errorf("The high watermark %d for topic %q partition %d should not be smaller than the received offset %d",
						highWatermark.Offset, t, p, o,
					)
					continue
				}

				if _, ok := consumePartitions[t]; !ok {
					consumePartitions[t] = make(map[int32]kgo.Offset)
				}
				consumePartitions[t][p] = recordOffset
			}
		}
		pendingTsRequestsLock.Unlock()

		if len(consumePartitions) == 0 {
			rmoi.log.Debugf("Finished fetching timestamps for offsets.")
			cancelPollFetches()
		}

		rmoi.client.AddConsumePartitions(consumePartitions)
	})
	partitionsPoller.Start()
	defer partitionsPoller.Stop()

	tsResults := make(timestampResults, len(tsRequests))
	pausedPartitions := make(map[string][]int32, len(tsRequests))
	for {
		// PollFetches may not return all the records we need in one go, so we loop and remove entries from
		// pendingTsRequests until all the tsRequests have been found or the top level ctx is canceled.
		// On the happy path, pollFetchesCtx will be canceled by partitionsPoller when all the requested timestamps have
		// been found.
		fetches := rmoi.client.PollFetches(pollFetchesCtx)
		if fetches.IsClientClosed() {
			return nil, errors.New("failed to read topic records: client closed")
		}

		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.Canceled) {
				// Resume any paused partitions before exiting the fetch loop and returning the results to the caller.
				rmoi.client.ResumeFetchPartitions(pausedPartitions)
				break
			}
			return nil, fmt.Errorf("failed to read topic records: %s", err)
		}

		pendingTsRequestsLock.Lock()
		fetches.EachTopic(func(t kgo.FetchTopic) {
			t.EachPartition(func(p kgo.FetchPartition) {
				p.EachRecord(func(rec *kgo.Record) {
					if _, ok := pendingTsRequests[rec.Topic]; !ok {
						return
					}
					if _, ok := pendingTsRequests[rec.Topic][rec.Partition]; !ok {
						return
					}

					if _, ok := tsResults[rec.Topic]; !ok {
						tsResults[rec.Topic] = make(map[int32]timestampResult)
					}
					// Only store the timestamp of the first record we encounter for each topic/partition pair.
					if _, ok := tsResults[rec.Topic][rec.Partition]; !ok {
						highWatermark, ok := highWatermarks.Lookup(rec.Topic, rec.Partition)
						if !ok {
							rmoi.log.Errorf("could not find high watermark for topic %q and partition %d", rec.Topic, rec.Partition)
						}

						tsResults[rec.Topic][rec.Partition] = timestampResult{
							timestamp:       rec.Timestamp.UnixMilli(),
							isHighWatermark: highWatermark.Offset == pendingTsRequests[rec.Topic][rec.Partition],
						}
					}

					// Remove this topic/partition pair from the next fetch cycles.
					delete(pendingTsRequests[rec.Topic], rec.Partition)
					if len(pendingTsRequests[rec.Topic]) == 0 {
						delete(pendingTsRequests, rec.Topic)
					}
				})
			})
		})
		pendingTsRequestsLock.Unlock()

		// Gradually pause fetch partitions as we get more results from PollFetches.
		for t, p := range tsResults {
			newP := slices.Concat(pausedPartitions[t], slices.Collect(maps.Keys(p)))
			slices.Sort(newP)
			pausedPartitions[t] = slices.Compact(newP)
		}
		rmoi.client.PauseFetchPartitions(pausedPartitions)
	}

	return tsResults, nil
}

func (rmoi *redpandaMigratorOffsetsInput) Connect(ctx context.Context) error {
	rmoi.stateMut.Lock()
	defer rmoi.stateMut.Unlock()

	if rmoi.poller != nil {
		return nil
	}

	var err error
	rmoi.client, err = NewFranzClient(ctx, rmoi.clientOpts...)
	if err != nil {
		return fmt.Errorf("failed to connect: %s", err)
	}

	// The default kadm client timeout is 15s. Do we need to make this configurable?
	rmoi.admClient = kadm.NewClient(rmoi.client)

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
			if !errors.Is(err, context.Canceled) {
				rmoi.log.Errorf("failed to list consumer groups: %s", err)
			}
			return
		}

		groups := describedGroups.Names()
		rmoi.log.Debugf("Discovered consumer groups: %s", groups)

		resp := adm.FetchManyOffsets(ctx, groups...)
		if err := resp.Error(); err != nil {
			if !errors.Is(err, context.Canceled) {
				rmoi.log.Errorf("failed to fetch consumer group offsets: %s", err)
			}
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

		if len(tsRequests) == 0 {
			rmoi.log.Debug("No pending consumer group updates")
			return
		}

		rmoi.log.Debugf("Processing consumer groups: %v", slices.Collect(maps.Keys(groupOffsetMetadata)))

		tsResults, err := rmoi.getTimestampsForCommittedOffsets(ctx, tsRequests)
		if err != nil {
			rmoi.log.Errorf("failed to get timestamps for committed offsets: %s", err)
			return
		}

		// Exclude the current consumer group offsets from future iterations if we couldn't read the timestamps for the
		// corresponding records. This can happen if the topic was truncated and the offsets are no longer available.
		for t, po := range tsRequests {
			if len(po) == len(tsResults[t]) {
				continue
			}
			for p := range po {
				if _, ok := tsResults[t][p]; !ok {
					cacheKey := pendingCacheKeys[t][p]
					offsetCache[cacheKey] = tsRequests[t][p]
				}
			}
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
	rmoi.stateMut.Lock()
	defer rmoi.stateMut.Unlock()

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
	rmoi.stateMut.Lock()
	defer rmoi.stateMut.Unlock()

	if rmoi.poller != nil {
		rmoi.poller.Stop()
		rmoi.client.Close()
		rmoi.poller = nil
	}

	return nil
}
