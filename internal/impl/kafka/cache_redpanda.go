// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	rcFieldTopic                  = "topic"
	rcFieldAllowAutoTopicCreation = "allow_auto_topic_creation"
)

func redpandaCacheConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Summary(`A Kafka cache using the https://github.com/twmb/franz-go[Franz Kafka client library^].`).
		Description(`
A cache that stores data in a Kafka topic.

This cache is useful for data that is written frequently and queried infreqently.
Reads of the cache require reading the entire topic partition, so if there is a need for frequent reads, it's recommended to put an in memory caching layer infront of this cache.

Topics that are used as caches should be compacted so that reads are less expensive when they rescan the topic, as only the latest value is needed.

This cache does not support any special TTL mechanism, any TTL should be handled by the Kafka topic itself using data retention policies.
`).
		Fields(FranzConnectionFields()...).
		Fields(
			service.NewStringField(rcFieldTopic).Description("The topic to store data in."),
			service.NewBoolField(rcFieldAllowAutoTopicCreation).
				Description("Enables topics to be auto created if they do not exist when fetching their metadata.").
				Default(true).
				Advanced(),
		)
}

func init() {
	service.MustRegisterCache(
		"redpanda",
		redpandaCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			opts, err := FranzConnectionOptsFromConfig(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			topic, err := conf.FieldString(rcFieldTopic)
			if err != nil {
				return nil, err
			}
			allowAutoTopicCreation, err := conf.FieldBool(rcFieldAllowAutoTopicCreation)
			if err != nil {
				return nil, err
			}
			if allowAutoTopicCreation {
				opts = append(opts, kgo.AllowAutoTopicCreation())
			}
			return NewRedpandaCache(opts, topic)
		})
}

// NewRedpandaCache creates a new cache using a Redpanda topic.
func NewRedpandaCache(opts []kgo.Opt, topic string) (service.Cache, error) {
	opts = append(
		opts,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	)
	producer, err := NewFranzClient(opts...)
	if err != nil {
		return nil, err
	}
	return &redpandaCache{
		producer: producer,
		opts:     opts,
		topic:    topic,
	}, nil
}

type redpandaCache struct {
	producer *kgo.Client
	opts     []kgo.Opt
	topic    string
}

var _ service.Cache = (*redpandaCache)(nil)

// Add implements service.Cache.
func (r *redpandaCache) Add(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	return r.producer.ProduceSync(ctx, kgo.KeySliceRecord([]byte(key), value)).FirstErr()
}

// Set implements service.Cache.
func (r *redpandaCache) Set(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	return r.producer.ProduceSync(ctx, kgo.KeySliceRecord([]byte(key), value)).FirstErr()
}

// Delete implements service.Cache.
func (r *redpandaCache) Delete(ctx context.Context, key string) error {
	return r.producer.ProduceSync(ctx, kgo.KeySliceRecord([]byte(key), nil)).FirstErr()
}

// Get implements service.Cache.
func (r *redpandaCache) Get(ctx context.Context, key string) ([]byte, error) {
	client, err := NewFranzClient(r.opts...)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	listed, err := admin.ListEndOffsets(ctx, r.topic)
	if err != nil {
		return nil, err
	}
	partitionOffsets := listed[r.topic]
	if len(partitionOffsets) == 0 {
		return nil, fmt.Errorf("missing or unknown topic %s", r.topic)
	}
	partition := int32(kgo.StickyKeyPartitioner(nil).ForTopic(r.topic).Partition(kgo.KeyStringRecord(key, ""), len(partitionOffsets)))
	var highWatermark int64 = -1
	if partition, ok := partitionOffsets[partition]; ok {
		// The offset here is the high watermark, so -1 gives the offset of the last existing record in the topic partition.
		highWatermark = partition.Offset - 1
	}
	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		r.topic: {partition: kgo.NewOffset().AtStart()},
	})
	var latest *kgo.Record
	latestOffset := int64(-1)
	for latestOffset < highWatermark {
		fetches := client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			return nil, err
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if string(r.Key) == key {
				latest = r
			}
			latestOffset = r.Offset
		})
	}
	if latest == nil || latest.Value == nil {
		return nil, service.ErrKeyNotFound
	}
	return latest.Value, nil
}

// Close implements service.Cache.
func (r *redpandaCache) Close(context.Context) error {
	r.producer.Close()
	return nil
}
