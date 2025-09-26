// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migrator_test

import (
	"context"
	"fmt"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/migrator"
)

func TestIntegrationListGroupOffsets(t *testing.T) {
	integration.CheckSkip(t)

	src, dst := startRedpandaSourceAndDestination(t)

	// Create topics
	const (
		topicFoo1 = "foo-topic-1"
		topicFoo2 = "foo-topic-2"
		topicBar  = "bar-topic"
	)
	src.CreateTopic(topicFoo1)
	src.CreateTopic(topicFoo2)
	src.CreateTopic(topicBar)

	// Write some messages to topics
	writeToTopic(src, 5, ProduceToTopicOpt(topicFoo1), ProduceToPartitionOpt(0))
	writeToTopic(src, 5, ProduceToTopicOpt(topicFoo1), ProduceToPartitionOpt(1))
	writeToTopic(src, 3, ProduceToTopicOpt(topicFoo2), ProduceToPartitionOpt(0))
	writeToTopic(src, 3, ProduceToTopicOpt(topicBar), ProduceToPartitionOpt(0))

	// Commit offsets for various groups
	const (
		groupFoo1 = "foo-group-1"
		groupFoo2 = "foo-group-2"
		groupBar  = "bar-group"
		groupDel  = "deleted-group"
	)
	src.CommitOffset(groupFoo1, topicFoo1, 0, 2)
	src.CommitOffset(groupFoo1, topicFoo1, 1, 3)
	src.CommitOffset(groupFoo2, topicFoo2, 0, 1)
	src.CommitOffset(groupBar, topicBar, 0, 2)
	src.CommitOffset(groupDel, topicFoo1, 0, 1)

	// Delete group
	_, err := src.Admin.DeleteGroup(t.Context(), groupDel)
	assert.NoError(t, err)

	// Helper to create migrator and list group offsets
	listGroupOffsets := func(t *testing.T, conf migrator.GroupsMigratorConfig, topics []string) []migrator.GroupOffset {
		t.Helper()
		gm := migrator.NewGroupsMigratorForTesting(t, conf, src.Client, src.Admin, dst.Admin)
		ctx, cancel := context.WithTimeout(t.Context(), redpandaTestWaitTimeout)
		defer cancel()
		offsets, err := gm.ListGroupOffsets(ctx, topics)
		require.NoError(t, err)
		return offsets
	}

	t.Run("all groups", func(t *testing.T) {
		t.Parallel()

		conf := migrator.GroupsMigratorConfig{}
		offsets := listGroupOffsets(t, conf, []string{topicFoo1, topicFoo2, topicBar})

		expected := []migrator.GroupOffset{
			{Group: groupFoo1, Offset: kadm.Offset{Topic: topicFoo1, Partition: 0, At: 2}},
			{Group: groupFoo1, Offset: kadm.Offset{Topic: topicFoo1, Partition: 1, At: 3}},
			{Group: groupFoo2, Offset: kadm.Offset{Topic: topicFoo2, Partition: 0, At: 1}},
			{Group: groupBar, Offset: kadm.Offset{Topic: topicBar, Partition: 0, At: 2}},
		}
		assert.ElementsMatch(t, expected, offsets)
	})

	t.Run("include pattern", func(t *testing.T) {
		t.Parallel()

		conf := migrator.GroupsMigratorConfig{Enabled: true}
		conf.Include = []*regexp.Regexp{regexp.MustCompile(`^foo-.*$`)}
		offsets := listGroupOffsets(t, conf, []string{topicFoo1, topicFoo2, topicBar})

		expected := []migrator.GroupOffset{
			{Group: groupFoo1, Offset: kadm.Offset{Topic: topicFoo1, Partition: 0, At: 2}},
			{Group: groupFoo1, Offset: kadm.Offset{Topic: topicFoo1, Partition: 1, At: 3}},
			{Group: groupFoo2, Offset: kadm.Offset{Topic: topicFoo2, Partition: 0, At: 1}},
		}
		assert.ElementsMatch(t, expected, offsets)
	})

	t.Run("include exclude pattern", func(t *testing.T) {
		t.Parallel()

		conf := migrator.GroupsMigratorConfig{Enabled: true}
		conf.Include = []*regexp.Regexp{regexp.MustCompile(`^foo-.*$`)}
		conf.Exclude = []*regexp.Regexp{regexp.MustCompile(`^foo-group-2$`)}
		offsets := listGroupOffsets(t, conf, []string{topicFoo1, topicFoo2, topicBar})

		expected := []migrator.GroupOffset{
			{Group: groupFoo1, Offset: kadm.Offset{Topic: topicFoo1, Partition: 0, At: 2}},
			{Group: groupFoo1, Offset: kadm.Offset{Topic: topicFoo1, Partition: 1, At: 3}},
		}
		assert.ElementsMatch(t, expected, offsets)
	})

	t.Run("exclude pattern only", func(t *testing.T) {
		t.Parallel()

		conf := migrator.GroupsMigratorConfig{Enabled: true}
		conf.Exclude = []*regexp.Regexp{regexp.MustCompile(`^bar-.*$`)}
		offsets := listGroupOffsets(t, conf, []string{topicFoo1, topicFoo2, topicBar})

		expected := []migrator.GroupOffset{
			{Group: groupFoo1, Offset: kadm.Offset{Topic: topicFoo1, Partition: 0, At: 2}},
			{Group: groupFoo1, Offset: kadm.Offset{Topic: topicFoo1, Partition: 1, At: 3}},
			{Group: groupFoo2, Offset: kadm.Offset{Topic: topicFoo2, Partition: 0, At: 1}},
		}
		assert.ElementsMatch(t, expected, offsets)
	})

	t.Run("topic filtering", func(t *testing.T) {
		t.Parallel()

		conf := migrator.GroupsMigratorConfig{Enabled: true}
		offsets := listGroupOffsets(t, conf, []string{topicFoo1})

		expected := []migrator.GroupOffset{
			{Group: groupFoo1, Offset: kadm.Offset{Topic: topicFoo1, Partition: 0, At: 2}},
			{Group: groupFoo1, Offset: kadm.Offset{Topic: topicFoo1, Partition: 1, At: 3}},
		}
		assert.ElementsMatch(t, expected, offsets)
	})

	t.Run("no matching topics", func(t *testing.T) {
		t.Parallel()

		conf := migrator.GroupsMigratorConfig{Enabled: true}
		offsets := listGroupOffsets(t, conf, []string{"nonexistent-topic"})

		assert.Empty(t, offsets)
	})
}

func TestIntegrationReadRecordTimestamp(t *testing.T) {
	integration.CheckSkip(t)

	src, _ := startRedpandaSourceAndDestination(t)

	secs := func(n int) time.Time {
		return time.Unix(int64(n), 0)
	}
	records := []struct {
		partition int32
		offset    int64
		timestamp time.Time
		value     string
	}{
		{0, 0, secs(0), "0/0"},
		{0, 1, secs(1), "0/1"},
		{0, 2, secs(2), "0/2"},
		{1, 0, secs(3), "1/0"},
		{1, 1, secs(4), "1/1"},
	}
	for _, rec := range records {
		res := src.Client.ProduceSync(t.Context(), &kgo.Record{
			Topic:     migratorTestTopic,
			Partition: rec.partition,
			Value:     []byte(rec.value),
			Timestamp: rec.timestamp,
		})
		require.NoError(t, res.FirstErr())

		// Verify the record was written to the expected offset
		r, err := res.First()
		assert.NoError(t, err)
		assert.Equal(t, rec.offset, r.Offset)
	}

	t.Run("all offsets", func(t *testing.T) {
		t.Parallel()
		for _, rec := range records {
			ts, err := migrator.ReadRecordTimestamp(t.Context(), src.Client,
				migratorTestTopic, kadm.TopicID{},
				rec.partition, rec.offset, redpandaTestOpTimeout)
			require.NoError(t, err)
			assert.Equal(t, rec.timestamp, ts)
		}
	})

	t.Run("nonexistent offset", func(t *testing.T) {
		t.Parallel()
		_, err := migrator.ReadRecordTimestamp(t.Context(), src.Client,
			migratorTestTopic, kadm.TopicID{},
			990, 999, redpandaTestOpTimeout)
		assert.Error(t, err)
		t.Log(err)
		assert.Contains(t, err.Error(), "partition error")
	})

	t.Run("nonexistent partition", func(t *testing.T) {
		t.Parallel()
		_, err := migrator.ReadRecordTimestamp(t.Context(), src.Client,
			migratorTestTopic, kadm.TopicID{},
			999, 0, redpandaTestOpTimeout)
		assert.Error(t, err)
		t.Log(err)
		assert.Contains(t, err.Error(), "partition error")
	})

	t.Run("nonexistent topic", func(t *testing.T) {
		t.Parallel()
		_, err := migrator.ReadRecordTimestamp(t.Context(), src.Client,
			"nonexistent-topic", kadm.TopicID{},
			0, 0, redpandaTestOpTimeout)
		assert.Error(t, err)
		t.Log(err)
	})

	t.Run("negative offset", func(t *testing.T) {
		t.Parallel()
		_, err := migrator.ReadRecordTimestamp(t.Context(), src.Client,
			migratorTestTopic, kadm.TopicID{},
			999, -1, redpandaTestOpTimeout)
		assert.Error(t, err)
		t.Log(err)
	})
}

func TestIntegrationGroupsOffsetSync(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("Given: source and destination Redpanda clusters")
	src, dst := startRedpandaSourceAndDestination(t)

	type TopicPartitionAt struct {
		Topic     string
		Partition int32
		At        int64
	}
	syncWithMapping := func(t *testing.T, group string, mapping migrator.TopicMapping) []TopicPartitionAt {
		conf := migrator.GroupsMigratorConfig{
			Enabled: true,
		}
		conf.Include = []*regexp.Regexp{regexp.MustCompile(fmt.Sprintf("^%s$", group))}
		gm := migrator.NewGroupsMigratorForTesting(t, conf, src.Client, src.Admin, dst.Admin)

		ctx, cancel := context.WithTimeout(t.Context(), redpandaTestWaitTimeout)
		defer cancel()
		mappings := func() []migrator.TopicMapping {
			return []migrator.TopicMapping{mapping}
		}
		require.NoError(t, gm.Sync(ctx, mappings))

		offsets, err := dst.Admin.FetchOffsets(ctx, group)
		require.NoError(t, err)

		var flat []TopicPartitionAt
		for _, o := range offsets.Sorted() {
			flat = append(flat, TopicPartitionAt{
				Topic:     o.Topic,
				Partition: o.Partition,
				At:        o.At,
			})
		}
		return flat
	}
	sync := func(t *testing.T, group, topic string) []TopicPartitionAt {
		mapping := migrator.TopicMapping{
			Src: migrator.TopicInfo{Topic: topic, Partitions: 2},
			Dst: migrator.TopicInfo{Topic: topic, Partitions: 2},
		}
		return syncWithMapping(t, group, mapping)
	}

	var idSeq atomic.Int32
	idSeq.Store(-1)
	next := func() (group, topic string) {
		id := idSeq.Add(1)

		group = fmt.Sprintf("test_cg_%d", id)
		topic = fmt.Sprintf("test_topic_%d", id)
		src.CreateTopic(topic)
		dst.CreateTopic(topic)

		return
	}

	// monotonic writes records to partition 0 and 1 alternately with monotonic
	// timestamps.
	//
	// p0: 0, 2, 4, 6, 8
	// p1:   1, 3, 5, 7, 9
	monotonic := func(topic string) func(r *kgo.Record) {
		n := 0
		return func(r *kgo.Record) {
			r.Topic = topic
			r.Partition = int32(n) % 2
			r.Timestamp = time.Unix(int64(n), 0)
			n++
		}
	}

	t.Run("monotonic", func(t *testing.T) {
		group, topic := next()
		writeToTopic(src, 10, monotonic(topic))
		writeToTopic(dst, 10, monotonic(topic))

		t.Run("6", func(t *testing.T) {
			src.CommitOffset(group, topic, 0, 6)
			assert.Nil(t, sync(t, group, topic))
		})
		t.Run("0", func(t *testing.T) {
			src.CommitOffset(group, topic, 0, 6)
			assert.Nil(t, sync(t, group, topic))
		})
		for i := 1; i <= 5; i++ {
			t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
				src.CommitOffset(group, topic, 0, i)
				want := []TopicPartitionAt{{Topic: topic, Partition: 0, At: int64(i)}}
				assert.Equal(t, want, sync(t, group, topic), "iteration %d", i)
			})
		}
	})

	t.Run("monotonic sub millisecond timestamp", func(t *testing.T) {
		// monotonic2 writes records to partition 0 with monotonic timestamps
		// with sub millisecond precision generating 4 records per millisecond.
		monotonicSubMillisecond := func(topic string) func(r *kgo.Record) {
			t0 := time.Unix(0, 0)
			delta := time.Millisecond / 4
			n := 0
			return func(r *kgo.Record) {
				r.Topic = topic
				r.Partition = 0
				r.Timestamp = t0.Add(time.Duration(n) * delta)
				n++
			}
		}

		group, topic := next()
		writeToTopic(src, 10, monotonicSubMillisecond(topic))
		writeToTopic(dst, 10, monotonicSubMillisecond(topic))

		for i := 1; i <= 10; i++ {
			t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
				src.CommitOffset(group, topic, 0, i)
				want := []TopicPartitionAt{
					{
						Topic:     topic,
						Partition: 0,
						At:        int64(4*((i-1)/4) + 1),
					}, // 1-4 -> 1, 5-8 -> 5, 9-12 -> 9
				}
				assert.Equal(t, want, sync(t, group, topic), "iteration %d", i)
			})
		}
	})

	t.Run("monotonic data missing", func(t *testing.T) {
		group, topic := next()

		t.Log("Given: data not fully synced")
		writeToTopic(src, 10, monotonic(topic))
		writeToTopic(dst, 5, monotonic(topic))

		t.Log("When: consumer group beyond last synced offset")
		src.CommitOffset(group, topic, 0, 4)

		t.Log("Then: consumer group is synced to the end offset")
		want := []TopicPartitionAt{{Topic: topic, Partition: 0, At: 3}}
		assert.Equal(t, want, sync(t, group, topic))
	})

	t.Run("monotonic truncated", func(t *testing.T) {
		group, topic := next()
		writeToTopic(src, 10, monotonic(topic))
		writeToTopic(dst, 10, monotonic(topic))

		t.Log("Given: consumer group with offsets on both partitions")
		src.CommitOffset(group, topic, 0, 2) // Points to offset 2 in partition 0
		src.CommitOffset(group, topic, 1, 3) // Points to offset 3 in partition 1

		t.Log("When: partition 0 is truncated from beginning")
		ctx, cancel := context.WithTimeout(t.Context(), redpandaTestWaitTimeout)
		defer cancel()
		var offsets kadm.Offsets
		offsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: 2})
		resp, err := src.Admin.DeleteRecords(ctx, offsets)
		require.NoError(t, err)
		require.NoError(t, resp.Error())

		t.Log("Then: only partition 1 is synced")
		want := []TopicPartitionAt{{Topic: topic, Partition: 1, At: 3}}
		assert.Equal(t, want, sync(t, group, topic))
	})

	t.Run("non-monotonic", func(t *testing.T) {
		group, topic := next()

		incTimestamp := func(d time.Duration) func(r *kgo.Record) {
			return func(r *kgo.Record) {
				r.Timestamp = r.Timestamp.Add(d)
			}
		}

		// Source: monotonic timestamps to partition 0
		writeToTopic(src, 5, monotonic(topic), ProduceToPartitionOpt(0))
		// Destination: non-monotonic timestamps creating overlapping ranges
		// Batch 1: offsets 0-2, timestamps 3-5
		writeToTopic(dst, 3, monotonic(topic), ProduceToPartitionOpt(0),
			incTimestamp(3*time.Second),
		)
		// Batch 2: offsets 3-5, timestamps 2-4 (overlapping with batch 1)
		writeToTopic(dst, 3, monotonic(topic), ProduceToPartitionOpt(0),
			incTimestamp(2*time.Second),
		)

		t.Run("timestamp 3", func(t *testing.T) {
			src.CommitOffset(group, topic, 0, 3)
			want := []TopicPartitionAt{{Topic: topic, Partition: 0, At: 0}} // First occurrence of timestamp 3
			assert.Equal(t, want, sync(t, group, topic))
		})

		t.Run("timestamp 4", func(t *testing.T) {
			src.CommitOffset(group, topic, 0, 4)
			want := []TopicPartitionAt{{Topic: topic, Partition: 0, At: 1}} // First occurrence of timestamp 4
			assert.Equal(t, want, sync(t, group, topic))
		})
	})

	t.Run("mapping", func(t *testing.T) {
		group, topic := next()

		dstTopic := "dst_" + topic
		dst.CreateTopic(dstTopic)

		writeToTopic(src, 5, monotonic(topic))
		writeToTopic(dst, 5, monotonic(dstTopic))

		src.CommitOffset(group, topic, 0, 2)

		mapping := migrator.TopicMapping{
			Src: migrator.TopicInfo{Topic: topic, Partitions: 2},
			Dst: migrator.TopicInfo{Topic: dstTopic, Partitions: 2},
		}
		want := []TopicPartitionAt{{Topic: dstTopic, Partition: 0, At: 2}}
		assert.Equal(t, want, syncWithMapping(t, group, mapping))
	})

	t.Run("no rewind dst", func(t *testing.T) {
		group, topic := next()

		writeToTopic(src, 5, monotonic(topic))
		writeToTopic(dst, 10, monotonic(topic))

		src.CommitOffset(group, topic, 0, 2)
		dst.CommitOffset(group, topic, 0, 5)

		want := []TopicPartitionAt{{Topic: topic, Partition: 0, At: 5}}
		assert.Equal(t, want, sync(t, group, topic))
	})

	t.Run("no rewind dst mapping", func(t *testing.T) {
		group, topic := next()

		dstTopic := "dst_" + topic
		dst.CreateTopic(dstTopic)

		writeToTopic(src, 5, monotonic(topic))
		writeToTopic(dst, 10, monotonic(dstTopic))

		src.CommitOffset(group, topic, 0, 2)
		dst.CommitOffset(group, dstTopic, 0, 5)

		mapping := migrator.TopicMapping{
			Src: migrator.TopicInfo{Topic: topic, Partitions: 2},
			Dst: migrator.TopicInfo{Topic: dstTopic, Partitions: 2},
		}
		want := []TopicPartitionAt{{Topic: dstTopic, Partition: 0, At: 5}}
		assert.Equal(t, want, syncWithMapping(t, group, mapping))
	})
}
