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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func createUpdateConsumerGroup(t *testing.T, client *kadm.Client, group string, offset kadm.Offset) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	var offsets kadm.Offsets
	offsets.Add(offset)
	_, err := client.CommitOffsets(ctx, group, offsets)
	require.NoError(t, err)
}

func populateKafkaBroker(t *testing.T, client *kgo.Client, topic, metadata, group string) {
	for i := 0; i < 6; i++ {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		rec := kgo.Record{
			Topic:     topic,
			Partition: int32(i % 2),
			Value:     []byte(strconv.Itoa(i)),
			Timestamp: time.UnixMilli(int64(i)),
		}
		err := client.ProduceSync(ctx, &rec).FirstErr()
		require.NoError(t, err)
		cancel()
	}

	adminClient := kadm.NewClient(client)
	createUpdateConsumerGroup(t, adminClient, group, kadm.Offset{
		Topic:     topic,
		Partition: 0,
		At:        0,
		Metadata:  metadata,
	})
	createUpdateConsumerGroup(t, adminClient, group, kadm.Offset{
		Topic:     topic,
		Partition: 1,
		At:        3,
		Metadata:  metadata,
	})
}

func assertCGUpdate(t *testing.T, msg *service.Message, topic, group, metadata string, timestamp int64, isHWM bool) {
	topicMeta, ok := msg.MetaGet("kafka_offset_topic")
	assert.True(t, ok)
	assert.Equal(t, topicMeta, topic)

	groupMeta, ok := msg.MetaGet("kafka_offset_group")
	assert.True(t, ok)
	assert.Equal(t, group, groupMeta)

	metadataMeta, ok := msg.MetaGet("kafka_offset_metadata")
	assert.True(t, ok)
	assert.Equal(t, metadata, metadataMeta)

	timestampMeta, ok := msg.MetaGet("kafka_offset_commit_timestamp")
	assert.True(t, ok)
	assert.Equal(t, strconv.FormatInt(timestamp, 10), timestampMeta)

	highWatermarkMeta, ok := msg.MetaGet("kafka_is_high_watermark")
	assert.True(t, ok)
	assert.Equal(t, strconv.FormatBool(isHWM), highWatermarkMeta)
}

func TestRedpandaMigratorOffsetsInput(t *testing.T) {
	dummyTopic := "foobar"
	dummyGroup := "foobar_cg"
	dummyMetadata := "foobar_metadata"

	broker, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(2, dummyTopic),
	)
	require.NoError(t, err)
	defer broker.Close()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker.ListenAddrs()...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	defer client.Close()

	populateKafkaBroker(t, client, dummyTopic, dummyMetadata, dummyGroup)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(`
redpanda_migrator_offsets:
  seed_brokers: %v
  topics: [ %s ]
  poll_interval: 3s
`, broker.ListenAddrs(), dummyTopic)))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))

	msgChan := make(chan *service.Message)
	err = streamBuilder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		msgChan <- msg
		return nil
	})
	require.NoError(t, err)

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	go func() {
		err := stream.Run(t.Context())
		require.NoError(t, err)
		close(msgChan)
	}()

	defer func() {
		err = stream.StopWithin(3 * time.Second)
		require.NoError(t, err)
	}()

	var msgs []*service.Message
	// We should only get two messages containing consumer group updates, one for each partition.
	for range 2 {
		select {
		case msg := <-msgChan:
			msgs = append(msgs, msg)
		case <-time.After(30 * time.Second):
			require.Fail(t, "timed out waiting for stream to finish")
		}
	}

	for _, msg := range msgs {
		partition, ok := msg.MetaGet("kafka_offset_partition")
		assert.True(t, ok)
		switch partition {
		case "0":
			assertCGUpdate(t, msg, dummyTopic, dummyGroup, dummyMetadata, 0, false)
		case "1":
			assertCGUpdate(t, msg, dummyTopic, dummyGroup, dummyMetadata, 5, true)
		default:
			require.Fail(t, "unexpected partition", partition)
		}
	}

	// Advance the consumer group offset on partition 0 and assert that we get a new consumer group update.
	adminClient := kadm.NewClient(client)
	createUpdateConsumerGroup(t, adminClient, dummyGroup, kadm.Offset{
		Topic:     dummyTopic,
		Partition: 0,
		At:        1,
		Metadata:  dummyMetadata,
	})

	var msg *service.Message
	select {
	case msg = <-msgChan:
	case <-time.After(30 * time.Second):
		require.Fail(t, "timed out waiting for stream to finish")
	}

	partition, ok := msg.MetaGet("kafka_offset_partition")
	assert.True(t, ok)
	assert.Equal(t, "0", partition)
	assertCGUpdate(t, msg, dummyTopic, dummyGroup, dummyMetadata, 2, false)
}

func TestRedpandaMigratorOffsetsInputTruncatedTopic(t *testing.T) {
	dummyTopic := "foobar"
	dummyGroup := "foobar_cg"
	dummyMetadata := "foobar_metadata"

	broker, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(2, dummyTopic),
	)
	require.NoError(t, err)
	defer broker.Close()

	t.Logf("Broker address: %s", broker.ListenAddrs()[0])

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker.ListenAddrs()...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	defer client.Close()

	populateKafkaBroker(t, client, dummyTopic, dummyMetadata, dummyGroup)

	adminClient := kadm.NewClient(client)

	// Delete the first record in partition 0, so Migrator is forced to skip the consumer group update.
	var offsets kadm.Offsets
	offsets.Add(kadm.Offset{
		Topic:     dummyTopic,
		Partition: 0,
		At:        1,
	})
	resp, err := adminClient.DeleteRecords(t.Context(), offsets)
	require.NoError(t, err)
	require.NoError(t, resp.Error())

	// Reset the internal client cache to ensure the topic is reloaded. I'm not entirely sure why this is necessary to
	// ensure that the call to `DeleteRecords` is persisted. The streamBuilder initialises a new client under the hood,
	// so it should not care about the current `client`	instance.
	client.PurgeTopicsFromClient(dummyTopic)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(`
redpanda_migrator_offsets:
  seed_brokers: %v
  topics: [ %s ]
  poll_interval: 3s
`, broker.ListenAddrs(), dummyTopic)))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))

	msgChan := make(chan *service.Message)
	err = streamBuilder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		msgChan <- msg
		return nil
	})
	require.NoError(t, err)

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	go func() {
		err := stream.Run(t.Context())
		require.NoError(t, err)
		close(msgChan)
	}()

	defer func() {
		err = stream.StopWithin(3 * time.Second)
		require.NoError(t, err)
	}()

	var msg *service.Message
	select {
	case msg = <-msgChan:
	case <-time.After(30 * time.Second):
		require.Fail(t, "timed out waiting for stream to finish")
	}

	partition, ok := msg.MetaGet("kafka_offset_partition")
	assert.True(t, ok)
	assert.Equal(t, "1", partition)
	assertCGUpdate(t, msg, dummyTopic, dummyGroup, dummyMetadata, 5, true)
}
