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

func TestRedpandaMigratorInputHappyFlow(t *testing.T) {
	dummyTopic := "foobar"
	dummyGroup := "foobar_cg"
	dummyMetadata := "foobar_metadata"

	c, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(2, dummyTopic),
	)
	require.NoError(t, err)

	defer c.Close()

	func() {
		cl, err := kgo.NewClient(
			kgo.DefaultProduceTopic(dummyTopic),
			kgo.SeedBrokers(c.ListenAddrs()...),
		)
		require.NoError(t, err)
		defer cl.Close()

		for i := 0; i < 6; i++ {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			rec := kgo.Record{
				Partition: int32(i % 2),
				Value:     []byte(strconv.Itoa(i)),
				Timestamp: time.UnixMilli(int64(i)),
			}
			err := cl.ProduceSync(ctx, &rec).FirstErr()
			cancel()
			require.NoError(t, err)
		}

		adm := kadm.NewClient(cl)

		var offsets kadm.Offsets
		offsets.Add(kadm.Offset{
			Topic:     dummyTopic,
			Partition: 0,
			At:        1,
			Metadata:  dummyMetadata,
		})
		offsets.Add(kadm.Offset{
			Topic:     dummyTopic,
			Partition: 1,
			At:        3,
			Metadata:  dummyMetadata,
		})

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		_, err = adm.CommitOffsets(ctx, dummyGroup, offsets)
		cancel()
		require.NoError(t, err)
	}()

	mgr := service.MockResources()
	input := redpandaMigratorOffsetsInput{
		clientOpts:   []kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...)},
		topics:       []string{dummyTopic},
		pollInterval: 1 * time.Second,
		msgChan:      make(chan *service.Message),
		log:          mgr.Logger(),
	}

	err = input.Connect(t.Context())
	require.NoError(t, err)

	var msgs []*service.Message
	select {
	case msg := <-input.msgChan:
		require.NotNil(t, msg)
		msgs = append(msgs, msg)
	case <-time.After(20 * time.Second):
		assert.Len(t, msgs, 2)
	}

	for _, msg := range msgs {
		topic, ok := msg.MetaGet("kafka_offset_topic")
		assert.True(t, ok)
		assert.Equal(t, dummyTopic, topic)

		group, ok := msg.MetaGet("kafka_offset_group")
		assert.True(t, ok)
		assert.Equal(t, dummyGroup, group)

		metadata, ok := msg.MetaGet("kafka_offset_metadata")
		assert.True(t, ok)
		assert.Equal(t, dummyMetadata, metadata)

		timestamp, ok := msg.MetaGet("kafka_offset_commit_timestamp")
		assert.True(t, ok)

		hwm, ok := msg.MetaGet("kafka_is_high_watermark")
		assert.True(t, ok)

		partition, ok := msg.MetaGet("kafka_offset_partition")
		assert.True(t, ok)
		switch partition {
		case "0":
			assert.Equal(t, "1", timestamp)
			assert.Equal(t, "false", hwm)
		case "1":
			assert.Equal(t, "2", timestamp)
			assert.Equal(t, "true", hwm)
		default:
			require.Fail(t, "unexpected partition", partition)
		}
	}

	println(msgs)
}
