// Copyright 2025 Redpanda Data, Inc.
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

package migrator_test

import (
	"context"
	"encoding/binary"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka/redpandatest"
	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/migrator"
)

const migratorTestTopic = "test_topic"

// EmbeddedRedpandaCluster represents a Redpanda cluster with client and admin access.
type EmbeddedRedpandaCluster struct {
	redpandatest.RedpandaEndpoints
	Client *kgo.Client
	Admin  *kadm.Client
	t      *testing.T
}

// startRedpandaSourceAndDestination starts two containers for Redpanda and
// returns the EmbeddedRedpandaCluster for each container.
func startRedpandaSourceAndDestination(t *testing.T) (src, dst EmbeddedRedpandaCluster) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	src = EmbeddedRedpandaCluster{t: t}
	dst = EmbeddedRedpandaCluster{t: t}

	src.RedpandaEndpoints, err = redpandatest.StartRedpanda(t, pool, true, false)
	require.NoError(t, err)

	dst.RedpandaEndpoints, err = redpandatest.StartRedpanda(t, pool, true, false)
	require.NoError(t, err)

	src.Client, err = kgo.NewClient(
		kgo.SeedBrokers(src.BrokerAddr),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	t.Cleanup(func() { src.Client.Close() })

	dst.Client, err = kgo.NewClient(
		kgo.SeedBrokers(dst.BrokerAddr),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ConsumeTopics(migratorTestTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { dst.Client.Close() })

	src.Admin = kadm.NewClient(src.Client)
	dst.Admin = kadm.NewClient(dst.Client)

	src.CreateTopic(migratorTestTopic)

	return src, dst
}

const (
	redpandaTestOpTimeout   = time.Second
	redpandaTestWaitTimeout = 5 * time.Second
)

// CreateTopic creates a topic if it doesn't exist
func (e *EmbeddedRedpandaCluster) CreateTopic(topic string) {
	e.t.Helper()
	e.CreateTopicWithConfigs(topic, nil)
}

func (e *EmbeddedRedpandaCluster) CreateTopicWithConfigs(topic string, configs map[string]*string) {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(e.t.Context(), redpandaTestOpTimeout)
	defer cancel()

	_, err := e.Admin.CreateTopic(ctx, 2, 1, configs, topic)
	if err != nil {
		e.t.Errorf("Failed to create topic %s: %v", topic, err)
	}
}

// CreateACLAllow creates an ALLOW ACL for a principal and operation on a topic.
func (e *EmbeddedRedpandaCluster) CreateACLAllow(topic, principal string, op kmsg.ACLOperation) {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(e.t.Context(), redpandaTestOpTimeout)
	defer cancel()

	b := kadm.NewACLs().
		Topics(topic).
		ResourcePatternType(kadm.ACLPatternLiteral).
		Operations(op).
		Allow(principal)
	_, err := e.Admin.CreateACLs(ctx, b)
	require.NoError(e.t, err)
}

// DescribeTopicACLs returns ACLs for a topic.
func (e *EmbeddedRedpandaCluster) DescribeTopicACLs(topic string) ([]kadm.DescribedACL, error) {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(e.t.Context(), redpandaTestOpTimeout)
	defer cancel()

	return migrator.DescribeACLs(ctx, e.Admin, topic)
}

// TopicConfig returns the value of the configuration entry with key `key` for
// topic `topic`, or nil if the key is not found.
func (e *EmbeddedRedpandaCluster) TopicConfig(topic, key string) *string {
	e.t.Helper()
	_, rc, err := migrator.TopicDetailsWithClient(e.t.Context(), e.Admin, topic)
	if err != nil {
		e.t.Errorf("Failed to get topic configs for topic %s: %v", topic, err)
	}
	for _, cfg := range rc.Configs {
		if cfg.Key == key {
			return cfg.Value
		}
	}
	return nil
}

// Produce sends a message with the given value to the specified topic
func (e *EmbeddedRedpandaCluster) Produce(topic string, value []byte, opts ...func(*kgo.Record)) {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(e.t.Context(), redpandaTestOpTimeout)
	defer cancel()

	record := &kgo.Record{
		Topic: topic,
		Value: value,
	}
	for _, opt := range opts {
		opt(record)
	}
	require.NoError(e.t, e.Client.ProduceSync(ctx, record).FirstErr())
}

func ProduceToTopicOpt(topic string) func(*kgo.Record) {
	return func(r *kgo.Record) {
		r.Topic = topic
	}
}

func ProduceToPartitionOpt(partition int) func(*kgo.Record) {
	return func(r *kgo.Record) {
		r.Partition = int32(partition)
	}
}

func ProduceWithSchemaIDOpt(schemaID int) func(*kgo.Record) {
	return func(r *kgo.Record) {
		hdr := make([]byte, 5)
		hdr[0] = 0
		binary.BigEndian.PutUint32(hdr[1:], uint32(schemaID))
		r.Value = append(hdr, r.Value...)
	}
}

func (e *EmbeddedRedpandaCluster) CommitOffset(group, topic string, part, at int) {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(e.t.Context(), redpandaTestOpTimeout)
	defer cancel()

	var offs kadm.Offsets
	offs.Add(kadm.Offset{
		Topic:     topic,
		Partition: int32(part),
		At:        int64(at),
	})
	_, err := e.Admin.CommitOffsets(ctx, group, offs)
	require.NoError(e.t, err)
}

// writeToTopic produces num messages to a topic.
func writeToTopic(cluster EmbeddedRedpandaCluster, numMessages int, opts ...func(*kgo.Record)) {
	for i := range numMessages {
		cluster.Produce(migratorTestTopic, []byte(strconv.Itoa(i)), opts...)
	}
	cluster.t.Logf("Successfully wrote %d messages to topic %s", numMessages, migratorTestTopic)
}

// assertTopicContent asserts that the specified number of messages created with
// the writeToTopic function are present in the topic.
func assertTopicContent(cluster EmbeddedRedpandaCluster, numMessages int) {
	assertTopicContentWithGoldenFunc(cluster, numMessages, goldenIntMsg)
}

// assertTopicContent asserts that the specified number of messages matching the
// golden function are present in the topic.
func assertTopicContentWithGoldenFunc(cluster EmbeddedRedpandaCluster, numMessages int, golden func(int) []byte) {
	ctx := cluster.t.Context()
	t := cluster.t
	client := cluster.Client

	messages := make([][]byte, 0, numMessages)
	for len(messages) < numMessages {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			require.NoError(t, errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			messages = append(messages, r.Value)
		})

		select {
		case <-ctx.Done():
			require.Fail(t, "Timed out waiting for messages")
			return
		default:
			if len(messages) < numMessages {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	t.Logf("Successfully read %d messages from topic %s", len(messages), migratorTestTopic)

	expected := make([][]byte, 0, numMessages)
	for i := 0; i < numMessages; i++ {
		expected = append(expected, golden(i))
	}
	assert.ElementsMatch(t, expected, messages)
}

func goldenIntMsg(i int) []byte {
	return []byte(strconv.Itoa(i))
}
