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
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka/redpandatest"
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

	src.Client, err = kgo.NewClient(kgo.SeedBrokers(src.BrokerAddr))
	require.NoError(t, err)
	t.Cleanup(func() { src.Client.Close() })

	dst.Client, err = kgo.NewClient(
		kgo.SeedBrokers(dst.BrokerAddr),
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

const redpandaTestOpTimeout = time.Second

// CreateTopic creates a topic if it doesn't exist
func (e *EmbeddedRedpandaCluster) CreateTopic(topic string) {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(e.t.Context(), redpandaTestOpTimeout)
	defer cancel()

	_, err := e.Admin.CreateTopic(ctx, 1, 1, nil, topic)
	if err != nil {
		e.t.Errorf("Failed to create topic %s: %v", topic, err)
	}
}

// Produce sends a message with the given value to the specified topic
func (e *EmbeddedRedpandaCluster) Produce(topic string, value []byte) {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(e.t.Context(), redpandaTestOpTimeout)
	defer cancel()

	record := &kgo.Record{
		Topic: topic,
		Value: value,
	}
	require.NoError(e.t, e.Client.ProduceSync(ctx, record).FirstErr())
}

// writeToTopic produces the specified number of messages to a topic.
func writeToTopic(cluster EmbeddedRedpandaCluster, numMessages int) {
	for i := range numMessages {
		cluster.Produce(migratorTestTopic, []byte(strconv.Itoa(i)))
	}
	cluster.t.Logf("Successfully wrote %d messages to topic %s", numMessages, migratorTestTopic)
}

// assertTopicContent asserts that the specified number of messages created with
// the writeToTopic function are present in the topic.
func assertTopicContent(cluster EmbeddedRedpandaCluster, numMessages int) {
	ctx := cluster.t.Context()
	t := cluster.t
	client := cluster.Client

	messages := make([]string, 0, numMessages)
	for len(messages) < numMessages {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			require.NoError(t, errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			messages = append(messages, string(r.Value))
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

	expected := make([]string, 0, numMessages)
	for i := 0; i < numMessages; i++ {
		expected = append(expected, strconv.Itoa(i))
	}
	assert.ElementsMatch(t, expected, messages)
}
