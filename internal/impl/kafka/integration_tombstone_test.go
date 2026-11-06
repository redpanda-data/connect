// Copyright 2026 Redpanda Data, Inc.
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

package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/redpandatest"
)

// consumeAllRecords reads count records from the start of topic.
func consumeAllRecords(t *testing.T, brokerAddr, topic string, count int) []*kgo.Record {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer client.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var records []*kgo.Record
	for len(records) < count {
		fetches := client.PollFetches(ctx)
		require.NoError(t, ctx.Err(), "timed out with %d of %d records", len(records), count)
		require.Empty(t, fetches.Errors())
		records = append(records, fetches.Records()...)
	}
	return records
}

func runStreamYAML(t *testing.T, yaml string) {
	t.Helper()

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(yaml))
	require.NoError(t, sb.SetLoggerYAML(`level: OFF`))
	stream, err := sb.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := stream.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = stream.StopWithin(5 * time.Second)
		<-done
	})
}

func TestRedpandaOutputTombstoneIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	rp, err := redpandatest.StartRedpanda(t, true)
	require.NoError(t, err)

	t.Run("config only", func(t *testing.T) {
		t.Parallel()

		topic := "tombstone-config-only"
		runStreamYAML(t, fmt.Sprintf(`
input:
  generate:
    count: 3
    interval: ""
    mapping: |
      let n = counter()
      meta key = "k%%d".format($n)
      meta delete = $n == 2
      root = if $n == 3 { "" } else { "v%%d".format($n) }

output:
  redpanda:
    seed_brokers: [ %s ]
    topic: %s
    key: ${! @key }
    tombstone: ${! @delete }
`, rp.BrokerAddr, topic))

		records := consumeAllRecords(t, rp.BrokerAddr, topic, 3)
		require.Len(t, records, 3)

		byKey := map[string]*kgo.Record{}
		for _, r := range records {
			byKey[string(r.Key)] = r
		}
		assert.Equal(t, []byte("v1"), byKey["k1"].Value)
		assert.Nil(t, byKey["k2"].Value, "expected a tombstone")
		require.NotNil(t, byKey["k3"].Value, "an empty payload must not become a tombstone")
		assert.Empty(t, byKey["k3"].Value)
	})

	t.Run("mirror keeps tombstones", func(t *testing.T) {
		t.Parallel()

		src, dst := "tombstone-mirror-src", "tombstone-mirror-dst"

		client, err := kgo.NewClient(kgo.SeedBrokers(rp.BrokerAddr), kgo.AllowAutoTopicCreation())
		require.NoError(t, err)
		defer client.Close()
		require.NoError(t, client.ProduceSync(t.Context(),
			&kgo.Record{Topic: src, Key: []byte("a"), Value: []byte("1")},
			&kgo.Record{Topic: src, Key: []byte("b"), Value: nil},
			&kgo.Record{Topic: src, Key: []byte("c"), Value: []byte{}},
		).FirstErr())

		runStreamYAML(t, fmt.Sprintf(`
input:
  redpanda:
    seed_brokers: [ %[1]s ]
    topics: [ %[2]s ]
    consumer_group: tombstone_mirror
    start_offset: earliest

output:
  redpanda:
    seed_brokers: [ %[1]s ]
    topic: %[3]s
    key: ${! @kafka_key }
    tombstone: ${! @kafka_tombstone_message }
`, rp.BrokerAddr, src, dst))

		records := consumeAllRecords(t, rp.BrokerAddr, dst, 3)
		require.Len(t, records, 3)

		byKey := map[string]*kgo.Record{}
		for _, r := range records {
			byKey[string(r.Key)] = r
		}
		assert.Equal(t, []byte("1"), byKey["a"].Value)
		assert.Nil(t, byKey["b"].Value, "expected the tombstone to be mirrored")
		require.NotNil(t, byKey["c"].Value, "an empty record must not become a tombstone")
		assert.Empty(t, byKey["c"].Value)
	})
}
