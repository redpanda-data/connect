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

package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka"

	"github.com/stretchr/testify/require"
)

func TestIntegrationCache(t *testing.T) {
	integration.CheckSkip(t)

	brokerAddr, kafkaPortStr := startRedpanda(t)

	require.Eventually(t, func() bool {
		return createKafkaTopic(t.Context(), brokerAddr, "testingconnection", 1) == nil
	}, time.Minute, time.Second)

	makeCache := func(p ...int32) (service.Cache, error) {
		uuid := uuid.Must(uuid.NewV4()).String()
		partitions := int32(1)
		if len(p) > 0 {
			partitions = p[0]
		}
		// NOTE: In real life these should be compacted topics
		err := createKafkaTopic(t.Context(), brokerAddr, uuid, partitions)
		if err != nil {
			return nil, err
		}
		return kafka.NewRedpandaCache(
			[]kgo.Opt{
				kgo.SeedBrokers("localhost:" + kafkaPortStr),
			},
			"topic-"+uuid,
		)
	}

	t.Run("empty data fetch", func(t *testing.T) {
		cache, err := makeCache()
		require.NoError(t, err)
		_, err = cache.Get(t.Context(), "foo")
		require.ErrorIs(t, err, service.ErrKeyNotFound)
	})
	t.Run("single record", func(t *testing.T) {
		cache, err := makeCache()
		require.NoError(t, err)
		require.NoError(t, cache.Set(t.Context(), "foo", []byte("bar"), nil))
		value, err := cache.Get(t.Context(), "foo")
		require.NoError(t, err)
		require.Equal(t, []byte("bar"), value)
	})
	t.Run("other records", func(t *testing.T) {
		cache, err := makeCache()
		require.NoError(t, err)
		require.NoError(t, cache.Set(t.Context(), "one", []byte("1"), nil))
		require.NoError(t, cache.Set(t.Context(), "two", []byte("2"), nil))
		require.NoError(t, cache.Set(t.Context(), "three", []byte("3"), nil))
		for k, v := range map[string]string{"one": "1", "two": "2", "three": "3"} {
			value, err := cache.Get(t.Context(), k)
			require.NoError(t, err)
			require.Equal(t, []byte(v), value)
		}
	})
	t.Run("many records", func(t *testing.T) {
		for _, partitions := range []int32{1, 8} {
			cache, err := makeCache(partitions)
			require.NoError(t, err)
			require.NoError(t, cache.Set(t.Context(), "foo", []byte("1"), nil))
			require.NoError(t, cache.Set(t.Context(), "foo", []byte("2"), nil))
			require.NoError(t, cache.Set(t.Context(), "foo", []byte("3"), nil))
			value, err := cache.Get(t.Context(), "foo")
			require.NoError(t, err)
			require.Equal(t, []byte("3"), value)
			require.NoError(t, cache.Set(t.Context(), "foo", []byte("4"), nil))
			value, err = cache.Get(t.Context(), "foo")
			require.NoError(t, err)
			require.Equal(t, []byte("4"), value)
		}
	})
	t.Run("tombstone records", func(t *testing.T) {
		cache, err := makeCache()
		require.NoError(t, err)
		require.NoError(t, cache.Set(t.Context(), "foo", []byte("bar"), nil))
		require.NoError(t, cache.Delete(t.Context(), "foo"))
		_, err = cache.Get(t.Context(), "foo")
		require.ErrorIs(t, err, service.ErrKeyNotFound)
	})
}

func TestIntegrationCacheStandardized(t *testing.T) {
	integration.CheckSkip(t)

	brokerAddr, kafkaPortStr := startRedpanda(t)

	require.Eventually(t, func() bool {
		return createKafkaTopic(t.Context(), brokerAddr, "testingconnection", 1) == nil
	}, time.Minute, time.Second)

	suite := integration.CacheTests(
		integration.CacheTestOpenClose(),
		integration.CacheTestMissingKey(),
		// This cache doesn't support add operations
		// integration.CacheTestDoubleAdd(),
		integration.CacheTestDelete(),
		integration.CacheTestGetAndSet(50),
	)
	template := `
cache_resources:
  - label: testcache
    redpanda:
      seed_brokers: ["localhost:$PORT"]
      topic: "topic-$ID"
`
	t.Run("single partition", func(t *testing.T) {
		suite.Run(
			t, template,
			integration.CacheTestOptPort(kafkaPortStr),
			integration.CacheTestOptPreTest(func(t testing.TB, _ context.Context, vars *integration.CacheTestConfigVars) {
				err := createKafkaTopic(t.Context(), brokerAddr, vars.ID, 1)
				require.NoError(t, err)
			}),
		)
	})
	t.Run("many partitions", func(t *testing.T) {
		suite.Run(
			t, template,
			integration.CacheTestOptPort(kafkaPortStr),
			integration.CacheTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.CacheTestConfigVars) {
				err := createKafkaTopic(ctx, brokerAddr, vars.ID, 16)
				require.NoError(t, err)
			}),
		)
	})
}
