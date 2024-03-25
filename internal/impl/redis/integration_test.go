package redis

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestIntegrationRedis(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("redis", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	urlStr := fmt.Sprintf("tcp://localhost:%v", resource.GetPort("6379/tcp"))
	uri, err := url.Parse(urlStr)
	if err != nil {
		t.Fatal(err)
	}

	client := redis.NewClient(&redis.Options{
		Addr:    uri.Host,
		Network: uri.Scheme,
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return client.Ping(context.Background()).Err()
	}))

	// STREAMS
	t.Run("streams", func(t *testing.T) {
		t.Parallel()
		template := `
output:
  redis_streams:
    url: tcp://localhost:$PORT
    stream: ${! meta("routing_stream_prefix") }-stream-$ID
    body_key: body
    max_length: 0
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]
    batching:
      count: $OUTPUT_BATCH_COUNT
  processors:
    - bloblang: meta routing_stream_prefix = "bar"

input:
  redis_streams:
    url: tcp://localhost:$PORT
    body_key: body
    streams: [ bar-stream-$ID ]
    limit: 10
    client_id: client-input-$ID
    consumer_group: group-$ID
`
		suite := integration.StreamTests(
			integration.StreamTestOpenClose(),
			integration.StreamTestMetadata(),
			integration.StreamTestMetadataFilter(),
			integration.StreamTestSendBatch(10),
			integration.StreamTestSendBatches(20, 100, 1),
			integration.StreamTestStreamSequential(1000),
			integration.StreamTestStreamParallel(1000),
			integration.StreamTestStreamParallelLossy(1000),
			integration.StreamTestStreamParallelLossyThroughReconnect(100),
			integration.StreamTestSendBatchCount(10),
		)
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
		)
		t.Run("with max in flight", func(t *testing.T) {
			t.Parallel()
			suite.Run(
				t, template,
				integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
				integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
				integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
				integration.StreamTestOptMaxInFlight(10),
			)
		})
	})

	t.Run("pubsub", func(t *testing.T) {
		t.Parallel()
		template := `
output:
  redis_pubsub:
    url: tcp://localhost:$PORT
    channel: channel-$ID
    max_in_flight: $MAX_IN_FLIGHT
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  redis_pubsub:
    url: tcp://localhost:$PORT
    channels: [ channel-$ID ]
`
		suite := integration.StreamTests(
			integration.StreamTestOpenClose(),
			integration.StreamTestSendBatch(10),
			integration.StreamTestSendBatches(20, 100, 1),
			integration.StreamTestStreamSequential(100),
			integration.StreamTestStreamParallel(100),
			integration.StreamTestStreamParallelLossy(100),
			integration.StreamTestSendBatchCount(10),
		)
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
		)
		t.Run("with max in flight", func(t *testing.T) {
			t.Parallel()
			suite.Run(
				t, template,
				integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
				integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
				integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
				integration.StreamTestOptMaxInFlight(10),
			)
		})
	})

	t.Run("list", func(t *testing.T) {
		t.Parallel()
		template := `
output:
  redis_list:
    url: tcp://localhost:$PORT
    key: key-$ID
    max_in_flight: $MAX_IN_FLIGHT
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  redis_list:
    url: tcp://localhost:$PORT
    key: key-$ID
`
		suite := integration.StreamTests(
			integration.StreamTestOpenClose(),
			integration.StreamTestSendBatch(10),
			integration.StreamTestSendBatches(20, 100, 1),
			integration.StreamTestStreamSequential(1000),
			integration.StreamTestStreamParallel(1000),
			integration.StreamTestStreamParallelLossy(1000),
			integration.StreamTestSendBatchCount(10),
		)
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
		)
		t.Run("with max in flight", func(t *testing.T) {
			t.Parallel()
			suite.Run(
				t, template,
				integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
				integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
				integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
				integration.StreamTestOptMaxInFlight(10),
			)
		})
	})

	// SCAN
	t.Run("scan", func(t *testing.T) {
		t.Parallel()
		template := `
input:
  redis_scan:
    url: 'tcp://localhost:$PORT'
    match: '*'
  processors:
    - mapping: 'root = this.value'

output:
  cache:
    target: rcache
    key: 'foo-${! counter() }'

cache_resources:
  - label: rcache
    redis:
      url: 'tcp://localhost:$PORT'
`
		suite := integration.StreamTests(
			integration.StreamTestStreamIsolated(1000),
		)
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
		)
	})

	// HASH
	t.Run("hash", func(t *testing.T) {
		t.Parallel()
		template := `
output:
  redis_hash:
    url: tcp://localhost:$PORT
    key: $ID-${! json("id") }
    fields:
      content: ${! content() }
`
		hashGetFn := func(ctx context.Context, testID, id string) (string, []string, error) {
			client := redis.NewClient(&redis.Options{
				Addr:    fmt.Sprintf("localhost:%v", resource.GetPort("6379/tcp")),
				Network: "tcp",
			})
			key := testID + "-" + id
			res, err := client.HGet(ctx, key, "content").Result()
			if err != nil {
				return "", nil, err
			}
			return res, nil, nil
		}
		suite := integration.StreamTests(
			integration.StreamTestOutputOnlySendSequential(10, hashGetFn),
			integration.StreamTestOutputOnlySendBatch(10, hashGetFn),
			integration.StreamTestOutputOnlyOverride(hashGetFn),
		)
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
		)
	})
}

func BenchmarkIntegrationRedis(b *testing.B) {
	integration.CheckSkip(b)

	pool, err := dockertest.NewPool("")
	require.NoError(b, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("redis", "latest", nil)
	require.NoError(b, err)
	b.Cleanup(func() {
		assert.NoError(b, pool.Purge(resource))
	})

	urlStr := fmt.Sprintf("tcp://localhost:%v", resource.GetPort("6379/tcp"))
	uri, err := url.Parse(urlStr)
	if err != nil {
		b.Fatal(err)
	}

	client := redis.NewClient(&redis.Options{
		Addr:    uri.Host,
		Network: uri.Scheme,
	})

	_ = resource.Expire(900)
	require.NoError(b, pool.Retry(func() error {
		return client.Ping(context.Background()).Err()
	}))

	// STREAMS
	b.Run("streams", func(b *testing.B) {
		template := `
output:
  redis_streams:
    url: tcp://localhost:$PORT
    stream: stream-$ID
    body_key: body
    max_length: 0
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]

input:
  redis_streams:
    url: tcp://localhost:$PORT
    body_key: body
    streams: [ stream-$ID ]
    limit: 10
    client_id: client-input-$ID
    consumer_group: group-$ID
`
		suite := integration.StreamBenchs(
			integration.StreamBenchSend(20, 1),
			integration.StreamBenchSend(10, 1),
			integration.StreamBenchSend(1, 1),
			integration.StreamBenchWrite(20),
			integration.StreamBenchWrite(10),
			integration.StreamBenchWrite(1),
		)
		suite.Run(
			b, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
		)
	})

	b.Run("pubsub", func(b *testing.B) {
		template := `
output:
  redis_pubsub:
    url: tcp://localhost:$PORT
    channel: channel-$ID
    max_in_flight: $MAX_IN_FLIGHT

input:
  redis_pubsub:
    url: tcp://localhost:$PORT
    channels: [ channel-$ID ]
`
		suite := integration.StreamBenchs(
			integration.StreamBenchSend(20, 1),
			integration.StreamBenchSend(10, 1),
			integration.StreamBenchSend(1, 1),
			integration.StreamBenchWrite(20),
			integration.StreamBenchWrite(10),
			integration.StreamBenchWrite(1),
		)
		suite.Run(
			b, template,
			integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
		)
	})

	b.Run("list", func(b *testing.B) {
		template := `
output:
  redis_list:
    url: tcp://localhost:$PORT
    key: key-$ID
    max_in_flight: $MAX_IN_FLIGHT

input:
  redis_list:
    url: tcp://localhost:$PORT
    key: key-$ID
`
		suite := integration.StreamBenchs(
			integration.StreamBenchSend(20, 1),
			integration.StreamBenchSend(10, 1),
			integration.StreamBenchSend(1, 1),
			integration.StreamBenchWrite(20),
			integration.StreamBenchWrite(10),
			integration.StreamBenchWrite(1),
		)
		suite.Run(
			b, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
		)
	})
}
