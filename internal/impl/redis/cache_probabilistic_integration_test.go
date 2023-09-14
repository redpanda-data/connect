package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func TestIntegrationRedisProbabilisticBloomCache(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.Run("redis/redis-stack-server", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		url := fmt.Sprintf("tcp://localhost:%v/1", resource.GetPort("6379/tcp"))

		confTemplate := `
url: %q
backend: bloom
`

		pConf, cErr := redisCacheConfig().ParseYAML(fmt.Sprintf(confTemplate, url), nil)
		if cErr != nil {
			return cErr
		}

		r, cErr := newRedisProbabilisticCacheFromConfig(pConf)
		if cErr != nil {
			return cErr
		}

		cErr = r.Set(context.Background(), "benthos_test_redis_connect", []byte("foo bar"), nil)
		return cErr
	}))

	template := `
cache_resources:
  - label: testcache
    redis_probabilistic:
      url: tcp://localhost:$PORT/1
      backend: bloom
`
	suite := integration.CacheTests(
		integration.CacheTestOpenClose(integration.WithValue([]byte("t"))),
		integration.CacheTestMissingKey(),
		integration.CacheTestDoubleAdd(integration.WithValue([]byte("t"))),
		// integration.CacheTestDelete(),
		integration.CacheTestGetAndSet(50, integration.WithValue([]byte("t"))),
	)
	suite.Run(
		t, template,
		integration.CacheTestOptPort(resource.GetPort("6379/tcp")),
	)
}

func TestIntegrationRedisProbabilisticCuckooCache(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.Run("redis/redis-stack-server", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		url := fmt.Sprintf("tcp://localhost:%v/1", resource.GetPort("6379/tcp"))

		confTemplate := `
url: %q
backend: cuckoo
`

		pConf, cErr := redisCacheConfig().ParseYAML(fmt.Sprintf(confTemplate, url), nil)
		if cErr != nil {
			return cErr
		}

		r, cErr := newRedisProbabilisticCacheFromConfig(pConf)
		if cErr != nil {
			return cErr
		}

		cErr = r.Set(context.Background(), "benthos_test_redis_connect", []byte("foo bar"), nil)
		return cErr
	}))

	template := `
cache_resources:
  - label: testcache
    redis_probabilistic:
      url: tcp://localhost:$PORT/1
      backend: cuckoo
`
	opts := []integration.Option{integration.WithValue([]byte("t"))}
	suite := integration.CacheTests(
		integration.CacheTestOpenClose(opts...),
		integration.CacheTestMissingKey(),
		integration.CacheTestDoubleAdd(opts...),
		integration.CacheTestDelete(opts...),
		integration.CacheTestGetAndSet(50, opts...),
	)
	suite.Run(
		t, template,
		integration.CacheTestOptPort(resource.GetPort("6379/tcp")),
	)
}
