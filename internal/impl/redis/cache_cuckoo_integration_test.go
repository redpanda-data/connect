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

func TestIntegrationRedisCuckooCache(t *testing.T) {
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

		confTemplate := `url: %q`

		pConf, cErr := redisCacheConfig().ParseYAML(fmt.Sprintf(confTemplate, url), nil)
		if cErr != nil {
			return cErr
		}

		r, cErr := newRedisBloomCacheFromConfig(pConf)
		if cErr != nil {
			return cErr
		}

		cErr = r.Set(context.Background(), "benthos_test_redis_connect", []byte("foo bar"), nil)
		return cErr
	}))

	template := `
cache_resources:
  - label: testcache
    redis_cuckoo:
      url: tcp://localhost:$PORT/1
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
