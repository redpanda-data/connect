package cache

import (
	"fmt"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ = registerIntegrationTest("redis", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.Run("redis", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		url := fmt.Sprintf("tcp://localhost:%v/1", resource.GetPort("6379/tcp"))
		conf := cache.NewConfig()
		conf.Redis.URL = url

		r, cErr := cache.NewRedis(conf, nil, log.Noop(), metrics.Noop())
		if cErr != nil {
			return cErr
		}
		cErr = r.Set("benthos_test_redis_connect", []byte("foo bar"))
		return cErr
	}))

	template := `
cache_resources:
  - label: testcache
    redis:
      url: tcp://localhost:$PORT/1
      prefix: $ID
`
	suite := integrationTests(
		integrationTestOpenClose(),
		integrationTestMissingKey(),
		integrationTestDoubleAdd(),
		integrationTestDelete(),
		integrationTestGetAndSet(50),
	)
	suite.Run(
		t, template,
		testOptPort(resource.GetPort("6379/tcp")),
	)
})
