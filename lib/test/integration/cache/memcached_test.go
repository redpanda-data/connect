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

var _ = registerIntegrationTest("memcached", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.Run("memcached", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		addrs := []string{fmt.Sprintf("localhost:%v", resource.GetPort("11211/tcp"))}

		conf := cache.NewConfig()
		conf.Memcached.Addresses = addrs

		_, cErr := cache.NewMemcached(conf, nil, log.Noop(), metrics.Noop())
		return cErr
	}))

	template := `
resources:
  caches:
    testcache:
      memcached:
        addresses: [ localhost:$PORT ]
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
		testOptPort(resource.GetPort("11211/tcp")),
	)
})
