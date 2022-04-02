package memcached

import (
	"fmt"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func TestIntegrationMemcachedCache(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.Run("memcached", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		client := memcache.New(fmt.Sprintf("localhost:%v", resource.GetPort("11211/tcp")))
		cErr := client.Set(&memcache.Item{
			Key:        "testkey",
			Value:      []byte("testvalue"),
			Expiration: 30,
		})
		if cErr != nil {
			return cErr
		}
		if _, cErr = client.Get("testkey"); cErr != nil {
			return cErr
		}
		return nil
	}))

	template := `
cache_resources:
  - label: testcache
    memcached:
      addresses: [ localhost:$PORT ]
      prefix: $ID
`
	suite := integration.CacheTests(
		integration.CacheTestOpenClose(),
		integration.CacheTestMissingKey(),
		integration.CacheTestDoubleAdd(),
		integration.CacheTestDelete(),
		integration.CacheTestGetAndSet(50),
	)
	suite.Run(
		t, template,
		integration.CacheTestOptPort(resource.GetPort("11211/tcp")),
	)
}
