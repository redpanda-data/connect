package etcd

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/public/service/integration"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationEtcdCache(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.Run("bitnami/etcd", "latest", []string{"ALLOW_NONE_AUTHENTICATION=yes"})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		conf, err := etcdCacheConfig().
			ParseYAML(fmt.Sprintf(`endpoints: [localhost:%v]`, resource.GetPort("2379/tcp")), nil)
		if err != nil {
			return err
		}

		e, err := newEtcdCacheFromConfig(conf)
		if err != nil {
			return err
		}

		err = e.Set(context.Background(), "benthos_test_etcd_connect", []byte("foo bar"), nil)
		return err
	}))

	template := `
cache_resources:
  - label: testcache
    etcd:
      endpoints: 
      	- localhost:$PORT/1
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
		integration.CacheTestOptPort(resource.GetPort("2379/tcp")),
	)
}
