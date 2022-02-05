package cache

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/integration"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ = registerIntegrationTest("redis_failover", func(t *testing.T) {
	t.Skip("Skipping as networking often fails for this test")
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Second * 30

	networks, _ := pool.Client.ListNetworks()
	hostIP := ""
	for _, network := range networks {
		if network.Name == "bridge" {
			hostIP = network.IPAM.Config[0].Gateway
		}
	}
	if runtime.GOOS == "darwin" {
		hostIP = "0.0.0.0"
	}

	net, err := pool.CreateNetwork("redis-sentinel")
	require.NoError(t, err)

	t.Cleanup(func() {
		pool.RemoveNetwork(net)
	})

	master, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:         "redis-master",
		Repository:   "bitnami/redis",
		Tag:          "6.0.9",
		Networks:     []*dockertest.Network{net},
		ExposedPorts: []string{"6379/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"6379/tcp": {{HostIP: "", HostPort: "6379/tcp"}},
		},
		Env: []string{
			"ALLOW_EMPTY_PASSWORD=yes",
		},
	})
	require.NoError(t, err)

	sentinel, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:       "redis-failover",
		Repository: "bitnami/redis-sentinel",
		Tag:        "6.0.9",
		Networks:   []*dockertest.Network{net},
		ExposedPorts: []string{
			"26379/tcp",
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"26379/tcp": {{HostIP: "", HostPort: "26379/tcp"}},
		},
		Env: []string{
			"REDIS_SENTINEL_ANNOUNCE_IP=" + hostIP,
			"REDIS_SENTINEL_QUORUM=1",
			"REDIS_MASTER_HOST=" + hostIP,
			"REDIS_MASTER_PORT_NUMBER=" + master.GetPort("6379/tcp"),
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(master))
		assert.NoError(t, pool.Purge(sentinel))
	})

	clusterURL := ""
	clusterURL += fmt.Sprintf("redis://%s:%s/0,", hostIP, sentinel.GetPort("26379/tcp"))
	clusterURL = strings.TrimSuffix(clusterURL, ",")

	require.NoError(t, pool.Retry(func() error {
		conf := cache.NewConfig()
		conf.Redis.URL = clusterURL
		conf.Redis.Kind = "failover"
		conf.Redis.Master = "mymaster"

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
      url: $VAR1
      kind: failover
      master: mymaster
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
		integration.CacheTestOptVarOne(clusterURL),
	)
})
