package redis

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func TestIntegrationRedisCache(t *testing.T) {
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

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		url := fmt.Sprintf("tcp://localhost:%v/1", resource.GetPort("6379/tcp"))
		pConf, cErr := redisCacheConfig().ParseYAML(fmt.Sprintf(`url: %v`, url), nil)
		if cErr != nil {
			return cErr
		}

		r, cErr := newRedisCacheFromConfig(pConf)
		if cErr != nil {
			return cErr
		}

		cErr = r.Set(context.Background(), "benthos_test_redis_connect", []byte("foo bar"), nil)
		return cErr
	}))

	template := `
cache_resources:
  - label: testcache
    redis:
      url: tcp://localhost:$PORT/1
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
		integration.CacheTestOptPort(resource.GetPort("6379/tcp")),
	)
}

func TestIntegrationRedisClusterCache(t *testing.T) {
	t.Skip("Skipping as networking often fails for this test")

	integration.CheckSkip(t)
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

	exposedPorts := make([]string, 12)
	portBindings := make(map[docker.Port][]docker.PortBinding, 12)
	for i := 0; i < 6; i++ {
		p1 := fmt.Sprintf("%d/tcp", 7000+i)
		p2 := fmt.Sprintf("%d/tcp", 17000+i)
		exposedPorts[i] = p1
		exposedPorts[i+6] = p2
		portBindings[docker.Port(p1)] = []docker.PortBinding{{HostIP: "", HostPort: p1}}
		portBindings[docker.Port(p2)] = []docker.PortBinding{{HostIP: "", HostPort: p2}}
	}

	cluster, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:         "redis-cluster",
		Repository:   "grokzen/redis-cluster",
		Tag:          "6.0.7",
		ExposedPorts: exposedPorts,
		PortBindings: portBindings,
		Env: []string{
			"IP=" + hostIP,
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(cluster))
	})

	clusterURL := ""
	for i := 0; i < 6; i++ {
		clusterURL += fmt.Sprintf("redis://%s:%s/0,", hostIP, fmt.Sprintf("%d", 7000+i))
	}
	clusterURL = strings.TrimSuffix(clusterURL, ",")

	require.NoError(t, pool.Retry(func() error {
		pConf, cErr := redisCacheConfig().ParseYAML(fmt.Sprintf(`
url: %v
kind: cluster
`, clusterURL), nil)
		if cErr != nil {
			return cErr
		}

		r, cErr := newRedisCacheFromConfig(pConf)
		if cErr != nil {
			return cErr
		}

		cErr = r.Set(context.Background(), "benthos_test_redis_connect", []byte("foo bar"), nil)
		return cErr
	}))

	template := `
cache_resources:
  - label: testcache
    redis:
      url: $VAR1
      kind: cluster
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
}

func TestIntegrationRedisFailoverCache(t *testing.T) {
	t.Skip("Skipping as networking often fails for this test")

	integration.CheckSkip(t)
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
		_ = pool.RemoveNetwork(net)
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
		pConf, cErr := redisCacheConfig().ParseYAML(fmt.Sprintf(`
url: %v
kind: failover
master: mymaster
`, clusterURL), nil)
		if cErr != nil {
			return cErr
		}

		r, cErr := newRedisCacheFromConfig(pConf)
		if cErr != nil {
			return cErr
		}

		cErr = r.Set(context.Background(), "benthos_test_redis_connect", []byte("foo bar"), nil)
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
}
