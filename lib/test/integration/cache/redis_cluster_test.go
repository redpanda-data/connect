package cache

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ = registerIntegrationTest("redis_cluster", func(t *testing.T) {
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
		conf := cache.NewConfig()
		conf.Redis.URL = clusterURL
		conf.Redis.Kind = "cluster"

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
      kind: cluster
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
		testOptVarOne(clusterURL),
	)
})
