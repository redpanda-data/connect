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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ = registerIntegrationTest("redis_cluster", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	net, err := pool.CreateNetwork("redis-cluster")
	require.NoError(t, err)
	defer pool.RemoveNetwork(net)

	redisNodes := make([]*dockertest.Resource, 6)
	for i := 0; i < 6; i++ {
		node, err := pool.RunWithOptions(&dockertest.RunOptions{
			Name:       fmt.Sprintf("node%d", i),
			Repository: "bitnami/redis-cluster",
			Tag:        "latest",
			Networks:   []*dockertest.Network{net},
			Env: []string{
				"REDIS_PASSWORD=benthos",
				"REDIS_CLUSTER_DYNAMIC_IPS=no",
				"REDIS_CLUSTER_ANNOUNCE_IP=localhost",
				"REDIS_NODES=node0 node1 node2 node3 node4 node5",
			},
		})
		require.NoError(t, err)
		redisNodes[i] = node
		node.Expire(900)
	}

	creator, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:       "creator",
		Repository: "bitnami/redis-cluster",
		Tag:        "latest",
		Networks:   []*dockertest.Network{net},
		Env: []string{
			"REDISCLI_AUTH=benthos",
			"REDIS_CLUSTER_REPLICAS=1",
			"REDIS_CLUSTER_ANNOUNCE_IP=localhost",
			"REDIS_NODES=node0 node1 node2 node3 node4 node5",
			"REDIS_CLUSTER_CREATOR=yes",
		},
	})
	require.NoError(t, err)
	creator.Expire(900)

	t.Cleanup(func() {
		for _, v := range redisNodes {
			assert.NoError(t, pool.Purge(v))
		}
		assert.NoError(t, pool.Purge(creator))
	})

	clusterURL := ""
	for _, v := range redisNodes {
		if runtime.GOOS == "darwin" {
			clusterURL += fmt.Sprintf("tcp://benthos:benthos@%s:%s/0,",
				v.GetBoundIP("6379/tcp"),
				v.GetPort("6379/tcp"),
			)
		} else {
			clusterURL += fmt.Sprintf("tcp://benthos:benthos@localhost:%s/0,", v.GetPort("6379/tcp"))
		}
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
resources:
  caches:
    testcache:
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
