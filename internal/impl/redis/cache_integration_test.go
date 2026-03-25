// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationRedisCache(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	ctr, err := testcontainers.Run(t.Context(), "redis:latest",
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("6379/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	redisPort, err := ctr.MappedPort(t.Context(), "6379/tcp")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		url := fmt.Sprintf("tcp://localhost:%v/1", redisPort.Port())
		pConf, cErr := redisCacheConfig().ParseYAML(fmt.Sprintf(`url: %v`, url), nil)
		if cErr != nil {
			return false
		}

		r, cErr := newRedisCacheFromConfig(pConf)
		if cErr != nil {
			return false
		}

		return r.Set(t.Context(), "benthos_test_redis_connect", []byte("foo bar"), nil) == nil
	}, 30*time.Second, time.Second)

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
		integration.CacheTestOptPort(redisPort.Port()),
	)
}

func TestIntegrationRedisClusterCache(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	hostIP := "127.0.0.1"

	exposedPorts := make([]string, 12)
	for i := range 6 {
		exposedPorts[i] = fmt.Sprintf("%d/tcp", 7000+i)
		exposedPorts[i+6] = fmt.Sprintf("%d/tcp", 17000+i)
	}

	ctr, err := testcontainers.Run(t.Context(), "grokzen/redis-cluster:6.0.7",
		testcontainers.WithExposedPorts(exposedPorts...),
		testcontainers.WithEnv(map[string]string{"IP": hostIP}),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = nat.PortMap{}
			for i := range 6 {
				p1 := nat.Port(fmt.Sprintf("%d/tcp", 7000+i))
				p2 := nat.Port(fmt.Sprintf("%d/tcp", 17000+i))
				hc.PortBindings[p1] = []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: fmt.Sprintf("%d", 7000+i)}}
				hc.PortBindings[p2] = []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: fmt.Sprintf("%d", 17000+i)}}
			}
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("7000/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	clusterURL := ""
	for i := range 6 {
		clusterURL += fmt.Sprintf("redis://%s:%d/0,", hostIP, 7000+i)
	}
	clusterURL = strings.TrimSuffix(clusterURL, ",")

	require.Eventually(t, func() bool {
		pConf, cErr := redisCacheConfig().ParseYAML(fmt.Sprintf(`
url: %v
kind: cluster
`, clusterURL), nil)
		if cErr != nil {
			return false
		}

		r, cErr := newRedisCacheFromConfig(pConf)
		if cErr != nil {
			return false
		}

		cErr = r.Set(t.Context(), "benthos_test_redis_connect", []byte("foo bar"), nil)
		return cErr == nil
	}, 60*time.Second, time.Second)

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
		integration.CacheTestOptVarSet("VAR1", clusterURL),
	)
}

func TestIntegrationRedisFailoverCache(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	rpNet, err := network.New(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rpNet.Remove(context.Background()) })

	masterPort, err := integration.GetFreePort()
	require.NoError(t, err)

	master, err := testcontainers.Run(t.Context(), "bitnami/redis:latest",
		network.WithNetwork([]string{"redis-master"}, rpNet),
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = nat.PortMap{
				"6379/tcp": []nat.PortBinding{{HostIP: "", HostPort: strconv.Itoa(masterPort)}},
			}
		}),
		testcontainers.WithEnv(map[string]string{"ALLOW_EMPTY_PASSWORD": "yes"}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("6379/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, master)
	require.NoError(t, err)

	sentinelPort, err := integration.GetFreePort()
	require.NoError(t, err)

	sentinel, err := testcontainers.Run(t.Context(), "bitnami/redis-sentinel:latest",
		network.WithNetwork([]string{"redis-failover"}, rpNet),
		testcontainers.WithExposedPorts("26379/tcp"),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = nat.PortMap{
				"26379/tcp": []nat.PortBinding{{HostIP: "", HostPort: strconv.Itoa(sentinelPort)}},
			}
			hc.ExtraHosts = []string{"host.docker.internal:host-gateway"}
		}),
		testcontainers.WithEnv(map[string]string{
			"REDIS_SENTINEL_ANNOUNCE_IP":   "127.0.0.1",
			"REDIS_SENTINEL_ANNOUNCE_PORT": strconv.Itoa(sentinelPort),
			"REDIS_SENTINEL_QUORUM":        "1",
			// Point sentinel at the master via host-accessible address so that
			// it stores and reports 127.0.0.1:masterPort to clients.
			"REDIS_MASTER_HOST":        "host.docker.internal",
			"REDIS_MASTER_PORT_NUMBER": strconv.Itoa(masterPort),
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("26379/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, sentinel)
	require.NoError(t, err)

	clusterURL := fmt.Sprintf("redis://127.0.0.1:%d/0", sentinelPort)

	require.Eventually(t, func() bool {
		pConf, cErr := redisCacheConfig().ParseYAML(fmt.Sprintf(`
url: %v
kind: failover
master: mymaster
`, clusterURL), nil)
		if cErr != nil {
			return false
		}

		r, cErr := newRedisCacheFromConfig(pConf)
		if cErr != nil {
			return false
		}

		cErr = r.Set(t.Context(), "benthos_test_redis_connect", []byte("foo bar"), nil)
		return cErr == nil
	}, 60*time.Second, time.Second)

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
		integration.CacheTestOptVarSet("VAR1", clusterURL),
	)
}
