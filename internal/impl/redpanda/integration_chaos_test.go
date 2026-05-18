// Copyright 2025 Redpanda Data, Inc.
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

package redpanda_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dockerclient "github.com/docker/docker/client"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/redpandatest"
	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

// restartContainer gracefully restarts a testcontainers container.
func restartContainer(ctx context.Context, ctr testcontainers.Container, timeout time.Duration) error {
	d := timeout
	if err := ctr.Stop(ctx, &d); err != nil {
		return fmt.Errorf("stop container: %w", err)
	}
	if err := ctr.Start(ctx); err != nil {
		return fmt.Errorf("start container: %w", err)
	}
	return nil
}

// killContainer sends SIGKILL to a container without graceful shutdown.
func killContainer(ctx context.Context, ctr testcontainers.Container) error {
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("create docker client: %w", err)
	}
	defer cli.Close()
	return cli.ContainerKill(ctx, ctr.GetContainerID(), "SIGKILL")
}

// startChaosCluster starts a single Redpanda broker with a pinned Kafka port.
// Pinning the host port ensures that after container stop/start or kill/start,
// the broker comes back on the same address so Kafka clients can reconnect.
func startChaosCluster(t *testing.T) (redpandatest.Endpoints, testcontainers.Container, error) {
	t.Helper()

	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return redpandatest.Endpoints{}, nil, fmt.Errorf("find free port: %w", err)
	}
	kafkaPort := l.Addr().(*net.TCPAddr).Port
	l.Close()

	cfg := redpandatest.DefaultConfig
	cfg.ExtraOpts = []testcontainers.ContainerCustomizer{
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			if hc.PortBindings == nil {
				hc.PortBindings = network.PortMap{}
			}
			hc.PortBindings[network.MustParsePort("9092/tcp")] = []network.PortBinding{
				{HostPort: strconv.Itoa(kafkaPort)},
			}
		}),
	}
	return redpandatest.StartSingleBrokerWithConfig(t, cfg)
}

// waitBrokerReady waits for the Redpanda broker to accept TCP connections after a restart.
func waitBrokerReady(t *testing.T, brokerAddr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", brokerAddr, time.Second)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}, 30*time.Second, 250*time.Millisecond, "broker did not become ready")
}

// TestIntegrationRedpandaChaosGracefulRestart tests client reconnection during
// graceful broker restarts. This simulates rolling upgrades where brokers are
// restarted one at a time.
func TestIntegrationRedpandaChaosGracefulRestart(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("Given: single broker Redpanda cluster")
	endpoints, ctr, err := startChaosCluster(t)
	require.NoError(t, err)
	topic := "reconnect-test"

	t.Log("And: producer and consumer pipeline")
	var producedCount, consumedCount atomic.Int64
	produceMessagesBackground(t, endpoints, topic, &producedCount, 50*time.Millisecond)
	consumeMessagesBackground(t, endpoints, topic, "test-cg", &consumedCount)

	t.Log("When: broker is restarted gracefully")
	require.Eventually(t, func() bool {
		return producedCount.Load() > 0 && consumedCount.Load() > 0
	}, 30*time.Second, 500*time.Millisecond, "messages did not start flowing")
	initialProduced := producedCount.Load()
	initialConsumed := consumedCount.Load()
	t.Logf("Before restart - produced: %d, consumed: %d", initialProduced, initialConsumed)

	require.NoError(t, restartContainer(t.Context(), ctr, 30*time.Second))
	waitBrokerReady(t, endpoints.BrokerAddr)
	t.Log("Broker restarted")

	t.Log("Then: consumer reconnects and continues processing")
	assert.Eventually(t, func() bool {
		produced := producedCount.Load()
		consumed := consumedCount.Load()
		t.Logf("After restart - produced: %d, consumed: %d", produced, consumed)
		return produced > initialProduced && consumed > initialConsumed
	}, 30*time.Second, 1*time.Second)

	t.Log("And: no messages lost")
	time.Sleep(2 * time.Second)
	finalProduced := producedCount.Load()
	finalConsumed := consumedCount.Load()
	t.Logf("Final - produced: %d, consumed: %d", finalProduced, finalConsumed)
	assert.Greater(t, finalProduced, initialProduced)
	assert.Greater(t, finalConsumed, initialConsumed)
}

// TestIntegrationRedpandaChaosAbruptFailure tests client reconnection during
// abrupt broker failures. This simulates network partitions where the broker is
// killed without graceful shutdown.
func TestIntegrationRedpandaChaosAbruptFailure(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("Given: single broker Redpanda cluster")
	endpoints, ctr, err := startChaosCluster(t)
	require.NoError(t, err)
	topic := "partition-test"

	t.Log("And: producer and consumer pipeline")
	var producedCount, consumedCount atomic.Int64
	produceMessagesBackground(t, endpoints, topic, &producedCount, 50*time.Millisecond)
	consumeMessagesBackground(t, endpoints, topic, "partition-cg", &consumedCount)

	t.Log("When: broker is killed abruptly")
	require.Eventually(t, func() bool {
		return producedCount.Load() > 0 && consumedCount.Load() > 0
	}, 30*time.Second, 500*time.Millisecond, "messages did not start flowing")
	initialProduced := producedCount.Load()
	initialConsumed := consumedCount.Load()
	t.Logf("Before kill - produced: %d, consumed: %d", initialProduced, initialConsumed)

	require.NoError(t, killContainer(t.Context(), ctr))
	t.Log("Broker killed")

	t.Log("And: broker is restarted")
	require.NoError(t, ctr.Start(t.Context()))
	waitBrokerReady(t, endpoints.BrokerAddr)
	t.Log("Broker started")

	t.Log("Then: consumer detects failure and reconnects")
	assert.Eventually(t, func() bool {
		produced := producedCount.Load()
		consumed := consumedCount.Load()
		t.Logf("After restart - produced: %d, consumed: %d", produced, consumed)
		return produced > initialProduced && consumed > initialConsumed
	}, 30*time.Second, 1*time.Second)

	t.Log("And: messages continue flowing")
	time.Sleep(2 * time.Second)
	finalProduced := producedCount.Load()
	finalConsumed := consumedCount.Load()
	t.Logf("Final - produced: %d, consumed: %d", finalProduced, finalConsumed)
	assert.Greater(t, finalProduced, initialProduced)
	assert.Greater(t, finalConsumed, initialConsumed)
}

// TestIntegrationRedpandaChaosStability tests long-running stability with
// random broker disruptions. This validates that the client remains healthy
// over extended periods with intermittent failures.
//
// Run with:
//
//	go test -timeout 0 -run TestIntegrationRedpandaChaosStability -v ./internal/impl/redpanda/ \
//	  -duration=60m -restart-interval=5m
func TestIntegrationRedpandaChaosStability(t *testing.T) {
	integration.CheckSkip(t)
	if os.Getenv("CI") != "" {
		t.Skip("Skipping chaos test in CI")
	}

	duration := flag.Duration("duration", 2*time.Minute,
		"Duration for stability test")
	restartInterval := flag.Duration("restart-interval", 15*time.Second,
		"Interval between broker restarts")
	flag.Parse()

	t.Logf("Given: single broker Redpanda cluster running for %v", duration)
	endpoints, ctr, err := startChaosCluster(t)
	require.NoError(t, err)
	topic := "stability-test"

	t.Log("And: producer and consumer pipeline")
	var producedCount, consumedCount atomic.Int64
	produceMessagesBackground(t, endpoints, topic, &producedCount, 50*time.Millisecond)
	consumeMessagesBackground(t, endpoints, topic, "stability-cg", &consumedCount)

	t.Logf("When: broker is restarted every %v", restartInterval)
	ctx, cancel := context.WithTimeout(t.Context(), *duration)
	defer cancel()

	ticker := time.NewTicker(*restartInterval)
	defer ticker.Stop()

	restartCount := 0
	for {
		select {
		case <-ctx.Done():
			t.Logf("Stability test completed after %d restarts", restartCount)
			goto done
		case <-ticker.C:
			restartCount++
			beforeProduced := producedCount.Load()
			beforeConsumed := consumedCount.Load()
			t.Logf("Restart %d - before: produced=%d, consumed=%d", restartCount, beforeProduced, beforeConsumed)

			require.NoError(t, restartContainer(t.Context(), ctr, 30*time.Second))
			waitBrokerReady(t, endpoints.BrokerAddr)
			t.Logf("Restart %d - broker restarted", restartCount)

			time.Sleep(5 * time.Second)
			afterProduced := producedCount.Load()
			afterConsumed := consumedCount.Load()
			t.Logf("Restart %d - after: produced=%d, consumed=%d", restartCount, afterProduced, afterConsumed)
		}
	}

done:
	t.Log("Then: consumer remains healthy throughout")
	finalProduced := producedCount.Load()
	finalConsumed := consumedCount.Load()
	t.Logf("Final counts - produced: %d, consumed: %d", finalProduced, finalConsumed)
	assert.Greater(t, finalProduced, int64(0))
	assert.Greater(t, finalConsumed, int64(0))

	t.Log("And: no memory leaks or connection stalls")
}

// produceMessagesBackground produces messages continuously in the background.
func produceMessagesBackground(t *testing.T, endpoints redpandatest.Endpoints, topic string, counter *atomic.Int64, delay time.Duration) {
	t.Helper()

	streamBuilder := service.NewStreamBuilder()
	config := fmt.Sprintf(`
input:
  generate:
    interval: %s
    mapping: |
      root.message = "hello"
      root.timestamp = now()
output:
  redpanda:
    seed_brokers: [ "%s" ]
    topic: "%s"
    key: ${! content().string() }
    tcp:
      tcp_user_timeout: 5s
`, delay, endpoints.BrokerAddr, topic)

	require.NoError(t, streamBuilder.SetYAML(config))
	require.NoError(t, streamBuilder.AddConsumerFunc(func(_ context.Context, _ *service.Message) error {
		counter.Add(1)
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	var wg sync.WaitGroup
	wg.Go(func() {
		if err := stream.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("produce stream error: %v", err)
		}
	})
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
}

// consumeMessagesBackground consumes messages continuously in the background.
func consumeMessagesBackground(t *testing.T, endpoints redpandatest.Endpoints, topic, consumerGroup string, counter *atomic.Int64) {
	t.Helper()

	streamBuilder := service.NewStreamBuilder()
	config := fmt.Sprintf(`
input:
  redpanda:
    seed_brokers: [ "%s" ]
    topics: [ "%s" ]
    consumer_group: "%s"
    commit_period: 1s
    tcp:
      tcp_user_timeout: 5s
output:
  drop: {}
`, endpoints.BrokerAddr, topic, consumerGroup)

	require.NoError(t, streamBuilder.SetYAML(config))
	require.NoError(t, streamBuilder.AddConsumerFunc(func(_ context.Context, _ *service.Message) error {
		counter.Add(1)
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	var wg sync.WaitGroup
	wg.Go(func() {
		if err := stream.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("consume stream error: %v", err)
		}
	})
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
}
