// Copyright 2026 Redpanda Data, Inc.
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

package kafka_test

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

const redpandaClusterEntrypoint = `#!/usr/bin/env bash
# Wait for testcontainer's injected redpanda config
until grep -q "# Injected by testcontainers" "/etc/redpanda/redpanda.yaml"; do
  sleep 0.1
done
exec /entrypoint.sh "$@"
`

type redpandaCluster struct {
	brokerAddrs []string
	containers  []*testcontainers.DockerContainer
}

// startRedpandaCluster starts a multi-broker Redpanda cluster using raw
// testcontainers (the redpanda module only supports single-node). It returns
// the cluster with host:port broker addresses and container references.
func startRedpandaCluster(t *testing.T, ctx context.Context, numBrokers int) redpandaCluster {
	t.Helper()

	rpNet, err := network.New(ctx)
	require.NoError(t, err, "failed to create docker network")
	t.Cleanup(func() {
		if err := rpNet.Remove(context.Background()); err != nil {
			t.Logf("failed to remove docker network: %v", err)
		}
	})

	containers := make([]*testcontainers.DockerContainer, numBrokers)

	for i := range numBrokers {
		alias := fmt.Sprintf("redpanda-%d", i)
		ctr, err := testcontainers.Run(ctx,
			"docker.redpanda.com/redpandadata/redpanda:latest",
			testcontainers.WithEntrypoint("/entrypoint-tc.sh"),
			testcontainers.WithFiles(testcontainers.ContainerFile{
				Reader:            strings.NewReader(redpandaClusterEntrypoint),
				ContainerFilePath: "/entrypoint-tc.sh",
				FileMode:          0o755,
			}),
			testcontainers.WithConfigModifier(func(c *container.Config) {
				c.User = "root:root"
			}),
			testcontainers.WithCmd("redpanda", "start", "--mode=dev-container", "--smp=1", "--memory=1G"),
			testcontainers.WithExposedPorts("9092/tcp", "9644/tcp"),
			testcontainers.WithWaitStrategy(wait.ForNop(func(context.Context, wait.StrategyTarget) error { return nil })),
			network.WithNetwork([]string{alias}, rpNet),
		)
		require.NoError(t, err, "failed to start redpanda broker %d", i)
		containers[i] = ctr

		t.Cleanup(func() {
			if err := ctr.Terminate(context.Background()); err != nil {
				t.Logf("failed to terminate redpanda broker %d: %v", i, err)
			}
		})
	}

	brokerAddrs := make([]string, numBrokers)
	for i, ctr := range containers {
		mappedPort, err := ctr.MappedPort(ctx, "9092/tcp")
		require.NoError(t, err, "failed to get mapped kafka port for broker %d", i)

		host, err := ctr.Host(ctx)
		require.NoError(t, err, "failed to get host for broker %d", i)

		brokerAddrs[i] = fmt.Sprintf("%s:%d", host, mappedPort.Int())

		cfg := fmt.Sprintf(`# Injected by testcontainers
redpanda:
  node_id: %d
  seed_servers:
    - host:
        address: redpanda-0
        port: 33145
  rpc_server:
    address: 0.0.0.0
    port: 33145
  advertised_rpc_api:
    address: redpanda-%d
    port: 33145
  kafka_api:
    - address: 0.0.0.0
      name: external
      port: 9092
    - address: 0.0.0.0
      name: internal
      port: 9093
  advertised_kafka_api:
    - address: %s
      name: external
      port: %d
    - address: redpanda-%d
      name: internal
      port: 9093
  developer_mode: true
`, i, i, host, mappedPort.Int(), i)

		err = ctr.CopyToContainer(ctx, []byte(cfg), "/etc/redpanda/redpanda.yaml", 0o644)
		require.NoError(t, err, "failed to copy config to broker %d", i)
	}

	for i, ctr := range containers {
		err := wait.ForLog("Successfully started Redpanda!").
			WithStartupTimeout(60*time.Second).
			WaitUntilReady(ctx, ctr)
		require.NoError(t, err, "broker %d did not start in time", i)
	}

	return redpandaCluster{brokerAddrs: brokerAddrs, containers: containers}
}

// transferLeadership moves the partition leader for the given topic/0 to a
// random broker by execing rpk inside broker 0 (which is always kept alive).
func transferLeadership(ctx context.Context, t *testing.T, containers []*testcontainers.DockerContainer, topic string) {
	t.Helper()

	target := rand.IntN(len(containers))
	code, reader, err := containers[0].Exec(ctx, []string{
		"rpk", "cluster", "partitions", "transfer-leadership",
		"-p", fmt.Sprintf("%s/0:%d", topic, target),
	})
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		t.Logf("leadership transfer exec failed: %v", err)
		return
	}
	out, _ := io.ReadAll(reader)
	if code != 0 {
		t.Logf("leadership transfer to broker %d failed (code %d): %s", target, code, string(out))
		return
	}
	t.Logf("transferred leadership of %s/0 to broker %d", topic, target)
}

func TestRedpandaRecordOrderSoakTest(t *testing.T) {
	// Soak test for record ordering under chaos. A continuous producer writes
	// sequentially-keyed messages to a source broker for soakDuration. A
	// Redpanda Connect pipeline migrates them to a 3-broker destination cluster
	// (1 partition, RF=3). Meanwhile two chaos goroutines run concurrently:
	//   1. Leadership transfers via rpk every ~2s
	//   2. Broker stop/start every ~5s
	// A verifier consumer reads from the destination and asserts that keys
	// arrive in strictly increasing order.
	//
	// To run overnight:
	//   nohup go test -timeout 0 -v -count 1000 -run ^TestRedpandaRecordOrderSoakTest$ ./internal/impl/kafka/ > soak.log 2>&1 &
	integration.CheckSkip(t)

	const soakDuration = 3 * time.Minute

	// --- infrastructure ---

	sourceContainer, err := redpanda.Run(t.Context(), "docker.redpanda.com/redpandadata/redpanda:latest")
	require.NoError(t, err, "failed to start source redpanda")
	t.Cleanup(func() {
		if err := sourceContainer.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate source: %v", err)
		}
	})

	sourceBroker, err := sourceContainer.KafkaSeedBroker(t.Context())
	require.NoError(t, err)

	dest := startRedpandaCluster(t, t.Context(), 3)

	t.Logf("Source: %s", sourceBroker)
	t.Logf("Dest:   %v", dest.brokerAddrs)

	// --- topics ---

	topic := "soak-ordered"
	retMs := strconv.Itoa(int((1 * time.Hour).Milliseconds()))

	srcAdmin, err := kgo.NewClient(kgo.SeedBrokers(sourceBroker))
	require.NoError(t, err)
	_, err = kadm.NewClient(srcAdmin).CreateTopic(t.Context(), 1, 1, map[string]*string{"retention.ms": &retMs}, topic)
	require.NoError(t, err, "failed to create source topic")
	srcAdmin.Close()

	destAdmin, err := kgo.NewClient(kgo.SeedBrokers(dest.brokerAddrs...))
	require.NoError(t, err)
	_, err = kadm.NewClient(destAdmin).CreateTopic(t.Context(), 1, 3, map[string]*string{"retention.ms": &retMs}, topic)
	require.NoError(t, err, "failed to create dest topic")
	destAdmin.Close()

	// --- continuous producer ---

	producerCtx, cancelProducer := context.WithCancel(t.Context())
	var totalProduced atomic.Int64

	go func() {
		cl, err := kgo.NewClient(kgo.SeedBrokers(sourceBroker))
		if err != nil {
			t.Logf("producer client error: %v", err)
			return
		}
		defer cl.Close()

		val := []byte(`{"test":"foo"}`)
		for i := 1; ; i++ {
			if producerCtx.Err() != nil {
				cl.Flush(context.Background())
				t.Logf("Producer stopped after %d messages", i-1)
				return
			}
			cl.Produce(producerCtx, &kgo.Record{
				Topic: topic,
				Key:   []byte(strconv.Itoa(i)),
				Value: val,
			}, func(_ *kgo.Record, err error) {
				if err != nil && producerCtx.Err() == nil {
					t.Logf("produce callback error: %v", err)
				}
			})
			totalProduced.Store(int64(i))
			time.Sleep(5 * time.Millisecond) // ~200 msgs/sec
		}
	}()

	// --- migration pipeline ---

	destBrokersYAML := strings.Join(dest.brokerAddrs, ", ")
	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  redpanda:
    seed_brokers: [ %s ]
    topics: [ %s ]
    consumer_group: migrator_cg
    start_from_oldest: true

output:
  redpanda:
    seed_brokers: [ %s ]
    topic: ${! @kafka_topic }
    key: ${! @kafka_key }
    timestamp_ms: ${! @kafka_timestamp_ms }
    compression: none
`, sourceBroker, topic, destBrokersYAML)))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: WARN`))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	closeChan := make(chan struct{})
	go func() {
		defer close(closeChan)
		if err := stream.Run(t.Context()); err != nil {
			t.Logf("stream: %v", err)
		}
		t.Log("Pipeline shut down")
	}()

	// --- chaos: leadership transfers every ~2s ---

	chaosCtx, cancelChaos := context.WithCancel(t.Context())

	go func() {
		time.Sleep(5 * time.Second) // let cluster settle
		for {
			select {
			case <-chaosCtx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			transferLeadership(chaosCtx, t, dest.containers, topic)
		}
	}()

	// --- cleanup (LIFO: this runs before container termination) ---

	t.Cleanup(func() {
		cancelProducer()
		cancelChaos()
		if err := stream.StopWithin(30 * time.Second); err != nil {
			t.Logf("pipeline stop timed out: %v", err)
		}
		<-closeChan
	})

	// --- verifier: consume from dest and assert strict ordering ---

	t.Log("Starting soak test")

	verifier, err := kgo.NewClient(
		kgo.SeedBrokers(dest.brokerAddrs...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup("verifier_cg"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer func() {
		_ = verifier.CommitUncommittedOffsets(context.Background())
		verifier.Close()
	}()

	deadline := time.After(soakDuration)
	var lastKey, totalConsumed int
	logTicker := time.NewTicker(10 * time.Second)
	defer logTicker.Stop()

	for {
		select {
		case <-deadline:
			t.Logf("Soak complete: produced=%d consumed=%d lastKey=%d", totalProduced.Load(), totalConsumed, lastKey)
			return
		case <-logTicker.C:
			t.Logf("Progress: produced=%d consumed=%d lastKey=%d", totalProduced.Load(), totalConsumed, lastKey)
		default:
		}

		pollCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		fetches := verifier.PollRecords(pollCtx, 500)
		cancel()

		if fetches.IsClientClosed() {
			t.Fatal("verifier client closed unexpectedly")
		}

		it := fetches.RecordIter()
		for !it.Done() {
			rec := it.Next()
			key, err := strconv.Atoi(string(rec.Key))
			require.NoError(t, err, "non-integer key: %q", string(rec.Key))

			if key <= lastKey {
				t.Fatalf("ORDER VIOLATION: got key %d after key %d (consumed %d records)", key, lastKey, totalConsumed)
			}
			lastKey = key
			totalConsumed++
		}

		_ = verifier.CommitUncommittedOffsets(t.Context())
	}
}
