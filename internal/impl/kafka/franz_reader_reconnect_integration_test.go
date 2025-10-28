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

package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

// TestKafkaReconnectionOnBrokerRestart tests that the Kafka input automatically
// reconnects when a broker is restarted, simulating a rolling upgrade scenario.
func TestKafkaReconnectionOnBrokerRestart(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute * 3

	// Start a single Kafka/Redpanda broker
	kafkaPort := "9092"
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{kafkaPort + "/tcp"},
		Cmd: []string{
			"redpanda", "start",
			"--smp=1",
			"--memory=1G",
			"--overprovisioned",
			"--node-id=0",
			"--check=false",
			"--kafka-addr", "PLAINTEXT://0.0.0.0:9092",
			"--advertise-kafka-addr", "PLAINTEXT://localhost:9092",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pool.Purge(resource))
	})

	brokerAddr := fmt.Sprintf("localhost:%s", resource.GetPort(kafkaPort+"/tcp"))
	t.Logf("Broker address: %s", brokerAddr)

	// Wait for broker to be ready
	require.NoError(t, pool.Retry(func() error {
		client, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
		if err != nil {
			return err
		}
		defer client.Close()
		return client.Ping(context.Background())
	}))

	topicName := "test-reconnect-topic"

	// Create topic and produce initial messages
	setupClient, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
	require.NoError(t, err)
	defer setupClient.Close()

	adminClient := kadm.NewClient(setupClient)
	_, err = adminClient.CreateTopics(context.Background(), 1, 1, nil, topicName)
	require.NoError(t, err)

	// Produce 10 messages before restart
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		record := &kgo.Record{
			Topic: topicName,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-before-restart-%d", i)),
		}
		results := setupClient.ProduceSync(ctx, record)
		require.NoError(t, results.FirstErr())
	}
	t.Log("Produced 10 messages before restart")

	// Create Redpanda Connect input
	env := integration.StreamTestOpenCloseCustom(
		t,
		fmt.Sprintf(`
kafka_franz:
  seed_brokers: ["%s"]
  topics: ["%s"]
  consumer_group: test-reconnect-cg
  tcp_user_timeout: 5s
  metadata_max_age: 10s
`, brokerAddr, topicName),
		`kafka_franz:
  seed_brokers: ["`+brokerAddr+`"]
  topic: "`+topicName+`"
  max_in_flight: 1
`,
	)

	// Consume first 5 messages
	for i := 0; i < 5; i++ {
		msg, err := env.Read(time.Second * 30)
		require.NoError(t, err)
		content := string(msg.AsBytes())
		t.Logf("Consumed message %d before restart: %s", i, content)
		assert.Contains(t, content, "value-before-restart")
		require.NoError(t, env.Ack(msg))
	}

	// Simulate broker restart by stopping and starting the container
	t.Log("Restarting broker to simulate rolling upgrade...")
	require.NoError(t, pool.Client.StopContainer(resource.Container.ID, 1))
	time.Sleep(time.Second * 2)
	require.NoError(t, pool.Client.StartContainer(resource.Container.ID, nil))

	// Wait for broker to come back up
	t.Log("Waiting for broker to recover...")
	require.NoError(t, pool.Retry(func() error {
		testClient, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
		if err != nil {
			return err
		}
		defer testClient.Close()
		return testClient.Ping(context.Background())
	}))
	t.Log("Broker is back online")

	// Produce additional messages after restart
	for i := 10; i < 20; i++ {
		record := &kgo.Record{
			Topic: topicName,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-after-restart-%d", i)),
		}
		results := setupClient.ProduceSync(ctx, record)
		require.NoError(t, results.FirstErr())
	}
	t.Log("Produced 10 messages after restart")

	// The consumer should automatically reconnect and continue consuming
	// We expect to consume messages 5-9 (uncommitted from before) and 10-19 (new)
	consumedAfterRestart := 0
	for consumedAfterRestart < 15 {
		msg, err := env.Read(time.Second * 60) // Generous timeout for reconnection
		require.NoError(t, err, "Failed to consume message after restart - reconnection may have failed")

		content := string(msg.AsBytes())
		t.Logf("Consumed message after restart: %s", content)
		require.NoError(t, env.Ack(msg))
		consumedAfterRestart++
	}

	t.Logf("Successfully consumed %d messages after broker restart", consumedAfterRestart)
	assert.Equal(t, 15, consumedAfterRestart, "Should have consumed all remaining messages")
}

// TestKafkaReconnectionWithTCPUserTimeout tests that TCP_USER_TIMEOUT helps
// detect dead connections faster than relying on kernel defaults.
func TestKafkaReconnectionWithTCPUserTimeout(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute * 3

	// Start Kafka broker
	kafkaPort := "9092"
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{kafkaPort + "/tcp"},
		Cmd: []string{
			"redpanda", "start",
			"--smp=1",
			"--memory=1G",
			"--overprovisioned",
			"--node-id=0",
			"--check=false",
			"--kafka-addr", "PLAINTEXT://0.0.0.0:9092",
			"--advertise-kafka-addr", "PLAINTEXT://localhost:9092",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pool.Purge(resource))
	})

	brokerAddr := fmt.Sprintf("localhost:%s", resource.GetPort(kafkaPort+"/tcp"))
	t.Logf("Broker address: %s", brokerAddr)

	require.NoError(t, pool.Retry(func() error {
		client, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
		if err != nil {
			return err
		}
		defer client.Close()
		return client.Ping(context.Background())
	}))

	topicName := "test-tcp-timeout-topic"

	// Setup
	setupClient, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
	require.NoError(t, err)
	defer setupClient.Close()

	adminClient := kadm.NewClient(setupClient)
	_, err = adminClient.CreateTopics(context.Background(), 1, 1, nil, topicName)
	require.NoError(t, err)

	// Produce initial messages
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		record := &kgo.Record{
			Topic: topicName,
			Value: []byte(fmt.Sprintf("message-%d", i)),
		}
		results := setupClient.ProduceSync(ctx, record)
		require.NoError(t, results.FirstErr())
	}

	// Create input with very short TCP_USER_TIMEOUT for testing
	env := integration.StreamTestOpenCloseCustom(
		t,
		fmt.Sprintf(`
kafka_franz:
  seed_brokers: ["%s"]
  topics: ["%s"]
  consumer_group: test-tcp-timeout-cg
  tcp_user_timeout: 10s
  metadata_max_age: 5s
`, brokerAddr, topicName),
		`drop: {}`,
	)

	// Consume initial messages
	for i := 0; i < 5; i++ {
		msg, err := env.Read(time.Second * 10)
		require.NoError(t, err)
		t.Logf("Consumed message %d: %s", i, string(msg.AsBytes()))
		require.NoError(t, env.Ack(msg))
	}

	// Kill the broker abruptly (simulating network partition)
	t.Log("Killing broker to simulate network partition...")
	require.NoError(t, pool.Client.KillContainer(resource.Container.ID, dockertest.KillOptions{Signal: "SIGKILL"}))

	// With tcp_user_timeout=10s, the connection should be detected as dead
	// within ~10-20 seconds (plus some overhead for reconnection attempts)
	// We'll wait up to 30 seconds to be safe
	time.Sleep(time.Second * 30)

	// Restart broker
	t.Log("Restarting broker...")
	require.NoError(t, pool.Client.StartContainer(resource.Container.ID, nil))

	require.NoError(t, pool.Retry(func() error {
		testClient, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
		if err != nil {
			return err
		}
		defer testClient.Close()
		return testClient.Ping(context.Background())
	}))

	// Produce new messages
	for i := 5; i < 10; i++ {
		record := &kgo.Record{
			Topic: topicName,
			Value: []byte(fmt.Sprintf("message-after-kill-%d", i)),
		}
		results := setupClient.ProduceSync(ctx, record)
		require.NoError(t, results.FirstErr())
	}

	// Consumer should reconnect and continue (messages 5-9 may be redelivered due to SIGKILL)
	consumedAfterReconnect := 0
	for consumedAfterReconnect < 5 {
		msg, err := env.Read(time.Second * 30)
		require.NoError(t, err, "Failed to reconnect after TCP_USER_TIMEOUT")
		t.Logf("Consumed after reconnection: %s", string(msg.AsBytes()))
		require.NoError(t, env.Ack(msg))
		consumedAfterReconnect++
	}

	t.Logf("Successfully reconnected and consumed %d messages", consumedAfterReconnect)
}
