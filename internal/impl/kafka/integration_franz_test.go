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

package kafka_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationFranz(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaPort, err := integration.GetFreePort()
	require.NoError(t, err)

	kafkaPortStr := strconv.Itoa(kafkaPort)

	options := &dockertest.RunOptions{
		Repository:   "docker.redpanda.com/redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{"9092/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr + "/tcp"}},
		},
		Cmd: []string{
			"redpanda",
			"start",
			"--node-id 0",
			"--mode dev-container",
			"--set rpk.additional_start_flags=[--reactor-backend=epoll]",
			"--kafka-addr 0.0.0.0:9092",
			fmt.Sprintf("--advertise-kafka-addr localhost:%v", kafkaPort),
		},
	}

	pool.MaxWait = time.Minute
	resource, err := pool.RunWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return createKafkaTopic(t.Context(), "localhost:"+kafkaPortStr, "testingconnection", 1)
	}))

	template := `
output:
  kafka_franz:
    seed_brokers: [ localhost:$PORT ]
    topic: topic-$ID
    max_in_flight: $MAX_IN_FLIGHT
    timeout: "5s"
    metadata:
      include_patterns: [ .* ]
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  kafka_franz:
    seed_brokers: [ localhost:$PORT ]
    topics: [ topic-$ID$VAR1 ]
    consumer_group: "$VAR4"
    checkpoint_limit: 100
    commit_period: "1s"
    batching:
      count: $INPUT_BATCH_COUNT
`

	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestMetadata(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallel(1000),
		integration.StreamTestStreamParallelLossy(1000),
		integration.StreamTestSendBatchCount(10),
		integration.StreamTestStreamSaturatedUnacked(200),
	)

	// In some modes include testing input level batching
	var suiteExt integration.StreamTestList
	suiteExt = append(suiteExt, suite...)
	suiteExt = append(suiteExt, integration.StreamTestReceiveBatchCount(10))

	suite.Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
			vars.General["VAR4"] = "group" + vars.ID
			require.NoError(t, createKafkaTopic(ctx, "localhost:"+kafkaPortStr, vars.ID, 4))
		}),
		integration.StreamTestOptPort(kafkaPortStr),
		integration.StreamTestOptVarSet("VAR1", ""),
	)

	t.Run("only one partition", func(t *testing.T) {
		t.Parallel()
		suiteExt.Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				vars.General["VAR4"] = "group" + vars.ID
				require.NoError(t, createKafkaTopic(ctx, "localhost:"+kafkaPortStr, vars.ID, 1))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
			integration.StreamTestOptVarSet("VAR1", ""),
		)
	})

	t.Run("explicit partitions", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				topicName := "topic-" + vars.ID
				vars.General["VAR1"] = fmt.Sprintf(":0,%v:1,%v:2,%v:3", topicName, topicName, topicName)
				require.NoError(t, createKafkaTopic(ctx, "localhost:"+kafkaPortStr, vars.ID, 4))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
			integration.StreamTestOptSleepAfterInput(time.Second*3),
			integration.StreamTestOptVarSet("VAR4", ""),
		)

		t.Run("range of partitions", func(t *testing.T) {
			t.Parallel()
			suite.Run(
				t, template,
				integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
					require.NoError(t, createKafkaTopic(ctx, "localhost:"+kafkaPortStr, vars.ID, 4))
				}),
				integration.StreamTestOptPort(kafkaPortStr),
				integration.StreamTestOptSleepAfterInput(time.Second*3),
				integration.StreamTestOptVarSet("VAR1", ":0-3"),
				integration.StreamTestOptVarSet("VAR4", ""),
			)
		})
	})

	manualPartitionTemplate := `
output:
  kafka_franz:
    seed_brokers: [ localhost:$PORT ]
    topic: topic-$ID
    max_in_flight: $MAX_IN_FLIGHT
    timeout: "5s"
    partitioner: manual
    partition: "0"
    metadata:
      include_patterns: [ .* ]
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  kafka_franz:
    seed_brokers: [ localhost:$PORT ]
    topics: [ topic-$ID$VAR1 ]
    consumer_group: "$VAR4"
    checkpoint_limit: 100
    commit_period: "1s"
`
	t.Run("manual_partitioner", func(t *testing.T) {
		suite.Run(
			t, manualPartitionTemplate,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				vars.General["VAR4"] = "group" + vars.ID
				require.NoError(t, createKafkaTopic(t.Context(), "localhost:"+kafkaPortStr, vars.ID, 1))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
			integration.StreamTestOptVarSet("VAR1", ""),
		)
	})
}

func TestIntegrationFranzSasl(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaPort, err := integration.GetFreePort()
	require.NoError(t, err)

	kafkaPortStr := strconv.Itoa(kafkaPort)

	options := &dockertest.RunOptions{
		Repository:   "docker.redpanda.com/redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{"9092/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr + "/tcp"}},
		},
		Cmd: []string{
			"redpanda",
			"start",
			"--node-id 0",
			"--mode dev-container",
			"--set rpk.additional_start_flags=[--reactor-backend=epoll]",
			"--kafka-addr 0.0.0.0:9092",
			"--set redpanda.enable_sasl=true",
			`--set redpanda.superusers=["admin"]`,
			fmt.Sprintf("--advertise-kafka-addr localhost:%v", kafkaPort),
		},
	}

	pool.MaxWait = time.Minute
	resource, err := pool.RunWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	adminCreated := false

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		if !adminCreated {
			var stdErr bytes.Buffer
			_, aerr := resource.Exec([]string{
				"rpk", "acl", "user", "create", "admin",
				"--password", "foobar",
				"--api-urls", "localhost:9644",
			}, dockertest.ExecOptions{
				StdErr: &stdErr,
			})
			if aerr != nil {
				return aerr
			}
			if stdErr.String() != "" {
				return errors.New(stdErr.String())
			}
			adminCreated = true
		}
		return createKafkaTopicSasl("localhost:"+kafkaPortStr, "testingconnection", 1)
	}))

	template := `
output:
  kafka_franz:
    seed_brokers: [ localhost:$PORT ]
    topic: topic-$ID
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      include_patterns: [ .* ]
    batching:
      count: $OUTPUT_BATCH_COUNT
    sasl:
      - mechanism: SCRAM-SHA-256
        username: admin
        password: foobar

input:
  kafka_franz:
    seed_brokers: [ localhost:$PORT ]
    topics: [ topic-$ID$VAR1 ]
    consumer_group: "$VAR4"
    sasl:
      - mechanism: SCRAM-SHA-256
        username: admin
        password: foobar
`

	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestMetadata(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallel(1000),
		integration.StreamTestStreamParallelLossy(1000),
		integration.StreamTestSendBatchCount(10),
	)

	suite.Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
			vars.General["VAR4"] = "group" + vars.ID
			require.NoError(t, createKafkaTopicSasl("localhost:"+kafkaPortStr, vars.ID, 4))
		}),
		integration.StreamTestOptPort(kafkaPortStr),
		integration.StreamTestOptVarSet("VAR1", ""),
	)
}

func TestIntegrationFranzOutputFixedTimestamp(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaPort, err := integration.GetFreePort()
	require.NoError(t, err)

	kafkaPortStr := strconv.Itoa(kafkaPort)

	options := &dockertest.RunOptions{
		Repository:   "docker.redpanda.com/redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{"9092/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr + "/tcp"}},
		},
		Cmd: []string{
			"redpanda",
			"start",
			"--node-id 0",
			"--mode dev-container",
			"--set rpk.additional_start_flags=[--reactor-backend=epoll]",
			"--kafka-addr 0.0.0.0:9092",
			fmt.Sprintf("--advertise-kafka-addr localhost:%v", kafkaPort),
		},
	}

	pool.MaxWait = time.Minute
	resource, err := pool.RunWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return createKafkaTopic(t.Context(), "localhost:"+kafkaPortStr, "testingconnection", 1)
	}))

	template := `
output:
  kafka_franz:
    seed_brokers: [ localhost:$PORT ]
    topic: topic-$ID
    timestamp_ms: 1000000000000

input:
  kafka_franz:
    seed_brokers: [ localhost:$PORT ]
    topics: [ topic-$ID ]
    consumer_group: "blobfish"
  processors:
    - mapping: |
        root = if metadata("kafka_timestamp_ms") != 1000000000000 { "error: invalid timestamp" }
`

	suite := integration.StreamTests(
		integration.StreamTestOpenCloseIsolated(),
	)

	suite.Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
			require.NoError(t, createKafkaTopic(ctx, "localhost:"+kafkaPortStr, vars.ID, 1))
		}),
		integration.StreamTestOptPort(kafkaPortStr),
	)
}

func BenchmarkIntegrationFranz(b *testing.B) {
	integration.CheckSkip(b)

	pool, err := dockertest.NewPool("")
	require.NoError(b, err)

	kafkaPort, err := integration.GetFreePort()
	require.NoError(b, err)

	kafkaPortStr := strconv.Itoa(kafkaPort)

	options := &dockertest.RunOptions{
		Repository:   "docker.redpanda.com/redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{"9092/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr + "/tcp"}},
		},
		Cmd: []string{
			"redpanda",
			"start",
			"--node-id 0",
			"--mode dev-container",
			"--set rpk.additional_start_flags=[--reactor-backend=epoll]",
			"--kafka-addr 0.0.0.0:9092",
			fmt.Sprintf("--advertise-kafka-addr localhost:%v", kafkaPort),
		},
	}

	pool.MaxWait = time.Minute
	resource, err := pool.RunWithOptions(options)
	require.NoError(b, err)
	b.Cleanup(func() {
		assert.NoError(b, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(b, pool.Retry(func() error {
		return createKafkaTopic(b.Context(), "localhost:"+kafkaPortStr, "testingconnection", 1)
	}))

	// Unordered (old) client
	b.Run("unordered", func(b *testing.B) {
		template := `
output:
  kafka_franz:
    seed_brokers: [ localhost:$PORT ]
    topic: topic-$ID
    max_in_flight: 128
    timeout: "5s"
    metadata:
      include_patterns: [ .* ]

input:
  kafka_franz:
    seed_brokers: [ localhost:$PORT ]
    topics: [ topic-$ID ]
    consumer_group: "$VAR3"
    checkpoint_limit: 100
    commit_period: "1s"
    batching:
      count: 20
      period: 1ms
`
		suite := integration.StreamBenchs(
			integration.StreamBenchSend(20, 1),
			integration.StreamBenchSend(10, 1),
			integration.StreamBenchSend(1, 1),
			// integration.StreamBenchReadSaturated(),
		)
		suite.Run(
			b, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				vars.General["VAR3"] = "group" + vars.ID
				require.NoError(t, createKafkaTopic(ctx, "localhost:"+kafkaPortStr, vars.ID, 1))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
		)
	})
}
