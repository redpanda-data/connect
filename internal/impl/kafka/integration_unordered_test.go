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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/stretchr/testify/require"
)

func TestIntegrationUnordered(t *testing.T) {
	integration.CheckSkip(t)

	brokerAddr, kafkaPortStr := startRedpanda(t)

	require.Eventually(t, func() bool {
		return createKafkaTopic(t.Context(), brokerAddr, "testingconnection", 1) == nil
	}, time.Minute, time.Second)

	template := `
output:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topic: topic-$ID
    max_in_flight: $MAX_IN_FLIGHT
    timeout: "5s"
    metadata:
      include_patterns: [ .* ]

input:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topics: [ topic-$ID$VAR1 ]
    consumer_group: "$VAR4"
    commit_period: "1s"
    unordered_processing:
      enabled: true
      checkpoint_limit: 100
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
		integration.StreamTestStreamSaturatedUnacked(200),
	)

	// Lighter suite for partition-specific groups that only need to validate
	// routing correctness, not throughput. Keeps total runtime well within
	// the parent test deadline so later groups don't get starved.
	partitionSuite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestMetadata(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamSequential(100),
		integration.StreamTestStreamParallel(100),
	)

	// In some modes include testing input level batching
	var partitionSuiteExt integration.StreamTestList
	partitionSuiteExt = append(partitionSuiteExt, partitionSuite...)
	partitionSuiteExt = append(partitionSuiteExt, integration.StreamTestReceiveBatchCount(10))

	suite.Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
			vars.General["VAR4"] = "group" + vars.ID
			require.NoError(t, createKafkaTopic(ctx, brokerAddr, vars.ID, 4))
		}),
		integration.StreamTestOptPort(kafkaPortStr),
		integration.StreamTestOptVarSet("VAR1", ""),
	)

	t.Run("only one partition", func(t *testing.T) {
		t.Parallel()
		partitionSuiteExt.Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				vars.General["VAR4"] = "group" + vars.ID
				require.NoError(t, createKafkaTopic(ctx, brokerAddr, vars.ID, 1))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
			integration.StreamTestOptVarSet("VAR1", ""),
		)
	})

	t.Run("explicit partitions", func(t *testing.T) {
		t.Parallel()
		partitionSuite.Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				topicName := "topic-" + vars.ID
				vars.General["VAR1"] = fmt.Sprintf(":0,%v:1,%v:2,%v:3", topicName, topicName, topicName)
				require.NoError(t, createKafkaTopic(ctx, brokerAddr, vars.ID, 4))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
			integration.StreamTestOptSleepAfterInput(time.Second*3),
			integration.StreamTestOptVarSet("VAR4", ""),
		)

		t.Run("range of partitions", func(t *testing.T) {
			t.Parallel()
			partitionSuite.Run(
				t, template,
				integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
					require.NoError(t, createKafkaTopic(ctx, brokerAddr, vars.ID, 4))
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
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topic: topic-$ID
    max_in_flight: $MAX_IN_FLIGHT
    timeout: "5s"
    partitioner: manual
    partition: "0"
    metadata:
      include_patterns: [ .* ]

input:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topics: [ topic-$ID$VAR1 ]
    consumer_group: "$VAR4"
    unordered_processing:
      enabled: true
      checkpoint_limit: 100
    commit_period: "1s"
`
	t.Run("manual_partitioner", func(t *testing.T) {
		t.Parallel()
		partitionSuite.Run(
			t, manualPartitionTemplate,
			integration.StreamTestOptPreTest(func(t testing.TB, _ context.Context, vars *integration.StreamTestConfigVars) {
				vars.General["VAR4"] = "group" + vars.ID
				require.NoError(t, createKafkaTopic(t.Context(), brokerAddr, vars.ID, 1))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
			integration.StreamTestOptVarSet("VAR1", ""),
		)
	})
}

func TestIntegrationUnorderedSasl(t *testing.T) {
	integration.CheckSkip(t)

	brokerAddr, kafkaPortStr := startRedpandaWithSASL(t)

	require.Eventually(t, func() bool {
		return createKafkaTopicSasl(brokerAddr, "testingconnection", 1) == nil
	}, time.Minute, time.Second)

	template := `
output:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topic: topic-$ID
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      include_patterns: [ .* ]
    sasl:
      - mechanism: SCRAM-SHA-256
        username: admin
        password: foobar

input:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topics: [ topic-$ID$VAR1 ]
    consumer_group: "$VAR4"
    sasl:
      - mechanism: SCRAM-SHA-256
        username: admin
        password: foobar
    unordered_processing:
      enabled: true
`

	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestMetadata(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallel(1000),
		integration.StreamTestStreamParallelLossy(1000),
	)

	suite.Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, _ context.Context, vars *integration.StreamTestConfigVars) {
			vars.General["VAR4"] = "group" + vars.ID
			require.NoError(t, createKafkaTopicSasl(brokerAddr, vars.ID, 4))
		}),
		integration.StreamTestOptPort(kafkaPortStr),
		integration.StreamTestOptVarSet("VAR1", ""),
	)
}

func BenchmarkIntegrationUnordered(b *testing.B) {
	integration.CheckSkip(b)

	brokerAddr, kafkaPortStr := startRedpanda(b)

	require.Eventually(b, func() bool {
		return createKafkaTopic(b.Context(), brokerAddr, "testingconnection", 1) == nil
	}, time.Minute, time.Second)

	// Unordered (old) client
	b.Run("unordered", func(b *testing.B) {
		template := `
output:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topic: topic-$ID
    max_in_flight: 128
    timeout: "5s"
    metadata:
      include_patterns: [ .* ]

input:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topics: [ topic-$ID ]
    consumer_group: "$VAR3"
    checkpoint_limit: 100
    commit_period: "1s"
    unordered_processing:
      enabled: true
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
				require.NoError(t, createKafkaTopic(ctx, brokerAddr, vars.ID, 1))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
		)
	})
}
