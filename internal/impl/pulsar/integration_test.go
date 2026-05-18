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

package pulsar

import (
	"fmt"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationPulsar(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "apachepulsar/pulsar:3.3.4",
		testcontainers.WithCmd("bin/pulsar", "standalone"),
		testcontainers.WithExposedPorts("6650/tcp", "8080/tcp"),
		testcontainers.WithWaitStrategyAndDeadline(3*time.Minute,
			wait.ForHTTP("/admin/v2/brokers/ready").WithPort("8080/tcp").WithStartupTimeout(3*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mappedPort, err := ctr.MappedPort(t.Context(), "6650/tcp")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		client, err := pulsar.NewClient(pulsar.ClientOptions{
			URL:    fmt.Sprintf("pulsar://localhost:%v/", mappedPort.Port()),
			Logger: NoopLogger(),
		})
		if err != nil {
			return false
		}
		prod, err := client.CreateProducer(pulsar.ProducerOptions{
			Topic: "benthos-connection-test",
		})
		if err == nil {
			prod.Close()
		}
		client.Close()
		return err == nil
	}, 2*time.Minute, time.Second)

	template := `
output:
  pulsar:
    url: pulsar://localhost:$PORT/
    topic: "topic-$ID"
    max_in_flight: $MAX_IN_FLIGHT

input:
  pulsar:
    url: pulsar://localhost:$PORT/
    topics: [ "topic-$ID" ]
    subscription_name: "sub-$ID"
`

	patternTemplate := `
output:
  pulsar:
    url: pulsar://localhost:$PORT/
    topic: "topic-$ID"
    max_in_flight: $MAX_IN_FLIGHT

input:
  pulsar:
    url: pulsar://localhost:$PORT/
    topics_pattern: "t.*c-$ID"
    subscription_name: "sub-$ID"
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallel(1000),
		integration.StreamTestStreamParallelLossy(1000),
		integration.StreamTestStreamParallelLossyThroughReconnect(1000),
		// StreamTestAtLeastOnceDelivery disabled due to upstream data race in
		// benthos stream_test_definitions.go:571-584 (concurrent map read/write).
	)

	suite.Run(
		t, template,
		integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
		integration.StreamTestOptPort(mappedPort.Port()),
	)

	t.Run("with topics pattern", func(t *testing.T) {
		suite.Run(
			t, patternTemplate,
			integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
			integration.StreamTestOptPort(mappedPort.Port()),
		)
	})

	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
			integration.StreamTestOptPort(mappedPort.Port()),
			integration.StreamTestOptMaxInFlight(10),
		)
	})
}
