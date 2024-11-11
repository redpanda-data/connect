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

package nats

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/stan.go"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationNatsStream(t *testing.T) {
	// integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("nats-streaming", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		natsConn, err := stan.Connect(
			"test-cluster", "benthos_test_client",
			stan.NatsURL(fmt.Sprintf("tcp://localhost:%v", resource.GetPort("4222/tcp"))),
		)
		if err != nil {
			return err
		}
		natsConn.Close()
		return nil
	}))

	template := `
output:
  nats_stream:
    urls: [ nats://localhost:$PORT ]
    cluster_id: test-cluster
    client_id: client-output-$ID
    subject: subject-$ID
    max_in_flight: $MAX_IN_FLIGHT

input:
  nats_stream:
    urls: [ nats://localhost:$PORT ]
    cluster_id: test-cluster
    client_id: client-input-$ID
    queue: queue-$ID
    subject: subject-$ID
    ack_wait: 5s
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		// integration.StreamTestMetadata(), TODO
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamParallel(1000),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallelLossy(1000),
		integration.StreamTestStreamParallelLossyThroughReconnect(1000),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
		integration.StreamTestOptPort(resource.GetPort("4222/tcp")),
	)
	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("4222/tcp")),
			integration.StreamTestOptMaxInFlight(10),
		)
	})
}
