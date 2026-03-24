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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationNatsJetstream(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	ctr, err := testcontainers.Run(t.Context(), "nats:latest",
		testcontainers.WithCmd("--js"),
		testcontainers.WithExposedPorts("4222/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("4222/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "4222/tcp")
	require.NoError(t, err)

	var natsConn *nats.Conn
	require.Eventually(t, func() bool {
		natsConn, err = nats.Connect(fmt.Sprintf("tcp://localhost:%v", mp.Port()))
		return err == nil
	}, 30*time.Second, time.Second)
	t.Cleanup(func() {
		natsConn.Close()
	})

	template := `
output:
  nats_jetstream:
    urls: [ nats://localhost:$PORT ]
    subject: subject-$ID

input:
  nats_jetstream:
    urls: [ nats://localhost:$PORT ]
    subject: subject-$ID
    durable: durable-$ID
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		// integration.StreamTestMetadata(), TODO
		integration.StreamTestSendBatch(10),
		// integration.StreamTestAtLeastOnceDelivery(), // TODO: SubscribeSync doesn't seem to honor durable setting
		integration.StreamTestStreamParallel(1000),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallelLossy(1000),
		integration.StreamTestStreamParallelLossyThroughReconnect(1000),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, _ context.Context, vars *integration.StreamTestConfigVars) {
			js, err := natsConn.JetStream()
			require.NoError(t, err)

			streamName := "stream-" + vars.ID

			_, err = js.AddStream(&nats.StreamConfig{
				Name:     streamName,
				Subjects: []string{"subject-" + vars.ID},
			})
			require.NoError(t, err)
		}),
		integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
		integration.StreamTestOptPort(mp.Port()),
	)
}

func TestIntegrationNatsPullConsumer(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	ctr, err := testcontainers.Run(t.Context(), "nats:latest",
		testcontainers.WithCmd("--js"),
		testcontainers.WithExposedPorts("4222/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("4222/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "4222/tcp")
	require.NoError(t, err)

	var natsConn *nats.Conn
	require.Eventually(t, func() bool {
		natsConn, err = nats.Connect(fmt.Sprintf("tcp://localhost:%v", mp.Port()))
		return err == nil
	}, 30*time.Second, time.Second)
	t.Cleanup(func() {
		natsConn.Close()
	})

	template := `
output:
  nats_jetstream:
    urls: [ nats://localhost:$PORT ]
    subject: subject-$ID

input:
  nats_jetstream:
    urls: [ nats://localhost:$PORT ]
    durable: durable-$ID
    stream: stream-$ID
    bind: true
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		// integration.StreamTestMetadata(), TODO
		integration.StreamTestSendBatch(10),
		// integration.StreamTestAtLeastOnceDelivery(), // TODO: SubscribeSync doesn't seem to honor durable setting
		integration.StreamTestStreamParallel(1000),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallelLossy(1000),
		integration.StreamTestStreamParallelLossyThroughReconnect(1000),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, _ context.Context, vars *integration.StreamTestConfigVars) {
			js, err := natsConn.JetStream()
			require.NoError(t, err)

			streamName := "stream-" + vars.ID

			_, err = js.AddStream(&nats.StreamConfig{
				Name:     streamName,
				Subjects: []string{"subject-" + vars.ID},
			})
			require.NoError(t, err)

			_, err = js.AddConsumer(streamName, &nats.ConsumerConfig{
				Durable:       "durable-" + vars.ID,
				DeliverPolicy: nats.DeliverAllPolicy,
				AckPolicy:     nats.AckExplicitPolicy,
			})
			require.NoError(t, err)
		}),
		integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
		integration.StreamTestOptPort(mp.Port()),
	)
}
