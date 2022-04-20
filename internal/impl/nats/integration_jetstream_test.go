package nats

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func TestIntegrationNatsJetstream(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "nats",
		Tag:        "latest",
		Cmd:        []string{"--js"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var natsConn *nats.Conn
	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		natsConn, err = nats.Connect(fmt.Sprintf("tcp://localhost:%v", resource.GetPort("4222/tcp")))
		return err
	}))
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
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
			js, err := natsConn.JetStream()
			require.NoError(t, err)

			streamName := "stream-" + testID

			_, err = js.AddStream(&nats.StreamConfig{
				Name:     streamName,
				Subjects: []string{"subject-" + testID},
			})
			require.NoError(t, err)
		}),
		integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
		integration.StreamTestOptPort(resource.GetPort("4222/tcp")),
	)
}

func TestIntegrationNatsPullConsumer(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "nats",
		Tag:        "latest",
		Cmd:        []string{"--js"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var natsConn *nats.Conn
	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		natsConn, err = nats.Connect(fmt.Sprintf("tcp://localhost:%v", resource.GetPort("4222/tcp")))
		return err
	}))
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
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
			js, err := natsConn.JetStream()
			require.NoError(t, err)

			streamName := "stream-" + testID

			_, err = js.AddStream(&nats.StreamConfig{
				Name:     streamName,
				Subjects: []string{"subject-" + testID},
			})
			require.NoError(t, err)

			_, err = js.AddConsumer(streamName, &nats.ConsumerConfig{
				Durable:       "durable-" + testID,
				DeliverPolicy: nats.DeliverAllPolicy,
				AckPolicy:     nats.AckExplicitPolicy,
			})
			require.NoError(t, err)
		}),
		integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
		integration.StreamTestOptPort(resource.GetPort("4222/tcp")),
	)
}
