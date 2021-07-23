package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ = registerIntegrationTest("nats_jetstream", func(t *testing.T) {
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
	resource.Expire(900)
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
	suite := integrationTests(
		integrationTestOpenClose(),
		// integrationTestMetadata(), TODO
		integrationTestSendBatch(10),
		integrationTestAtLeastOnceDelivery(), // TODO: SubscribeSync doesn't seem to honor durable setting
		integrationTestStreamParallel(1000),
		integrationTestStreamSequential(1000),
		integrationTestStreamParallelLossy(1000),
		integrationTestStreamParallelLossyThroughReconnect(1000),
	)
	suite.Run(
		t, template,
		testOptPreTest(func(t testing.TB, env *testEnvironment) {
			js, err := natsConn.JetStream()
			require.NoError(t, err)

			streamName := "stream-" + env.configVars.id

			_, err = js.AddStream(&nats.StreamConfig{
				Name:     streamName,
				Subjects: []string{"subject-" + env.configVars.id},
			})
			require.NoError(t, err)
		}),
		testOptSleepAfterInput(100*time.Millisecond),
		testOptSleepAfterOutput(100*time.Millisecond),
		testOptPort(resource.GetPort("4222/tcp")),
	)
})
