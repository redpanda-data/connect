package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/stan.go"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ = registerIntegrationTest("nats_stream", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("nats-streaming", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	resource.Expire(900)
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
	suite := integrationTests(
		integrationTestOpenClose(),
		// integrationTestMetadata(), TODO
		integrationTestSendBatch(10),
		integrationTestStreamParallel(1000),
		integrationTestStreamSequential(1000),
		integrationTestStreamParallelLossy(1000),
		integrationTestStreamParallelLossyThroughReconnect(1000),
	)
	suite.Run(
		t, template,
		testOptSleepAfterInput(100*time.Millisecond),
		testOptSleepAfterOutput(100*time.Millisecond),
		testOptPort(resource.GetPort("4222/tcp")),
		testOptTimeout(time.Second*60),
	)
	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			testOptSleepAfterInput(100*time.Millisecond),
			testOptSleepAfterOutput(100*time.Millisecond),
			testOptPort(resource.GetPort("4222/tcp")),
			testOptMaxInFlight(10),
		)
	})
})
