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

var _ = registerIntegrationTest("nats", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("nats", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		natsConn, err := nats.Connect(fmt.Sprintf("tcp://localhost:%v", resource.GetPort("4222/tcp")))
		if err != nil {
			return err
		}
		natsConn.Close()
		return nil
	}))

	template := `
output:
  nats:
    urls: [ tcp://localhost:$PORT ]
    subject: subject-$ID
    max_in_flight: $MAX_IN_FLIGHT

input:
  nats:
    urls: [ tcp://localhost:$PORT ]
    queue: queue-$ID
    subject: subject-$ID
    prefetch_count: 1048
`
	suite := integrationTests(
		integrationTestOpenClose(),
		// integrationTestMetadata(), TODO
		integrationTestSendBatch(10),
		integrationTestStreamParallel(500),
		integrationTestStreamParallelLossy(500),
	)
	suite.Run(
		t, template,
		testOptSleepAfterInput(100*time.Millisecond),
		testOptSleepAfterOutput(100*time.Millisecond),
		testOptPort(resource.GetPort("4222/tcp")),
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
