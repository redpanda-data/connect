package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ = registerIntegrationTest("pulsar", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("apachepulsar/pulsar-standalone", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		client, err := pulsar.NewClient(pulsar.ClientOptions{
			URL: fmt.Sprintf("pulsar://localhost:%v/", resource.GetPort("6650/tcp")),
		})
		if err != nil {
			return err
		}
		prod, err := client.CreateProducer(pulsar.ProducerOptions{
			Topic: "benthos-connection-test",
		})
		client.Close()
		if err != nil {
			return err
		}
		prod.Close()
		return nil
	}))

	template := `
output:
  pulsar:
    url: pulsar://localhost:$PORT/
    topic: "topic-$ID"
    max_in_flight: $MAX_IN_FLIGHT

input:
  pulsar:
    url: pulsar://localhost:$PORT/
    topic: "topic-$ID"
    subscription_name: "sub-$ID"
`
	suite := integrationTests(
		integrationTestOpenClose(),
		integrationTestSendBatch(10),
		integrationTestStreamSequential(1000),
		integrationTestStreamParallel(1000),
		integrationTestStreamParallelLossy(1000),
		integrationTestStreamParallelLossyThroughReconnect(1000),
	)
	suite.Run(
		t, template,
		testOptSleepAfterInput(100*time.Millisecond),
		testOptSleepAfterOutput(100*time.Millisecond),
		testOptPort(resource.GetPort("6650/tcp")),
	)
	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			testOptSleepAfterInput(100*time.Millisecond),
			testOptSleepAfterOutput(100*time.Millisecond),
			testOptPort(resource.GetPort("6650/tcp")),
			testOptMaxInFlight(10),
		)
	})
})
