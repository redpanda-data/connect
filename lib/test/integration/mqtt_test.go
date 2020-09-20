package integration

import (
	"fmt"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ = registerIntegrationTest("mqtt", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("ncarlier/mqtt", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		inConf := mqtt.NewClientOptions().SetClientID("UNIT_TEST")
		inConf = inConf.AddBroker(fmt.Sprintf("tcp://localhost:%v", resource.GetPort("1883/tcp")))

		mIn := mqtt.NewClient(inConf)
		tok := mIn.Connect()
		tok.Wait()
		if cErr := tok.Error(); cErr != nil {
			return cErr
		}
		mIn.Disconnect(0)
		return nil
	}))

	template := `
output:
  mqtt:
    urls: [ tcp://localhost:$PORT ]
    qos: 1
    topic: topic-$ID
    client_id: client-output-$ID
    max_in_flight: $MAX_IN_FLIGHT

input:
  mqtt:
    urls: [ tcp://localhost:$PORT ]
    topics: [ topic-$ID ]
    client_id: client-input-$ID
`
	suite := integrationTests(
		integrationTestOpenClose(),
		// integrationTestMetadata(), TODO
		integrationTestSendBatch(10),
		integrationTestStreamParallel(1000),
		integrationTestStreamParallelLossy(1000),
	)
	suite.Run(
		t, template,
		testOptSleepAfterInput(100*time.Millisecond),
		testOptSleepAfterOutput(100*time.Millisecond),
		testOptPort(resource.GetPort("1883/tcp")),
	)
	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			testOptSleepAfterInput(100*time.Millisecond),
			testOptSleepAfterOutput(100*time.Millisecond),
			testOptPort(resource.GetPort("1883/tcp")),
			testOptMaxInFlight(10),
		)
	})
})
