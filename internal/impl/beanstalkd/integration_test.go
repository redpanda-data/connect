package beanstalkd

import (
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func TestIntegrationBeanstalkd(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("websmurf/beanstalkd", "1.12-alpine-3.14", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return nil
	}))

	template := `
output:
  beanstalkd:
    tcp_address: localhost:$PORT
    max_in_flight: $MAX_IN_FLIGHT

input:
  beanstalkd:
    tcp_address: localhost:$PORT
`
	suite := integration.StreamTests(
		integration.StreamTestSendBatch(10),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("11300/tcp")),
	)
}
