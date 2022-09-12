package beanstalkd

import (
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func TestIntegrationBeanstalkdOpenClose(t *testing.T) {
	integration.CheckSkip(t)

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
		integration.StreamTestOpenClose(),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("11300/tcp")),
	)
}

func TestIntegrationBeanstalkdSendBatch(t *testing.T) {
	integration.CheckSkip(t)

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

func TestIntegrationBeanstalkdStreamSequential(t *testing.T) {
	integration.CheckSkip(t)

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
		integration.StreamTestStreamSequential(100),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("11300/tcp")),
	)
}

func TestIntegrationBeanstalkdStreamParallel(t *testing.T) {
	integration.CheckSkip(t)

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
		integration.StreamTestStreamParallel(100),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("11300/tcp")),
	)
}
