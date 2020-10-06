package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ = registerIntegrationTest("gcp_pubsub", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "singularities/pubsub-emulator",
		Tag:          "latest",
		ExposedPorts: []string{"8432/tcp"},
		Env: []string{
			"PUBSUB_LISTEN_ADDRESS=0.0.0.0:8432",
			"PUBSUB_PROJECT_ID=benthos-test-project",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	require.NoError(t, os.Setenv("PUBSUB_EMULATOR_HOST", fmt.Sprintf("localhost:%v", resource.GetPort("8432/tcp"))))
	require.NotEqual(t, "localhost:", os.Getenv("PUBSUB_EMULATOR_HOST"))

	resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client, err := pubsub.NewClient(ctx, "benthos-test-project")
		if err != nil {
			return err
		}
		_, err = client.CreateTopic(ctx, "test-probe-topic-name")
		client.Close()
		return err
	}))

	template := `
output:
  gcp_pubsub:
    project: benthos-test-project
    topic: topic-$ID
    max_in_flight: $MAX_IN_FLIGHT

input:
  gcp_pubsub:
    project: benthos-test-project
    subscription: sub-$ID
`
	suiteOpts := []testOptFunc{
		testOptSleepAfterInput(100 * time.Millisecond),
		testOptSleepAfterOutput(100 * time.Millisecond),
		testOptTimeout(time.Minute * 5),
		testOptPreTest(func(t *testing.T, env *testEnvironment) {
			client, err := pubsub.NewClient(env.ctx, "benthos-test-project")
			require.NoError(t, err)

			topic, err := client.CreateTopic(env.ctx, fmt.Sprintf("topic-%v", env.configVars.id))
			require.NoError(t, err)

			_, err = client.CreateSubscription(env.ctx, fmt.Sprintf("sub-%v", env.configVars.id), pubsub.SubscriptionConfig{
				AckDeadline: time.Second * 10,
				RetryPolicy: &pubsub.RetryPolicy{
					MaximumBackoff: time.Millisecond * 10,
					MinimumBackoff: time.Millisecond * 10,
				},
				Topic: topic,
			})
			require.NoError(t, err)

			client.Close()
		}),
	}
	suite := integrationTests(
		integrationTestOpenClose(),
		integrationTestMetadata(),
		integrationTestSendBatch(10),
		integrationTestStreamSequential(1000),
		integrationTestStreamParallel(1000),
		integrationTestStreamParallelLossy(1000),
	)
	suite.Run(t, template, suiteOpts...)
	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			append([]testOptFunc{testOptMaxInFlight(10)}, suiteOpts...)...,
		)
	})
})
