package gcp

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func TestIntegrationGCPPubSub(t *testing.T) {
	integration.CheckSkip(t)
	if runtime.GOOS == "darwin" {
		t.Skip("skipping test on macos")
	}

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

	_ = resource.Expire(900)
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
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]

input:
  gcp_pubsub:
    project: benthos-test-project
    subscription: sub-$ID
    create_subscription:
      enabled: true
      topic: topic-$ID
`
	suiteOpts := []integration.StreamTestOptFunc{
		integration.StreamTestOptSleepAfterInput(100 * time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(100 * time.Millisecond),
		integration.StreamTestOptTimeout(time.Minute * 5),
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
			client, err := pubsub.NewClient(ctx, "benthos-test-project")
			require.NoError(t, err)

			_, err = client.CreateTopic(ctx, fmt.Sprintf("topic-%v", testID))
			require.NoError(t, err)

			client.Close()
		}),
	}
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestMetadata(),
		integration.StreamTestMetadataFilter(),
		integration.StreamTestSendBatches(10, 1000, 10),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallel(1000),
		integration.StreamTestStreamParallelLossy(1000),
		// integration.StreamTestAtLeastOnceDelivery(),
	)
	suite.Run(t, template, suiteOpts...)
	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			append([]integration.StreamTestOptFunc{integration.StreamTestOptMaxInFlight(10)}, suiteOpts...)...,
		)
	})
}
