package amqp1

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationAMQP1(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("rmohr/activemq", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	ctx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		client, err := amqp.Dial(ctx, fmt.Sprintf("amqp://guest:guest@localhost:%v/", resource.GetPort("5672/tcp")), nil)
		if err == nil {
			client.Close()
		}
		return err
	}))

	templateWithFieldURL := `
output:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    target_address: "queue:/$ID"
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]

input:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    source_address: "queue:/$ID"
`

	templateWithFieldURLS := `
output:
  amqp_1:
    urls:
      - amqp://guest:guest@localhost:1234/
      - amqp://guest:guest@localhost:$PORT/ # fallback URL
      - amqp://guest:guest@localhost:4567/
    target_address: "queue:/$ID"
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]

input:
  amqp_1:
    urls:
      - amqp://guest:guest@localhost:1234/
      - amqp://guest:guest@localhost:$PORT/ # fallback URL
      - amqp://guest:guest@localhost:4567/
    source_address: "queue:/$ID"
`

	testcases := []struct {
		label    string
		template string
	}{
		{
			label:    "should handle old field url",
			template: templateWithFieldURL,
		},
		{
			label:    "should handle new field urls",
			template: templateWithFieldURLS,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			suite := integration.StreamTests(
				integration.StreamTestOpenClose(),
				integration.StreamTestSendBatch(10),
				integration.StreamTestStreamSequential(1000),
				integration.StreamTestStreamParallel(1000),
				integration.StreamTestMetadata(),
				integration.StreamTestMetadataFilter(),
			)
			suite.Run(
				t, tc.template,
				integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
				integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
				integration.StreamTestOptPort(resource.GetPort("5672/tcp")),
			)

			t.Run("with max in flight", func(t *testing.T) {
				t.Parallel()
				suite.Run(
					t, tc.template,
					integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
					integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
					integration.StreamTestOptPort(resource.GetPort("5672/tcp")),
					integration.StreamTestOptMaxInFlight(10),
				)
			})
		})
	}
}
