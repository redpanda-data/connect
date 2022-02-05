package integration

import (
	"context"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/integration"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ = registerIntegrationTest("kinesis", func(t *testing.T) {
	// Skip until annoying logs can be removed.
	t.Skip()

	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "localstack/localstack",
		ExposedPorts: []string{"4566/tcp"},
		Env:          []string{"SERVICES=dynamodb,kinesis"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	resource.Expire(900)

	require.NoError(t, pool.Retry(func() error {
		return createKinesisShards(context.Background(), resource.GetPort("4566/tcp"), "testtable", 2)
	}))

	template := `
output:
  kinesis:
    endpoint: http://localhost:$PORT
    region: us-east-1
    stream: stream-$ID
    partition_key: ${! uuid_v4() }
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  kinesis_balanced:
    endpoint: http://localhost:$PORT
    stream: stream-$ID
    dynamodb_table: stream-$ID
    start_from_oldest: true
    region: us-east-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
	integration.StreamTests(
		integration.StreamTestOpenClose(),
		// integration.StreamTestMetadata(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestSendBatchCount(10),
		integration.StreamTestStreamSequential(10),
		integration.StreamTestStreamParallel(10),
		integration.StreamTestStreamParallelLossy(10),
		integration.StreamTestStreamParallelLossyThroughReconnect(10),
	).Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
			require.NoError(t, createKinesisShards(ctx, resource.GetPort("4566/tcp"), testID, 2))
		}),
		integration.StreamTestOptPort(resource.GetPort("4566/tcp")),
		integration.StreamTestOptAllowDupes(),
	)
})
