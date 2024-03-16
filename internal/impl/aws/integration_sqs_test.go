package aws

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func sqsIntegrationSuite(t *testing.T, lsPort string) {
	template := `
output:
  aws_sqs:
    url: http://localhost:$PORT/000000000000/queue-$ID
    endpoint: http://localhost:$PORT
    region: eu-west-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    max_in_flight: $MAX_IN_FLIGHT
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  aws_sqs:
    url: http://localhost:$PORT/000000000000/queue-$ID
    endpoint: http://localhost:$PORT
    region: eu-west-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
	integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamSequential(50),
		integration.StreamTestStreamParallel(50),
		integration.StreamTestStreamParallelLossy(50),
		integration.StreamTestStreamParallelLossyThroughReconnect(50),
	).Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
			require.NoError(t, createBucketQueue(ctx, "", lsPort, testID))
		}),
		integration.StreamTestOptPort(lsPort),
	)
}
