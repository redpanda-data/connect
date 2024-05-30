package aws

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	_ "github.com/redpanda-data/connect/v4/public/components/pure"
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
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
			require.NoError(t, createBucketQueue(ctx, "", lsPort, vars.ID))
		}),
		integration.StreamTestOptPort(lsPort),
	)
}
