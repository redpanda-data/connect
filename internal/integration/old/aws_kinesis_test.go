package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/integration"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createKinesisShards(ctx context.Context, awsPort, id string, numShards int) error {
	endpoint := fmt.Sprintf("http://localhost:%v", awsPort)

	client := kinesis.New(session.Must(session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		Endpoint:    aws.String(endpoint),
		Region:      aws.String("us-east-1"),
	})))

	_, err := client.CreateStreamWithContext(ctx, &kinesis.CreateStreamInput{
		ShardCount: aws.Int64(int64(numShards)),
		StreamName: aws.String("stream-" + id),
	})
	if err != nil {
		return err
	}

	// wait for stream to exist
	return client.WaitUntilStreamExistsWithContext(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String("stream-" + id),
	})
}

var _ = registerIntegrationTest("aws_kinesis", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute * 2
	if dline, ok := t.Deadline(); ok && time.Until(dline) < pool.MaxWait {
		pool.MaxWait = time.Until(dline)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "localstack/localstack",
		Tag:          "0.12.12", // Looks like we're bitten by https://github.com/localstack/localstack/issues/4522
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
  aws_kinesis:
    endpoint: http://localhost:$PORT
    region: us-east-1
    stream: stream-$ID
    partition_key: ${! uuid_v4() }
    max_in_flight: $MAX_IN_FLIGHT
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  aws_kinesis:
    endpoint: http://localhost:$PORT
    streams: [ stream-$ID$VAR1 ]
    checkpoint_limit: $VAR2
    dynamodb:
      table: stream-$ID
      create: true
    start_from_oldest: true
    region: us-east-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`

	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestSendBatchCount(10),
		integration.StreamTestStreamSequential(200),
		integration.StreamTestStreamParallel(200),
		integration.StreamTestStreamParallelLossy(200),
		integration.StreamTestStreamParallelLossyThroughReconnect(200),
	)

	t.Run("with static shards", func(t *testing.T) {
		suite.Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
				streamName := "stream-" + testID
				vars.Var1 = fmt.Sprintf(":0,%v:1", streamName)
				require.NoError(t, createKinesisShards(ctx, resource.GetPort("4566/tcp"), testID, 2))
			}),
			integration.StreamTestOptPort(resource.GetPort("4566/tcp")),
			integration.StreamTestOptAllowDupes(),
			integration.StreamTestOptVarTwo("10"),
		)
	})

	t.Run("with balanced shards", func(t *testing.T) {
		suite.Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createKinesisShards(ctx, resource.GetPort("4566/tcp"), testID, 2))
			}),
			integration.StreamTestOptPort(resource.GetPort("4566/tcp")),
			integration.StreamTestOptAllowDupes(),
			integration.StreamTestOptVarTwo("10"),
		)
	})

	t.Run("single shard", func(t *testing.T) {
		integration.StreamTests(
			integration.StreamTestCheckpointCapture(),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createKinesisShards(ctx, resource.GetPort("4566/tcp"), testID, 1))
			}),
			integration.StreamTestOptPort(resource.GetPort("4566/tcp")),
			integration.StreamTestOptAllowDupes(),
			integration.StreamTestOptVarOne(":0"),
			integration.StreamTestOptVarTwo("10"),
		)
	})
})
