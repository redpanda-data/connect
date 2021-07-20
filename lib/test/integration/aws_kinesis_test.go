package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

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

	suite := integrationTests(
		integrationTestOpenClose(),
		integrationTestSendBatch(10),
		integrationTestSendBatchCount(10),
		integrationTestStreamSequential(200),
		integrationTestStreamParallel(200),
		integrationTestStreamParallelLossy(200),
		integrationTestStreamParallelLossyThroughReconnect(200),
	)

	t.Run("with static shards", func(t *testing.T) {
		suite.Run(
			t, template,
			testOptPreTest(func(t testing.TB, env *testEnvironment) {
				streamName := "stream-" + env.configVars.id
				env.configVars.var1 = fmt.Sprintf(":0,%v:1", streamName)
				require.NoError(t, createKinesisShards(env.ctx, resource.GetPort("4566/tcp"), env.configVars.id, 2))
			}),
			testOptPort(resource.GetPort("4566/tcp")),
			testOptAllowDupes(),
			testOptVarTwo("10"),
		)
	})

	t.Run("with balanced shards", func(t *testing.T) {
		suite.Run(
			t, template,
			testOptPreTest(func(t testing.TB, env *testEnvironment) {
				require.NoError(t, createKinesisShards(env.ctx, resource.GetPort("4566/tcp"), env.configVars.id, 2))
			}),
			testOptPort(resource.GetPort("4566/tcp")),
			testOptAllowDupes(),
			testOptVarTwo("10"),
		)
	})

	t.Run("single shard", func(t *testing.T) {
		integrationTests(
			integrationTestCheckpointCapture(),
		).Run(
			t, template,
			testOptPreTest(func(t testing.TB, env *testEnvironment) {
				require.NoError(t, createKinesisShards(env.ctx, resource.GetPort("4566/tcp"), env.configVars.id, 1))
			}),
			testOptPort(resource.GetPort("4566/tcp")),
			testOptAllowDupes(),
			testOptVarOne(":0"),
			testOptVarTwo("10"),
		)
	})
})
