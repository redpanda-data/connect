package aws

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func createKinesisShards(ctx context.Context, tb testing.TB, awsPort, id string, numShards int32) ([]string, error) {
	tb.Helper()

	endpoint := fmt.Sprintf("http://localhost:%v", awsPort)

	conf, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(tb, err)

	conf.BaseEndpoint = &endpoint
	client := kinesis.NewFromConfig(conf)

	strmID := "stream-" + id
	for {
		tb.Logf("Creating stream '%v'", id)
		_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
			ShardCount: &numShards,
			StreamName: &strmID,
		})
		if err == nil {
			tb.Logf("Created stream '%v'", id)
			break
		}

		tb.Logf("Failed to create stream '%v': %v", id, err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	// wait for stream to exist
	waiter := kinesis.NewStreamExistsWaiter(client)
	err = waiter.Wait(ctx, &kinesis.DescribeStreamInput{
		StreamName: &strmID,
	}, time.Second*30)
	if err != nil {
		return nil, err
	}

	info, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String("stream-" + id),
	})
	if err != nil {
		return nil, err
	}
	var shards []string
	for _, shard := range info.StreamDescription.Shards {
		shards = append(shards, *shard.ShardId)
	}
	return shards, nil
}

func kinesisIntegrationSuite(t *testing.T, lsPort string) {
	t.Helper()

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
			integration.StreamTestOptPreTest(func(tb testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
				tb.Helper()

				streamName := "stream-" + testID
				shards, err := createKinesisShards(ctx, tb, lsPort, testID, 2)
				require.NoError(tb, err)

				for i, shard := range shards {
					if i == 0 {
						vars.Var1 = fmt.Sprintf(":%v", shard)
					} else {
						vars.Var1 += fmt.Sprintf(",%v:%v", streamName, shard)
					}
				}
			}),
			integration.StreamTestOptPort(lsPort),
			integration.StreamTestOptAllowDupes(),
			integration.StreamTestOptVarTwo("10"),
		)
	})

	t.Run("with balanced shards", func(t *testing.T) {
		suite.Run(
			t, template,
			integration.StreamTestOptPreTest(func(tb testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
				tb.Helper()

				_, err := createKinesisShards(ctx, tb, lsPort, testID, 2)
				require.NoError(tb, err)
			}),
			integration.StreamTestOptPort(lsPort),
			integration.StreamTestOptAllowDupes(),
			integration.StreamTestOptVarTwo("10"),
		)
	})

	t.Run("single shard", func(t *testing.T) {
		integration.StreamTests(
			integration.StreamTestCheckpointCapture(),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(tb testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
				tb.Helper()

				shards, err := createKinesisShards(ctx, tb, lsPort, testID, 1)
				require.NoError(tb, err)
				vars.Var1 = ":" + shards[0]
			}),
			integration.StreamTestOptPort(lsPort),
			integration.StreamTestOptAllowDupes(),
			integration.StreamTestOptVarTwo("10"),
		)
	})
}
