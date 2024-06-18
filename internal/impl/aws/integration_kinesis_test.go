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

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func createKinesisShards(ctx context.Context, t testing.TB, awsPort, id string, numShards int32) ([]string, error) {
	endpoint := fmt.Sprintf("http://localhost:%v", awsPort)

	conf, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	conf.BaseEndpoint = &endpoint
	client := kinesis.NewFromConfig(conf)

	strmID := "stream-" + id
	for {
		t.Logf("Creating stream '%v'", id)
		_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
			ShardCount: &numShards,
			StreamName: &strmID,
		})
		if err == nil {
			t.Logf("Created stream '%v'", id)
			break
		}

		t.Logf("Failed to create stream '%v': %v", id, err)
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
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				streamName := "stream-" + vars.ID
				shards, err := createKinesisShards(ctx, t, lsPort, vars.ID, 2)
				require.NoError(t, err)

				for i, shard := range shards {
					if i == 0 {
						vars.General["VAR1"] = fmt.Sprintf(":%v", shard)
					} else {
						vars.General["VAR1"] += fmt.Sprintf(",%v:%v", streamName, shard)
					}
				}
			}),
			integration.StreamTestOptPort(lsPort),
			integration.StreamTestOptAllowDupes(),
			integration.StreamTestOptVarSet("VAR1", ""),
			integration.StreamTestOptVarSet("VAR2", "10"),
		)
	})

	t.Run("with balanced shards", func(t *testing.T) {
		suite.Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				_, err := createKinesisShards(ctx, t, lsPort, vars.ID, 2)
				require.NoError(t, err)
			}),
			integration.StreamTestOptPort(lsPort),
			integration.StreamTestOptAllowDupes(),
			integration.StreamTestOptVarSet("VAR1", ""),
			integration.StreamTestOptVarSet("VAR2", "10"),
		)
	})

	t.Run("single shard", func(t *testing.T) {
		integration.StreamTests(
			integration.StreamTestCheckpointCapture(),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				shards, err := createKinesisShards(ctx, t, lsPort, vars.ID, 1)
				require.NoError(t, err)
				vars.General["VAR1"] = ":" + shards[0]
			}),
			integration.StreamTestOptPort(lsPort),
			integration.StreamTestOptAllowDupes(),
			integration.StreamTestOptVarSet("VAR2", "10"),
		)
	})
}
