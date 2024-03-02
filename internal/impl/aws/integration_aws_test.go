package aws

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/gofrs/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func createBucketQueue(ctx context.Context, s3Port, sqsPort, id string) error {
	endpoint := fmt.Sprintf("http://localhost:%v", s3Port)
	bucket := "bucket-" + id
	sqsQueue := "queue-" + id
	sqsEndpoint := fmt.Sprintf("http://localhost:%v", sqsPort)
	// sqsQueueURL := fmt.Sprintf("%v/queue/%v", sqsEndpoint, sqsQueue)
	// https://github.com/localstack/localstack/issues/9185
	sqsQueueURL := fmt.Sprintf("%v/000000000000/%v", sqsEndpoint, sqsQueue)

	var s3Client *s3.Client
	if s3Port != "" {
		conf, err := config.LoadDefaultConfig(ctx,
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		)
		if err != nil {
			return err
		}
		conf.BaseEndpoint = &endpoint

		s3Client = s3.NewFromConfig(conf, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	var sqsClient *sqs.Client
	if sqsPort != "" {
		conf, err := config.LoadDefaultConfig(ctx,
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
			config.WithRegion("eu-west-1"),
		)
		if err != nil {
			return err
		}
		conf.BaseEndpoint = &sqsEndpoint
		sqsClient = sqs.NewFromConfig(conf)
	}

	if s3Client != nil {
		if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: &bucket,
		}); err != nil {
			return fmt.Errorf("create bucket: %w", err)
		}
	}

	if sqsClient != nil {
		if _, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName: aws.String(sqsQueue),
		}); err != nil {
			return fmt.Errorf("create queue: %w", err)
		}
	}

	if s3Client != nil {
		waiter := s3.NewBucketExistsWaiter(s3Client)
		if err := waiter.Wait(ctx, &s3.HeadBucketInput{
			Bucket: &bucket,
		}, time.Minute); err != nil {
			return err
		}
	}

	var sqsQueueArn string
	if sqsPort != "" {
		res, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       &sqsQueueURL,
			AttributeNames: []sqstypes.QueueAttributeName{"All"},
		})
		if err != nil {
			return fmt.Errorf("get queue attributes: %w", err)
		}
		sqsQueueArn = res.Attributes["QueueArn"]
	}

	if s3Port != "" && sqsPort != "" {
		if _, err := s3Client.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
			Bucket: &bucket,
			NotificationConfiguration: &s3types.NotificationConfiguration{
				QueueConfigurations: []s3types.QueueConfiguration{
					{
						Events: []s3types.Event{
							s3types.EventS3ObjectCreated,
						},
						QueueArn: &sqsQueueArn,
					},
				},
			},
		}); err != nil {
			return fmt.Errorf("put bucket notification config: %w", err)
		}
	}
	return nil
}

func TestIntegrationAWS(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute * 3

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "localstack/localstack",
		ExposedPorts: []string{"4566/tcp"},
		Env:          []string{"SERVICES=s3,sqs"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	servicePort := resource.GetPort("4566/tcp")

	require.NoError(t, pool.Retry(func() (err error) {
		u4, err := uuid.NewV4()
		require.NoError(t, err)

		return createBucketQueue(context.Background(), servicePort, servicePort, u4.String())
	}))

	t.Run("s3_to_sqs", func(t *testing.T) {
		template := `
output:
  aws_s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    path: ${!count("$ID")}.txt
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  aws_s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    delete_objects: true
    sqs:
      url: http://localhost:$PORT/000000000000/queue-$ID
      key_path: Records.*.s3.object.key
      endpoint: http://localhost:$PORT
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
		integration.StreamTests(
			integration.StreamTestOpenClose(),
			// integration.StreamTestMetadata(), Does dumb stuff with rewriting keys.
			// integration.StreamTestSendBatch(10),
			integration.StreamTestSendBatchCount(10),
			integration.StreamTestStreamSequential(10),
			// integration.StreamTestStreamParallel(10),
			// integration.StreamTestStreamParallelLossy(10),
			integration.StreamTestStreamParallelLossyThroughReconnect(10),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createBucketQueue(ctx, servicePort, servicePort, testID))
			}),
			integration.StreamTestOptPort(servicePort),
			integration.StreamTestOptAllowDupes(),
		)
	})

	t.Run("s3_to_sqs_lines", func(t *testing.T) {
		template := `
output:
  aws_s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    path: ${!count("$ID")}.txt
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    batching:
      count: $OUTPUT_BATCH_COUNT
      processors:
        - archive:
            format: lines

input:
  aws_s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    delete_objects: true
    scanner: { lines: {} }
    sqs:
      url: http://localhost:$PORT/000000000000/queue-$ID
      key_path: Records.*.s3.object.key
      endpoint: http://localhost:$PORT
      delay_period: 1s
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
		integration.StreamTests(
			integration.StreamTestOpenClose(),
			integration.StreamTestStreamSequential(20),
			integration.StreamTestSendBatchCount(10),
			integration.StreamTestStreamParallelLossyThroughReconnect(20),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
				if vars.OutputBatchCount == 0 {
					vars.OutputBatchCount = 1
				}
				require.NoError(t, createBucketQueue(ctx, servicePort, servicePort, testID))
			}),
			integration.StreamTestOptPort(servicePort),
			integration.StreamTestOptAllowDupes(),
		)
	})

	t.Run("s3_to_sqs_lines_old_codec", func(t *testing.T) {
		template := `
output:
  aws_s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    path: ${!count("$ID")}.txt
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    batching:
      count: $OUTPUT_BATCH_COUNT
      processors:
        - archive:
            format: lines

input:
  aws_s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    delete_objects: true
    codec: lines
    sqs:
      url: http://localhost:$PORT/000000000000/queue-$ID
      key_path: Records.*.s3.object.key
      endpoint: http://localhost:$PORT
      delay_period: 1s
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
		integration.StreamTests(
			integration.StreamTestOpenClose(),
			integration.StreamTestStreamSequential(20),
			integration.StreamTestSendBatchCount(10),
			integration.StreamTestStreamParallelLossyThroughReconnect(20),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
				if vars.OutputBatchCount == 0 {
					vars.OutputBatchCount = 1
				}
				require.NoError(t, createBucketQueue(ctx, servicePort, servicePort, testID))
			}),
			integration.StreamTestOptPort(servicePort),
			integration.StreamTestOptAllowDupes(),
		)
	})

	t.Run("s3", func(t *testing.T) {
		template := `
output:
  aws_s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    path: ${!count("$ID")}.txt
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  aws_s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    delete_objects: true
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createBucketQueue(ctx, servicePort, "", testID))
			}),
			integration.StreamTestOptPort(servicePort),
			integration.StreamTestOptVarOne("false"),
		)
	})

	t.Run("sqs", func(t *testing.T) {
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
				require.NoError(t, createBucketQueue(ctx, "", servicePort, testID))
			}),
			integration.StreamTestOptPort(servicePort),
		)
	})
}
