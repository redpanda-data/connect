package aws

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/gofrs/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func createBucketQueue(s3Port, sqsPort, id string) error {
	endpoint := fmt.Sprintf("http://localhost:%v", s3Port)
	bucket := "bucket-" + id
	sqsQueue := "queue-" + id
	sqsEndpoint := fmt.Sprintf("http://localhost:%v", sqsPort)
	sqsQueueURL := fmt.Sprintf("%v/queue/%v", sqsEndpoint, sqsQueue)

	var s3Client *s3.S3
	if s3Port != "" {
		s3Client = s3.New(session.Must(session.NewSession(&aws.Config{
			S3ForcePathStyle: aws.Bool(true),
			Credentials:      credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
			Endpoint:         aws.String(endpoint),
			Region:           aws.String("eu-west-1"),
		})))
	}

	var sqsClient *sqs.SQS
	if sqsPort != "" {
		sqsClient = sqs.New(session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
			Endpoint:    aws.String(sqsEndpoint),
			Region:      aws.String("eu-west-1"),
		})))
	}

	if s3Client != nil {
		if _, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: &bucket,
		}); err != nil {
			return err
		}
	}

	if sqsClient != nil {
		if _, err := sqsClient.CreateQueue(&sqs.CreateQueueInput{
			QueueName: aws.String(sqsQueue),
		}); err != nil {
			return err
		}
	}

	if s3Client != nil {
		if err := s3Client.WaitUntilBucketExists(&s3.HeadBucketInput{
			Bucket: &bucket,
		}); err != nil {
			return err
		}
	}

	var sqsQueueArn *string
	if sqsPort != "" {
		res, err := sqsClient.GetQueueAttributes(&sqs.GetQueueAttributesInput{
			QueueUrl:       &sqsQueueURL,
			AttributeNames: []*string{aws.String("All")},
		})
		if err != nil {
			return err
		}
		sqsQueueArn = res.Attributes["QueueArn"]
	}

	if s3Port != "" && sqsPort != "" {
		if _, err := s3Client.PutBucketNotificationConfiguration(&s3.PutBucketNotificationConfigurationInput{
			Bucket: &bucket,
			NotificationConfiguration: &s3.NotificationConfiguration{
				QueueConfigurations: []*s3.QueueConfiguration{
					{
						Events: []*string{
							aws.String("s3:ObjectCreated:*"),
						},
						QueueArn: sqsQueueArn,
					},
				},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func TestIntegrationAWS(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

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

	require.NoError(t, pool.Retry(func() error {
		u4, err := uuid.NewV4()
		require.NoError(t, err)

		return createBucketQueue(servicePort, servicePort, u4.String())
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
      url: http://localhost:$PORT/queue/queue-$ID
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
				require.NoError(t, createBucketQueue(servicePort, servicePort, testID))
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
    codec: lines
    sqs:
      url: http://localhost:$PORT/queue/queue-$ID
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
				require.NoError(t, createBucketQueue(servicePort, servicePort, testID))
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
				require.NoError(t, createBucketQueue(servicePort, "", testID))
			}),
			integration.StreamTestOptPort(servicePort),
			integration.StreamTestOptVarOne("false"),
		)
	})

	t.Run("sqs", func(t *testing.T) {
		template := `
output:
  aws_sqs:
    url: http://localhost:$PORT/queue/queue-$ID
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
    url: http://localhost:$PORT/queue/queue-$ID
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
				require.NoError(t, createBucketQueue("", servicePort, testID))
			}),
			integration.StreamTestOptPort(servicePort),
		)
	})
}
