package integration

import (
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
)

func createBucketQueue(port, portTwo, id string) error {
	endpoint := fmt.Sprintf("http://localhost:%v", port)
	bucket := "bucket-" + id
	sqsQueue := "queue-" + id
	sqsEndpoint := fmt.Sprintf("http://localhost:%v", portTwo)
	sqsQueueURL := fmt.Sprintf("%v/queue/%v", sqsEndpoint, sqsQueue)

	s3Client := s3.New(session.Must(session.NewSession(&aws.Config{
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		Endpoint:         aws.String(endpoint),
		Region:           aws.String("eu-west-1"),
	})))

	sqsClient := sqs.New(session.Must(session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		Endpoint:    aws.String(sqsEndpoint),
		Region:      aws.String("eu-west-1"),
	})))

	if _, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: &bucket,
	}); err != nil {
		return err
	}

	if _, err := sqsClient.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(sqsQueue),
	}); err != nil {
		return err
	}

	if err := s3Client.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: &bucket,
	}); err != nil {
		return err
	}

	res, err := sqsClient.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl:       &sqsQueueURL,
		AttributeNames: []*string{aws.String("All")},
	})
	if err != nil {
		return err
	}

	sqsQueueArn := res.Attributes["QueueArn"]

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
	return nil
}

var _ = registerIntegrationTest("aws", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "localstack/localstack",
		ExposedPorts: []string{"4572/tcp"},
		Env:          []string{"SERVICES=s3,sqs"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	resource.Expire(900)

	s3Port := resource.GetPort("4572/tcp")
	sqsPort := resource.GetPort("4576/tcp")

	require.NoError(t, pool.Retry(func() error {
		u4, err := uuid.NewV4()
		require.NoError(t, err)

		return createBucketQueue(s3Port, sqsPort, u4.String())
	}))

	t.Run("s3_to_sqs", func(t *testing.T) {
		template := `
output:
  s3:
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
  s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    sqs_url: http://localhost:$PORT_TWO/queue/queue-$ID
    sqs_body_path: Records.*.s3.object.key
    sqs_endpoint: http://localhost:$PORT_TWO
    region: eu-west-1
    delete_objects: true
    download_manager:
      enabled: $VAR1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
		integrationTests(
			integrationTestOpenClose(),
			// integrationTestMetadata(), Does dumb stuff with rewriting keys.
			integrationTestSendBatch(10),
			integrationTestSendBatchCount(10),
			integrationTestLotsOfDataSequential(10),
		).Run(
			t, template,
			testOptPreTest(func(t *testing.T, env *testEnvironment) {
				require.NoError(t, createBucketQueue(s3Port, sqsPort, env.configVars.id))
			}),
			testOptPort(s3Port),
			testOptPortTwo(sqsPort),
			testOptVarOne("false"),
		)

		t.Run("with download manager", func(t *testing.T) {
			t.Parallel()

			integrationTests(
				integrationTestOpenClose(),
				integrationTestSendBatch(10),
				integrationTestSendBatchCount(10),
				integrationTestLotsOfDataSequential(10),
			).Run(
				t, template,
				testOptPreTest(func(t *testing.T, env *testEnvironment) {
					require.NoError(t, createBucketQueue(s3Port, sqsPort, env.configVars.id))
				}),
				testOptPort(s3Port),
				testOptPortTwo(sqsPort),
				testOptVarOne("true"),
			)
		})
	})

	t.Run("s3", func(t *testing.T) {
		template := `
output:
  s3:
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
  s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    delete_objects: true
    download_manager:
      enabled: $VAR1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
		integrationTests(
			integrationTestOpenCloseIsolated(),
			integrationTestLotsOfDataIsolated(10),
		).Run(
			t, template,
			testOptPreTest(func(t *testing.T, env *testEnvironment) {
				require.NoError(t, createBucketQueue(s3Port, sqsPort, env.configVars.id))
			}),
			testOptPort(s3Port),
			testOptVarOne("false"),
		)

		t.Run("with download manager", func(t *testing.T) {
			integrationTests(
				integrationTestOpenCloseIsolated(),
				integrationTestLotsOfDataIsolated(10),
			).Run(
				t, template,
				testOptPreTest(func(t *testing.T, env *testEnvironment) {
					require.NoError(t, createBucketQueue(s3Port, sqsPort, env.configVars.id))
				}),
				testOptPort(s3Port),
				testOptVarOne("true"),
			)
		})
	})

	t.Run("sqs", func(t *testing.T) {
		template := `
output:
  sqs:
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
  sqs:
    url: http://localhost:$PORT/queue/queue-$ID
    endpoint: http://localhost:$PORT
    region: eu-west-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
		integrationTests(
			integrationTestOpenClose(),
			integrationTestSendBatch(10),
			integrationTestLotsOfDataSequential(50),
			integrationTestLotsOfDataParallel(50),
		).Run(
			t, template,
			testOptPreTest(func(t *testing.T, env *testEnvironment) {
				require.NoError(t, createBucketQueue(s3Port, sqsPort, env.configVars.id))
			}),
			testOptPort(sqsPort),
		)
	})
})
