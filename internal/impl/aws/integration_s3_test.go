package aws

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func createBucket(ctx context.Context, s3Port, bucket string) error {
	endpoint := fmt.Sprintf("http://localhost:%v", s3Port)

	conf, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-west-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           endpoint,
				SigningRegion: "eu-west-1",
			}, nil
		})),
	)
	if err != nil {
		return err
	}

	client := s3.NewFromConfig(conf, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &bucket,
		CreateBucketConfiguration: &s3types.CreateBucketConfiguration{
			Location: &s3types.LocationInfo{
				Name: aws.String("eu-west-1"),
				Type: s3types.LocationTypeAvailabilityZone,
			},
			LocationConstraint: s3types.BucketLocationConstraintEuWest1,
		},
	})
	if err != nil {
		return err
	}

	waiter := s3.NewBucketExistsWaiter(client)
	return waiter.Wait(ctx, &s3.HeadBucketInput{
		Bucket: &bucket,
	}, time.Minute)
}

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
			config.WithRegion("eu-west-1"),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
			config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           endpoint,
					SigningRegion: "eu-west-1",
				}, nil
			})),
		)
		if err != nil {
			return err
		}

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
			CreateBucketConfiguration: &s3types.CreateBucketConfiguration{
				Location: &s3types.LocationInfo{
					Name: aws.String("eu-west-1"),
					Type: s3types.LocationTypeAvailabilityZone,
				},
				LocationConstraint: s3types.BucketLocationConstraintEuWest1,
			},
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

func s3IntegrationSuite(t *testing.T, lsPort string) {
	t.Run("via_sqs", func(t *testing.T) {
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
				require.NoError(t, createBucketQueue(ctx, lsPort, lsPort, testID))
			}),
			integration.StreamTestOptPort(lsPort),
			integration.StreamTestOptAllowDupes(),
		)
	})

	t.Run("via_sqs_lines", func(t *testing.T) {
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
				require.NoError(t, createBucketQueue(ctx, lsPort, lsPort, testID))
			}),
			integration.StreamTestOptPort(lsPort),
			integration.StreamTestOptAllowDupes(),
		)
	})

	t.Run("via_sqs_lines_old_codec", func(t *testing.T) {
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
				require.NoError(t, createBucketQueue(ctx, lsPort, lsPort, testID))
			}),
			integration.StreamTestOptPort(lsPort),
			integration.StreamTestOptAllowDupes(),
		)
	})

	t.Run("batch", func(t *testing.T) {
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
				require.NoError(t, createBucketQueue(ctx, lsPort, "", testID))
			}),
			integration.StreamTestOptPort(lsPort),
			integration.StreamTestOptVarOne("false"),
		)
	})

	t.Run("cache", func(t *testing.T) {
		template := `
cache_resources:
  - label: testcache
    aws_s3:
      endpoint: http://localhost:$PORT
      region: eu-west-1
      force_path_style_urls: true
      bucket: $ID
      credentials:
        id: xxxxx
        secret: xxxxx
        token: xxxxx
`
		suite := integration.CacheTests(
			integration.CacheTestOpenClose(),
			integration.CacheTestMissingKey(),
			integration.CacheTestDoubleAdd(),
			integration.CacheTestDelete(),
			integration.CacheTestGetAndSet(1),
		)
		suite.Run(
			t, template,
			integration.CacheTestOptPort(lsPort),
			integration.CacheTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.CacheTestConfigVars) {
				require.NoError(t, createBucket(ctx, lsPort, testID))
			}),
		)
	})
}

func TestS3OutputThroughput(t *testing.T) {
	// Skipping as this test was only put together out of curiosity
	t.Skip()

	servicePort := getLocalStack(t)

	ctx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	bucketName := "bench-test-bucket"
	require.NoError(t, createBucket(context.Background(), servicePort, bucketName))

	dataTarget := 1024 * 1024 * 1024 * 1 // 1GB
	dataFiles := 1000

	testData := make([]byte, dataTarget/dataFiles)
	_, err := rand.Read(testData)
	require.NoError(t, err)

	outConf, err := output.FromAny(bundle.GlobalEnvironment, map[string]any{
		"aws_s3": map[string]any{
			"bucket":                bucketName,
			"endpoint":              fmt.Sprintf("http://localhost:%v", servicePort),
			"force_path_style_urls": true,
			"region":                "eu-west-1",
			"path":                  `${!counter()}.txt`,
			"credentials": map[string]any{
				"id":     "xxxxx",
				"secret": "xxxxx",
				"token":  "xxxxx",
			},
			"batching": map[string]any{
				"count": 1,
			},
		},
	})
	require.NoError(t, err)

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	out, err := mgr.NewOutput(outConf)
	require.NoError(t, err)

	tStarted := time.Now()

	tChan := make(chan message.Transaction)
	require.NoError(t, out.Consume(tChan))

	var wg sync.WaitGroup
	wg.Add(dataFiles)

	for i := 0; i < dataFiles; i++ {
		select {
		case tChan <- message.NewTransactionFunc(message.Batch{
			message.NewPart(testData),
		}, func(ctx context.Context, err error) error {
			wg.Done()
			assert.NoError(t, err)
			return nil
		}):
		case <-ctx.Done():
			t.Fatal("timed out")
		}
	}

	wg.Wait()
	t.Logf("Delivery took %v total", time.Since(tStarted))

	out.TriggerCloseNow()
	require.NoError(t, out.WaitForClose(ctx))
}
