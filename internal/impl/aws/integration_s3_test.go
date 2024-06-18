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
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
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
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createBucketQueue(ctx, lsPort, lsPort, vars.ID))
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
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				if tmp := vars.General["OUTPUT_BATCH_COUNT"]; tmp == "0" || tmp == "" {
					vars.General["OUTPUT_BATCH_COUNT"] = "1"
				}
				require.NoError(t, createBucketQueue(ctx, lsPort, lsPort, vars.ID))
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
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				if tmp := vars.General["OUTPUT_BATCH_COUNT"]; tmp == "0" || tmp == "" {
					vars.General["OUTPUT_BATCH_COUNT"] = "1"
				}
				require.NoError(t, createBucketQueue(ctx, lsPort, lsPort, vars.ID))
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
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createBucketQueue(ctx, lsPort, "", vars.ID))
			}),
			integration.StreamTestOptPort(lsPort),
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
			integration.CacheTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.CacheTestConfigVars) {
				require.NoError(t, createBucket(ctx, lsPort, vars.ID))
			}),
		)
	})
}
