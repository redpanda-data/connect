// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package awstest provides shared test helpers for AWS integration tests.
package awstest

import (
	"context"
	"fmt"
	"os"
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
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	localStackOnce sync.Once
	localStackCtr  testcontainers.Container
	localStackErr  error
	localStackPort string
)

// GetLocalStack returns the service port of a single, package-wide LocalStack
// container. It starts the container on first use.
//
// One container per test binary keeps the package within its `go test -timeout`
// budget and lets the tests of a package run in parallel. Callers must use
// unique per-test resource names (streams, buckets, queues, log groups). The
// stream-test harness already does this through its generated $ID.
//
// The container outlives the test that starts it. Call TestMain from the
// package's own TestMain to terminate it after all tests complete.
func GetLocalStack(t testing.TB) (port string) {
	t.Helper()

	localStackOnce.Do(func() {
		// context.Background(), not t.Context(): the container outlives the
		// first test that triggers startup.
		ctx := context.Background()
		ctr, err := testcontainers.Run(ctx, "localstack/localstack:3",
			testcontainers.WithExposedPorts("4566/tcp"),
			testcontainers.WithWaitStrategy(
				wait.ForHTTP("/_localstack/health").
					WithPort("4566/tcp").
					WithStartupTimeout(2*time.Minute),
			),
		)
		if err != nil {
			if ctr != nil {
				_ = ctr.Terminate(ctx)
			}
			localStackErr = err
			return
		}
		localStackCtr = ctr

		mappedPort, err := ctr.MappedPort(ctx, "4566/tcp")
		if err != nil {
			localStackErr = err
			return
		}
		localStackPort = mappedPort.Port()
	})
	require.NoError(t, localStackErr)

	return localStackPort
}

// TestMain runs the package's tests, terminates the shared LocalStack container
// (if one was started), and exits with the test result code. Call it from the
// TestMain of every package that uses GetLocalStack.
func TestMain(m *testing.M) {
	code := m.Run()
	if localStackCtr != nil {
		_ = localStackCtr.Terminate(context.Background())
	}
	os.Exit(code)
}

// CreateBucket creates an S3 bucket on a LocalStack instance.
func CreateBucket(ctx context.Context, s3Port, bucket string) error {
	endpoint := fmt.Sprintf("http://localhost:%v", s3Port)

	conf, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-west-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
	)
	if err != nil {
		return err
	}
	conf.BaseEndpoint = &endpoint

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

// CreateBucketQueue creates an S3 bucket and/or SQS queue on a LocalStack instance,
// optionally configuring S3 bucket notifications to the SQS queue.
func CreateBucketQueue(ctx context.Context, s3Port, sqsPort, id string) error {
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
