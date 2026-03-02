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
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

// GetLocalStack starts a LocalStack container and returns the service port.
func GetLocalStack(t testing.TB) (port string) {
	portInt, err := integration.GetFreePort()
	require.NoError(t, err)

	port = strconv.Itoa(portInt)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "localstack/localstack",
		ExposedPorts: []string{port + "/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			docker.Port(port + "/tcp"): {
				docker.PortBinding{HostIP: "", HostPort: port + "/tcp"},
			},
		},
		Env: []string{
			fmt.Sprintf("GATEWAY_LISTEN=0.0.0.0:%v", port),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	require.NoError(t, pool.Retry(func() (err error) {
		defer func() {
			if err != nil {
				t.Logf("localstack probe error: %v", err)
			}
		}()
		return CreateBucket(t.Context(), port, "test-bucket")
	}))
	return
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
