// Copyright (c) 2019 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/ory/dockertest"
)

func TestAWSS3ToSQSIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "localstack/localstack",
		ExposedPorts: []string{"4572/tcp"},
		Env:          []string{"SERVICES=s3,sqs"},
	})
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}
	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()
	resource.Expire(900)

	endpoint := fmt.Sprintf("http://localhost:%v", resource.GetPort("4572/tcp"))
	bucket := "benthos-test-bucket"
	sqsQueue := "benthos-test-queue"
	sqsEndpoint := fmt.Sprintf("http://localhost:%v", resource.GetPort("4576/tcp"))
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

	bucketCreated := false

	if err = pool.Retry(func() error {
		var berr error
		if !bucketCreated {
			if _, berr = s3Client.CreateBucket(&s3.CreateBucketInput{
				Bucket: &bucket,
			}); berr != nil {
				return berr
			}
			bucketCreated = true
		}
		if _, berr = sqsClient.CreateQueue(&sqs.CreateQueueInput{
			QueueName: aws.String(sqsQueue),
		}); berr != nil {
			return berr
		}
		return s3Client.WaitUntilBucketExists(&s3.HeadBucketInput{
			Bucket: &bucket,
		})
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	res, err := sqsClient.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl:       &sqsQueueURL,
		AttributeNames: []*string{aws.String("All")},
	})
	if err != nil {
		t.Error(err)
		return
	}
	sqsQueueArn := res.Attributes["QueueArn"]

	if _, err = s3Client.PutBucketNotificationConfiguration(&s3.PutBucketNotificationConfigurationInput{
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
		t.Error(err)
		return
	}

	t.Run("testS3ToSQSStreams", func(t *testing.T) {
		testS3ToSQSStreams(t, endpoint, sqsEndpoint, sqsQueueURL, bucket)
	})
	t.Run("testS3ToSQSStreamsAsync", func(t *testing.T) {
		testS3ToSQSStreamsAsync(t, endpoint, sqsEndpoint, sqsQueueURL, bucket)
	})
}

func testS3ToSQSStreams(t *testing.T, endpoint, sqsEndpoint, sqsURL, bucket string) {
	inconf := reader.NewAmazonS3Config()
	inconf.Endpoint = endpoint
	inconf.Credentials.ID = "xxxxx"
	inconf.Credentials.Secret = "xxxxx"
	inconf.Credentials.Token = "xxxxx"
	inconf.Region = "eu-west-1"
	inconf.Bucket = bucket
	inconf.ForcePathStyleURLs = true
	inconf.Timeout = "10s"
	inconf.SQSURL = sqsURL
	inconf.SQSEndpoint = sqsEndpoint

	outconf := writer.NewAmazonS3Config()
	outconf.Endpoint = endpoint
	outconf.Credentials.ID = "xxxxx"
	outconf.Credentials.Secret = "xxxxx"
	outconf.Credentials.Token = "xxxxx"
	outconf.Region = "eu-west-1"
	outconf.Bucket = bucket
	outconf.ForcePathStyleURLs = true
	outconf.Path = "foo ^%&$ ${!count:s3uploaddownload}.txt"

	outputCtr := func() (mOutput writer.Type, err error) {
		if mOutput, err = writer.NewAmazonS3(outconf, log.Noop(), metrics.Noop()); err != nil {
			return
		}
		if err = mOutput.Connect(); err != nil {
			return
		}
		return
	}
	inputCtr := func() (mInput reader.Type, err error) {
		if mInput, err = reader.NewAmazonS3(inconf, log.Noop(), metrics.Noop()); err != nil {
			return
		}
		if err = mInput.Connect(); err != nil {
			return
		}
		return
	}

	checkALOSynchronous(outputCtr, inputCtr, t)
	checkALOSynchronousAndDie(outputCtr, inputCtr, t)

	inconf.DownloadManager.Enabled = false

	checkALOSynchronous(outputCtr, inputCtr, t)
	checkALOSynchronousAndDie(outputCtr, inputCtr, t)
}

func testS3ToSQSStreamsAsync(t *testing.T, endpoint, sqsEndpoint, sqsURL, bucket string) {
	inconf := reader.NewAmazonS3Config()
	inconf.Endpoint = endpoint
	inconf.Credentials.ID = "xxxxx"
	inconf.Credentials.Secret = "xxxxx"
	inconf.Credentials.Token = "xxxxx"
	inconf.Region = "eu-west-1"
	inconf.Bucket = bucket
	inconf.ForcePathStyleURLs = true
	inconf.Timeout = "10s"
	inconf.SQSURL = sqsURL
	inconf.SQSEndpoint = sqsEndpoint

	outconf := writer.NewAmazonS3Config()
	outconf.Endpoint = endpoint
	outconf.Credentials.ID = "xxxxx"
	outconf.Credentials.Secret = "xxxxx"
	outconf.Credentials.Token = "xxxxx"
	outconf.Region = "eu-west-1"
	outconf.Bucket = bucket
	outconf.ForcePathStyleURLs = true
	outconf.Path = "${!count:s3uploaddownload}.txt"

	outputCtr := func() (mOutput writer.Type, err error) {
		if mOutput, err = writer.NewAmazonS3(outconf, log.Noop(), metrics.Noop()); err != nil {
			return
		}
		if err = mOutput.Connect(); err != nil {
			return
		}
		return
	}
	inputCtr := func() (mInput reader.Async, err error) {
		ctx, done := context.WithTimeout(context.Background(), time.Second*60)
		defer done()

		if mInput, err = reader.NewAmazonS3(inconf, log.Noop(), metrics.Noop()); err != nil {
			return
		}
		if err = mInput.ConnectWithContext(ctx); err != nil {
			return
		}
		return
	}

	checkALOSynchronousAsync(outputCtr, inputCtr, t)
	checkALOSynchronousAndDieAsync(outputCtr, inputCtr, t)
}
