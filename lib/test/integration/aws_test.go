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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/input/reader"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/output/writer"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/ory/dockertest"
)

func TestAWSIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "localstack/localstack",
		ExposedPorts: []string{"4572/tcp"},
	})
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}
	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	endpoint := fmt.Sprintf("http://localhost:%v", resource.GetPort("4572/tcp"))
	bucket := "benthos-test-bucket"
	sqsQueue := "benthos-test-queue"
	sqsEndpoint := fmt.Sprintf("http://localhost:%v", resource.GetPort("4576/tcp"))
	sqsQueueURL := fmt.Sprintf("%v/queue/%v", sqsEndpoint, sqsQueue)

	sqsFIFOQueue := "benthos-test-fifo-queue.fifo"
	sqsFIFOEndpoint := fmt.Sprintf("http://localhost:%v", resource.GetPort("4576/tcp"))
	sqsFIFOQueueURL := fmt.Sprintf("%v/queue/%v", sqsFIFOEndpoint, sqsFIFOQueue)

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
		if _, berr = sqsClient.CreateQueue(&sqs.CreateQueueInput{
			QueueName: aws.String(sqsFIFOQueue),
			Attributes: map[string]*string{
				"FifoQueue":                 aws.String("true"),
				"ContentBasedDeduplication": aws.String("true"),
			},
		}); berr != nil {
			return berr
		}
		return s3Client.WaitUntilBucketExists(&s3.HeadBucketInput{
			Bucket: &bucket,
		})
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	t.Run("testS3UploadDownload", func(t *testing.T) {
		testS3UploadDownload(t, endpoint, bucket)
	})
	t.Run("testSQSSinglePart", func(t *testing.T) {
		testSQSSinglePart(t, sqsEndpoint, sqsQueueURL)
	})
	t.Run("testSQSFIFOSinglePart", func(t *testing.T) {
		testSQSFIFOSinglePart(t, sqsFIFOEndpoint, sqsFIFOQueueURL)
	})
}

func createSQSInputOutput(
	inConf reader.AmazonSQSConfig, outConf writer.AmazonSQSConfig,
) (mInput reader.Type, mOutput writer.Type, err error) {
	if mOutput, err = writer.NewAmazonSQS(outConf, log.Noop(), metrics.Noop()); err != nil {
		return
	}
	if err = mOutput.Connect(); err != nil {
		return
	}
	if mInput, err = reader.NewAmazonSQS(inConf, log.Noop(), metrics.Noop()); err != nil {
		return
	}
	if err = mInput.Connect(); err != nil {
		return
	}
	return
}

func testSQSSinglePart(t *testing.T, endpoint, url string) {
	outConf := writer.NewAmazonSQSConfig()
	outConf.URL = url
	outConf.Endpoint = endpoint
	outConf.Credentials.ID = "xxxxx"
	outConf.Credentials.Secret = "xxxxx"
	outConf.Credentials.Token = "xxxxx"
	outConf.Region = "eu-west-1"

	inConf := reader.NewAmazonSQSConfig()
	inConf.URL = url
	inConf.Endpoint = endpoint
	inConf.Credentials.ID = "xxxxx"
	inConf.Credentials.Secret = "xxxxx"
	inConf.Credentials.Token = "xxxxx"
	inConf.Region = "eu-west-1"

	mInput, mOutput, err := createSQSInputOutput(inConf, outConf)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		mInput.CloseAsync()
		if cErr := mInput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
		mOutput.CloseAsync()
		if cErr := mOutput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	N := 10

	wg := sync.WaitGroup{}
	wg.Add(N)

	testMsgs := map[string]struct{}{}
	for i := 0; i < N; i++ {
		str := fmt.Sprintf("hello world: %v", i)
		testMsgs[str] = struct{}{}
		go func(testStr string) {
			msg := message.New([][]byte{
				[]byte(testStr),
			})
			if gerr := mOutput.Write(msg); gerr != nil {
				t.Fatal(gerr)
			}
			wg.Done()
		}(str)
	}

	lMsgs := len(testMsgs)
	for lMsgs > 0 {
		var actM types.Message
		actM, err = mInput.Read()
		if err != nil {
			t.Error(err)
		} else {
			act := string(actM.Get(0).Get())
			if _, exists := testMsgs[act]; !exists {
				t.Errorf("Unexpected message: %v", act)
			}
			delete(testMsgs, act)
			actM.Get(0).Metadata().Iter(func(k, v string) error {
				return nil
			})
		}
		if err = mInput.Acknowledge(nil); err != nil {
			t.Error(err)
		}
		lMsgs = len(testMsgs)
	}

	wg.Wait()
}

func testSQSFIFOSinglePart(t *testing.T, endpoint, url string) {
	outConf := writer.NewAmazonSQSConfig()
	outConf.URL = url
	outConf.Endpoint = endpoint
	outConf.Credentials.ID = "xxxxx"
	outConf.Credentials.Secret = "xxxxx"
	outConf.Credentials.Token = "xxxxx"
	outConf.Region = "eu-west-1"
	outConf.MessageGroupID = "foogroup"
	outConf.MessageDeduplicationID = "${!json_field:id}"

	inConf := reader.NewAmazonSQSConfig()
	inConf.URL = url
	inConf.Endpoint = endpoint
	inConf.Credentials.ID = "xxxxx"
	inConf.Credentials.Secret = "xxxxx"
	inConf.Credentials.Token = "xxxxx"
	inConf.Region = "eu-west-1"

	mInput, mOutput, err := createSQSInputOutput(inConf, outConf)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		mInput.CloseAsync()
		if cErr := mInput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
		mOutput.CloseAsync()
		if cErr := mOutput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	N := 10

	testMsgs := []string{}
	for i := 0; i < N; i++ {
		str := fmt.Sprintf(`{"text":"hello world","id":%v}`, i)
		testMsgs = append(testMsgs, str)
		msg := message.New([][]byte{
			[]byte(str),
		})
		if gerr := mOutput.Write(msg); gerr != nil {
			t.Fatal(gerr)
		}

		// Send a duplicate
		str = fmt.Sprintf(`{"text":"DUPLICATE","id":%v}`, i)
		msg = message.New([][]byte{
			[]byte(str),
		})
		if gerr := mOutput.Write(msg); gerr != nil {
			t.Fatal(gerr)
		}
	}

	for _, exp := range testMsgs {
		var actM types.Message
		actM, err = mInput.Read()
		if err != nil {
			t.Fatal(err)
		}
		if act := string(actM.Get(0).Get()); exp != act {
			t.Errorf("Wrong message contents: %v != %v", act, exp)
		}
		if err = mInput.Acknowledge(nil); err != nil {
			t.Error(err)
		}
	}
}

func createS3InputOutput(
	inConf reader.AmazonS3Config, outConf writer.AmazonS3Config,
) (mInput reader.Type, mOutput writer.Type, err error) {
	if mOutput, err = writer.NewAmazonS3(outConf, log.Noop(), metrics.Noop()); err != nil {
		return
	}
	if err = mOutput.Connect(); err != nil {
		return
	}
	if mInput, err = reader.NewAmazonS3(inConf, log.Noop(), metrics.Noop()); err != nil {
		return
	}
	if err = mInput.Connect(); err != nil {
		return
	}
	return
}

func testS3UploadDownload(t *testing.T, endpoint, bucket string) {
	inconf := reader.NewAmazonS3Config()
	inconf.Endpoint = endpoint
	inconf.Credentials.ID = "xxxxx"
	inconf.Credentials.Secret = "xxxxx"
	inconf.Credentials.Token = "xxxxx"
	inconf.Region = "eu-west-1"
	inconf.Bucket = bucket
	inconf.ForcePathStyleURLs = true
	inconf.Timeout = "1s"

	outconf := writer.NewAmazonS3Config()
	outconf.Endpoint = endpoint
	outconf.Credentials.ID = "xxxxx"
	outconf.Credentials.Secret = "xxxxx"
	outconf.Credentials.Token = "xxxxx"
	outconf.Region = "eu-west-1"
	outconf.Bucket = bucket
	outconf.ForcePathStyleURLs = true
	outconf.Path = "${!count:s3uploaddownload}.txt"

	mOutput, err := writer.NewAmazonS3(outconf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	if err = mOutput.Connect(); err != nil {
		t.Fatal(err)
	}
	mInput, err := reader.NewAmazonS3(inconf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		mInput.CloseAsync()
		if cErr := mInput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
		mOutput.CloseAsync()
		if cErr := mOutput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	N := 9
	for i := 0; i < N; i++ {
		if err := mOutput.Write(message.New([][]byte{
			[]byte(fmt.Sprintf("foo%v", i)),
		})); err != nil {
			t.Error(err)
		}
	}

	if err = mInput.Connect(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < N; i++ {
		msg, err := mInput.Read()
		if err != nil {
			t.Fatal(err)
		}
		exp := fmt.Sprintf("foo%v", i)
		mBytes := message.GetAllBytes(msg)
		if len(mBytes) == 0 {
			t.Fatal("Empty message received")
		}
		if act := string(mBytes[0]); exp != act {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
	}
}
