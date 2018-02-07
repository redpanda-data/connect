// Copyright (c) 2018 Ashley Jeffs
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

package reader

import (
	"fmt"
	"strings"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/Jeffail/gabs"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
)

//------------------------------------------------------------------------------

// AmazonS3CredentialsConfig contains configuration params for AWS credentials.
type AmazonS3CredentialsConfig struct {
	ID     string `json:"id"`
	Secret string `json:"secret"`
	Token  string `json:"token"`
}

// AmazonS3Config is configuration values for the input type.
type AmazonS3Config struct {
	Region        string                    `json:"region" yaml:"region"`
	Bucket        string                    `json:"bucket" yaml:"bucket"`
	DeleteObjects bool                      `json:"delete_objects" yaml:"delete_objects"`
	SQSURL        string                    `json:"sqs_url" yaml:"sqs_url"`
	SQSBodyPath   string                    `json:"sqs_body_path" yaml:"sqs_body_path"`
	Credentials   AmazonS3CredentialsConfig `json:"credentials" yaml:"credentials"`
	TimeoutS      int64                     `json:"timeout_s" yaml:"timeout_s"`
}

// NewAmazonS3Config creates a new Config with default values.
func NewAmazonS3Config() AmazonS3Config {
	return AmazonS3Config{
		Region:        "eu-west-1",
		Bucket:        "",
		DeleteObjects: false,
		SQSURL:        "",
		SQSBodyPath:   "Records.s3.object.key",
		Credentials: AmazonS3CredentialsConfig{
			ID:     "",
			Secret: "",
			Token:  "",
		},
		TimeoutS: 5,
	}
}

//------------------------------------------------------------------------------

// AmazonS3 is a benthos reader.Type implementation that reads messages from an
// Amazon S3 bucket.
type AmazonS3 struct {
	conf AmazonS3Config

	sqsBodyPath []string

	pendingHandles []string
	ackHandles     []string

	readKeys   []string
	targetKeys []string

	session    *session.Session
	s3         *s3.S3
	downloader *s3manager.Downloader
	sqs        *sqs.SQS

	log   log.Modular
	stats metrics.Type
}

// NewAmazonS3 creates a new Amazon S3 bucket reader.Type.
func NewAmazonS3(
	conf AmazonS3Config,
	log log.Modular,
	stats metrics.Type,
) *AmazonS3 {
	var path []string
	if len(conf.SQSBodyPath) > 0 {
		path = strings.Split(conf.SQSBodyPath, ".")
	}
	return &AmazonS3{
		conf:        conf,
		sqsBodyPath: path,
		log:         log.NewModule(".input.amazon_s3"),
		stats:       stats,
	}
}

// Connect attempts to establish a connection to the target S3 bucket and any
// relevant queues used to traverse the objects (SQS, etc).
func (a *AmazonS3) Connect() error {
	if a.session != nil {
		return nil
	}

	awsConf := aws.NewConfig()
	if len(a.conf.Region) > 0 {
		awsConf = awsConf.WithRegion(a.conf.Region)
	}
	if len(a.conf.Credentials.ID) > 0 {
		awsConf = awsConf.WithCredentials(credentials.NewStaticCredentials(
			a.conf.Credentials.ID,
			a.conf.Credentials.Secret,
			a.conf.Credentials.Token,
		))
	}

	sess, err := session.NewSession(awsConf)
	if err != nil {
		return err
	}

	sThree := s3.New(sess)
	dler := s3manager.NewDownloader(sess)

	if len(a.conf.SQSURL) == 0 {
		listInput := &s3.ListObjectsInput{
			Bucket: aws.String(a.conf.Bucket),
		}
		objList, err := sThree.ListObjects(listInput)
		if err != nil {
			return fmt.Errorf("failed to list objects: %v", err)
		}
		for _, obj := range objList.Contents {
			a.targetKeys = append(a.targetKeys, *obj.Key)
		}
	} else {
		a.sqs = sqs.New(sess)
	}

	a.log.Infof("Receiving amazon s3 objects from bucket: %s\n", a.conf.Bucket)

	a.session = sess
	a.downloader = dler
	a.s3 = sThree
	return nil
}

func (a *AmazonS3) readSQSEvents() error {
	var messageHandles []*sqs.DeleteMessageBatchRequestEntry

	output, err := a.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(a.conf.SQSURL),
		MaxNumberOfMessages: aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(a.conf.TimeoutS),
	})
	if err != nil {
		return err
	}

	for _, sqsMsg := range output.Messages {
		if sqsMsg.ReceiptHandle != nil {
			messageHandles = append(messageHandles, &sqs.DeleteMessageBatchRequestEntry{
				Id:            sqsMsg.MessageId,
				ReceiptHandle: sqsMsg.ReceiptHandle,
			})
		}

		if sqsMsg.Body == nil {
			continue
		}

		gObj, err := gabs.ParseJSON([]byte(*sqsMsg.Body))
		if err != nil {
			a.log.Errorf("Failed to parse SQS message body: %v\n", err)
			continue
		}

		switch t := gObj.S(a.sqsBodyPath...).Data().(type) {
		case string:
			a.targetKeys = append(a.targetKeys, t)
		case []interface{}:
			for _, jStr := range t {
				if p, ok := jStr.(string); ok {
					a.targetKeys = append(a.targetKeys, p)
				}
			}
		}
	}

	// Ignore errors here for now, it would be nice to clear these after ack,
	// but there could be more than one target key from each message, so it's
	// very hard to map SQS message handles to discrete benthos messages.
	a.sqs.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(a.conf.SQSURL),
		Entries:  messageHandles,
	})
	return types.ErrTimeout
}

// Read attempts to read a new message from the target S3 bucket.
func (a *AmazonS3) Read() (types.Message, error) {
	if a.session == nil {
		return types.Message{}, types.ErrNotConnected
	}

	if len(a.targetKeys) == 0 {
		if a.sqs != nil {
			if err := a.readSQSEvents(); err != nil {
				return types.Message{}, err
			}
		} else {
			// If we aren't using SQS but exhausted our targets we are done.
			return types.Message{}, types.ErrTypeClosed
		}
	}
	if len(a.targetKeys) == 0 {
		return types.Message{}, types.ErrTimeout
	}

	target := a.targetKeys[0]

	buff := &aws.WriteAtBuffer{}

	// Write the contents of S3 Object to the file
	if _, err := a.downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(a.conf.Bucket),
		Key:    aws.String(target),
	}); err != nil {
		return types.Message{}, fmt.Errorf("failed to download file, %v", err)
	}

	if len(a.targetKeys) > 1 {
		a.targetKeys = a.targetKeys[1:]
	} else {
		a.targetKeys = nil
	}

	if a.conf.DeleteObjects {
		a.readKeys = append(a.readKeys, target)
	}

	return types.Message{
		Parts: [][]byte{buff.Bytes()},
	}, nil
}

// Acknowledge confirms whether or not our unacknowledged messages have been
// successfully propagated or not.
func (a *AmazonS3) Acknowledge(err error) error {
	if err == nil && a.conf.DeleteObjects {
		for _, key := range a.readKeys {
			_, err := a.s3.DeleteObject(&s3.DeleteObjectInput{
				Bucket: aws.String(a.conf.Bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				a.log.Errorf("Failed to delete consumed object: %v\n", err)
			}
		}
		a.readKeys = nil
	}
	return nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *AmazonS3) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *AmazonS3) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
