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
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

//------------------------------------------------------------------------------

// AmazonSQSConfig is configuration values for the input type.
type AmazonSQSConfig struct {
	Region      string                     `json:"region" yaml:"region"`
	URL         string                     `json:"url" yaml:"url"`
	Credentials AmazonAWSCredentialsConfig `json:"credentials" yaml:"credentials"`
	TimeoutS    int64                      `json:"timeout_s" yaml:"timeout_s"`
}

// NewAmazonSQSConfig creates a new Config with default values.
func NewAmazonSQSConfig() AmazonSQSConfig {
	return AmazonSQSConfig{
		Region: "eu-west-1",
		URL:    "",
		Credentials: AmazonAWSCredentialsConfig{
			ID:     "",
			Secret: "",
			Token:  "",
			Role:   "",
		},
		TimeoutS: 5,
	}
}

//------------------------------------------------------------------------------

// AmazonSQS is a benthos reader.Type implementation that reads messages from an
// Amazon S3 bucket.
type AmazonSQS struct {
	conf AmazonSQSConfig

	pendingHandles []*sqs.DeleteMessageBatchRequestEntry

	session *session.Session
	sqs     *sqs.SQS

	log   log.Modular
	stats metrics.Type
}

// NewAmazonSQS creates a new Amazon S3 bucket reader.Type.
func NewAmazonSQS(
	conf AmazonSQSConfig,
	log log.Modular,
	stats metrics.Type,
) *AmazonSQS {
	return &AmazonSQS{
		conf:  conf,
		log:   log.NewModule(".input.amazon_sqs"),
		stats: stats,
	}
}

// Connect attempts to establish a connection to the target SQS queue.
func (a *AmazonSQS) Connect() error {
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

	if len(a.conf.Credentials.Role) > 0 {
		sess.Config = sess.Config.WithCredentials(
			stscreds.NewCredentials(sess, a.conf.Credentials.Role),
		)
	}

	a.sqs = sqs.New(sess)
	a.session = sess

	a.log.Infof("Receiving Amazon SQS messages from URL: %v\n", a.conf.URL)
	return nil
}

// Read attempts to read a new message from the target SQS.
func (a *AmazonSQS) Read() (types.Message, error) {
	if a.session == nil {
		return nil, types.ErrNotConnected
	}

	output, err := a.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(a.conf.URL),
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(a.conf.TimeoutS),
	})
	if err != nil {
		return nil, err
	}

	msg := types.NewMessage(nil)

	if len(output.Messages) == 0 {
		return nil, types.ErrTimeout
	}

	for _, sqsMsg := range output.Messages {
		if sqsMsg.ReceiptHandle != nil {
			a.pendingHandles = append(a.pendingHandles, &sqs.DeleteMessageBatchRequestEntry{
				Id:            sqsMsg.MessageId,
				ReceiptHandle: sqsMsg.ReceiptHandle,
			})
		}

		if sqsMsg.Body != nil {
			msg.Append([]byte(*sqsMsg.Body))
		}
	}

	if msg.Len() == 0 {
		return nil, types.ErrTimeout
	}

	return msg, nil
}

// Acknowledge confirms whether or not our unacknowledged messages have been
// successfully propagated or not.
func (a *AmazonSQS) Acknowledge(err error) error {
	if _, err := a.sqs.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(a.conf.URL),
		Entries:  a.pendingHandles,
	}); err != nil {
		return err
	}

	a.pendingHandles = nil
	return nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *AmazonSQS) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *AmazonSQS) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
