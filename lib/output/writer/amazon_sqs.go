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

package writer

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
		},
	}
}

//------------------------------------------------------------------------------

// AmazonSQS is a benthos writer.Type implementation that writes messages to an
// Amazon S3 bucket.
type AmazonSQS struct {
	conf AmazonSQSConfig

	session *session.Session
	sqs     *sqs.SQS

	log   log.Modular
	stats metrics.Type
}

// NewAmazonSQS creates a new Amazon S3 bucket writer.Type.
func NewAmazonSQS(
	conf AmazonSQSConfig,
	log log.Modular,
	stats metrics.Type,
) *AmazonSQS {
	return &AmazonSQS{
		conf:  conf,
		log:   log.NewModule(".output.amazon_sqs"),
		stats: stats,
	}
}

// Connect attempts to establish a connection to the target S3 bucket and any
// relevant queues used to traverse the objects (SQS, etc).
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

	a.session = sess
	a.sqs = sqs.New(sess)

	a.log.Infof("Sending messages to Amazon SQS URL: %v\n", a.conf.URL)
	return nil
}

// Write attempts to write message contents to a target S3 bucket as files.
func (a *AmazonSQS) Write(msg types.Message) error {
	if a.session == nil {
		return types.ErrNotConnected
	}

	/*
		msgs := []*sqs.SendMessageBatchRequestEntry{}
		for _, part := range msg.GetAll() {
			msgs = append(msgs, &sqs.SendMessageBatchRequestEntry{
				MessageBody: aws.String(string(part)),
			})
		}

		res, err := a.sqs.SendMessageBatch(&sqs.SendMessageBatchInput{
			QueueUrl: aws.String(a.conf.URL),
			Entries:  msgs,
		})
		if err != nil {
			return err
		}
		if nFailed := len(res.Failed); nFailed > 0 {
			return fmt.Errorf("%v batch items failed", nFailed)
		}
	*/

	for _, part := range msg.GetAll() {
		if _, err := a.sqs.SendMessage(&sqs.SendMessageInput{
			QueueUrl:    aws.String(a.conf.URL),
			MessageBody: aws.String(string(part)),
		}); err != nil {
			return err
		}
	}
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
