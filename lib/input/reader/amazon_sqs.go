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
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	sess "github.com/Jeffail/benthos/lib/util/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

//------------------------------------------------------------------------------

// AmazonSQSConfig contains configuration values for the input type.
type AmazonSQSConfig struct {
	sess.Config `json:",inline" yaml:",inline"`
	URL         string `json:"url" yaml:"url"`
	TimeoutS    int64  `json:"timeout_s" yaml:"timeout_s"`
}

// NewAmazonSQSConfig creates a new Config with default values.
func NewAmazonSQSConfig() AmazonSQSConfig {
	return AmazonSQSConfig{
		Config:   sess.NewConfig(),
		URL:      "",
		TimeoutS: 5,
	}
}

//------------------------------------------------------------------------------

// AmazonSQS is a benthos reader.Type implementation that reads messages from an
// Amazon SQS queue.
type AmazonSQS struct {
	conf AmazonSQSConfig

	pendingHandles []*sqs.DeleteMessageBatchRequestEntry

	session *session.Session
	sqs     *sqs.SQS

	log   log.Modular
	stats metrics.Type
}

// NewAmazonSQS creates a new Amazon SQS reader.Type.
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

	sess, err := a.conf.GetSession()
	if err != nil {
		return err
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

	msg := message.New(nil)

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
			msg.Append(message.NewPart([]byte(*sqsMsg.Body)))
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
