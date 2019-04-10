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
	sess.Config         `json:",inline" yaml:",inline"`
	URL                 string `json:"url" yaml:"url"`
	Timeout             string `json:"timeout" yaml:"timeout"`
	MaxNumberOfMessages int64  `json:"max_number_of_messages" yaml:"max_number_of_messages"`
}

// NewAmazonSQSConfig creates a new Config with default values.
func NewAmazonSQSConfig() AmazonSQSConfig {
	return AmazonSQSConfig{
		Config:              sess.NewConfig(),
		URL:                 "",
		Timeout:             "5s",
		MaxNumberOfMessages: 1,
	}
}

//------------------------------------------------------------------------------

// AmazonSQS is a benthos reader.Type implementation that reads messages from an
// Amazon SQS queue.
type AmazonSQS struct {
	conf AmazonSQSConfig

	pendingHandles map[string]string

	session *session.Session
	sqs     *sqs.SQS
	timeout time.Duration

	log   log.Modular
	stats metrics.Type
}

// NewAmazonSQS creates a new Amazon SQS reader.Type.
func NewAmazonSQS(
	conf AmazonSQSConfig,
	log log.Modular,
	stats metrics.Type,
) (*AmazonSQS, error) {
	var timeout time.Duration
	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}
	return &AmazonSQS{
		conf:           conf,
		log:            log,
		stats:          stats,
		timeout:        timeout,
		pendingHandles: map[string]string{},
	}, nil
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
		MaxNumberOfMessages: aws.Int64(a.conf.MaxNumberOfMessages),
		WaitTimeSeconds:     aws.Int64(int64(a.timeout.Seconds())),
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
			a.pendingHandles[*sqsMsg.MessageId] = *sqsMsg.ReceiptHandle
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
	if err == nil {
		for len(a.pendingHandles) > 0 {
			input := sqs.DeleteMessageBatchInput{
				QueueUrl: aws.String(a.conf.URL),
			}

		delHandleLoop:
			for k, v := range a.pendingHandles {
				input.Entries = append(input.Entries, &sqs.DeleteMessageBatchRequestEntry{
					Id:            aws.String(k),
					ReceiptHandle: aws.String(v),
				})
				delete(a.pendingHandles, k)
				if len(input.Entries) == 10 {
					break delHandleLoop
				}
			}

			if res, serr := a.sqs.DeleteMessageBatch(&input); serr != nil {
				a.log.Errorf("Failed to delete consumed SQS messages: %v\n", serr)
			} else {
				for _, fail := range res.Failed {
					a.log.Errorf("Failed to delete consumed SQS message '%v', response code: %v\n", fail.Id, fail.Code)
				}
			}
		}
	} else {
		for len(a.pendingHandles) > 0 {
			input := sqs.ChangeMessageVisibilityBatchInput{
				QueueUrl: aws.String(a.conf.URL),
			}

		visHandleLoop:
			for k, v := range a.pendingHandles {
				input.Entries = append(input.Entries, &sqs.ChangeMessageVisibilityBatchRequestEntry{
					Id:                aws.String(k),
					ReceiptHandle:     aws.String(v),
					VisibilityTimeout: aws.Int64(0),
				})
				delete(a.pendingHandles, k)
				if len(input.Entries) == 10 {
					break visHandleLoop
				}
			}

			if res, serr := a.sqs.ChangeMessageVisibilityBatch(&input); serr != nil {
				a.log.Errorf("Failed to change consumed SQS message visibility: %v\n", serr)
			} else {
				for _, fail := range res.Failed {
					a.log.Errorf("Failed to change consumed SQS message '%v' visibility, response code: %v\n", fail.Id, fail.Code)
				}
			}
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
