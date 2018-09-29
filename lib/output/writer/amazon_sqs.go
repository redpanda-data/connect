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
	sess "github.com/Jeffail/benthos/lib/util/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

//------------------------------------------------------------------------------

// AmazonSQSConfig contains configuration fields for the output AmazonSQS type.
type AmazonSQSConfig struct {
	sess.Config `json:",inline" yaml:",inline"`
	URL         string `json:"url" yaml:"url"`
}

// NewAmazonSQSConfig creates a new Config with default values.
func NewAmazonSQSConfig() AmazonSQSConfig {
	return AmazonSQSConfig{
		Config: sess.NewConfig(),
		URL:    "",
	}
}

//------------------------------------------------------------------------------

// AmazonSQS is a benthos writer.Type implementation that writes messages to an
// Amazon SQS queue.
type AmazonSQS struct {
	conf AmazonSQSConfig

	session *session.Session
	sqs     *sqs.SQS

	log   log.Modular
	stats metrics.Type
}

// NewAmazonSQS creates a new Amazon SQS writer.Type.
func NewAmazonSQS(
	conf AmazonSQSConfig,
	log log.Modular,
	stats metrics.Type,
) *AmazonSQS {
	return &AmazonSQS{
		conf:  conf,
		log:   log.NewModule(".output.sqs"),
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

	a.session = sess
	a.sqs = sqs.New(sess)

	a.log.Infof("Sending messages to Amazon SQS URL: %v\n", a.conf.URL)
	return nil
}

// Write attempts to write message contents to a target SQS.
func (a *AmazonSQS) Write(msg types.Message) error {
	if a.session == nil {
		return types.ErrNotConnected
	}

	/*
		msgs := []*sqs.SendMessageBatchRequestEntry{}
		for _, part := range message.GetAllBytes(msg) {
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

	return msg.Iter(func(i int, p types.Part) error {
		if _, err := a.sqs.SendMessage(&sqs.SendMessageInput{
			QueueUrl:    aws.String(a.conf.URL),
			MessageBody: aws.String(string(p.Get())),
		}); err != nil {
			return err
		}
		return nil
	})
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
