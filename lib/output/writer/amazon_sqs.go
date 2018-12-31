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
	"fmt"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	sess "github.com/Jeffail/benthos/lib/util/aws/session"
	"github.com/Jeffail/benthos/lib/util/retries"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/cenkalti/backoff"
)

//------------------------------------------------------------------------------

const (
	sqsMaxRecordsCount = 10
)

//------------------------------------------------------------------------------

// AmazonSQSConfig contains configuration fields for the output AmazonSQS type.
type AmazonSQSConfig struct {
	sessionConfig  `json:",inline" yaml:",inline"`
	URL            string `json:"url" yaml:"url"`
	retries.Config `json:",inline" yaml:",inline"`
}

// NewAmazonSQSConfig creates a new Config with default values.
func NewAmazonSQSConfig() AmazonSQSConfig {
	rConf := retries.NewConfig()
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"
	return AmazonSQSConfig{
		sessionConfig: sessionConfig{
			Config: sess.NewConfig(),
		},
		URL:    "",
		Config: rConf,
	}
}

//------------------------------------------------------------------------------

// AmazonSQS is a benthos writer.Type implementation that writes messages to an
// Amazon SQS queue.
type AmazonSQS struct {
	conf AmazonSQSConfig

	backoff backoff.BackOff
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
) (*AmazonSQS, error) {
	s := &AmazonSQS{
		conf:  conf,
		log:   log,
		stats: stats,
	}

	var err error
	if s.backoff, err = conf.Config.Get(); err != nil {
		return nil, err
	}
	return s, nil
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

	entries := []*sqs.SendMessageBatchRequestEntry{}
	msg.Iter(func(i int, p types.Part) error {
		entries = append(entries, &sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(strconv.FormatInt(int64(i), 10)),
			MessageBody: aws.String(string(p.Get())),
		})
		return nil
	})

	input := &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(a.conf.URL),
		Entries:  entries,
	}

	// trim input input length to max sqs batch size
	if len(entries) > sqsMaxRecordsCount {
		input.Entries, entries = entries[:sqsMaxRecordsCount], entries[sqsMaxRecordsCount:]
	} else {
		entries = nil
	}

	var err error
	for len(input.Entries) > 0 {
		wait := a.backoff.NextBackOff()

		var batchResult *sqs.SendMessageBatchOutput
		if batchResult, err = a.sqs.SendMessageBatch(input); err != nil {
			a.log.Warnf("SQS error: %v\n", err)
			// bail if a message is too large or all retry attempts expired
			if wait == backoff.Stop {
				return err
			}
			continue
		}

		if unproc := batchResult.Failed; len(unproc) > 0 {
			input.Entries = []*sqs.SendMessageBatchRequestEntry{}
			for _, v := range unproc {
				if *v.SenderFault {
					err = fmt.Errorf("record failed with code: %v", *v.Code)
					a.log.Errorf("SQS record error: %v\n", err)
					return err
				}
				input.Entries = append(input.Entries, &sqs.SendMessageBatchRequestEntry{
					Id:          v.Id,
					MessageBody: v.Message,
				})
			}
			err = fmt.Errorf("failed to send %v messages", len(unproc))
		} else {
			input.Entries = nil
		}

		if err != nil {
			if wait == backoff.Stop {
				break
			}
			time.After(wait)
		}

		// add remaining records to batch
		l := len(input.Entries)
		if n := len(entries); n > 0 && l < sqsMaxRecordsCount {
			if remaining := sqsMaxRecordsCount - l; remaining < n {
				input.Entries, entries = append(input.Entries, entries[:remaining]...), entries[remaining:]
			} else {
				input.Entries, entries = append(input.Entries, entries...), nil
			}
		}
	}

	if err == nil {
		a.backoff.Reset()
	}
	return err
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
