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
	"github.com/Jeffail/benthos/lib/util/retries"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/cenkalti/backoff"
)

//------------------------------------------------------------------------------

// AmazonSNSConfig contains configuration fields for the output AmazonSNS type.
type AmazonSNSConfig struct {
	TopicArn       string `json:"topic_arn" yaml:"topic_arn"`
	sessionConfig  `json:",inline" yaml:",inline"`
	retries.Config `json:",inline" yaml:",inline"`
}

// NewAmazonSNSConfig creates a new Config with default values.
func NewAmazonSNSConfig() AmazonSNSConfig {
	rConf := retries.NewConfig()
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"
	return AmazonSNSConfig{
		sessionConfig: sessionConfig{
			Config: sess.NewConfig(),
		},
		TopicArn: "",
		Config:   rConf,
	}
}

//------------------------------------------------------------------------------

// AmazonSNS is a benthos writer.Type implementation that writes messages to an
// Amazon SNS queue.
type AmazonSNS struct {
	conf AmazonSNSConfig

	backoff backoff.BackOff
	session *session.Session
	sns     *sns.SNS

	log   log.Modular
	stats metrics.Type
}

// NewAmazonSNS creates a new Amazon SNS writer.Type.
func NewAmazonSNS(conf AmazonSNSConfig, log log.Modular, stats metrics.Type) (*AmazonSNS, error) {
	s := &AmazonSNS{
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

// Connect attempts to establish a connection to the target SNS queue.
func (a *AmazonSNS) Connect() error {
	if a.session != nil {
		return nil
	}

	sess, err := a.conf.GetSession()
	if err != nil {
		return err
	}

	a.session = sess
	a.sns = sns.New(sess)

	a.log.Infof("Sending messages to Amazon SNS ARN: %v\n", a.conf.TopicArn)
	return nil
}

// Write attempts to write message contents to a target SNS.
func (a *AmazonSNS) Write(msg types.Message) error {
	if a.session == nil {
		return types.ErrNotConnected
	}

	return msg.Iter(func(i int, p types.Part) error {
		message := &sns.PublishInput{
			TopicArn: aws.String(a.conf.TopicArn),
			Message:  aws.String(string(p.Get())),
		}
		_, err := a.sns.Publish(message)
		if err != nil {
			return err
		}
		return nil
	})
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *AmazonSNS) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *AmazonSNS) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
