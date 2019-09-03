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
	"context"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

//------------------------------------------------------------------------------

// SNSConfig contains configuration fields for the output SNS type.
type SNSConfig struct {
	TopicArn      string `json:"topic_arn" yaml:"topic_arn"`
	sessionConfig `json:",inline" yaml:",inline"`
	Timeout       string `json:"timeout" yaml:"timeout"`
}

// NewSNSConfig creates a new Config with default values.
func NewSNSConfig() SNSConfig {
	return SNSConfig{
		sessionConfig: sessionConfig{
			Config: sess.NewConfig(),
		},
		TopicArn: "",
		Timeout:  "5s",
	}
}

//------------------------------------------------------------------------------

// SNS is a benthos writer.Type implementation that writes messages to an
// Amazon SNS queue.
type SNS struct {
	conf SNSConfig

	session *session.Session
	sns     *sns.SNS

	tout time.Duration

	log   log.Modular
	stats metrics.Type
}

// NewSNS creates a new Amazon SNS writer.Type.
func NewSNS(conf SNSConfig, log log.Modular, stats metrics.Type) (*SNS, error) {
	s := &SNS{
		conf:  conf,
		log:   log,
		stats: stats,
	}
	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if s.tout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout period string: %v", err)
		}
	}
	return s, nil
}

// Connect attempts to establish a connection to the target SNS queue.
func (a *SNS) Connect() error {
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
func (a *SNS) Write(msg types.Message) error {
	if a.session == nil {
		return types.ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(
		aws.BackgroundContext(), a.tout,
	)
	defer cancel()

	return msg.Iter(func(i int, p types.Part) error {
		message := &sns.PublishInput{
			TopicArn: aws.String(a.conf.TopicArn),
			Message:  aws.String(string(p.Get())),
		}
		_, err := a.sns.PublishWithContext(ctx, message)
		return err
	})
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *SNS) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *SNS) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
