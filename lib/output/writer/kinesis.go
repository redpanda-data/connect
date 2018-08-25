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
	"errors"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/text"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

//------------------------------------------------------------------------------

// KinesisConfig contains configuration fields for the output Kinesis type.
type KinesisConfig struct {
	Region       string                     `json:"region" yaml:"region"`
	Stream       string                     `json:"stream" yaml:"stream"`
	HashKey      string                     `json:"hash_key" yaml:"hash_key"`
	PartitionKey string                     `json:"partition_key" yaml:"partition_key"`
	Credentials  AmazonAWSCredentialsConfig `json:"credentials" yaml:"credentials"`
}

// NewKinesisConfig creates a new Config with default values.
func NewKinesisConfig() KinesisConfig {
	return KinesisConfig{
		Region:       "eu-west-1",
		Stream:       "",
		HashKey:      "",
		PartitionKey: "",
		Credentials: AmazonAWSCredentialsConfig{
			ID:     "",
			Secret: "",
			Token:  "",
		},
	}
}

//------------------------------------------------------------------------------

// Kinesis is a benthos writer.Type implementation that writes messages to an
// Amazon SQS queue.
type Kinesis struct {
	conf KinesisConfig

	session *session.Session
	kinesis *kinesis.Kinesis

	hashKey      *text.InterpolatedString
	partitionKey *text.InterpolatedString

	log   log.Modular
	stats metrics.Type
}

// NewKinesis creates a new Amazon SQS writer.Type.
func NewKinesis(
	conf KinesisConfig,
	log log.Modular,
	stats metrics.Type,
) (*Kinesis, error) {
	if len(conf.PartitionKey) == 0 {
		return nil, errors.New("partition key must not be empty")
	}
	return &Kinesis{
		conf:         conf,
		log:          log.NewModule(".output.kinesis"),
		stats:        stats,
		hashKey:      text.NewInterpolatedString(conf.HashKey),
		partitionKey: text.NewInterpolatedString(conf.PartitionKey),
	}, nil
}

// Connect attempts to establish a connection to the target SQS queue.
func (a *Kinesis) Connect() error {
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
	a.kinesis = kinesis.New(sess)

	a.log.Infof("Sending messages to Kinesis stream: %v\n", a.conf.Stream)
	return nil
}

// Write attempts to write message contents to a target SQS.
func (a *Kinesis) Write(msg types.Message) error {
	if a.session == nil {
		return types.ErrNotConnected
	}

	partKey := a.partitionKey.Get(msg)
	partStr := &partKey

	var hashStr *string
	if hashKey := a.hashKey.Get(msg); len(hashKey) > 0 {
		hashStr = &hashKey
	}

	return msg.Iter(func(i int, p types.Part) error {
		if _, err := a.kinesis.PutRecord(&kinesis.PutRecordInput{
			Data:            p.Get(),
			PartitionKey:    partStr,
			ExplicitHashKey: hashStr,
			StreamName:      aws.String(a.conf.Stream),
		}); err != nil {
			return err
		}
		return nil
	})
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *Kinesis) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *Kinesis) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
