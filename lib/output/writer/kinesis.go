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
	"fmt"
	"regexp"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/text"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/cenkalti/backoff"
)

//------------------------------------------------------------------------------

const (
	kinesisMaxRecordsCount = 500
	mebibyte               = 1048576
)

var (
	kinesisPayloadLimitExceeded = regexp.MustCompile("Member must have length less than or equal to")
)

// KinesisConfig contains configuration fields for the Kinesis output type.
type KinesisConfig struct {
	Endpoint     string                     `json:"endpoint" yaml:"endpoint"`
	Region       string                     `json:"region" yaml:"region"`
	Stream       string                     `json:"stream" yaml:"stream"`
	HashKey      string                     `json:"hash_key" yaml:"hash_key"`
	PartitionKey string                     `json:"partition_key" yaml:"partition_key"`
	Credentials  AmazonAWSCredentialsConfig `json:"credentials" yaml:"credentials"`
	MaxRetries   uint64                     `json:"retries" yaml:"retries"`
	Backoff      KinesisConfigBackoff       `json:"backoff" yaml:"backoff"`
}

// KinesisConfigBackoff contains backoff configuration fields for the Kinesis output type.
type KinesisConfigBackoff struct {
	InitialInterval string `json:"initial_interval" yaml:"initial_interval"`
	MaxInterval     string `json:"max_interval" yaml:"max_interval"`
	MaxElapsedTime  string `json:"max_elapsed_time" yaml:"max_elapsed_time"`
}

// NewKinesisConfig creates a new Config with default values.
func NewKinesisConfig() KinesisConfig {
	return KinesisConfig{
		Endpoint:     "",
		Region:       "eu-west-1",
		Stream:       "",
		HashKey:      "",
		PartitionKey: "",
		Credentials: AmazonAWSCredentialsConfig{
			ID:     "",
			Secret: "",
			Token:  "",
		},
		MaxRetries: 3,
		Backoff: KinesisConfigBackoff{
			InitialInterval: "500ms",
			MaxInterval:     "3s",
			MaxElapsedTime:  "10s",
		},
	}
}

//------------------------------------------------------------------------------

// Kinesis is a benthos writer.Type implementation that writes messages to an
// Amazon Kinesis stream.
type Kinesis struct {
	conf KinesisConfig

	session *session.Session
	kinesis kinesisiface.KinesisAPI

	backoff      backoff.BackOff
	endpoint     *string
	hashKey      *text.InterpolatedString
	partitionKey *text.InterpolatedString
	streamName   *string

	log   log.Modular
	stats metrics.Type
}

// NewKinesis creates a new Amazon Kinesis writer.Type.
func NewKinesis(
	conf KinesisConfig,
	log log.Modular,
	stats metrics.Type,
) (*Kinesis, error) {
	if len(conf.PartitionKey) == 0 {
		return nil, errors.New("partition key must not be empty")
	}

	k := Kinesis{
		conf:         conf,
		log:          log.NewModule(".output.kinesis"),
		stats:        stats,
		hashKey:      text.NewInterpolatedString(conf.HashKey),
		partitionKey: text.NewInterpolatedString(conf.PartitionKey),
		streamName:   aws.String(conf.Stream),
	}

	b := backoff.NewExponentialBackOff()
	if conf.Backoff.InitialInterval != "" {
		d, err := time.ParseDuration(conf.Backoff.InitialInterval)
		if err != nil {
			return nil, fmt.Errorf("invalid backoff initial interval: %v", err)
		}
		b.InitialInterval = d
	}
	if conf.Backoff.MaxInterval != "" {
		d, err := time.ParseDuration(conf.Backoff.MaxInterval)
		if err != nil {
			return nil, fmt.Errorf("invalid backoff max interval: %v", err)
		}
		b.MaxInterval = d
	}
	if conf.Backoff.MaxElapsedTime != "" {
		d, err := time.ParseDuration(conf.Backoff.MaxElapsedTime)
		if err != nil {
			return nil, fmt.Errorf("invalid backoff max elapsed interval: %v", err)
		}
		b.MaxElapsedTime = d
	}
	k.backoff = backoff.WithMaxRetries(b, conf.MaxRetries)

	return &k, nil
}

//------------------------------------------------------------------------------

// toRecords converts an individual benthos message into a slice of Kinesis
// batch put entries by promoting each message part into a single part message
// and passing each new message through the partition and hash key interpolation
// process, allowing the user to define the partition and hash key per message
// part.
func (a *Kinesis) toRecords(msg types.Message) ([]*kinesis.PutRecordsRequestEntry, error) {
	entries := make([]*kinesis.PutRecordsRequestEntry, msg.Len())

	err := msg.Iter(func(i int, p types.Part) error {
		m := message.Lock(msg, i)

		entry := kinesis.PutRecordsRequestEntry{
			Data:         p.Get(),
			PartitionKey: aws.String(a.partitionKey.Get(m)),
		}

		if len(entry.Data) > mebibyte {
			a.log.Errorf("part %d exceeds the maximum Kinesis payload limit of 1 MiB\n", i)
			return types.ErrMessageTooLarge
		}

		if hashKey := a.hashKey.Get(m); hashKey != "" {
			entry.ExplicitHashKey = aws.String(hashKey)
		}

		entries[i] = &entry
		return nil
	})

	return entries, err
}

//------------------------------------------------------------------------------

// Connect creates a new Kinesis client and ensures that the target Kinesis
// stream exists.
func (a *Kinesis) Connect() error {
	if a.session != nil {
		return nil
	}

	awsConf := aws.NewConfig()
	if len(a.conf.Region) > 0 {
		awsConf = awsConf.WithRegion(a.conf.Region)
	}
	if len(a.conf.Endpoint) > 0 {
		awsConf = awsConf.WithEndpoint(a.conf.Endpoint)
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

	if err := a.kinesis.WaitUntilStreamExists(&kinesis.DescribeStreamInput{
		StreamName: a.streamName,
	}); err != nil {
		return err
	}

	a.log.Infof("Sending messages to Kinesis stream: %v\n", a.conf.Stream)
	return nil
}

// Write attempts to write message contents to a target Kinesis stream in batches of 500.
// If throttling is detected, failed messages are retried according to the configurable
// backoff settings.
func (a *Kinesis) Write(msg types.Message) error {
	if a.session == nil {
		return types.ErrNotConnected
	}

	records, err := a.toRecords(msg)
	if err != nil {
		return err
	}

	input := &kinesis.PutRecordsInput{
		Records:    records,
		StreamName: a.streamName,
	}

	// trim input record length to max kinesis batch size
	if len(records) > kinesisMaxRecordsCount {
		input.Records, records = records[:kinesisMaxRecordsCount], records[kinesisMaxRecordsCount:]
	} else {
		records = nil
	}

	var failed []*kinesis.PutRecordsRequestEntry
	a.backoff.Reset()
	for len(input.Records) > 0 {
		wait := a.backoff.NextBackOff()

		// batch write to kinesis
		output, err := a.kinesis.PutRecords(input)
		if err != nil {
			a.log.Warnf("kinesis error: %v\n", err)
			// bail if a message is too large or all retry attempts expired
			if wait == backoff.Stop {
				return err
			}
			continue
		}

		// requeue any individual records that failed due to throttling
		failed = nil
		if output.FailedRecordCount != nil {
			for i, entry := range output.Records {
				if entry.ErrorCode != nil {
					failed = append(failed, input.Records[i])
					if *entry.ErrorCode != kinesis.ErrCodeProvisionedThroughputExceededException && *entry.ErrorCode != kinesis.ErrCodeKMSThrottlingException {
						err = fmt.Errorf("record failed with code [%s] %s: %+v", *entry.ErrorCode, *entry.ErrorMessage, input.Records[i])
						a.log.Errorf("kinesis record error: %v\n", err)
						return err
					}
				}
			}
		}
		input.Records = failed

		// if throttling errors detected, pause briefly
		l := len(failed)
		if l > 0 {
			a.log.Warnf("scheduling retry of throttled records (%d)\n", l)
			if wait == backoff.Stop {
				return types.ErrTimeout
			}
			time.Sleep(wait)
		}

		// add remaining records to batch
		if n := len(records); n > 0 && l < kinesisMaxRecordsCount {
			if remaining := kinesisMaxRecordsCount - l; remaining < n {
				input.Records, records = append(input.Records, records[:remaining]...), records[remaining:]
			} else {
				input.Records, records = append(input.Records, records...), nil
			}
		}
	}
	return nil
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
