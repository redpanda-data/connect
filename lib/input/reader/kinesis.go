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
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	sess "github.com/Jeffail/benthos/lib/util/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

//------------------------------------------------------------------------------

// KinesisConfig is configuration values for the input type.
type KinesisConfig struct {
	sess.Config     `json:",inline" yaml:",inline"`
	Limit           int64  `json:"limit" yaml:"limit"`
	Stream          string `json:"stream" yaml:"stream"`
	Shard           string `json:"shard" yaml:"shard"`
	DynamoDBTable   string `json:"dynamodb_table" yaml:"dynamodb_table"`
	ClientID        string `json:"client_id" yaml:"client_id"`
	CommitPeriodMS  int    `json:"commit_period_ms" yaml:"commit_period_ms"`
	StartFromOldest bool   `json:"start_from_oldest" yaml:"start_from_oldest"`
	TimeoutMS       int64  `json:"timeout_ms" yaml:"timeout_ms"`
}

// NewKinesisConfig creates a new Config with default values.
func NewKinesisConfig() KinesisConfig {
	return KinesisConfig{
		Config:          sess.NewConfig(),
		Limit:           100,
		Stream:          "",
		Shard:           "0",
		DynamoDBTable:   "",
		ClientID:        "benthos_consumer",
		CommitPeriodMS:  1000,
		StartFromOldest: true,
		TimeoutMS:       5000,
	}
}

//------------------------------------------------------------------------------

// Kinesis is a benthos reader.Type implementation that reads messages from an
// Amazon Kinesis stream.
type Kinesis struct {
	conf KinesisConfig

	session *session.Session
	kinesis *kinesis.Kinesis
	dynamo  *dynamodb.DynamoDB

	offsetLastCommitted time.Time
	sharditerCommit     string
	sharditer           string
	namespace           string

	timeout time.Duration

	log   log.Modular
	stats metrics.Type
}

// NewKinesis creates a new Amazon Kinesis stream reader.Type.
func NewKinesis(
	conf KinesisConfig,
	log log.Modular,
	stats metrics.Type,
) *Kinesis {
	return &Kinesis{
		conf:      conf,
		log:       log.NewModule(".input.kinesis"),
		timeout:   time.Duration(conf.TimeoutMS) * time.Millisecond,
		namespace: fmt.Sprintf("%v-%v", conf.ClientID, conf.Stream),
		stats:     stats,
	}
}

// Connect attempts to establish a connection to the target SQS queue.
func (k *Kinesis) Connect() error {
	if k.session != nil {
		return nil
	}

	sess, err := k.conf.GetSession()
	if err != nil {
		return err
	}

	dynamo := dynamodb.New(sess)
	kin := kinesis.New(sess)

	if len(k.sharditer) == 0 && len(k.conf.DynamoDBTable) > 0 {
		resp, err := dynamo.GetItemWithContext(
			aws.BackgroundContext(),
			&dynamodb.GetItemInput{
				TableName:      aws.String(k.conf.DynamoDBTable),
				ConsistentRead: aws.Bool(true),
				Key: map[string]*dynamodb.AttributeValue{
					"namespace": {
						S: aws.String(k.namespace),
					},
					"shard_id": {
						S: aws.String(k.conf.Shard),
					},
				},
			},
			request.WithResponseReadTimeout(k.timeout),
		)
		if err != nil {
			if err.Error() == request.ErrCodeResponseTimeout {
				return types.ErrTimeout
			}
			return err
		}
		if seqAttr := resp.Item["sequence_number"]; seqAttr != nil {
			if seqAttr.S != nil {
				k.sharditer = *seqAttr.S
			}
		}
	}

	if len(k.sharditer) == 0 {
		// Otherwise start from somewhere
		iterType := kinesis.ShardIteratorTypeTrimHorizon
		if !k.conf.StartFromOldest {
			iterType = kinesis.ShardIteratorTypeLatest
		}
		getShardIter := kinesis.GetShardIteratorInput{
			ShardId:           &k.conf.Shard,
			StreamName:        &k.conf.Stream,
			ShardIteratorType: &iterType,
		}
		res, err := kin.GetShardIteratorWithContext(
			aws.BackgroundContext(),
			&getShardIter,
			request.WithResponseReadTimeout(k.timeout),
		)
		if err != nil {
			if err.Error() == request.ErrCodeResponseTimeout {
				return types.ErrTimeout
			}
			return err
		}
		if res.ShardIterator == nil {
			return errors.New("received nil shard iterator")
		}
		k.sharditer = *res.ShardIterator
	}

	if len(k.sharditer) == 0 {
		return errors.New("failed to obtain shard iterator")
	}

	k.sharditerCommit = k.sharditer

	k.kinesis = kin
	k.dynamo = dynamo
	k.session = sess

	k.log.Infof("Receiving Amazon Kinesis messages from stream: %v\n", k.conf.Stream)
	return nil
}

// Read attempts to read a new message from the target SQS.
func (k *Kinesis) Read() (types.Message, error) {
	if k.session == nil {
		return nil, types.ErrNotConnected
	}

	getRecords := kinesis.GetRecordsInput{
		Limit:         &k.conf.Limit,
		ShardIterator: &k.sharditer,
	}
	res, err := k.kinesis.GetRecordsWithContext(
		aws.BackgroundContext(),
		&getRecords,
		request.WithResponseReadTimeout(k.timeout),
	)
	if err != nil {
		if err.Error() == request.ErrCodeResponseTimeout {
			return nil, types.ErrTimeout
		}
		return nil, err
	}

	if len(res.Records) == 0 {
		return nil, types.ErrTimeout
	}

	msg := message.New(nil)
	for _, rec := range res.Records {
		if rec.Data != nil {
			part := message.NewPart(rec.Data)
			part.Metadata().Set("kinesis_shard", k.conf.Shard)
			part.Metadata().Set("kinesis_stream", k.conf.Stream)

			msg.Append(part)
		}
	}

	if msg.Len() == 0 {
		return nil, types.ErrTimeout
	}

	k.sharditer = *res.NextShardIterator
	return msg, nil
}

func (k *Kinesis) commit() error {
	if k.session == nil {
		return nil
	}
	if len(k.conf.DynamoDBTable) > 0 {
		if _, err := k.dynamo.PutItemWithContext(
			aws.BackgroundContext(),
			&dynamodb.PutItemInput{
				TableName: aws.String(k.conf.DynamoDBTable),
				Item: map[string]*dynamodb.AttributeValue{
					"namespace": {
						S: aws.String(k.namespace),
					},
					"shard_id": {
						S: aws.String(k.conf.Shard),
					},
					"sequence_number": {
						S: aws.String(k.sharditerCommit),
					},
				},
			},
			request.WithResponseReadTimeout(k.timeout),
		); err != nil {
			return err
		}
		k.offsetLastCommitted = time.Now()
	}
	return nil
}

// Acknowledge confirms whether or not our unacknowledged messages have been
// successfully propagated or not.
func (k *Kinesis) Acknowledge(err error) error {
	if err == nil {
		k.sharditerCommit = k.sharditer
	}

	if time.Since(k.offsetLastCommitted) <
		(time.Millisecond * time.Duration(k.conf.CommitPeriodMS)) {
		return nil
	}

	return k.commit()
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (k *Kinesis) CloseAsync() {
	go k.commit()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (k *Kinesis) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
