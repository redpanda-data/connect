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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

//------------------------------------------------------------------------------

// KinesisConfig is configuration values for the input type.
type KinesisConfig struct {
	sess.Config     `json:",inline" yaml:",inline"`
	Limit           int64              `json:"limit" yaml:"limit"`
	Stream          string             `json:"stream" yaml:"stream"`
	Shard           string             `json:"shard" yaml:"shard"`
	DynamoDBTable   string             `json:"dynamodb_table" yaml:"dynamodb_table"`
	ClientID        string             `json:"client_id" yaml:"client_id"`
	CommitPeriod    string             `json:"commit_period" yaml:"commit_period"`
	StartFromOldest bool               `json:"start_from_oldest" yaml:"start_from_oldest"`
	Timeout         string             `json:"timeout" yaml:"timeout"`
	Batching        batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewKinesisConfig creates a new Config with default values.
func NewKinesisConfig() KinesisConfig {
	batchConf := batch.NewPolicyConfig()
	batchConf.Count = 1
	return KinesisConfig{
		Config:          sess.NewConfig(),
		Limit:           100,
		Stream:          "",
		Shard:           "0",
		DynamoDBTable:   "",
		ClientID:        "benthos_consumer",
		CommitPeriod:    "1s",
		StartFromOldest: true,
		Timeout:         "5s",
		Batching:        batchConf,
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
	sequenceCommit      string
	sequence            string
	sharditer           string
	namespace           string

	commitPeriod time.Duration
	timeout      time.Duration

	log   log.Modular
	stats metrics.Type
}

// NewKinesis creates a new Amazon Kinesis stream reader.Type.
func NewKinesis(
	conf KinesisConfig,
	log log.Modular,
	stats metrics.Type,
) (*Kinesis, error) {
	var timeout, commitPeriod time.Duration
	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}
	if tout := conf.CommitPeriod; len(tout) > 0 {
		var err error
		if commitPeriod, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse commit period string: %v", err)
		}
	}
	return &Kinesis{
		conf:         conf,
		log:          log,
		timeout:      timeout,
		commitPeriod: commitPeriod,
		namespace:    fmt.Sprintf("%v-%v", conf.ClientID, conf.Stream),
		stats:        stats,
	}, nil
}

func (k *Kinesis) getIter() error {
	if len(k.sequenceCommit) == 0 && len(k.conf.DynamoDBTable) > 0 {
		resp, err := k.dynamo.GetItemWithContext(
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
		if seqAttr := resp.Item["sequence"]; seqAttr != nil {
			if seqAttr.S != nil {
				k.sequenceCommit = *seqAttr.S
				k.sequence = *seqAttr.S
			}
		}
	}

	if len(k.sharditer) == 0 && len(k.sequence) > 0 {
		getShardIter := kinesis.GetShardIteratorInput{
			ShardId:                &k.conf.Shard,
			StreamName:             &k.conf.Stream,
			StartingSequenceNumber: &k.sequence,
			ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		}
		res, err := k.kinesis.GetShardIteratorWithContext(
			aws.BackgroundContext(),
			&getShardIter,
			request.WithResponseReadTimeout(k.timeout),
		)
		if err != nil {
			if err.Error() == request.ErrCodeResponseTimeout {
				return types.ErrTimeout
			} else if err.Error() == kinesis.ErrCodeInvalidArgumentException {
				k.log.Errorf("Failed to receive iterator from sequence number: %v\n", err.Error())
			} else {
				return err
			}
		}
		if res.ShardIterator != nil {
			k.sharditer = *res.ShardIterator
		}
	}

	if len(k.sharditer) == 0 {
		// Otherwise start from somewhere
		iterType := kinesis.ShardIteratorTypeTrimHorizon
		if !k.conf.StartFromOldest {
			iterType = kinesis.ShardIteratorTypeLatest
		}
		// If we failed to obtain from a sequence we start from beginning
		if len(k.sequence) > 0 {
			iterType = kinesis.ShardIteratorTypeTrimHorizon
		}
		getShardIter := kinesis.GetShardIteratorInput{
			ShardId:           &k.conf.Shard,
			StreamName:        &k.conf.Stream,
			ShardIteratorType: &iterType,
		}
		res, err := k.kinesis.GetShardIteratorWithContext(
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
		if res.ShardIterator != nil {
			k.sharditer = *res.ShardIterator
		}
	}

	if len(k.sharditer) == 0 {
		return errors.New("failed to obtain shard iterator")
	}
	return nil
}

// Connect attempts to establish a connection to the target SQS queue.
func (k *Kinesis) Connect() error {
	return k.ConnectWithContext(context.Background())
}

// ConnectWithContext attempts to establish a connection to the target Kinesis
// shard.
func (k *Kinesis) ConnectWithContext(ctx context.Context) error {
	if k.session != nil {
		return nil
	}

	sess, err := k.conf.GetSession()
	if err != nil {
		return err
	}

	k.dynamo = dynamodb.New(sess)
	k.kinesis = kinesis.New(sess)
	k.session = sess

	if err = k.getIter(); err != nil {
		k.dynamo = nil
		k.kinesis = nil
		k.session = nil
		return err
	}

	k.log.Infof("Receiving Amazon Kinesis messages from stream: %v\n", k.conf.Stream)
	return nil
}

// Read attempts to read a new message from the target SQS.
func (k *Kinesis) Read() (types.Message, error) {
	return k.ReadNextWithContext(context.Background())
}

// ReadNextWithContext attempts to read a new message from the target Kinesis
// shard.
func (k *Kinesis) ReadNextWithContext(ctx context.Context) (types.Message, error) {
	if k.session == nil {
		return nil, types.ErrNotConnected
	}
	if len(k.sharditer) == 0 {
		if err := k.getIter(); err != nil {
			return nil, fmt.Errorf("failed to obtain iterator: %v", err)
		}
	}

	getRecords := kinesis.GetRecordsInput{
		Limit:         &k.conf.Limit,
		ShardIterator: &k.sharditer,
	}
	res, err := k.kinesis.GetRecordsWithContext(
		ctx,
		&getRecords,
		request.WithResponseReadTimeout(k.timeout),
	)
	if err != nil {
		if err.Error() == request.ErrCodeResponseTimeout {
			return nil, types.ErrTimeout
		} else if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			return nil, types.ErrTimeout
		} else if err.Error() == kinesis.ErrCodeExpiredIteratorException {
			k.log.Warnln("Shard iterator expired, attempting to refresh")
			return nil, types.ErrTimeout
		}
		return nil, err
	}

	if res.NextShardIterator != nil {
		k.sharditer = *res.NextShardIterator
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
			if rec.SequenceNumber != nil {
				k.sequence = *rec.SequenceNumber
			}
		}
	}

	if msg.Len() == 0 {
		return nil, types.ErrTimeout
	}

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
					"sequence": {
						S: aws.String(k.sequenceCommit),
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
	return k.AcknowledgeWithContext(context.Background(), err)
}

// AcknowledgeWithContext confirms whether or not our unacknowledged messages
// have been successfully propagated or not.
func (k *Kinesis) AcknowledgeWithContext(ctx context.Context, err error) error {
	if err == nil {
		k.sequenceCommit = k.sequence
	}

	if time.Since(k.offsetLastCommitted) < k.commitPeriod {
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
