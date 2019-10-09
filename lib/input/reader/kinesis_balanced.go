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

// +build !wasm

package reader

import (
	"context"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/patrobinson/gokini"
)

//------------------------------------------------------------------------------

// KinesisBalancedConfig is configuration values for the input type.
type KinesisBalancedConfig struct {
	sess.Config           `json:",inline" yaml:",inline"`
	Stream                string `json:"stream" yaml:"stream"`
	DynamoDBTable         string `json:"dynamodb_table" yaml:"dynamodb_table"`
	DynamoDBBillingMode   string `json:"dynamodb_billing_mode" yaml:"dynamodb_billing_mode"`
	DynamoDBReadCapacity  int64  `json:"dynamodb_read_provision" yaml:"dynamodb_read_provision"`
	DynamoDBWriteCapacity int64  `json:"dynamodb_write_provision" yaml:"dynamodb_write_provision"`
	// TODO: V4 Remove this.
	MaxBatchCount   int                `json:"max_batch_count" yaml:"max_batch_count"`
	Batching        batch.PolicyConfig `json:"batching" yaml:"batching"`
	StartFromOldest bool               `json:"start_from_oldest" yaml:"start_from_oldest"`
}

// NewKinesisBalancedConfig creates a new Config with default values.
func NewKinesisBalancedConfig() KinesisBalancedConfig {
	batchConf := batch.NewPolicyConfig()
	batchConf.Count = 1
	s := sess.NewConfig()
	return KinesisBalancedConfig{
		Config:                s,
		Stream:                "",
		DynamoDBTable:         "",
		DynamoDBBillingMode:   "",
		DynamoDBReadCapacity:  0,
		DynamoDBWriteCapacity: 0,
		MaxBatchCount:         1,
		Batching:              batchConf,
		StartFromOldest:       true,
	}
}

//------------------------------------------------------------------------------

// KinesisBalanced is a benthos reader.Type implementation that reads messages
// from an Amazon Kinesis stream.
type KinesisBalanced struct {
	conf KinesisBalancedConfig

	session *session.Session

	lastSequence *string
	namespace    string

	log     log.Modular
	stats   metrics.Type
	kc      *gokini.KinesisConsumer
	records chan *gokini.Records
	shardID string
}

// NewKinesisBalanced creates a new Amazon Kinesis stream reader.Type.
func NewKinesisBalanced(
	conf KinesisBalancedConfig,
	log log.Modular,
	stats metrics.Type,
) (*KinesisBalanced, error) {
	consumer := &KinesisBalanced{
		conf:  conf,
		log:   log,
		stats: stats,
	}
	sess, err := conf.GetSession()
	if err != nil {
		return nil, err
	}
	kc := &gokini.KinesisConsumer{
		StreamName:                  conf.Stream,
		ShardIteratorType:           "TRIM_HORIZON",
		RecordConsumer:              consumer,
		TableName:                   conf.DynamoDBTable,
		EmptyRecordBackoffMs:        1000,
		DisableAutomaticCheckpoints: true,
		Session:                     sess,
	}
	if !consumer.conf.StartFromOldest {
		kc.ShardIteratorType = "LATEST"
	}
	if consumer.conf.DynamoDBTable != "" {
		kc.TableName = consumer.conf.DynamoDBTable
	}
	if consumer.conf.DynamoDBBillingMode != "" {
		kc.DynamoBillingMode = &consumer.conf.DynamoDBBillingMode
	}
	if consumer.conf.DynamoDBReadCapacity != 0 && consumer.conf.DynamoDBWriteCapacity != 0 {
		kc.DynamoReadCapacityUnits = &consumer.conf.DynamoDBReadCapacity
		kc.DynamoWriteCapacityUnits = &consumer.conf.DynamoDBWriteCapacity
	}

	consumer.kc = kc
	return consumer, nil
}

// Connect attempts to establish a connection to the target Kinesis stream.
func (k *KinesisBalanced) Connect() error {
	return k.ConnectWithContext(context.Background())
}

// ConnectWithContext attempts to establish a connection to the target Kinesis
// stream.
func (k *KinesisBalanced) ConnectWithContext(ctx context.Context) error {
	err := k.kc.StartConsumer()

	k.log.Infof("Receiving Amazon Kinesis messages from stream: %v\n", k.conf.Stream)
	return err
}

func (k *KinesisBalanced) setMetadata(record *gokini.Records, p types.Part) {
	met := p.Metadata()
	met.Set("kinesis_shard", k.shardID)
	met.Set("kinesis_partition_key", record.PartitionKey)
	met.Set("kinesis_sequence_number", record.SequenceNumber)
}

// ReadWithContext attempts to read a new message from the target Kinesis
// stream.
func (k *KinesisBalanced) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	var record *gokini.Records
	select {
	case record = <-k.records:
	case <-ctx.Done():
		return nil, nil, types.ErrTimeout
	}
	if record == nil {
		return nil, nil, fmt.Errorf("shard '%s' has closed", k.shardID)
	}

	part := message.NewPart(record.Data)
	k.setMetadata(record, part)

	msg := message.New(nil)
	msg.Append(part)

	return msg, func(rctx context.Context, res types.Response) error {
		return k.kc.Checkpoint(k.shardID, record.SequenceNumber)
	}, nil
}

// Read attempts to read a new message from the target Kinesis stream.
func (k *KinesisBalanced) Read() (types.Message, error) {
	msg := message.New(nil)

	record := <-k.records
	if record == nil {
		return nil, fmt.Errorf("shard '%s' has closed", k.shardID)
	}
	k.lastSequence = &record.SequenceNumber
	{
		part := message.NewPart(record.Data)
		k.setMetadata(record, part)
		msg.Append(part)
	}

batchLoop:
	for i := 1; i < k.conf.MaxBatchCount; i++ {
		select {
		case record := <-k.records:
			if record != nil {
				k.lastSequence = &record.SequenceNumber
				part := message.NewPart(record.Data)
				k.setMetadata(record, part)
				msg.Append(part)
			} else {
				break batchLoop
			}
		default:
			// Drained the buffer
			break batchLoop
		}
	}

	return msg, nil
}

// Acknowledge confirms whether or not our unacknowledged messages have been
// successfully propagated or not.
func (k *KinesisBalanced) Acknowledge(err error) error {
	if err == nil && k.lastSequence != nil {
		return k.kc.Checkpoint(k.shardID, *k.lastSequence)
	}
	return nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (k *KinesisBalanced) CloseAsync() {
	go k.kc.Shutdown()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (k *KinesisBalanced) WaitForClose(time.Duration) error {
	k.kc.Shutdown()
	return nil
}

// Init is required by the KinesisConsumer interface
func (k *KinesisBalanced) Init(shardID string) error {
	k.shardID = shardID
	return nil
}

// ProcessRecords implements the KinesisConsumer interface
func (k *KinesisBalanced) ProcessRecords(records []*gokini.Records, _ *gokini.KinesisConsumer) {
	for _, record := range records {
		k.records <- record
	}
}

// Shutdown implements the KinesisConsumer interface
func (k *KinesisBalanced) Shutdown() {
	k.log.Infof("Stopping processing of Stream %s Shard %s", k.conf.Stream, k.shardID)
	close(k.records)
}

//------------------------------------------------------------------------------
