package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/checkpoint"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/message"
)

type awsKinesisRecordBatcher struct {
	streamID string
	shardID  string

	batchPolicy  *policy.Batcher
	checkpointer *checkpoint.Capped[string]

	flushedMessage message.Batch

	batchedSequence string

	ackedSequence string
	ackedMut      sync.Mutex
	ackedWG       sync.WaitGroup
}

func (k *kinesisReader) newAWSKinesisRecordBatcher(streamID, shardID, sequence string) (*awsKinesisRecordBatcher, error) {
	batchPolicy, err := policy.New(k.conf.Batching, k.mgr.IntoPath("aws_kinesis", "batching"))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize batch policy for shard consumer: %w", err)
	}

	return &awsKinesisRecordBatcher{
		streamID:      streamID,
		shardID:       shardID,
		batchPolicy:   batchPolicy,
		checkpointer:  checkpoint.NewCapped[string](int64(k.conf.CheckpointLimit)),
		ackedSequence: sequence,
	}, nil
}

func (a *awsKinesisRecordBatcher) AddRecord(r *kinesis.Record) bool {
	p := message.NewPart(r.Data)
	p.MetaSetMut("kinesis_stream", a.streamID)
	p.MetaSetMut("kinesis_shard", a.shardID)
	if r.PartitionKey != nil {
		p.MetaSetMut("kinesis_partition_key", *r.PartitionKey)
	}
	p.MetaSetMut("kinesis_sequence_number", *r.SequenceNumber)

	a.batchedSequence = *r.SequenceNumber
	if a.flushedMessage != nil {
		// Upstream shouldn't really be adding records if a prior flush was
		// unsuccessful. However, we can still accommodate this by appending it
		// to the flushed message.
		a.flushedMessage = append(a.flushedMessage, p)
		return true
	}
	return a.batchPolicy.Add(p)
}

func (a *awsKinesisRecordBatcher) HasPendingMessage() bool {
	return a.flushedMessage != nil
}

func (a *awsKinesisRecordBatcher) FlushMessage(ctx context.Context) (asyncMessage, error) {
	if a.flushedMessage == nil {
		if a.flushedMessage = a.batchPolicy.Flush(ctx); a.flushedMessage == nil {
			return asyncMessage{}, nil
		}
	}

	resolveFn, err := a.checkpointer.Track(ctx, a.batchedSequence, int64(a.flushedMessage.Len()))
	if err != nil {
		if errors.Is(err, component.ErrTimeout) {
			err = nil
		}
		return asyncMessage{}, err
	}

	a.ackedWG.Add(1)
	aMsg := asyncMessage{
		msg: a.flushedMessage,
		ackFn: func(ctx context.Context, res error) error {
			topSequence := resolveFn()
			if topSequence != nil {
				a.ackedMut.Lock()
				a.ackedSequence = *topSequence
				a.ackedMut.Unlock()
			}
			a.ackedWG.Done()
			return err
		},
	}
	a.flushedMessage = nil
	return aMsg, nil
}

func (a *awsKinesisRecordBatcher) UntilNext() time.Duration {
	return a.batchPolicy.UntilNext()
}

func (a *awsKinesisRecordBatcher) GetSequence() string {
	a.ackedMut.Lock()
	seq := a.ackedSequence
	a.ackedMut.Unlock()
	return seq
}

func (a *awsKinesisRecordBatcher) Close(ctx context.Context, blocked bool) {
	if blocked {
		a.ackedWG.Wait()
	}
	_ = a.batchPolicy.Close(ctx)
}
