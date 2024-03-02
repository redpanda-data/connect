package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/benthosdev/benthos/v4/internal/checkpoint"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"
)

type awsKinesisRecordBatcher struct {
	streamID string
	shardID  string

	batchPolicy  *service.Batcher
	checkpointer *checkpoint.Capped[string]

	flushedMessage service.MessageBatch

	batchedSequence string

	ackedSequence string
	ackedMut      sync.Mutex
	ackedWG       sync.WaitGroup
}

func (k *kinesisReader) newAWSKinesisRecordBatcher(info streamInfo, shardID, sequence string) (*awsKinesisRecordBatcher, error) {
	batchPolicy, err := k.batcher.NewBatcher(k.mgr)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize batch policy for shard consumer: %w", err)
	}

	return &awsKinesisRecordBatcher{
		streamID:      info.id,
		shardID:       shardID,
		batchPolicy:   batchPolicy,
		checkpointer:  checkpoint.NewCapped[string](int64(k.conf.CheckpointLimit)),
		ackedSequence: sequence,
	}, nil
}

func (a *awsKinesisRecordBatcher) AddRecord(r types.Record) bool {
	p := service.NewMessage(r.Data)
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
		var err error
		if a.flushedMessage, err = a.batchPolicy.Flush(ctx); err != nil || a.flushedMessage == nil {
			return asyncMessage{}, err
		}
	}

	resolveFn, err := a.checkpointer.Track(ctx, a.batchedSequence, int64(len(a.flushedMessage)))
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, component.ErrTimeout) {
			// No need to log this error, just continue with no message.
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

func (a *awsKinesisRecordBatcher) UntilNext() (time.Duration, bool) {
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
