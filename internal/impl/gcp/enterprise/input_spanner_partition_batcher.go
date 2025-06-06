// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/ack"
	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams"
)

// spannerPartitionBatchIter goes over changestreams.DataChangeRecord.Mods,
// for every mod it creates a message and adds it to the batch, if the batch is
// full, it yields the batch and creates a new one.
//
// Iff batch is returned with nonzero time, when acked the partition watermark
// should be updated to this time.
type spannerPartitionBatchIter struct {
	*spannerPartitionBatcher
	dcr *changestreams.DataChangeRecord
	err error
}

func (s *spannerPartitionBatchIter) Iter(ctx context.Context) iter.Seq2[service.MessageBatch, time.Time] {
	return func(yield func(service.MessageBatch, time.Time) bool) {
		if s.err != nil {
			return
		}

		lastFlushed := false
		defer func() {
			if lastFlushed {
				s.last = nil
			} else {
				s.last = s.dcr
			}
		}()

		first := true
		for i, m := range s.dcr.Mods {
			b, err := json.Marshal(m)
			if err != nil {
				s.err = err
				return
			}

			msg := service.NewMessage(b)
			msg.MetaSet("table_name", s.dcr.TableName)
			msg.MetaSet("mod_type", s.dcr.ModType)
			msg.MetaSetMut("commit_timestamp", s.dcr.CommitTimestamp)
			msg.MetaSet("record_sequence", s.dcr.RecordSequence)
			msg.MetaSet("server_transaction_id", s.dcr.ServerTransactionID)
			msg.MetaSetMut("is_last_record_in_transaction_in_partition", s.dcr.IsLastRecordInTransactionInPartition)
			msg.MetaSet("value_capture_type", s.dcr.ValueCaptureType)
			msg.MetaSetMut("number_of_records_in_transaction", s.dcr.NumberOfRecordsInTransaction)
			msg.MetaSetMut("number_of_partitions_in_transaction", s.dcr.NumberOfPartitionsInTransaction)
			msg.MetaSet("transaction_tag", s.dcr.TransactionTag)
			msg.MetaSetMut("is_system_transaction", s.dcr.IsSystemTransaction)

			if !s.batcher.Add(msg) {
				continue
			}

			mb, err := s.flush(ctx)
			if err != nil {
				s.err = err
				return
			}

			// Return the watermark to be updated after processing the batch.
			// Not every batch should update the watermark, we update watermark
			// only after processing the whole DataChangeRecord.
			var watermark time.Time
			if first && s.last != nil {
				watermark = s.last.CommitTimestamp
				first = false
			}
			if i == len(s.dcr.Mods)-1 {
				watermark = s.dcr.CommitTimestamp
				lastFlushed = true
			}
			if !yield(mb, watermark) {
				return
			}
		}
	}
}

// Err returns any error that occurred during iteration.
func (s *spannerPartitionBatchIter) Err() error {
	return s.err
}

type spannerPartitionBatcher struct {
	batcher *service.Batcher
	last    *changestreams.DataChangeRecord
	period  *time.Timer
	acks    []*ack.Once
	rm      func()
}

func (s *spannerPartitionBatcher) MaybeFlushWith(dcr *changestreams.DataChangeRecord) *spannerPartitionBatchIter {
	return &spannerPartitionBatchIter{spannerPartitionBatcher: s, dcr: dcr}
}

func (s *spannerPartitionBatcher) Flush(ctx context.Context) (service.MessageBatch, time.Time, error) {
	if s.last == nil {
		return nil, time.Time{}, nil
	}
	defer func() {
		s.last = nil
	}()

	msg, err := s.flush(ctx)
	return msg, s.last.CommitTimestamp, err
}

func (s *spannerPartitionBatcher) flush(ctx context.Context) (service.MessageBatch, error) {
	msg, err := s.batcher.Flush(ctx)
	if d, ok := s.batcher.UntilNext(); ok {
		s.period.Reset(d)
	}
	return msg, err
}

func (s *spannerPartitionBatcher) AddAck(ack *ack.Once) {
	if ack == nil {
		return
	}
	s.acks = append(s.acks, ack)
}

func (s *spannerPartitionBatcher) WaitAcks(ctx context.Context) error {
	var merr []error
	for _, ack := range s.acks {
		if err := ack.Wait(ctx); err != nil {
			merr = append(merr, err)
		}
	}
	return errors.Join(merr...)
}

func (s *spannerPartitionBatcher) AckError() error {
	for _, ack := range s.acks {
		if _, err := ack.TryWait(); err != nil {
			return err
		}
	}
	return nil
}

func (s *spannerPartitionBatcher) Close(ctx context.Context) error {
	defer s.rm()
	if s.period != nil {
		s.period.Stop()
	}
	return s.batcher.Close(ctx)
}

// spannerPartitionBatcherFactory caches active spannerPartitionBatcher instances.
type spannerPartitionBatcherFactory struct {
	batching service.BatchPolicy
	res      *service.Resources

	mu         sync.RWMutex
	partitions map[string]*spannerPartitionBatcher
}

func newSpannerPartitionBatcherFactory(
	batching service.BatchPolicy,
	res *service.Resources,
) *spannerPartitionBatcherFactory {
	return &spannerPartitionBatcherFactory{
		batching:   batching,
		res:        res,
		partitions: make(map[string]*spannerPartitionBatcher),
	}
}

func (f *spannerPartitionBatcherFactory) forPartition(partitionToken string) (*spannerPartitionBatcher, bool, error) {
	f.mu.RLock()
	spb, ok := f.partitions[partitionToken]
	f.mu.RUnlock()

	if !ok {
		b, err := f.batching.NewBatcher(f.res)
		if err != nil {
			return nil, false, err
		}

		spb = &spannerPartitionBatcher{
			batcher: b,
			rm: func() {
				f.mu.Lock()
				delete(f.partitions, partitionToken)
				f.mu.Unlock()
			},
		}
		if d, ok := spb.batcher.UntilNext(); ok {
			spb.period = time.NewTimer(d)
		}

		f.mu.Lock()
		f.partitions[partitionToken] = spb
		f.mu.Unlock()
	}
	return spb, ok, nil
}
