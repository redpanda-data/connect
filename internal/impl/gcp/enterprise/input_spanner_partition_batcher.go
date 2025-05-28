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
	"fmt"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/ack"
	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams"
)

type spannerPartitionBatcher struct {
	batcher *service.Batcher
	last    *changestreams.DataChangeRecord
	period  *time.Timer
	acks    []*ack.Once
	rm      func()
}

func (s *spannerPartitionBatcher) MaybeFlushWith(ctx context.Context, dcr *changestreams.DataChangeRecord) (service.MessageBatch, error) {
	b, err := json.Marshal(dcr)
	if err != nil {
		return nil, fmt.Errorf("marshal data change record as JSON: %w", err)
	}

	s.last = dcr

	if !s.batcher.Add(service.NewMessage(b)) {
		return nil, nil
	}

	return s.flush(ctx)
}

func (s *spannerPartitionBatcher) Flush(ctx context.Context) (service.MessageBatch, *changestreams.DataChangeRecord, error) {
	if s.last == nil {
		return nil, nil, nil
	}

	last := s.last
	msg, err := s.flush(ctx)
	return msg, last, err
}

func (s *spannerPartitionBatcher) flush(ctx context.Context) (service.MessageBatch, error) {
	msg, err := s.batcher.Flush(ctx)
	s.last = nil
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
