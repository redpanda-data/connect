// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestCheckpointTracker_RevokedPartitionBlocksCommit(t *testing.T) {
	var mu sync.Mutex
	var committed []*kgo.Record

	batchChan := make(chan batchWithAckFn, 100)
	ct := newCheckpointTracker(
		service.MockResources(),
		batchChan,
		func(*kgo.Record) {},
		service.BatchPolicy{},
	)
	// Wrap commitFn to check revoked status, same as Connect() does.
	ct.commitFn = func(r *kgo.Record) {
		if ct.isRevoked(r.Topic, r.Partition) {
			return
		}
		mu.Lock()
		committed = append(committed, r)
		mu.Unlock()
	}

	ctx := context.Background()

	// Add records to two partitions.
	for i := range 3 {
		ct.addRecord(ctx, &msgWithRecord{
			msg: service.NewMessage(nil),
			r:   &kgo.Record{Topic: "topic-a", Partition: 0, Offset: int64(i)},
		}, 1024)
	}
	for i := range 3 {
		ct.addRecord(ctx, &msgWithRecord{
			msg: service.NewMessage(nil),
			r:   &kgo.Record{Topic: "topic-a", Partition: 1, Offset: int64(i)},
		}, 1024)
	}

	// Drain all batches into a slice so we can ack them after revoke.
	var batches []batchWithAckFn
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for {
			select {
			case b := <-batchChan:
				batches = append(batches, b)
			case <-time.After(200 * time.Millisecond):
				return
			}
		}
	}()
	<-drainDone

	require.NotEmpty(t, batches, "should have received batches")

	// Simulate rebalance: revoke partition 0 only.
	ct.removeTopicPartitions(ctx, map[string][]int32{
		"topic-a": {0},
	})

	assert.True(t, ct.isRevoked("topic-a", 0), "partition 0 should be revoked")
	assert.False(t, ct.isRevoked("topic-a", 1), "partition 1 should not be revoked")

	// Now ack all in-flight batches. This simulates the pipeline completing
	// after the rebalance already revoked partition 0.
	for _, b := range batches {
		b.onAck()
	}

	// Only partition 1's records should have been committed.
	mu.Lock()
	defer mu.Unlock()
	for _, r := range committed {
		assert.NotEqual(t, int32(0), r.Partition,
			"commitFn should not be called for revoked partition 0, but got offset %d", r.Offset)
	}
}

func TestCheckpointTracker_ClearRevokedOnReassign(t *testing.T) {
	batchChan := make(chan batchWithAckFn, 100)
	ct := newCheckpointTracker(
		service.MockResources(),
		batchChan,
		func(_ *kgo.Record) {},
		service.BatchPolicy{},
	)

	ctx := context.Background()

	// Add a record so the partition tracker exists.
	ct.addRecord(ctx, &msgWithRecord{
		msg: service.NewMessage(nil),
		r:   &kgo.Record{Topic: "topic-a", Partition: 0, Offset: 0},
	}, 1024)

	// Revoke partition 0.
	ct.removeTopicPartitions(ctx, map[string][]int32{
		"topic-a": {0},
	})
	assert.True(t, ct.isRevoked("topic-a", 0))

	// Simulate re-assignment of the same partition.
	ct.clearRevoked(map[string][]int32{
		"topic-a": {0},
	})
	assert.False(t, ct.isRevoked("topic-a", 0),
		"partition 0 should no longer be revoked after re-assignment")
}

func TestCheckpointTracker_SoftStopFlushesBatcher(t *testing.T) {
	batchChan := make(chan batchWithAckFn, 100)
	res := service.MockResources()

	bp, err := service.NewConfigSpec().
		Field(service.NewBatchPolicyField("test")).
		ParseYAML(`test: {count: 10}`, service.NewEnvironment())
	require.NoError(t, err)

	batchPol, err := bp.FieldBatchPolicy("test")
	require.NoError(t, err)

	ct := newCheckpointTracker(
		res,
		batchChan,
		func(_ *kgo.Record) {},
		batchPol,
	)

	ctx := context.Background()

	// Add fewer messages than the batch count threshold so they stay
	// buffered in the batcher.
	for i := range 3 {
		ct.addRecord(ctx, &msgWithRecord{
			msg: service.NewMessage([]byte("hello")),
			r:   &kgo.Record{Topic: "topic-a", Partition: 0, Offset: int64(i)},
		}, 1024)
	}

	// Close the partition tracker (simulates what removeTopicPartitions does).
	// Before this fix, close() would trigger SoftStop and loop() would return
	// without flushing the batcher, losing the 3 buffered messages.
	ct.mut.Lock()
	tracker := ct.topics["topic-a"][0]
	ct.mut.Unlock()

	require.NotNil(t, tracker)
	require.NoError(t, tracker.close(ctx))

	// The flushed batch should appear on batchChan.
	select {
	case b := <-batchChan:
		assert.Len(t, b.batch, 3, "all buffered messages should be flushed on SoftStop")
		b.onAck()
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for flushed batch after SoftStop")
	}
}
