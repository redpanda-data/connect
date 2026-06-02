// Copyright 2026 Redpanda Data, Inc.
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// committedRecord captures the topic/partition/offset of a record that the
// reader decided to commit, so tests can assert exactly what was (and was not)
// committed.
type committedRecord struct {
	topic     string
	partition int32
	offset    int64
}

type commitRecorder struct {
	mu         sync.Mutex
	committed  []committedRecord
	commitFunc func(r *kgo.Record)
}

func newCommitRecorder() *commitRecorder {
	c := &commitRecorder{}
	c.commitFunc = func(r *kgo.Record) {
		c.mu.Lock()
		c.committed = append(c.committed, committedRecord{r.Topic, r.Partition, r.Offset})
		c.mu.Unlock()
	}
	return c
}

func (c *commitRecorder) offsetsFor(topic string, partition int32) []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []int64
	for _, r := range c.committed {
		if r.topic == topic && r.partition == partition {
			out = append(out, r.offset)
		}
	}
	return out
}

// drainBatches collects every batch currently available on the channel without
// acking them, so tests can control ack ordering relative to a revoke.
func drainBatches(ch <-chan batchWithAckFn, n int) []batchWithAckFn {
	out := make([]batchWithAckFn, 0, n)
	for range n {
		out = append(out, <-ch)
	}
	return out
}

func unorderedRecord(topic string, partition int32, offset int64) *msgWithRecord {
	return &msgWithRecord{
		msg: service.NewMessage([]byte("hello")),
		r:   &kgo.Record{Topic: topic, Partition: partition, Offset: offset},
	}
}

// TestUnorderedRevokedPartitionBlocksCommit verifies that once a partition is
// revoked, a late ack for an in-flight batch of that partition does not commit
// its offset, while other partitions are unaffected.
func TestUnorderedRevokedPartitionBlocksCommit(t *testing.T) {
	rec := newCommitRecorder()
	batchChan := make(chan batchWithAckFn, 100)
	ct := newCheckpointTracker(service.MockResources(), batchChan, rec.commitFunc, service.BatchPolicy{})

	ctx := context.Background()
	const limit = 1024

	// Three records on each of two partitions. With a noop batch policy each
	// record is dispatched as its own batch immediately.
	for i := range int64(3) {
		ct.addRecord(ctx, unorderedRecord("t", 0, i), limit)
		ct.addRecord(ctx, unorderedRecord("t", 1, i), limit)
	}

	batches := drainBatches(batchChan, 6)

	// Revoke partition 0 only.
	ct.removeTopicPartitions(ctx, map[string][]int32{"t": {0}})

	// Ack everything in flight, after the revoke.
	for _, b := range batches {
		b.onAck()
	}

	assert.Empty(t, rec.offsetsFor("t", 0), "revoked partition 0 must not commit any offsets")
	assert.Equal(t, []int64{0, 1, 2}, rec.offsetsFor("t", 1), "partition 1 should commit normally")
}

// TestUnorderedReassignedPartitionUsesFreshTracker is the critical test for the
// per-tracker design: after a partition is revoked and then reassigned, a new
// tracker object is created and commits resume for it, while a stale ack still
// holding the old (revoked) tracker can never commit. A design using a shared
// revoked-set keyed by topic/partition (cleared on reassignment) would fail
// this test, because the stale ack would slip through after the clear.
func TestUnorderedReassignedPartitionUsesFreshTracker(t *testing.T) {
	rec := newCommitRecorder()
	batchChan := make(chan batchWithAckFn, 100)
	ct := newCheckpointTracker(service.MockResources(), batchChan, rec.commitFunc, service.BatchPolicy{})

	ctx := context.Background()
	const limit = 1024

	// Old generation: a record on t/0 at offset 5, left in flight (not acked).
	ct.addRecord(ctx, unorderedRecord("t", 0, 5), limit)
	oldBatch := <-batchChan

	// Revoke t/0.
	ct.removeTopicPartitions(ctx, map[string][]int32{"t": {0}})

	// Reassignment: a new record on t/0 at offset 100 creates a fresh tracker.
	ct.addRecord(ctx, unorderedRecord("t", 0, 100), limit)
	newBatch := <-batchChan

	// Ack the new generation: commit must go through.
	newBatch.onAck()
	assert.Equal(t, []int64{100}, rec.offsetsFor("t", 0), "reassigned partition should commit via fresh tracker")

	// Ack the stale old-generation batch: must NOT commit.
	oldBatch.onAck()
	assert.Equal(t, []int64{100}, rec.offsetsFor("t", 0), "stale ack from revoked tracker must not commit")
	assert.NotContains(t, rec.offsetsFor("t", 0), int64(5), "old offset 5 must never be committed")
}

// TestUnorderedRevokeRaceNoCommitLeak exercises concurrent acks racing against a
// revoke under the race detector. The invariant checked is that committed
// offsets are always a subset of delivered offsets and that no data race occurs
// on the revoked flag or the tracker maps.
func TestUnorderedRevokeRaceNoCommitLeak(t *testing.T) {
	rec := newCommitRecorder()
	batchChan := make(chan batchWithAckFn, 1000)
	ct := newCheckpointTracker(service.MockResources(), batchChan, rec.commitFunc, service.BatchPolicy{})

	ctx := context.Background()
	const limit = 1 << 20
	const n = 200

	delivered := map[int64]struct{}{}
	for i := range int64(n) {
		ct.addRecord(ctx, unorderedRecord("t", 0, i), limit)
		delivered[i] = struct{}{}
	}
	batches := drainBatches(batchChan, n)

	var wg sync.WaitGroup
	wg.Go(func() {
		ct.removeTopicPartitions(ctx, map[string][]int32{"t": {0}})
	})
	for _, b := range batches {
		wg.Go(func() {
			b.onAck()
		})
	}
	wg.Wait()

	for _, off := range rec.offsetsFor("t", 0) {
		_, ok := delivered[off]
		require.True(t, ok, "committed offset %d was never delivered", off)
	}
}
