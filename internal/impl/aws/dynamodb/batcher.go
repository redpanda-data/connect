// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"context"
	"fmt"
	"sync"

	"github.com/Jeffail/checkpoint"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// RecordBatcher tracks in-flight message batches and persists shard
// checkpoints in stream order.
//
// Batches are tracked per shard in dispatch order via an ordered checkpoint
// tracker, so the persisted sequence number only ever advances to the highest
// *contiguous* acknowledged batch. Acks completing out of order (multiple
// batches in flight against a parallel output) can therefore never push the
// checkpoint past a batch that has not been acknowledged yet — a crash never
// skips unacked data on restart.
//
// A nacked batch is removed from tracking without being resolved, which pins
// the shard's checkpoint frontier at the position before that batch. Later
// acks keep accumulating but are not persisted past the gap; a restart
// resumes from the pinned checkpoint and redelivers the nacked records.
//
// To bound checkpoint-table writes, persistence happens only after at least
// checkpointLimit messages have been acknowledged on a shard since the last
// persisted checkpoint.
type RecordBatcher struct {
	maxTrackedShards   int
	maxTrackedMessages int
	log                *service.Logger

	mu sync.Mutex

	// batches maps every message of a tracked batch to the batch's shared
	// tracking state. The batch is the ack unit (each dispatched batch has
	// exactly one ack callback), so resolving any of its messages resolves
	// the whole batch.
	batches map[*service.Message]*trackedBatch
	// shards holds the per-shard ordered ack trackers. Entries are never
	// removed; DynamoDB stream shards rotate within 24h so the map stays
	// small, and maxTrackedShards guards against pathological growth.
	shards map[string]*shardAckTracker
	// trackedMessages counts messages across all in-flight batches, used for
	// backpressure via ShouldThrottle.
	trackedMessages int
}

type trackedBatch struct {
	shardID string
	size    int
	// msgs holds the batch's messages so all map entries can be dropped when
	// the batch settles.
	msgs []*service.Message
	// resolve marks the batch as acknowledged in the shard's ordered tracker
	// and returns the new highest contiguous sequence, or nil if the frontier
	// did not move (an earlier batch is still outstanding).
	resolve func() *string
}

type shardAckTracker struct {
	tracker *checkpoint.Uncapped[string]
	// pending counts acked messages since the last persisted checkpoint.
	pending int
	// frontier is the highest contiguous acked sequence ("" until the first
	// batch resolves in order).
	frontier string
	// persisted is the last sequence written to the checkpoint store.
	persisted string
}

// NewRecordBatcher creates a new [RecordBatcher] for DynamoDB CDC.
func NewRecordBatcher(maxTrackedShards, checkpointLimit int, log *service.Logger) *RecordBatcher {
	// Set max tracked messages to 10x the checkpoint limit to allow for some buffering.
	// This prevents unbounded growth while allowing parallel processing.
	maxTrackedMessages := max(checkpointLimit*10,
		// Minimum reasonable size
		1000)

	return &RecordBatcher{
		maxTrackedShards:   maxTrackedShards,
		maxTrackedMessages: maxTrackedMessages,
		log:                log,
		batches:            make(map[*service.Message]*trackedBatch),
		shards:             make(map[string]*shardAckTracker),
	}
}

// AddMessages tracks a batch of messages against a shard's ordered checkpoint
// tracker. The batch must be in stream order (GetRecords returns records
// ordered by sequence number), so the last message's sequence number is the
// batch's checkpoint payload. Must be called from the shard's single reader
// goroutine so batches are tracked in dispatch order.
func (b *RecordBatcher) AddMessages(batch service.MessageBatch, shardID string) service.MessageBatch {
	if len(batch) == 0 {
		return batch
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Check if we're approaching memory limits
	if b.trackedMessages+len(batch) > b.maxTrackedMessages {
		b.log.Warnf("Message tracker near capacity: %d/%d tracked messages (adding %d from shard %s)",
			b.trackedMessages, b.maxTrackedMessages, len(batch), shardID)
		// Still add messages but warn - this indicates downstream is slow
	}

	st, ok := b.shards[shardID]
	if !ok {
		st = &shardAckTracker{tracker: checkpoint.NewUncapped[string]()}
		b.shards[shardID] = st
	}

	seq, _ := batch[len(batch)-1].MetaGet("dynamodb_sequence_number")
	tb := &trackedBatch{
		shardID: shardID,
		size:    len(batch),
		msgs:    batch,
		resolve: st.tracker.Track(seq, int64(len(batch))),
	}
	for _, msg := range batch {
		b.batches[msg] = tb
	}
	b.trackedMessages += len(batch)

	return batch
}

// dropBatchLocked removes all of a batch's map entries and its message count.
// Callers must hold b.mu.
func (b *RecordBatcher) dropBatchLocked(tb *trackedBatch) {
	for _, msg := range tb.msgs {
		delete(b.batches, msg)
	}
	b.trackedMessages -= tb.size
}

// RemoveMessages drops a batch from tracking without resolving it (used when
// messages are nacked, or acked after close). The shard's checkpoint frontier
// stays pinned before this batch, so later acks cannot persist past it and a
// restart redelivers the dropped records.
func (b *RecordBatcher) RemoveMessages(batch service.MessageBatch) {
	if b == nil || len(batch) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if tb, ok := b.batches[batch[0]]; ok {
		b.dropBatchLocked(tb)
	}
}

type checkpointer interface {
	Set(ctx context.Context, shardID, sequenceNumber string) error
	CheckpointLimit() int
}

// AckMessages marks a batch as acknowledged, advances the shard's contiguous
// frontier, and persists a checkpoint once enough messages have been acked
// since the last persisted position.
func (b *RecordBatcher) AckMessages(
	ctx context.Context,
	cp checkpointer,
	batch service.MessageBatch,
) error {
	if b == nil || len(batch) == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	tb, ok := b.batches[batch[0]]
	if !ok {
		// Already removed (nacked or double-acked); nothing to do.
		return nil
	}

	if len(b.shards) > b.maxTrackedShards {
		return fmt.Errorf("checkpoint map exceeded maximum size (%d shards) - possible memory leak", b.maxTrackedShards)
	}

	b.dropBatchLocked(tb)

	st := b.shards[tb.shardID]
	if frontier := tb.resolve(); frontier != nil {
		st.frontier = *frontier
	}
	st.pending += tb.size

	// Persist once enough messages are acked AND the frontier has moved past
	// the last persisted position. A pinned frontier (nacked batch ahead of
	// us in stream order) accumulates pending acks without persisting.
	if st.pending >= cp.CheckpointLimit() && st.frontier != "" && st.frontier != st.persisted {
		if err := cp.Set(ctx, tb.shardID, st.frontier); err != nil {
			return err
		}
		st.persisted = st.frontier
		st.pending = 0
		b.log.Debugf("Checkpointed shard %s at sequence %s", tb.shardID, st.frontier)
	}

	return nil
}

// PendingCheckpoints returns, per shard, the highest contiguous acked
// sequence that has not been persisted yet. Used to flush checkpoints on
// shutdown.
func (b *RecordBatcher) PendingCheckpoints() map[string]string {
	b.mu.Lock()
	defer b.mu.Unlock()

	checkpoints := make(map[string]string, len(b.shards))
	for shardID, st := range b.shards {
		if st.frontier != "" && st.frontier != st.persisted {
			checkpoints[shardID] = st.frontier
		}
	}
	return checkpoints
}

// ShouldThrottle returns true if the message tracker is near capacity and
// backpressure should be applied.
func (b *RecordBatcher) ShouldThrottle() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	// Throttle at 90% capacity to leave some headroom
	return b.trackedMessages >= (b.maxTrackedMessages * 9 / 10)
}

// PendingCount returns the count of acked-but-not-persisted messages for a
// shard. Exported for testing.
func (b *RecordBatcher) PendingCount(shardID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if st, ok := b.shards[shardID]; ok {
		return st.pending
	}
	return 0
}

// TrackedMessageCount returns the number of tracked messages. Exported for testing.
func (b *RecordBatcher) TrackedMessageCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.trackedMessages
}

// LastCheckpoint returns the highest contiguous acked sequence for a shard.
// Exported for testing.
func (b *RecordBatcher) LastCheckpoint(shardID string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if st, ok := b.shards[shardID]; ok {
		return st.frontier
	}
	return ""
}

// LastCheckpointsCount returns the number of shards with an unpersisted
// frontier. Exported for testing.
func (b *RecordBatcher) LastCheckpointsCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, st := range b.shards {
		if st.frontier != "" && st.frontier != st.persisted {
			n++
		}
	}
	return n
}
