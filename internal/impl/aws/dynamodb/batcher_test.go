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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// createTestMessages builds a batch of count messages with sequence numbers
// seq(startSeq) .. seq(startSeq+count-1), zero-padded so lexicographic order
// matches numeric order.
func createTestMessages(count int, shardID string, startSeq int) service.MessageBatch {
	batch := make(service.MessageBatch, count)
	for i := range count {
		msg := service.NewMessage(nil)
		msg.MetaSetMut("dynamodb_shard_id", shardID)
		msg.MetaSetMut("dynamodb_sequence_number", fmt.Sprintf("%05d", startSeq+i))
		batch[i] = msg
	}
	return batch
}

// mockCheckpointer is a mock checkpointer for testing.
type mockCheckpointer struct {
	mu              sync.Mutex
	checkpoints     map[string]string
	timestamps      map[string]string
	checkpointLimit int
	setCallCount    int
}

func (m *mockCheckpointer) Set(_ context.Context, shardID, sequenceNumber, approxCreationTime string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.checkpoints == nil {
		m.checkpoints = make(map[string]string)
	}
	if m.timestamps == nil {
		m.timestamps = make(map[string]string)
	}
	m.checkpoints[shardID] = sequenceNumber
	m.timestamps[shardID] = approxCreationTime
	m.setCallCount++
	return nil
}

func (m *mockCheckpointer) CheckpointLimit() int {
	return m.checkpointLimit
}

func (m *mockCheckpointer) get(shardID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.checkpoints[shardID]
}

func (m *mockCheckpointer) timestamp(shardID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.timestamps[shardID]
}

func (m *mockCheckpointer) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.setCallCount
}

// msgWithTime builds a single-message batch carrying both a sequence number and
// an approximate creation time, for exercising timestamp persistence.
func msgWithTime(shardID, seq, approxTime string) service.MessageBatch {
	msg := service.NewMessage(nil)
	msg.MetaSetMut("dynamodb_shard_id", shardID)
	msg.MetaSetMut("dynamodb_sequence_number", seq)
	msg.MetaSetMut("dynamodb_approximate_creation_time", approxTime)
	return service.MessageBatch{msg}
}

// Global-table mode persists the record timestamp alongside the frontier
// sequence so a failed-over region can resume by time.
func TestBatcher_PersistsApproxCreationTimeWithFrontier(t *testing.T) {
	b := NewRecordBatcher(10000, 1, service.MockResources().Logger())
	cp := &mockCheckpointer{checkpointLimit: 1}

	batch := msgWithTime("shard-001", "00001", "2026-06-16T10:00:00Z")
	b.AddMessages(batch, "shard-001")
	require.NoError(t, b.AckMessages(context.Background(), cp, batch))

	assert.Equal(t, "00001", cp.get("shard-001"))
	assert.Equal(t, "2026-06-16T10:00:00Z", cp.timestamp("shard-001"))
}

// When acks complete out of order, the persisted timestamp must be the one of
// the contiguous frontier record, not of the batch whose ack moved the
// frontier.
func TestBatcher_FrontierTimestampFollowsContiguousSequence(t *testing.T) {
	b := NewRecordBatcher(10000, 1, service.MockResources().Logger())
	cp := &mockCheckpointer{checkpointLimit: 1}

	b1 := msgWithTime("shard-001", "00001", "2026-06-16T10:00:00Z")
	b2 := msgWithTime("shard-001", "00002", "2026-06-16T10:01:00Z")
	b.AddMessages(b1, "shard-001")
	b.AddMessages(b2, "shard-001")

	// Ack the second batch first: frontier is pinned (b1 still outstanding), so
	// nothing is persisted yet.
	require.NoError(t, b.AckMessages(context.Background(), cp, b2))
	assert.Equal(t, 0, cp.calls(), "frontier pinned until the earlier batch acks")

	// Ack the first batch: frontier jumps to 00002, and the persisted timestamp
	// must be 00002's (10:01), not b1's (10:00).
	require.NoError(t, b.AckMessages(context.Background(), cp, b1))
	assert.Equal(t, "00002", cp.get("shard-001"))
	assert.Equal(t, "2026-06-16T10:01:00Z", cp.timestamp("shard-001"))
}

func TestBatcherAddMessages(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := NewRecordBatcher(10000, 1000, logger)

	// Add messages for shard-001
	batch1 := createTestMessages(5, "shard-001", 0)
	result1 := batcher.AddMessages(batch1, "shard-001")

	assert.Len(t, result1, 5)
	// pendingCount should be 0 until messages are acked
	assert.Equal(t, 0, batcher.PendingCount("shard-001"))
	assert.Equal(t, 5, batcher.TrackedMessageCount())

	// Add more messages for same shard
	batch2 := createTestMessages(3, "shard-001", 5)
	result2 := batcher.AddMessages(batch2, "shard-001")

	assert.Len(t, result2, 3)
	assert.Equal(t, 0, batcher.PendingCount("shard-001"))
	assert.Equal(t, 8, batcher.TrackedMessageCount())

	// Add messages for different shard
	batch3 := createTestMessages(4, "shard-002", 0)
	result3 := batcher.AddMessages(batch3, "shard-002")

	assert.Len(t, result3, 4)
	assert.Equal(t, 0, batcher.PendingCount("shard-001"))
	assert.Equal(t, 0, batcher.PendingCount("shard-002"))
	assert.Equal(t, 12, batcher.TrackedMessageCount())
}

func TestBatcherRemoveMessages(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := NewRecordBatcher(10000, 1000, logger)

	// Add two batches (the batch is the ack/removal unit)
	batch1 := createTestMessages(5, "shard-001", 0)
	batch2 := createTestMessages(5, "shard-001", 5)
	batcher.AddMessages(batch1, "shard-001")
	batcher.AddMessages(batch2, "shard-001")

	assert.Equal(t, 0, batcher.PendingCount("shard-001"))
	assert.Equal(t, 10, batcher.TrackedMessageCount())

	// Remove one batch (simulating nack)
	batcher.RemoveMessages(batch1)

	// pendingCount is still 0 since we never acked these messages
	assert.Equal(t, 0, batcher.PendingCount("shard-001"))
	assert.Equal(t, 5, batcher.TrackedMessageCount())

	// Remove the other batch
	batcher.RemoveMessages(batch2)

	assert.Equal(t, 0, batcher.PendingCount("shard-001"))
	assert.Equal(t, 0, batcher.TrackedMessageCount())
}

func TestBatcherAckMessagesWithCheckpointing(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := NewRecordBatcher(10000, 1000, logger)

	checkpointer := &mockCheckpointer{
		checkpointLimit: 5, // Low threshold for testing
	}

	// Dispatch three batches in stream order
	batch1 := createTestMessages(3, "shard-001", 0) // 00000..00002
	batch2 := createTestMessages(3, "shard-001", 3) // 00003..00005
	batch3 := createTestMessages(4, "shard-001", 6) // 00006..00009
	batcher.AddMessages(batch1, "shard-001")
	batcher.AddMessages(batch2, "shard-001")
	batcher.AddMessages(batch3, "shard-001")

	// Ack batch 1 - pending count 3, no checkpoint yet (< 5)
	require.NoError(t, batcher.AckMessages(t.Context(), checkpointer, batch1))
	assert.Equal(t, 3, batcher.PendingCount("shard-001"), "Should have 3 pending after acking 3")
	assert.Equal(t, 7, batcher.TrackedMessageCount())
	assert.Equal(t, 0, checkpointer.calls(), "Should not checkpoint yet (3 < 5)")

	// Ack batch 2 - pending count reaches 6 (>= 5), should checkpoint at the
	// contiguous frontier: batch 2's last sequence.
	require.NoError(t, batcher.AckMessages(t.Context(), checkpointer, batch2))
	assert.Equal(t, 0, batcher.PendingCount("shard-001"), "Should reset to 0 after checkpoint")
	assert.Equal(t, 4, batcher.TrackedMessageCount())
	assert.Equal(t, 1, checkpointer.calls(), "Should checkpoint once (6 >= 5)")
	assert.Equal(t, "00005", checkpointer.get("shard-001"))
}

// TestBatcherOutOfOrderAckDoesNotSkipUnacked is the regression test for the
// checkpoint-ordering data-loss bug: when a later batch acks before an
// earlier one, the checkpoint must NOT advance past the unacked batch. A
// crash after such a premature checkpoint would permanently skip the earlier
// batch's records on restart.
func TestBatcherOutOfOrderAckDoesNotSkipUnacked(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := NewRecordBatcher(10000, 1000, logger)

	checkpointer := &mockCheckpointer{checkpointLimit: 2}

	batchOld := createTestMessages(2, "shard-001", 100) // dispatched first
	batchNew := createTestMessages(2, "shard-001", 200) // dispatched second
	batcher.AddMessages(batchOld, "shard-001")
	batcher.AddMessages(batchNew, "shard-001")

	// The NEWER batch acks first (out of order). Enough messages are acked
	// to hit the checkpoint limit, but the frontier is pinned before the
	// unacked older batch, so nothing may be persisted.
	require.NoError(t, batcher.AckMessages(t.Context(), checkpointer, batchNew))
	assert.Equal(t, 0, checkpointer.calls(), "checkpoint must not advance past the unacked older batch")
	assert.Empty(t, batcher.LastCheckpoint("shard-001"))

	// Once the older batch acks, the frontier jumps to the newest contiguous
	// sequence and the accumulated acks persist.
	require.NoError(t, batcher.AckMessages(t.Context(), checkpointer, batchOld))
	assert.Equal(t, 1, checkpointer.calls())
	assert.Equal(t, "00201", checkpointer.get("shard-001"), "checkpoint should cover both batches once contiguous")
}

// TestBatcherNackPinsCheckpointFrontier verifies that a nacked batch pins the
// shard's checkpoint before it: later acks accumulate but are never persisted
// past the gap, so a restart redelivers the nacked records.
func TestBatcherNackPinsCheckpointFrontier(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := NewRecordBatcher(10000, 1000, logger)

	checkpointer := &mockCheckpointer{checkpointLimit: 1}

	batch1 := createTestMessages(2, "shard-001", 0)
	batch2 := createTestMessages(2, "shard-001", 2)
	batch3 := createTestMessages(2, "shard-001", 4)
	batcher.AddMessages(batch1, "shard-001")
	batcher.AddMessages(batch2, "shard-001")
	batcher.AddMessages(batch3, "shard-001")

	// Batch 1 acks and persists.
	require.NoError(t, batcher.AckMessages(t.Context(), checkpointer, batch1))
	assert.Equal(t, "00001", checkpointer.get("shard-001"))

	// Batch 2 is nacked; batch 3 acks afterwards. The checkpoint must stay
	// at batch 1's frontier.
	batcher.RemoveMessages(batch2)
	require.NoError(t, batcher.AckMessages(t.Context(), checkpointer, batch3))
	assert.Equal(t, "00001", checkpointer.get("shard-001"), "nacked batch must pin the checkpoint")
	assert.Empty(t, batcher.PendingCheckpoints(), "no unpersisted frontier may exist past the nacked batch")
}

func TestBatcherAckMessagesMultipleShards(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := NewRecordBatcher(10000, 1000, logger)

	// Add messages for multiple shards
	batch1 := createTestMessages(6, "shard-001", 0)
	batch2 := createTestMessages(6, "shard-002", 0)

	batcher.AddMessages(batch1, "shard-001")
	batcher.AddMessages(batch2, "shard-002")

	checkpointer := &mockCheckpointer{
		checkpointLimit: 100, // High limit so we don't checkpoint
	}

	// Ack messages from both shards
	require.NoError(t, batcher.AckMessages(t.Context(), checkpointer, batch1))
	require.NoError(t, batcher.AckMessages(t.Context(), checkpointer, batch2))

	assert.Equal(t, 6, batcher.PendingCount("shard-001"))
	assert.Equal(t, 6, batcher.PendingCount("shard-002"))

	// Each shard's frontier advances independently.
	assert.Equal(t, "00005", batcher.LastCheckpoint("shard-001"))
	assert.Equal(t, "00005", batcher.LastCheckpoint("shard-002"))
}

// Regression test: Verify pending count increments on ack.
func TestBatcherPendingCountIncrementsOnAck(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := NewRecordBatcher(10000, 1000, logger)

	checkpointer := &mockCheckpointer{
		checkpointLimit: 100, // High limit so we don't checkpoint
	}

	// Add 10 messages
	batch := createTestMessages(10, "shard-001", 0)
	batcher.AddMessages(batch, "shard-001")
	assert.Equal(t, 0, batcher.PendingCount("shard-001"), "Should be 0 before ack")

	// Ack messages - pending count should increment
	require.NoError(t, batcher.AckMessages(t.Context(), checkpointer, batch))

	// Pending count should be 10 after acking 10 messages
	assert.Equal(t, 10, batcher.PendingCount("shard-001"))
}

// TestBatcherPendingCheckpointsFlush verifies that the shutdown flush only
// surfaces frontiers that have not been persisted yet.
func TestBatcherPendingCheckpointsFlush(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := NewRecordBatcher(10000, 1000, logger)

	// High limit: acks accumulate without persisting.
	checkpointer := &mockCheckpointer{checkpointLimit: 100}

	batch1 := createTestMessages(3, "shard-001", 0)
	batch2 := createTestMessages(3, "shard-002", 0)
	batcher.AddMessages(batch1, "shard-001")
	batcher.AddMessages(batch2, "shard-002")
	require.NoError(t, batcher.AckMessages(t.Context(), checkpointer, batch1))
	require.NoError(t, batcher.AckMessages(t.Context(), checkpointer, batch2))

	pending := batcher.PendingCheckpoints()
	assert.Equal(t, map[string]CheckpointValue{
		"shard-001": {SequenceNumber: "00002"},
		"shard-002": {SequenceNumber: "00002"},
	}, pending)

	// Low limit: the next ack persists immediately, leaving nothing pending
	// for that shard.
	lowLimit := &mockCheckpointer{checkpointLimit: 1}
	batch3 := createTestMessages(2, "shard-001", 3)
	batcher.AddMessages(batch3, "shard-001")
	require.NoError(t, batcher.AckMessages(t.Context(), lowLimit, batch3))
	assert.Equal(t, "00004", lowLimit.get("shard-001"))

	pending = batcher.PendingCheckpoints()
	assert.Equal(t, map[string]CheckpointValue{"shard-002": {SequenceNumber: "00002"}}, pending)
}

// Test concurrent access to batcher.
func TestBatcherConcurrentAccess(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := NewRecordBatcher(10000, 1000, logger)

	// Add messages concurrently
	done := make(chan bool, 2)

	go func() {
		for i := range 10 {
			batch := createTestMessages(5, "shard-001", i*5)
			batcher.AddMessages(batch, "shard-001")
			batcher.RemoveMessages(batch)
		}
		done <- true
	}()

	go func() {
		for i := range 10 {
			batch := createTestMessages(5, "shard-002", i*5)
			batcher.AddMessages(batch, "shard-002")
			batcher.RemoveMessages(batch)
		}
		done <- true
	}()

	<-done
	<-done

	// Verify no race conditions - all messages should be processed
	assert.Equal(t, 0, batcher.TrackedMessageCount(), "All messages should be removed")
}

func TestBatcherNackAndReAdd(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := NewRecordBatcher(10000, 1000, logger)

	// Add messages
	batch := createTestMessages(5, "shard-001", 0)
	batcher.AddMessages(batch, "shard-001")

	// pendingCount should be 0 until ack
	assert.Equal(t, 0, batcher.PendingCount("shard-001"))

	// Simulate nack by removing messages
	batcher.RemoveMessages(batch)

	assert.Equal(t, 0, batcher.PendingCount("shard-001"))
	assert.Equal(t, 0, batcher.TrackedMessageCount())

	// Re-add the same logical messages (new message objects)
	newBatch := createTestMessages(5, "shard-001", 0)
	batcher.AddMessages(newBatch, "shard-001")

	// Still 0 until ack
	assert.Equal(t, 0, batcher.PendingCount("shard-001"))
	assert.Equal(t, 5, batcher.TrackedMessageCount())
}

// Test that max tracked shards limit is enforced.
func TestBatcherMaxTrackedShardsLimit(t *testing.T) {
	logger := service.MockResources().Logger()
	// Create batcher with small limit for testing
	batcher := NewRecordBatcher(5, 1, logger)

	checkpointer := &mockCheckpointer{checkpointLimit: 100}

	// Add and ack messages for 5 shards (at the limit)
	for i := range 5 {
		shardID := fmt.Sprintf("shard-%03d", i)
		batch := createTestMessages(2, shardID, 0)
		batcher.AddMessages(batch, shardID)
		require.NoError(t, batcher.AckMessages(t.Context(), checkpointer, batch))
	}

	// Verify we're tracking exactly 5 unpersisted shard frontiers
	assert.Equal(t, 5, batcher.LastCheckpointsCount())

	// Now try to add and ack a 6th shard (should exceed limit)
	batch := createTestMessages(2, "shard-006", 0)
	batcher.AddMessages(batch, "shard-006")

	err := batcher.AckMessages(t.Context(), checkpointer, batch)
	assert.Error(t, err, "Should fail when exceeding max tracked shards")
	assert.Contains(t, err.Error(), "exceeded maximum size")
	assert.Contains(t, err.Error(), "5 shards")
}

// Test that ShouldThrottle works correctly.
func TestBatcherShouldThrottle(t *testing.T) {
	logger := service.MockResources().Logger()
	// Create batcher with small limit for testing (checkpointLimit=10 -> maxTrackedMessages=1000)
	batcher := NewRecordBatcher(100, 10, logger)

	// Initially should not throttle
	assert.False(t, batcher.ShouldThrottle(), "Should not throttle when empty")

	// Add messages up to 80% capacity (should not throttle)
	for i := range 800 {
		batch := createTestMessages(1, "shard-001", i)
		batcher.AddMessages(batch, "shard-001")
	}
	assert.False(t, batcher.ShouldThrottle(), "Should not throttle at 80% capacity")

	// Add more to reach 90% capacity (should throttle)
	for i := 800; i < 900; i++ {
		batch := createTestMessages(1, "shard-001", i)
		batcher.AddMessages(batch, "shard-001")
	}
	assert.True(t, batcher.ShouldThrottle(), "Should throttle at 90% capacity")

	// Add even more to exceed 90%
	for i := 900; i < 950; i++ {
		batch := createTestMessages(1, "shard-001", i)
		batcher.AddMessages(batch, "shard-001")
	}
	assert.True(t, batcher.ShouldThrottle(), "Should still throttle above 90% capacity")
}
