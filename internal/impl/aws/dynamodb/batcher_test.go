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
	"sync/atomic"
	"testing"
	"time"

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
	tbBatch := b.AddMessages(batch, "shard-001")
	require.NoError(t, b.AckBatch(context.Background(), cp, tbBatch))

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
	tbB1 := b.AddMessages(b1, "shard-001")
	tbB2 := b.AddMessages(b2, "shard-001")

	// Ack the second batch first: frontier is pinned (b1 still outstanding), so
	// nothing is persisted yet.
	require.NoError(t, b.AckBatch(context.Background(), cp, tbB2))
	assert.Equal(t, 0, cp.calls(), "frontier pinned until the earlier batch acks")

	// Ack the first batch: frontier jumps to 00002, and the persisted timestamp
	// must be 00002's (10:01), not b1's (10:00).
	require.NoError(t, b.AckBatch(context.Background(), cp, tbB1))
	assert.Equal(t, "00002", cp.get("shard-001"))
	assert.Equal(t, "2026-06-16T10:01:00Z", cp.timestamp("shard-001"))
}

func TestBatcherAddMessages(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := NewRecordBatcher(10000, 1000, logger)

	// Add messages for shard-001
	batch1 := createTestMessages(5, "shard-001", 0)
	result1 := batcher.AddMessages(batch1, "shard-001")

	assert.Equal(t, 5, result1.size)
	// pendingCount should be 0 until messages are acked
	assert.Equal(t, 0, batcher.PendingCount("shard-001"))
	assert.Equal(t, 5, batcher.TrackedMessageCount())

	// Add more messages for same shard
	batch2 := createTestMessages(3, "shard-001", 5)
	result2 := batcher.AddMessages(batch2, "shard-001")

	assert.Equal(t, 3, result2.size)
	assert.Equal(t, 0, batcher.PendingCount("shard-001"))
	assert.Equal(t, 8, batcher.TrackedMessageCount())

	// Add messages for different shard
	batch3 := createTestMessages(4, "shard-002", 0)
	result3 := batcher.AddMessages(batch3, "shard-002")

	assert.Equal(t, 4, result3.size)
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
	tbBatch1 := batcher.AddMessages(batch1, "shard-001")
	tbBatch2 := batcher.AddMessages(batch2, "shard-001")

	assert.Equal(t, 0, batcher.PendingCount("shard-001"))
	assert.Equal(t, 10, batcher.TrackedMessageCount())

	// Remove one batch (simulating nack)
	batcher.RemoveBatch(tbBatch1)

	// pendingCount is still 0 since we never acked these messages
	assert.Equal(t, 0, batcher.PendingCount("shard-001"))
	assert.Equal(t, 5, batcher.TrackedMessageCount())

	// Remove the other batch
	batcher.RemoveBatch(tbBatch2)

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
	tbBatch1 := batcher.AddMessages(batch1, "shard-001")
	tbBatch2 := batcher.AddMessages(batch2, "shard-001")
	batcher.AddMessages(batch3, "shard-001")

	// Ack batch 1 - pending count 3, no checkpoint yet (< 5)
	require.NoError(t, batcher.AckBatch(t.Context(), checkpointer, tbBatch1))
	assert.Equal(t, 3, batcher.PendingCount("shard-001"), "Should have 3 pending after acking 3")
	assert.Equal(t, 7, batcher.TrackedMessageCount())
	assert.Equal(t, 0, checkpointer.calls(), "Should not checkpoint yet (3 < 5)")

	// Ack batch 2 - pending count reaches 6 (>= 5), should checkpoint at the
	// contiguous frontier: batch 2's last sequence.
	require.NoError(t, batcher.AckBatch(t.Context(), checkpointer, tbBatch2))
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
	tbBatchold := batcher.AddMessages(batchOld, "shard-001")
	tbBatchnew := batcher.AddMessages(batchNew, "shard-001")

	// The NEWER batch acks first (out of order). Enough messages are acked
	// to hit the checkpoint limit, but the frontier is pinned before the
	// unacked older batch, so nothing may be persisted.
	require.NoError(t, batcher.AckBatch(t.Context(), checkpointer, tbBatchnew))
	assert.Equal(t, 0, checkpointer.calls(), "checkpoint must not advance past the unacked older batch")
	assert.Empty(t, batcher.LastCheckpoint("shard-001"))

	// Once the older batch acks, the frontier jumps to the newest contiguous
	// sequence and the accumulated acks persist.
	require.NoError(t, batcher.AckBatch(t.Context(), checkpointer, tbBatchold))
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
	tbBatch1 := batcher.AddMessages(batch1, "shard-001")
	tbBatch2 := batcher.AddMessages(batch2, "shard-001")
	tbBatch3 := batcher.AddMessages(batch3, "shard-001")

	// Batch 1 acks and persists.
	require.NoError(t, batcher.AckBatch(t.Context(), checkpointer, tbBatch1))
	assert.Equal(t, "00001", checkpointer.get("shard-001"))

	// Batch 2 is nacked; batch 3 acks afterwards. The checkpoint must stay
	// at batch 1's frontier.
	batcher.RemoveBatch(tbBatch2)
	require.NoError(t, batcher.AckBatch(t.Context(), checkpointer, tbBatch3))
	assert.Equal(t, "00001", checkpointer.get("shard-001"), "nacked batch must pin the checkpoint")
	assert.Empty(t, batcher.PendingCheckpoints(), "no unpersisted frontier may exist past the nacked batch")
}

func TestBatcherAckMessagesMultipleShards(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := NewRecordBatcher(10000, 1000, logger)

	// Add messages for multiple shards
	batch1 := createTestMessages(6, "shard-001", 0)
	batch2 := createTestMessages(6, "shard-002", 0)

	tbBatch1 := batcher.AddMessages(batch1, "shard-001")
	tbBatch2 := batcher.AddMessages(batch2, "shard-002")

	checkpointer := &mockCheckpointer{
		checkpointLimit: 100, // High limit so we don't checkpoint
	}

	// Ack messages from both shards
	require.NoError(t, batcher.AckBatch(t.Context(), checkpointer, tbBatch1))
	require.NoError(t, batcher.AckBatch(t.Context(), checkpointer, tbBatch2))

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
	tbBatch := batcher.AddMessages(batch, "shard-001")
	assert.Equal(t, 0, batcher.PendingCount("shard-001"), "Should be 0 before ack")

	// Ack messages - pending count should increment
	require.NoError(t, batcher.AckBatch(t.Context(), checkpointer, tbBatch))

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
	tbBatch1 := batcher.AddMessages(batch1, "shard-001")
	tbBatch2 := batcher.AddMessages(batch2, "shard-002")
	require.NoError(t, batcher.AckBatch(t.Context(), checkpointer, tbBatch1))
	require.NoError(t, batcher.AckBatch(t.Context(), checkpointer, tbBatch2))

	pending := batcher.PendingCheckpoints()
	assert.Equal(t, map[string]CheckpointValue{
		"shard-001": {SequenceNumber: "00002"},
		"shard-002": {SequenceNumber: "00002"},
	}, pending)

	// Low limit: the next ack persists immediately, leaving nothing pending
	// for that shard.
	lowLimit := &mockCheckpointer{checkpointLimit: 1}
	batch3 := createTestMessages(2, "shard-001", 3)
	tbBatch3 := batcher.AddMessages(batch3, "shard-001")
	require.NoError(t, batcher.AckBatch(t.Context(), lowLimit, tbBatch3))
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
			tbBatch := batcher.AddMessages(batch, "shard-001")
			batcher.RemoveBatch(tbBatch)
		}
		done <- true
	}()

	go func() {
		for i := range 10 {
			batch := createTestMessages(5, "shard-002", i*5)
			tbBatch := batcher.AddMessages(batch, "shard-002")
			batcher.RemoveBatch(tbBatch)
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
	tbBatch := batcher.AddMessages(batch, "shard-001")

	// pendingCount should be 0 until ack
	assert.Equal(t, 0, batcher.PendingCount("shard-001"))

	// Simulate nack by removing messages
	batcher.RemoveBatch(tbBatch)

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
		tbBatch := batcher.AddMessages(batch, shardID)
		require.NoError(t, batcher.AckBatch(t.Context(), checkpointer, tbBatch))
	}

	// Verify we're tracking exactly 5 unpersisted shard frontiers
	assert.Equal(t, 5, batcher.LastCheckpointsCount())

	// Now try to add and ack a 6th shard (should exceed limit)
	batch := createTestMessages(2, "shard-006", 0)
	tbBatch := batcher.AddMessages(batch, "shard-006")

	err := batcher.AckBatch(t.Context(), checkpointer, tbBatch)
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

// blockingCheckpointer blocks Set calls for one designated shard until
// released, signalling entry exactly once. Other shards persist normally.
type blockingCheckpointer struct {
	mockCheckpointer
	blockShard  string
	entered     chan struct{}
	enteredOnce sync.Once
	release     chan struct{}
}

func (b *blockingCheckpointer) Set(ctx context.Context, shardID, sequenceNumber, approxCreationTime string) error {
	if shardID == b.blockShard {
		b.enteredOnce.Do(func() { close(b.entered) })
		<-b.release
	}
	return b.mockCheckpointer.Set(ctx, shardID, sequenceNumber, approxCreationTime)
}

// TestBatcherCheckpointWriteDoesNotBlockOtherShards: a slow or hung
// checkpoint write for one shard must not stall the whole input. While one
// shard's persist is in flight, ShouldThrottle (polled by every shard
// reader), AddMessages, and other shards' acks must all proceed. This is the
// silent global-freeze hazard behind INC-2974's prod pipeline: holding the
// batcher mutex across the PutItem parks every shard reader with no logs.
func TestBatcherCheckpointWriteDoesNotBlockOtherShards(t *testing.T) {
	batcher := NewRecordBatcher(100, 1, service.MockResources().Logger())
	cp := &blockingCheckpointer{
		mockCheckpointer: mockCheckpointer{checkpointLimit: 1},
		blockShard:       "shard-001",
		entered:          make(chan struct{}),
		release:          make(chan struct{}),
	}

	batch1 := createTestMessages(3, "shard-001", 1)
	tbBatch1 := batcher.AddMessages(batch1, "shard-001")

	ackErr := make(chan error, 1)
	go func() { ackErr <- batcher.AckBatch(t.Context(), cp, tbBatch1) }()

	select {
	case <-cp.entered:
		// shard-001's checkpoint write is now in flight and blocked
	case <-time.After(2 * time.Second):
		t.Fatal("expected the ack to reach the checkpoint write")
	}

	assertPrompt := func(name string, fn func()) {
		t.Helper()
		done := make(chan struct{})
		go func() { fn(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s blocked behind an in-flight checkpoint write for another shard", name)
		}
	}

	assertPrompt("ShouldThrottle", func() { batcher.ShouldThrottle() })
	batch2 := createTestMessages(3, "shard-002", 1)
	var tbBatch2 *trackedBatch
	assertPrompt("AddMessages", func() { tbBatch2 = batcher.AddMessages(batch2, "shard-002") })
	// Errors are captured and asserted from the test goroutine: require's
	// FailNow inside assertPrompt's spawned goroutine would Goexit before
	// close(done) and misreport the failure as a blocked call.
	var otherShardErr error
	assertPrompt("AckMessages(other shard)", func() {
		otherShardErr = batcher.AckBatch(t.Context(), cp, tbBatch2)
	})
	require.NoError(t, otherShardErr)
	assert.Equal(t, "00003", cp.get("shard-002"),
		"another shard's checkpoint must persist while shard-001's write is blocked")

	// Even the SAME shard's acks settle without queueing behind the write:
	// they skip the persist (single-flighted) and the in-flight writer's
	// post-write re-check picks their progress up.
	batch3 := createTestMessages(3, "shard-001", 4)
	tbBatch3 := batcher.AddMessages(batch3, "shard-001")
	var sameShardErr error
	assertPrompt("AckMessages(same shard)", func() {
		sameShardErr = batcher.AckBatch(t.Context(), cp, tbBatch3)
	})
	require.NoError(t, sameShardErr)

	close(cp.release)
	require.NoError(t, <-ackErr)
	assert.Equal(t, "00006", cp.get("shard-001"),
		"the writer's post-write re-check must persist progress settled during the write")
	assert.Equal(t, 0, batcher.TrackedMessageCount())
}

// TestBatcherAckOverShardCapStillDrainsTracker: the tracked-shards safety
// guard must still settle the acked batch AND persist its checkpoint.
// Returning the guard error without dropping the batch leaks tracked
// messages forever (permanently pinning ShouldThrottle once enough acks have
// been swallowed), and skipping the persist would silently freeze durable
// checkpoints pipeline-wide for as long as the guard trips.
func TestBatcherAckOverShardCapStillDrainsTracker(t *testing.T) {
	batcher := NewRecordBatcher(1, 1, service.MockResources().Logger())
	cp := &mockCheckpointer{checkpointLimit: 1}

	batch1 := createTestMessages(2, "shard-001", 1)
	tbBatch1 := batcher.AddMessages(batch1, "shard-001")
	batch2 := createTestMessages(2, "shard-002", 1)
	tbBatch2 := batcher.AddMessages(batch2, "shard-002")

	require.Error(t, batcher.AckBatch(t.Context(), cp, tbBatch1),
		"the shard-cap guard should still surface an error")
	require.Error(t, batcher.AckBatch(t.Context(), cp, tbBatch2))
	assert.Equal(t, 0, batcher.TrackedMessageCount(),
		"acked batches must drain the tracker even when the shard-cap guard trips")
	assert.Equal(t, "00002", cp.get("shard-001"),
		"checkpoints must keep persisting even when the shard-cap guard trips")
	assert.Equal(t, "00002", cp.get("shard-002"))
}

// TestBatcherWarnsWhenThrottlePinned: a throttle that never releases is an
// otherwise-silent stall, so ShouldThrottle must surface it once the tracker
// has been pinned past the warn threshold, and dropping below the threshold
// must reset the pin clock.
func TestBatcherWarnsWhenThrottlePinned(t *testing.T) {
	batcher := NewRecordBatcher(100, 100, service.MockResources().Logger())

	// Two shards fill the global budget with unsettled messages (each within
	// its 250 per-shard cap would block first, so use larger holdings via the
	// single-batch admission rule).
	batchA := createTestMessages(500, "shard-001", 0)
	tbBatcha := batcher.AddMessages(batchA, "shard-001")
	batchB := createTestMessages(450, "shard-002", 0)
	tbBatchb := batcher.AddMessages(batchB, "shard-002")

	require.False(t, batcher.TryReserve("shard-003", 100),
		"the global budget is exhausted")

	// Backdate the pin start beyond the warn threshold.
	batcher.mu.Lock()
	batcher.throttledSince = time.Now().Add(-2 * throttlePinWarnAfter)
	batcher.mu.Unlock()

	require.False(t, batcher.TryReserve("shard-003", 100))
	batcher.mu.Lock()
	warned := !batcher.lastPinWarn.IsZero()
	batcher.mu.Unlock()
	assert.True(t, warned, "a continuously pinned budget must emit the stall warning")

	// Draining resets the pin clock on the next successful reservation.
	batcher.RemoveBatch(tbBatcha)
	batcher.RemoveBatch(tbBatchb)
	require.True(t, batcher.TryReserve("shard-003", 100))
	batcher.mu.Lock()
	reset := batcher.throttledSince.IsZero()
	batcher.mu.Unlock()
	assert.True(t, reset, "a successful reservation must reset the pin clock")
}

// TestBatcherWarnsWhenShardPinned: a shard parked at its per-shard cap never
// reaches the global-budget branch (with fewer than four pinned shards the
// global budget stays under 100%), so the per-shard refusal must drive its own
// pin clock and warning or a single-shard wedge is completely silent.
func TestBatcherWarnsWhenShardPinned(t *testing.T) {
	// checkpointLimit 400 -> maxTrackedMessages 4000 -> per-shard cap 1000
	batcher := NewRecordBatcher(100, 400, service.MockResources().Logger())

	require.True(t, batcher.TryReserve("shard-001", 1000))
	tb := batcher.AddMessages(createTestMessages(1000, "shard-001", 1), "shard-001")

	// A second shard is active, so the isolation cap engages for shard-001.
	require.True(t, batcher.TryReserve("shard-002", 100))
	batcher.AddMessages(createTestMessages(100, "shard-002", 1), "shard-002")

	require.False(t, batcher.TryReserve("shard-001", 100),
		"the shard is at its in-flight cap")

	// Backdate the shard's pin start beyond the warn threshold.
	batcher.mu.Lock()
	st := batcher.shards["shard-001"]
	st.throttledSince = time.Now().Add(-2 * throttlePinWarnAfter)
	batcher.mu.Unlock()

	require.False(t, batcher.TryReserve("shard-001", 100))
	batcher.mu.Lock()
	warned := !st.lastPinWarn.IsZero()
	globalUntouched := batcher.throttledSince.IsZero()
	batcher.mu.Unlock()
	assert.True(t, warned, "a continuously pinned shard must emit the stall warning")
	assert.True(t, globalUntouched, "a per-shard refusal must not start the global pin clock")

	// Settling the batch drains the shard; the next successful reservation
	// resets its pin clock.
	batcher.RemoveBatch(tb)
	require.True(t, batcher.TryReserve("shard-001", 100))
	batcher.mu.Lock()
	reset := st.throttledSince.IsZero()
	batcher.mu.Unlock()
	assert.True(t, reset, "a successful reservation must reset the shard's pin clock")
}

// TestBatcherPerShardRefusalClearsGlobalClock: a reservation that passes the
// global check proves the global budget is NOT pinned, even when the shard's
// own cap then refuses it. The per-shard refusal must clear the global pin
// clock, or a later global exhaustion reports a pin duration spanning the
// interval in which the budget had actually drained.
func TestBatcherPerShardRefusalClearsGlobalClock(t *testing.T) {
	// checkpointLimit 400 -> maxTrackedMessages 4000 -> per-shard cap 1000
	batcher := NewRecordBatcher(100, 400, service.MockResources().Logger())

	// Two capped shards plus two more batches exhaust the global budget.
	tbs := make([]*trackedBatch, 0, 4)
	for i, shard := range []string{"shard-001", "shard-002", "shard-003", "shard-004"} {
		require.True(t, batcher.TryReserve(shard, 1000), "batch %d", i)
		tbs = append(tbs, batcher.AddMessages(createTestMessages(1000, shard, 1), shard))
	}
	require.False(t, batcher.TryReserve("shard-005", 100),
		"the global budget is exhausted")
	batcher.mu.Lock()
	pinned := !batcher.throttledSince.IsZero()
	batcher.mu.Unlock()
	require.True(t, pinned)

	// Drain one batch: the global budget has room again, but shard-001 is
	// still at its own cap, so its reservation exits via the per-shard branch.
	batcher.RemoveBatch(tbs[3])
	require.False(t, batcher.TryReserve("shard-001", 1000),
		"shard-001 is still at its per-shard cap")
	batcher.mu.Lock()
	cleared := batcher.throttledSince.IsZero()
	batcher.mu.Unlock()
	assert.True(t, cleared,
		"passing the global check must clear the global pin clock even when the per-shard cap refuses")
}

// TestBatcherEmptyTrackerAdmitsOversizedBatch: the global check must admit
// one batch when nothing is tracked or reserved (the same idle escape the
// per-shard cap has), or a reservation larger than the derived budget would
// be refused forever on a completely empty batcher and the input would hang
// from startup without ever issuing a read. Config validation bounds
// batch_size to 1000 (the budget's floor), so this is defense in depth.
func TestBatcherEmptyTrackerAdmitsOversizedBatch(t *testing.T) {
	// checkpointLimit 25 -> maxTrackedMessages 1000
	batcher := NewRecordBatcher(100, 25, service.MockResources().Logger())

	require.True(t, batcher.TryReserve("shard-001", 2000),
		"an empty batcher must admit one batch even above the global budget")
	assert.False(t, batcher.TryReserve("shard-002", 100),
		"the global budget binds again while the oversized batch is outstanding")
	batcher.Release("shard-001", 2000)
	assert.True(t, batcher.TryReserve("shard-002", 100))
}

// TestBatcherIdleReservationsDoNotMaterializeShardState: reserve/release
// cycles from readers polling shards that never yield records must not
// populate the shard-tracker map - entries there are never removed and count
// against the maxTrackedShards guard, which poisons every ack once exceeded.
// Only AddMessages (a shard actually producing records) may create state.
func TestBatcherIdleReservationsDoNotMaterializeShardState(t *testing.T) {
	batcher := NewRecordBatcher(100, 400, service.MockResources().Logger())

	for i := range 200 {
		shard := fmt.Sprintf("shard-%03d", i)
		require.True(t, batcher.TryReserve(shard, 100))
		batcher.Release(shard, 100)
	}

	batcher.mu.Lock()
	tracked := len(batcher.shards)
	reservedEntries := len(batcher.reservedByShard)
	batcher.mu.Unlock()
	assert.Zero(t, tracked,
		"idle reserve/release cycles must not create shard-tracker entries")
	assert.Zero(t, reservedEntries,
		"fully released reservations must be pruned")
}

// --- Reservation-based admission control (INC-2974 ack-path stall) ---

// TestBatcherReserveBoundsGlobalBudget: readers must reserve tracker budget
// BEFORE reading, so concurrent first reads can never overshoot the global
// cap (the ~300k first-wave overshoot seen on the wedged pipelines).
func TestBatcherReserveBoundsGlobalBudget(t *testing.T) {
	// checkpointLimit 100 -> maxTrackedMessages 1000
	batcher := NewRecordBatcher(100, 100, service.MockResources().Logger())

	require.True(t, batcher.TryReserve("shard-001", 600))
	assert.False(t, batcher.TryReserve("shard-002", 600),
		"a reservation over the remaining global budget must be refused")
	batcher.Release("shard-001", 600)
	assert.True(t, batcher.TryReserve("shard-002", 600),
		"released budget must become available again")
}

// TestBatcherReserveConsumedByAddMessages: AddMessages converts the shard's
// outstanding reservation into tracked messages; the unused remainder must be
// explicitly released by the reader.
func TestBatcherReserveConsumedByAddMessages(t *testing.T) {
	batcher := NewRecordBatcher(100, 100, service.MockResources().Logger())

	require.True(t, batcher.TryReserve("shard-001", 600))
	batcher.AddMessages(createTestMessages(400, "shard-001", 1), "shard-001")
	assert.Equal(t, 400, batcher.TrackedMessageCount())

	assert.False(t, batcher.TryReserve("shard-002", 500),
		"tracked (400) + outstanding reservation (200) + 500 exceeds the 1000 budget")
	batcher.Release("shard-001", 200)
	assert.True(t, batcher.TryReserve("shard-002", 500))
}

// TestBatcherPerShardCapIsolatesPinnedShard: a shard whose batches never
// settle downstream must park alone at its per-shard cap; other shards keep
// reserving and reading. This is the isolation fix for the ack-path stall:
// before it, one never-settling shard's messages pinned the global tracker
// and parked every reader in the input.
func TestBatcherPerShardCapIsolatesPinnedShard(t *testing.T) {
	// checkpointLimit 400 -> maxTrackedMessages 4000 -> per-shard cap 1000
	batcher := NewRecordBatcher(100, 400, service.MockResources().Logger())

	// Poison shard: fill to its per-shard cap with unsettled messages.
	require.True(t, batcher.TryReserve("shard-poison", 1000))
	poison := createTestMessages(1000, "shard-poison", 1)
	tbPoison := batcher.AddMessages(poison, "shard-poison")

	// A healthy shard is also active, so the isolation cap engages.
	require.True(t, batcher.TryReserve("shard-healthy", 100))
	batcher.AddMessages(createTestMessages(100, "shard-healthy", 1), "shard-healthy")

	assert.False(t, batcher.TryReserve("shard-poison", 100),
		"a shard at its in-flight cap must not reserve more")
	assert.True(t, batcher.TryReserve("shard-healthy", 100),
		"other shards must keep flowing while one shard is pinned")

	// Settling the poison batch frees the shard again.
	batcher.RemoveBatch(tbPoison)
	assert.True(t, batcher.TryReserve("shard-poison", 100))
}

// TestBatcherSoleActiveShardUsesGlobalBudget: the per-shard cap exists purely
// to isolate a never-settling shard from OTHER shards, so it must not bind
// while no other shard is active - a one-partition table has a single active
// shard, and capping it would strand three quarters of the configured budget
// (the pre-cap gate allowed ~90% of it). The cap re-engages as soon as a
// second shard becomes active.
func TestBatcherSoleActiveShardUsesGlobalBudget(t *testing.T) {
	// checkpointLimit 400 -> maxTrackedMessages 4000 -> per-shard cap 1000
	batcher := NewRecordBatcher(100, 400, service.MockResources().Logger())

	// A sole active shard may fill the whole global budget, well past its cap.
	tbs := make([]*trackedBatch, 0, 4)
	for range 4 {
		require.True(t, batcher.TryReserve("shard-001", 1000),
			"a sole active shard must not be bound by the per-shard cap")
		tbs = append(tbs, batcher.AddMessages(createTestMessages(1000, "shard-001", 1), "shard-001"))
	}
	assert.False(t, batcher.TryReserve("shard-001", 1000),
		"the global budget still binds a sole active shard")

	// Drain below the global budget but keep the shard above its cap, then
	// activate a second shard: the isolation cap must re-engage.
	batcher.RemoveBatch(tbs[0])
	batcher.RemoveBatch(tbs[1])
	require.True(t, batcher.TryReserve("shard-002", 100))
	batcher.AddMessages(createTestMessages(100, "shard-002", 1), "shard-002")
	assert.False(t, batcher.TryReserve("shard-001", 1000),
		"the per-shard cap must re-engage once another shard is active")
}

// TestBatcherReserveAlwaysAdmitsFirstBatch: when the derived per-shard cap is
// smaller than a single read (small checkpoint_limit with the max batch
// size), a shard with nothing in flight must still be allowed one batch, or
// its reader could never make progress at all.
func TestBatcherReserveAlwaysAdmitsFirstBatch(t *testing.T) {
	// checkpointLimit 25 -> maxTrackedMessages 1000 -> per-shard cap 250
	batcher := NewRecordBatcher(100, 25, service.MockResources().Logger())

	require.True(t, batcher.TryReserve("shard-001", 500),
		"an idle shard must always admit a single batch larger than its cap")
	batcher.AddMessages(createTestMessages(500, "shard-001", 1), "shard-001")

	// Activate a second shard so the isolation cap engages.
	require.True(t, batcher.TryReserve("shard-002", 100))
	batcher.AddMessages(createTestMessages(100, "shard-002", 1), "shard-002")

	assert.False(t, batcher.TryReserve("shard-001", 1),
		"beyond the first in-flight batch the cap applies")
}

// TestBatcherConcurrentReservesNeverOvershoot: N readers racing to reserve
// must collectively never exceed the global budget.
func TestBatcherConcurrentReservesNeverOvershoot(t *testing.T) {
	// checkpointLimit 100 -> maxTrackedMessages 1000
	batcher := NewRecordBatcher(1000, 100, service.MockResources().Logger())

	var granted atomic.Int64
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if batcher.TryReserve(fmt.Sprintf("shard-%03d", i), 100) {
				granted.Add(1)
			}
		}(i)
	}
	wg.Wait()
	assert.LessOrEqual(t, granted.Load(), int64(10),
		"50 concurrent 100-message reservations must never exceed the 1000 budget")
	assert.Positive(t, granted.Load())
}

// --- Ack settlement must survive the auto_replay_nacks wrapper (INC-2974) ---
//
// benthos's AutoRetryNacksBatched read hook REPLACES every element of the
// batch slice returned by ReadBatch with a new *Message (tagging parts for
// retry tracking) before the pipeline sees it. Any ack bookkeeping keyed by
// the original message pointers silently no-ops afterwards: the ack "hits"
// nothing, tracked messages never drain, and every shard reader parks in
// backpressure forever. Settlement must therefore go through the handle
// returned at tracking time, never through a pointer lookup.

// wrapperMutate simulates the wrapper: same slice header, new pointers.
func wrapperMutate(batch service.MessageBatch) {
	for i := range batch {
		batch[i] = batch[i].Copy()
	}
}

func TestBatcherAckSurvivesWrapperPointerReplacement(t *testing.T) {
	batcher := NewRecordBatcher(100, 1, service.MockResources().Logger())
	cp := &mockCheckpointer{checkpointLimit: 1}

	batch := createTestMessages(3, "shard-001", 1)
	tb := batcher.AddMessages(batch, "shard-001")
	wrapperMutate(batch)

	require.NoError(t, batcher.AckBatch(t.Context(), cp, tb))
	assert.Equal(t, 0, batcher.TrackedMessageCount(),
		"settlement must not depend on the batch slice's message pointers")
	assert.Equal(t, "00003", cp.get("shard-001"),
		"the checkpoint must advance for an acked batch even after pointer replacement")
}

func TestBatcherNackSurvivesWrapperPointerReplacement(t *testing.T) {
	batcher := NewRecordBatcher(100, 1, service.MockResources().Logger())

	batch := createTestMessages(3, "shard-001", 1)
	tb := batcher.AddMessages(batch, "shard-001")
	wrapperMutate(batch)

	batcher.RemoveBatch(tb)
	assert.Equal(t, 0, batcher.TrackedMessageCount(),
		"nack settlement must not depend on the batch slice's message pointers")
}

// TestBatcherSettleIsIdempotent: a batch settles exactly once - a double ack,
// or an ack racing a nack, must not double-count.
func TestBatcherSettleIsIdempotent(t *testing.T) {
	batcher := NewRecordBatcher(100, 100, service.MockResources().Logger())
	cp := &mockCheckpointer{checkpointLimit: 100}

	batch := createTestMessages(3, "shard-001", 1)
	tb := batcher.AddMessages(batch, "shard-001")

	require.NoError(t, batcher.AckBatch(t.Context(), cp, tb))
	require.NoError(t, batcher.AckBatch(t.Context(), cp, tb))
	batcher.RemoveBatch(tb)
	assert.Equal(t, 0, batcher.TrackedMessageCount(),
		"repeat settles must be no-ops, not double decrements")

	other := createTestMessages(2, "shard-001", 4)
	batcher.AddMessages(other, "shard-001")
	assert.Equal(t, 2, batcher.TrackedMessageCount())
}
