// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamocdc

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func createTestMessages(count int, shardID string, startSeq int) service.MessageBatch {
	batch := make(service.MessageBatch, count)
	for i := range count {
		msg := service.NewMessage(nil)
		msg.MetaSetMut("dynamodb_shard_id", shardID)
		msg.MetaSetMut("dynamodb_sequence_number", string(rune('A'+startSeq+i)))
		batch[i] = msg
	}
	return batch
}

// mockCheckpointer is a mock checkpointer for testing.
type mockCheckpointer struct {
	mu              sync.Mutex
	checkpoints     map[string]string
	checkpointLimit int
	setCallCount    int
}

func (m *mockCheckpointer) Set(_ context.Context, shardID, sequenceNumber string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkpoints[shardID] = sequenceNumber
	m.setCallCount++
	return nil
}

func (m *mockCheckpointer) GetCheckpointLimit() int {
	return m.checkpointLimit
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

	// Add messages
	batch := createTestMessages(10, "shard-001", 0)
	batcher.AddMessages(batch, "shard-001")

	// pendingCount should be 0 until messages are acked
	assert.Equal(t, 0, batcher.PendingCount("shard-001"))
	assert.Equal(t, 10, batcher.TrackedMessageCount())

	// Remove some messages (simulating nack)
	toRemove := batch[:5]
	batcher.RemoveMessages(toRemove)

	// pendingCount is still 0 since we never acked these messages
	assert.Equal(t, 0, batcher.PendingCount("shard-001"))
	assert.Equal(t, 5, batcher.TrackedMessageCount())

	// Remove remaining messages
	batcher.RemoveMessages(batch[5:])

	assert.Equal(t, 0, batcher.PendingCount("shard-001"))
	assert.Equal(t, 0, batcher.TrackedMessageCount())
}

func TestBatcherAckMessagesWithCheckpointing(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := NewRecordBatcher(10000, 1000, logger)

	checkpointer := &mockCheckpointer{
		checkpoints:     make(map[string]string),
		checkpointLimit: 5, // Low threshold for testing
	}

	// Add 10 messages
	batch := createTestMessages(10, "shard-001", 0)
	batcher.AddMessages(batch, "shard-001")

	// Ack first 3 messages - pending count increments to 3, no checkpoint yet (< 5)
	toAck1 := batch[:3]
	err := batcher.AckMessages(context.Background(), checkpointer, toAck1)
	assert.NoError(t, err)

	assert.Equal(t, 3, batcher.PendingCount("shard-001"), "Should have 3 pending after acking 3")
	assert.Equal(t, 7, batcher.TrackedMessageCount())
	assert.Equal(t, 0, checkpointer.setCallCount, "Should not checkpoint yet (3 < 5)")

	// Ack 3 more messages - pending count reaches 6 (>= 5), should checkpoint
	toAck2 := batch[3:6]
	err = batcher.AckMessages(context.Background(), checkpointer, toAck2)
	assert.NoError(t, err)

	assert.Equal(t, 0, batcher.PendingCount("shard-001"), "Should reset to 0 after checkpoint")
	assert.Equal(t, 4, batcher.TrackedMessageCount())
	assert.Equal(t, 1, checkpointer.setCallCount, "Should checkpoint once (6 >= 5)")
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
	err := batcher.AckMessages(context.Background(), checkpointer, batch1)
	assert.NoError(t, err)
	err = batcher.AckMessages(context.Background(), checkpointer, batch2)
	assert.NoError(t, err)

	assert.Equal(t, 6, batcher.PendingCount("shard-001"))
	assert.Equal(t, 6, batcher.PendingCount("shard-002"))
}

// Regression test: Ensure sequence numbers are tracked per message, not per batch.
func TestBatcherSequenceNumberPerMessage(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := NewRecordBatcher(10000, 1000, logger)

	// Create messages with different sequence numbers
	batch := make(service.MessageBatch, 3)
	for i := range 3 {
		msg := service.NewMessage(nil)
		msg.MetaSetMut("dynamodb_shard_id", "shard-001")
		msg.MetaSetMut("dynamodb_sequence_number", string(rune('A'+i))) // A, B, C
		batch[i] = msg
	}

	batcher.AddMessages(batch, "shard-001")

	// Verify each message has its own sequence number
	_, seq0, exists0 := batcher.GetMessageCheckpoint(batch[0])
	_, seq1, exists1 := batcher.GetMessageCheckpoint(batch[1])
	_, seq2, exists2 := batcher.GetMessageCheckpoint(batch[2])

	assert.True(t, exists0)
	assert.True(t, exists1)
	assert.True(t, exists2)
	assert.Equal(t, "A", seq0)
	assert.Equal(t, "B", seq1)
	assert.Equal(t, "C", seq2)
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
	err := batcher.AckMessages(context.Background(), checkpointer, batch)
	assert.NoError(t, err)

	// Pending count should be 10 after acking 10 messages
	assert.Equal(t, 10, batcher.PendingCount("shard-001"))
}

// Regression test: Verify latest sequence number is used for checkpointing.
func TestBatcherUsesLatestSequenceForCheckpoint(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := NewRecordBatcher(10000, 1000, logger)

	// Create messages with sequence numbers in order
	batch := make(service.MessageBatch, 5)
	seqNumbers := []string{"00001", "00002", "00003", "00004", "00005"}
	for i := range 5 {
		msg := service.NewMessage(nil)
		msg.MetaSetMut("dynamodb_shard_id", "shard-001")
		msg.MetaSetMut("dynamodb_sequence_number", seqNumbers[i])
		batch[i] = msg
	}

	batcher.AddMessages(batch, "shard-001")

	// Process messages out of order
	outOfOrder := service.MessageBatch{batch[2], batch[0], batch[4], batch[1]}

	latestSeq := ""
	for _, msg := range outOfOrder {
		_, seq, exists := batcher.GetMessageCheckpoint(msg)
		if exists && seq > latestSeq {
			latestSeq = seq
		}
	}

	// The latest sequence should be "00005" (from batch[4])
	assert.Equal(t, "00005", latestSeq)
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

// Test that last checkpoints are updated correctly.
func TestBatcherLastCheckpointsTracking(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := NewRecordBatcher(10000, 1000, logger)

	// Add messages for two shards
	batch1 := createTestMessages(3, "shard-001", 0)
	batch2 := createTestMessages(3, "shard-002", 0)

	batcher.AddMessages(batch1, "shard-001")
	batcher.AddMessages(batch2, "shard-002")

	// Manually update last checkpoints
	batcher.SetLastCheckpoint("shard-001", "C")
	batcher.SetLastCheckpoint("shard-002", "C")

	assert.Equal(t, "C", batcher.LastCheckpoint("shard-001"))
	assert.Equal(t, "C", batcher.LastCheckpoint("shard-002"))
}

// Test that max tracked shards limit is enforced.
func TestBatcherMaxTrackedShardsLimit(t *testing.T) {
	logger := service.MockResources().Logger()
	// Create batcher with small limit for testing
	batcher := NewRecordBatcher(5, 1, logger)

	checkpointer := &Checkpointer{
		tableName:       "test-checkpoints",
		streamArn:       "test-stream",
		checkpointLimit: 1,
		log:             logger,
	}

	// Add messages for 5 shards (at the limit)
	for i := range 5 {
		shardID := fmt.Sprintf("shard-%03d", i)
		batch := createTestMessages(2, shardID, 0)
		batcher.AddMessages(batch, shardID)

		// Manually set pending count high enough to trigger checkpoint
		batcher.SetPendingCount(shardID, 2)
		for _, msg := range batch {
			_, seq, exists := batcher.GetMessageCheckpoint(msg)
			if exists {
				batcher.SetLastCheckpoint(shardID, seq)
			}
		}
	}

	// Verify we're tracking exactly 5 shards
	assert.Equal(t, 5, batcher.LastCheckpointsCount())

	// Now try to add and ack a 6th shard (should exceed limit)
	batch := createTestMessages(2, "shard-006", 0)
	batcher.AddMessages(batch, "shard-006")

	batcher.SetPendingCount("shard-006", 2)

	err := batcher.AckMessages(context.Background(), checkpointer, batch)
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
	for i := 0; i < 800; i++ {
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
