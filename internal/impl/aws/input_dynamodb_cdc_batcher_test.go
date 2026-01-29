// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package aws

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

// Mock checkpointer for testing
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
	batcher := newDynamoDBCDCRecordBatcher(10000, 1000, logger)

	// Add messages for shard-001
	batch1 := createTestMessages(5, "shard-001", 0)
	result1 := batcher.AddMessages(batch1, "shard-001")

	assert.Len(t, result1, 5)
	// pendingCount should be 0 until messages are acked
	assert.Equal(t, 0, batcher.pendingCount["shard-001"])
	assert.Len(t, batcher.messageTracker, 5)

	// Add more messages for same shard
	batch2 := createTestMessages(3, "shard-001", 5)
	result2 := batcher.AddMessages(batch2, "shard-001")

	assert.Len(t, result2, 3)
	assert.Equal(t, 0, batcher.pendingCount["shard-001"])
	assert.Len(t, batcher.messageTracker, 8)

	// Add messages for different shard
	batch3 := createTestMessages(4, "shard-002", 0)
	result3 := batcher.AddMessages(batch3, "shard-002")

	assert.Len(t, result3, 4)
	assert.Equal(t, 0, batcher.pendingCount["shard-001"])
	assert.Equal(t, 0, batcher.pendingCount["shard-002"])
	assert.Len(t, batcher.messageTracker, 12)
}

func TestBatcherRemoveMessages(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := newDynamoDBCDCRecordBatcher(10000, 1000, logger)

	// Add messages
	batch := createTestMessages(10, "shard-001", 0)
	batcher.AddMessages(batch, "shard-001")

	// pendingCount should be 0 until messages are acked
	assert.Equal(t, 0, batcher.pendingCount["shard-001"])
	assert.Len(t, batcher.messageTracker, 10)

	// Remove some messages (simulating nack)
	toRemove := batch[:5]
	batcher.RemoveMessages(toRemove)

	// pendingCount is still 0 since we never acked these messages
	assert.Equal(t, 0, batcher.pendingCount["shard-001"])
	assert.Len(t, batcher.messageTracker, 5)

	// Remove remaining messages
	batcher.RemoveMessages(batch[5:])

	assert.Equal(t, 0, batcher.pendingCount["shard-001"])
	assert.Empty(t, batcher.messageTracker)
}

func TestBatcherAckMessagesWithCheckpointing(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := newDynamoDBCDCRecordBatcher(10000, 1000, logger)

	mockCheckpointer := &mockCheckpointer{
		checkpoints:     make(map[string]string),
		checkpointLimit: 5, // Low threshold for testing
	}

	// Add 10 messages
	batch := createTestMessages(10, "shard-001", 0)
	batcher.AddMessages(batch, "shard-001")

	// Ack first 3 messages - pending count increments to 3, no checkpoint yet (< 5)
	toAck1 := batch[:3]
	err := batcher.AckMessages(context.Background(), mockCheckpointer, toAck1)
	assert.NoError(t, err)

	assert.Equal(t, 3, batcher.pendingCount["shard-001"], "Should have 3 pending after acking 3")
	assert.Len(t, batcher.messageTracker, 7)
	assert.Equal(t, 0, mockCheckpointer.setCallCount, "Should not checkpoint yet (3 < 5)")

	// Ack 3 more messages - pending count reaches 6 (>= 5), should checkpoint
	toAck2 := batch[3:6]
	err = batcher.AckMessages(context.Background(), mockCheckpointer, toAck2)
	assert.NoError(t, err)

	assert.Equal(t, 0, batcher.pendingCount["shard-001"], "Should reset to 0 after checkpoint")
	assert.Len(t, batcher.messageTracker, 4)
	assert.Equal(t, 1, mockCheckpointer.setCallCount, "Should checkpoint once (6 >= 5)")
}

func TestBatcherAckMessagesMultipleShards(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := newDynamoDBCDCRecordBatcher(10000, 1000, logger)

	// Add messages for multiple shards
	batch1 := createTestMessages(6, "shard-001", 0)
	batch2 := createTestMessages(6, "shard-002", 0)

	batcher.AddMessages(batch1, "shard-001")
	batcher.AddMessages(batch2, "shard-002")

	mockCheckpointer := &mockCheckpointer{
		checkpointLimit: 100, // High limit so we don't checkpoint
	}

	checkpointer := &dynamoDBCDCCheckpointer{
		checkpointLimit: mockCheckpointer.checkpointLimit,
	}

	// Ack messages from both shards
	err := batcher.AckMessages(context.Background(), checkpointer, batch1)
	assert.NoError(t, err)
	err = batcher.AckMessages(context.Background(), checkpointer, batch2)
	assert.NoError(t, err)

	assert.Equal(t, 6, batcher.pendingCount["shard-001"])
	assert.Equal(t, 6, batcher.pendingCount["shard-002"])

	// Test that both shards are tracked independently
	batcher.mu.Lock()
	assert.Contains(t, batcher.pendingCount, "shard-001")
	assert.Contains(t, batcher.pendingCount, "shard-002")
	batcher.mu.Unlock()
}

// Regression test: Ensure sequence numbers are tracked per message, not per batch
func TestBatcherSequenceNumberPerMessage(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := newDynamoDBCDCRecordBatcher(10000, 1000, logger)

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
	batcher.mu.Lock()
	assert.Equal(t, "A", batcher.messageTracker[batch[0]].sequenceNumber)
	assert.Equal(t, "B", batcher.messageTracker[batch[1]].sequenceNumber)
	assert.Equal(t, "C", batcher.messageTracker[batch[2]].sequenceNumber)
	batcher.mu.Unlock()
}

// Regression test: Verify pending count increments on ack
func TestBatcherPendingCountDoesNotIncrementOnAck(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := newDynamoDBCDCRecordBatcher(10000, 1000, logger)

	mockCheckpointer := &mockCheckpointer{
		checkpointLimit: 100, // High limit so we don't checkpoint
	}

	checkpointer := &dynamoDBCDCCheckpointer{
		checkpointLimit: mockCheckpointer.checkpointLimit,
	}

	// Add 10 messages
	batch := createTestMessages(10, "shard-001", 0)
	batcher.AddMessages(batch, "shard-001")
	assert.Equal(t, 0, batcher.pendingCount["shard-001"], "Should be 0 before ack")

	// Ack messages - pending count should increment
	err := batcher.AckMessages(context.Background(), checkpointer, batch)
	assert.NoError(t, err)

	// Pending count should be 10 after acking 10 messages
	assert.Equal(t, 10, batcher.pendingCount["shard-001"])
}

// Regression test: Verify latest sequence number is used for checkpointing
func TestBatcherUsesLatestSequenceForCheckpoint(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := newDynamoDBCDCRecordBatcher(10000, 1000, logger)

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

	batcher.mu.Lock()
	latestSeq := ""
	for _, msg := range outOfOrder {
		if cp, exists := batcher.messageTracker[msg]; exists {
			// Track the latest (highest) sequence number
			if cp.sequenceNumber > latestSeq {
				latestSeq = cp.sequenceNumber
			}
			delete(batcher.messageTracker, msg)
		}
	}
	batcher.mu.Unlock()

	// The latest sequence should be "00005" (from batch[4])
	assert.Equal(t, "00005", latestSeq)
}

// Test concurrent access to batcher
func TestBatcherConcurrentAccess(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := newDynamoDBCDCRecordBatcher(10000, 1000, logger)

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
	assert.Empty(t, batcher.messageTracker, "All messages should be removed")
}

func TestBatcherNackAndReAdd(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := newDynamoDBCDCRecordBatcher(10000, 1000, logger)

	// Add messages
	batch := createTestMessages(5, "shard-001", 0)
	batcher.AddMessages(batch, "shard-001")

	// pendingCount should be 0 until ack
	assert.Equal(t, 0, batcher.pendingCount["shard-001"])

	// Simulate nack by removing messages
	batcher.RemoveMessages(batch)

	assert.Equal(t, 0, batcher.pendingCount["shard-001"])
	assert.Empty(t, batcher.messageTracker)

	// Re-add the same logical messages (new message objects)
	newBatch := createTestMessages(5, "shard-001", 0)
	batcher.AddMessages(newBatch, "shard-001")

	// Still 0 until ack
	assert.Equal(t, 0, batcher.pendingCount["shard-001"])
	assert.Len(t, batcher.messageTracker, 5)
}

// Test that last checkpoints are updated correctly
func TestBatcherLastCheckpointsTracking(t *testing.T) {
	logger := service.MockResources().Logger()
	batcher := newDynamoDBCDCRecordBatcher(10000, 1000, logger)

	// Add messages for two shards
	batch1 := createTestMessages(3, "shard-001", 0)
	batch2 := createTestMessages(3, "shard-002", 0)

	batcher.AddMessages(batch1, "shard-001")
	batcher.AddMessages(batch2, "shard-002")

	// Manually update last checkpoints
	batcher.mu.Lock()
	batcher.lastCheckpoints["shard-001"] = "C" // Last message in batch1
	batcher.lastCheckpoints["shard-002"] = "C" // Last message in batch2
	batcher.mu.Unlock()

	assert.Equal(t, "C", batcher.lastCheckpoints["shard-001"])
	assert.Equal(t, "C", batcher.lastCheckpoints["shard-002"])
}

// Test that max tracked shards limit is enforced
func TestBatcherMaxTrackedShardsLimit(t *testing.T) {
	logger := service.MockResources().Logger()
	// Create batcher with small limit for testing
	batcher := newDynamoDBCDCRecordBatcher(5, 1, logger)

	mockCheckpointer := &mockCheckpointer{
		checkpointLimit: 1,
	}

	// Wrap mockCheckpointer with the real checkpointer struct
	checkpointer := &dynamoDBCDCCheckpointer{
		tableName:       "test-checkpoints",
		streamArn:       "test-stream",
		checkpointLimit: mockCheckpointer.checkpointLimit,
		log:             logger,
	}

	// Add messages for 5 shards (at the limit)
	for i := range 5 {
		shardID := fmt.Sprintf("shard-%03d", i)
		batch := createTestMessages(2, shardID, 0)
		batcher.AddMessages(batch, shardID)

		// Manually set pending count high enough to trigger checkpoint
		batcher.mu.Lock()
		batcher.pendingCount[shardID] = 2
		for _, msg := range batch {
			if cp, exists := batcher.messageTracker[msg]; exists {
				batcher.lastCheckpoints[shardID] = cp.sequenceNumber
			}
		}
		batcher.mu.Unlock()
	}

	// Verify we're tracking exactly 5 shards
	assert.Len(t, batcher.lastCheckpoints, 5)

	// Now try to add and ack a 6th shard (should exceed limit)
	batch := createTestMessages(2, "shard-006", 0)
	batcher.AddMessages(batch, "shard-006")

	batcher.mu.Lock()
	batcher.pendingCount["shard-006"] = 2
	batcher.mu.Unlock()

	err := batcher.AckMessages(context.Background(), checkpointer, batch)
	assert.Error(t, err, "Should fail when exceeding max tracked shards")
	assert.Contains(t, err.Error(), "exceeded maximum size")
	assert.Contains(t, err.Error(), "5 shards")
}

// Test that ShouldThrottle works correctly
func TestBatcherShouldThrottle(t *testing.T) {
	logger := service.MockResources().Logger()
	// Create batcher with small limit for testing (checkpointLimit=10 -> maxTrackedMessages=1000)
	batcher := newDynamoDBCDCRecordBatcher(100, 10, logger)

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
