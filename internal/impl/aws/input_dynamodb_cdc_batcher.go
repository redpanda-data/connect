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
	"maps"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// dynamoDBCDCRecordBatcher tracks messages and their checkpoints for DynamoDB CDC.
//
// This batcher implements a batched checkpointing strategy to optimize performance by
// checkpointing only after a configurable threshold of messages have been acknowledged
// per shard, rather than after every message.
//
// Message tracking
type dynamoDBCDCRecordBatcher struct {
	mu             sync.Mutex
	messageTracker map[*service.Message]*messageCheckpoint

	// Checkpoint state per shard
	pendingCount    map[string]int    // Count of acked but not-yet-checkpointed messages
	lastCheckpoints map[string]string // Most recent sequence number per shard

	// Configuration
	maxTrackedShards   int // Memory safety limit for number of unique shards
	maxTrackedMessages int // Memory safety limit for in-flight messages

	log *service.Logger
}

type messageCheckpoint struct {
	shardID        string
	sequenceNumber string
}

// newDynamoDBCDCRecordBatcher creates a new record batcher for DynamoDB CDC.
func newDynamoDBCDCRecordBatcher(maxTrackedShards, checkpointLimit int, log *service.Logger) *dynamoDBCDCRecordBatcher {
	// Set max tracked messages to 10x the checkpoint limit to allow for some buffering
	// This prevents unbounded growth while allowing parallel processing
	maxTrackedMessages := checkpointLimit * 10
	if maxTrackedMessages < 1000 {
		maxTrackedMessages = 1000 // Minimum reasonable size
	}

	return &dynamoDBCDCRecordBatcher{
		messageTracker:     make(map[*service.Message]*messageCheckpoint),
		log:                log,
		pendingCount:       make(map[string]int),
		lastCheckpoints:    make(map[string]string),
		maxTrackedShards:   maxTrackedShards,
		maxTrackedMessages: maxTrackedMessages,
	}
}

// AddMessages tracks a batch of messages with their shard and sequence information.
// Each message should have its sequence number in metadata under "dynamodb_sequence_number".
func (b *dynamoDBCDCRecordBatcher) AddMessages(batch service.MessageBatch, shardID string) service.MessageBatch {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Check if we're approaching memory limits
	if len(b.messageTracker)+len(batch) > b.maxTrackedMessages {
		b.log.Warnf("Message tracker near capacity: %d/%d tracked messages (adding %d from shard %s)",
			len(b.messageTracker), b.maxTrackedMessages, len(batch), shardID)
		// Still add messages but warn - this indicates downstream is slow
	}

	for _, msg := range batch {
		// Extract sequence number from message metadata
		sequenceNumber, _ := msg.MetaGet("dynamodb_sequence_number")
		b.messageTracker[msg] = &messageCheckpoint{
			shardID:        shardID,
			sequenceNumber: sequenceNumber,
		}
	}

	return batch
}

// RemoveMessages removes messages from tracking (used when messages are nacked).
func (b *dynamoDBCDCRecordBatcher) RemoveMessages(batch service.MessageBatch) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, msg := range batch {
		delete(b.messageTracker, msg)
	}
}

// checkpointerInterface defines the interface for checkpointing operations.
type checkpointerInterface interface {
	Set(ctx context.Context, shardID, sequenceNumber string) error
	GetCheckpointLimit() int
}

// GetCheckpointLimit returns the checkpoint limit for the checkpointer.
func (c *dynamoDBCDCCheckpointer) GetCheckpointLimit() int {
	return c.checkpointLimit
}

// AckMessages marks messages as acknowledged and checkpoints if threshold is reached.
func (b *dynamoDBCDCRecordBatcher) AckMessages(ctx context.Context, checkpointer checkpointerInterface, batch service.MessageBatch) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Track sequence numbers and message counts per shard
	shardSequences := make(map[string]string)
	shardMessageCounts := make(map[string]int)

	// Collect the highest sequence number and count messages for each shard in this batch
	for _, msg := range batch {
		if cp, exists := b.messageTracker[msg]; exists {
			// Only update if this sequence is higher (lexicographic comparison works for DynamoDB sequence numbers)
			if current, ok := shardSequences[cp.shardID]; !ok || cp.sequenceNumber > current {
				shardSequences[cp.shardID] = cp.sequenceNumber
			}
			shardMessageCounts[cp.shardID]++
			delete(b.messageTracker, msg)
		}
	}

	// Update pending counts and checkpoint if needed
	for shardID, seq := range shardSequences {
		b.lastCheckpoints[shardID] = seq

		// Enforce memory bounds on checkpoint map
		if len(b.lastCheckpoints) > b.maxTrackedShards {
			return fmt.Errorf("checkpoint map exceeded maximum size (%d shards) - possible memory leak", b.maxTrackedShards)
		}

		// Increment pending count with the number of messages acked for this shard
		b.pendingCount[shardID] += shardMessageCounts[shardID]

		// Check if we should checkpoint
		if b.pendingCount[shardID] >= checkpointer.GetCheckpointLimit() {
			if err := checkpointer.Set(ctx, shardID, seq); err != nil {
				return err
			}

			b.log.Debugf("Checkpointed shard %s at sequence %s", shardID, seq)
			// Reset counter after successful checkpoint
			b.pendingCount[shardID] = 0
		}
	}

	return nil
}

// GetPendingCheckpoints returns a copy of all pending checkpoints that haven't been persisted yet.
func (b *dynamoDBCDCRecordBatcher) GetPendingCheckpoints() map[string]string {
	b.mu.Lock()
	defer b.mu.Unlock()

	checkpoints := make(map[string]string, len(b.lastCheckpoints))
	maps.Copy(checkpoints, b.lastCheckpoints)
	return checkpoints
}

// ShouldThrottle returns true if the message tracker is near capacity and backpressure should be applied.
func (b *dynamoDBCDCRecordBatcher) ShouldThrottle() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Throttle at 90% capacity to leave some headroom
	return len(b.messageTracker) >= (b.maxTrackedMessages * 9 / 10)
}
