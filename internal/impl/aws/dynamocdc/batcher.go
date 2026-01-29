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
	"maps"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// RecordBatcher tracks messages and their checkpoints for DynamoDB CDC.
//
// This batcher implements a batched checkpointing strategy to optimize performance
// by checkpointing only after a configurable threshold of messages has been
// acknowledged per shard, rather than after every message.
type RecordBatcher struct {
	maxTrackedShards   int
	maxTrackedMessages int
	log                *service.Logger

	mu             sync.Mutex
	messageTracker map[*service.Message]*messageCheckpoint

	// Checkpoint state per shard
	pendingCount    map[string]int    // Count of acked but not-yet-checkpointed messages
	lastCheckpoints map[string]string // Most recent sequence number per shard
}

type messageCheckpoint struct {
	shardID        string
	sequenceNumber string
}

// NewRecordBatcher creates a new [RecordBatcher] for DynamoDB CDC.
func NewRecordBatcher(maxTrackedShards, checkpointLimit int, log *service.Logger) *RecordBatcher {
	// Set max tracked messages to 10x the checkpoint limit to allow for some buffering.
	// This prevents unbounded growth while allowing parallel processing.
	maxTrackedMessages := checkpointLimit * 10
	if maxTrackedMessages < 1000 {
		maxTrackedMessages = 1000 // Minimum reasonable size
	}

	return &RecordBatcher{
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
func (b *RecordBatcher) AddMessages(batch service.MessageBatch, shardID string) service.MessageBatch {
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
func (b *RecordBatcher) RemoveMessages(batch service.MessageBatch) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, msg := range batch {
		delete(b.messageTracker, msg)
	}
}

type checkpointer interface {
	Set(ctx context.Context, shardID, sequenceNumber string) error
	GetCheckpointLimit() int
}

// AckMessages marks messages as acknowledged and checkpoints if threshold is reached.
func (b *RecordBatcher) AckMessages(
	ctx context.Context,
	cp checkpointer,
	batch service.MessageBatch,
) error {
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
		if b.pendingCount[shardID] >= cp.GetCheckpointLimit() {
			if err := cp.Set(ctx, shardID, seq); err != nil {
				return err
			}

			b.log.Debugf("Checkpointed shard %s at sequence %s", shardID, seq)
			// Reset counter after successful checkpoint
			b.pendingCount[shardID] = 0
		}
	}

	return nil
}

// GetPendingCheckpoints returns a copy of all pending checkpoints that haven't
// been persisted yet.
func (b *RecordBatcher) GetPendingCheckpoints() map[string]string {
	b.mu.Lock()
	defer b.mu.Unlock()

	checkpoints := make(map[string]string, len(b.lastCheckpoints))
	maps.Copy(checkpoints, b.lastCheckpoints)
	return checkpoints
}

// ShouldThrottle returns true if the message tracker is near capacity and
// backpressure should be applied.
func (b *RecordBatcher) ShouldThrottle() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Throttle at 90% capacity to leave some headroom
	return len(b.messageTracker) >= (b.maxTrackedMessages * 9 / 10)
}

// PendingCount returns the pending count for a shard. Exported for testing.
func (b *RecordBatcher) PendingCount(shardID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pendingCount[shardID]
}

// TrackedMessageCount returns the number of tracked messages. Exported for testing.
func (b *RecordBatcher) TrackedMessageCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.messageTracker)
}

// LastCheckpoint returns the last checkpoint for a shard. Exported for testing.
func (b *RecordBatcher) LastCheckpoint(shardID string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastCheckpoints[shardID]
}

// SetLastCheckpoint sets the last checkpoint for a shard. Exported for testing.
func (b *RecordBatcher) SetLastCheckpoint(shardID, seq string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastCheckpoints[shardID] = seq
}

// SetPendingCount sets the pending count for a shard. Exported for testing.
func (b *RecordBatcher) SetPendingCount(shardID string, count int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pendingCount[shardID] = count
}

// GetMessageCheckpoint returns the checkpoint info for a message. Exported for testing.
func (b *RecordBatcher) GetMessageCheckpoint(msg *service.Message) (shardID, sequenceNumber string, exists bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp, ok := b.messageTracker[msg]
	if !ok {
		return "", "", false
	}
	return cp.shardID, cp.sequenceNumber, true
}

// LastCheckpointsCount returns the number of shards with checkpoints. Exported for testing.
func (b *RecordBatcher) LastCheckpointsCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.lastCheckpoints)
}
