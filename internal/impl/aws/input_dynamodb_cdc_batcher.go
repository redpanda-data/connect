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
type dynamoDBCDCRecordBatcher struct {
	mu               sync.Mutex
	messageTracker   map[*service.Message]*messageCheckpoint
	log              *service.Logger
	pendingCount     map[string]int
	lastCheckpoints  map[string]string
	maxTrackedShards int
}

type messageCheckpoint struct {
	shardID        string
	sequenceNumber string
}

// newDynamoDBCDCRecordBatcher creates a new record batcher for DynamoDB CDC.
func newDynamoDBCDCRecordBatcher(maxTrackedShards int, log *service.Logger) *dynamoDBCDCRecordBatcher {
	return &dynamoDBCDCRecordBatcher{
		messageTracker:   make(map[*service.Message]*messageCheckpoint),
		log:              log,
		pendingCount:     make(map[string]int),
		lastCheckpoints:  make(map[string]string),
		maxTrackedShards: maxTrackedShards,
	}
}

// AddMessages tracks a batch of messages with their shard and sequence information.
// Each message should have its sequence number in metadata under "dynamodb_sequence_number".
func (b *dynamoDBCDCRecordBatcher) AddMessages(batch service.MessageBatch, shardID string) service.MessageBatch {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, msg := range batch {
		// Extract sequence number from message metadata
		sequenceNumber, _ := msg.MetaGet("dynamodb_sequence_number")
		b.messageTracker[msg] = &messageCheckpoint{
			shardID:        shardID,
			sequenceNumber: sequenceNumber,
		}
	}

	b.pendingCount[shardID] += len(batch)
	return batch
}

// RemoveMessages removes messages from tracking (used when messages are nacked).
func (b *dynamoDBCDCRecordBatcher) RemoveMessages(batch service.MessageBatch) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, msg := range batch {
		if cp, exists := b.messageTracker[msg]; exists {
			b.pendingCount[cp.shardID]--
			delete(b.messageTracker, msg)
		}
	}
}

// AckMessages marks messages as acknowledged and checkpoints if threshold is reached.
func (b *dynamoDBCDCRecordBatcher) AckMessages(ctx context.Context, checkpointer *dynamoDBCDCCheckpointer, batch service.MessageBatch) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Track sequence numbers and message counts per shard
	shardSequences := make(map[string]string)
	shardMessageCounts := make(map[string]int)

	// Collect the last sequence number and count messages for each shard in this batch
	for _, msg := range batch {
		if cp, exists := b.messageTracker[msg]; exists {
			shardSequences[cp.shardID] = cp.sequenceNumber
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

		// Check if we should checkpoint BEFORE decrementing
		// pendingCount tracks how many messages have been acked since last checkpoint
		if b.pendingCount[shardID] >= checkpointer.checkpointLimit {
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
