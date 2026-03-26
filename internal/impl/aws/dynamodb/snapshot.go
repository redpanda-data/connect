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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	smithytime "github.com/aws/smithy-go/time"
	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// DynamoItems is a slice of DynamoDB attribute maps representing table items.
type DynamoItems = []map[string]dynamodbtypes.AttributeValue

// SnapshotScannerConfig holds configuration for snapshot scanning.
type SnapshotScannerConfig struct {
	Client             *dynamodb.Client
	Table              string
	Segments           int
	BatchSize          int
	Throttle           time.Duration
	MaxBackoff         time.Duration // Maximum backoff on throttling errors (0 = no limit).
	Checkpointer       *Checkpointer
	CheckpointInterval int // Checkpoint every N batches (default: 10).
	Logger             *service.Logger
}

// SnapshotScanner performs a parallel scan of a DynamoDB table using the
// DynamoDB Scan API with configurable segment parallelism. It supports
// resumable checkpointing, adaptive backoff on throttling, and reports
// progress through user-supplied callbacks.
type SnapshotScanner struct {
	client             *dynamodb.Client
	table              string
	segments           int
	batchSize          int
	throttle           time.Duration
	maxBackoff         time.Duration
	checkpointer       *Checkpointer
	checkpointInterval int // Checkpoint every N batches (0 = every batch)
	log                *service.Logger

	// Callbacks
	onBatch            func(ctx context.Context, items DynamoItems, segment int) error
	onProgress         func(segment, totalSegments int, recordsRead int64)
	onCheckpointFailed func(segment int, err error)
	onSegmentComplete  func(segment int, duration time.Duration, recordsRead int64)

	// State tracking
	activeSegments atomic.Int32
}

// NewSnapshotScanner creates a new snapshot scanner.
func NewSnapshotScanner(conf SnapshotScannerConfig) *SnapshotScanner {
	checkpointInterval := conf.CheckpointInterval
	if checkpointInterval == 0 {
		checkpointInterval = 10 // Default: checkpoint every 10 batches.
	}

	return &SnapshotScanner{
		client:             conf.Client,
		table:              conf.Table,
		segments:           conf.Segments,
		batchSize:          conf.BatchSize,
		throttle:           conf.Throttle,
		maxBackoff:         conf.MaxBackoff,
		checkpointer:       conf.Checkpointer,
		checkpointInterval: checkpointInterval,
		log:                conf.Logger,
	}
}

// SetBatchCallback sets the callback for processing batches of items.
func (s *SnapshotScanner) SetBatchCallback(fn func(ctx context.Context, items DynamoItems, segment int) error) {
	s.onBatch = fn
}

// SetProgressCallback sets the callback for progress updates.
func (s *SnapshotScanner) SetProgressCallback(fn func(segment, totalSegments int, recordsRead int64)) {
	s.onProgress = fn
}

// SetCheckpointFailedCallback sets the callback for checkpoint failures.
func (s *SnapshotScanner) SetCheckpointFailedCallback(fn func(segment int, err error)) {
	s.onCheckpointFailed = fn
}

// SetSegmentCompleteCallback sets the callback for segment completion with duration tracking.
func (s *SnapshotScanner) SetSegmentCompleteCallback(fn func(segment int, duration time.Duration, recordsRead int64)) {
	s.onSegmentComplete = fn
}

// ActiveSegments returns the current number of active scan segments.
func (s *SnapshotScanner) ActiveSegments() int {
	return int(s.activeSegments.Load())
}

// Scan performs the snapshot scan, optionally resuming from a checkpoint.
func (s *SnapshotScanner) Scan(ctx context.Context, resume *SnapshotCheckpoint) error {
	if s.onBatch == nil {
		return errors.New("batch callback must be set before scanning")
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(s.segments)

	s.log.Infof("Starting snapshot scan with %d segments", s.segments)

	// Start a goroutine for each segment, skipping already-completed segments.
	for segment := 0; segment < s.segments; segment++ {
		segmentID := segment

		if resume.SegmentComplete(segmentID) {
			s.log.Debugf("Skipping already-completed segment %d", segmentID)
			continue
		}

		startKey := resume.SegmentStartKey(segmentID)

		g.Go(func() error {
			return s.scanSegment(ctx, segmentID, startKey)
		})
	}

	// Wait for all segments to complete
	if err := g.Wait(); err != nil {
		return fmt.Errorf("snapshot scan failed: %w", err)
	}

	s.log.Info("Snapshot scan completed successfully")
	return nil
}

// scanSegment scans a single segment of the table.
func (s *SnapshotScanner) scanSegment(ctx context.Context, segment int, startKey map[string]dynamodbtypes.AttributeValue) error {
	s.activeSegments.Add(1)
	defer s.activeSegments.Add(-1)

	startTime := time.Now()
	s.log.Debugf("Starting scan for segment %d", segment)

	var (
		lastEvaluatedKey = startKey
		recordsRead      int64
		batchCount       int
		throttleTicker   = time.NewTicker(s.throttle)
		firstRequest     = true
	)
	defer throttleTicker.Stop()

	boff := backoff.NewExponentialBackOff()
	boff.InitialInterval = 200 * time.Millisecond
	boff.MaxInterval = 5 * time.Second
	boff.MaxElapsedTime = s.maxBackoff

	for {
		select {
		case <-ctx.Done():
			s.log.Debugf("Segment %d cancelled after %d records", segment, recordsRead)
			return ctx.Err()
		default:
		}

		if !firstRequest {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-throttleTicker.C:
			}
		}
		firstRequest = false

		result, err := s.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(s.table),
			Limit:             aws.Int32(int32(s.batchSize)),
			Segment:           aws.Int32(int32(segment)),
			TotalSegments:     aws.Int32(int32(s.segments)),
			ExclusiveStartKey: lastEvaluatedKey,
			ConsistentRead:    aws.Bool(false),
		})
		if err != nil {
			if isThrottlingError(err) {
				wait := boff.NextBackOff()
				if wait == backoff.Stop {
					return fmt.Errorf("scan throttle backoff exceeded max time for segment %d: %w", segment, err)
				}
				s.log.Warnf("Segment %d throttled, backing off for %v", segment, wait)
				if err := smithytime.SleepWithContext(ctx, wait); err != nil {
					return ctx.Err()
				}
				continue
			}
			return fmt.Errorf("scan failed for segment %d: %w", segment, err)
		}
		boff.Reset()

		if len(result.Items) == 0 {
			lastEvaluatedKey = result.LastEvaluatedKey
			if lastEvaluatedKey == nil {
				return s.completeSegment(segment, startTime, recordsRead)
			}
			continue
		}

		if err := s.onBatch(ctx, result.Items, segment); err != nil {
			return fmt.Errorf("processing batch for segment %d: %w", segment, err)
		}
		recordsRead += int64(len(result.Items))
		batchCount++

		if s.shouldCheckpoint(batchCount, result.LastEvaluatedKey) {
			if err := s.checkpointer.UpdateSnapshotProgress(ctx, segment, result.LastEvaluatedKey, recordsRead); err != nil {
				s.log.Warnf("Failed to update checkpoint for segment %d: %v", segment, err)
				if s.onCheckpointFailed != nil {
					s.onCheckpointFailed(segment, err)
				}
			} else {
				s.log.Debugf("Checkpointed segment %d at %d records (%d batches)", segment, recordsRead, batchCount)
			}
		}

		if s.onProgress != nil {
			s.onProgress(segment, s.segments, recordsRead)
		}

		lastEvaluatedKey = result.LastEvaluatedKey
		if lastEvaluatedKey == nil {
			return s.completeSegment(segment, startTime, recordsRead)
		}
	}
}

// shouldCheckpoint returns true when a checkpoint should be written.
func (s *SnapshotScanner) shouldCheckpoint(batchCount int, lastKey map[string]dynamodbtypes.AttributeValue) bool {
	if s.checkpointer == nil || batchCount == 0 {
		return false
	}
	return batchCount%s.checkpointInterval == 0 || lastKey == nil
}

// completeSegment logs segment completion and fires the callback.
func (s *SnapshotScanner) completeSegment(segment int, startTime time.Time, recordsRead int64) error {
	duration := time.Since(startTime)
	s.log.Infof("Segment %d completed: %d records read in %v", segment, recordsRead, duration)
	if s.onSegmentComplete != nil {
		s.onSegmentComplete(segment, duration, recordsRead)
	}
	return nil
}

// isThrottlingError checks if an error is due to AWS throttling.
// It checks both dynamodb/types and dynamodbstreams/types variants because this
// function is called from both the snapshot path (DynamoDB Scan API) and the CDC
// path (DynamoDB Streams API), which return distinct concrete types.
func isThrottlingError(err error) bool {
	if err == nil {
		return false
	}
	// DynamoDB table API types (snapshot scan path).
	_, isLimit := errors.AsType[*dynamodbtypes.LimitExceededException](err)
	_, isProvisioned := errors.AsType[*dynamodbtypes.ProvisionedThroughputExceededException](err)
	// DynamoDB Streams API types (CDC reader path).
	_, isStreamsLimit := errors.AsType[*streamstypes.LimitExceededException](err)
	return isLimit || isProvisioned || isStreamsLimit
}

// isTrimmedDataAccessError returns true when a GetRecords call fails because the
// shard iterator points past the oldest available stream record. This typically
// happens when DynamoDB reorganises stream shards (e.g. after splits/merges or,
// in DynamoDB Local, after the first write to an empty-stream shard). The caller
// should obtain a fresh shard iterator and retry.
func isTrimmedDataAccessError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := errors.AsType[*streamstypes.TrimmedDataAccessException](err)
	return ok
}

// SnapshotCheckpoint holds the progress of a snapshot scan.
type SnapshotCheckpoint struct {
	Complete        bool
	SegmentProgress map[int]*SegmentState
	mu              sync.RWMutex
}

// SegmentState holds the state of a single scan segment.
type SegmentState struct {
	LastKey     map[string]dynamodbtypes.AttributeValue
	RecordsRead int64
	Complete    bool
}

// NewSnapshotCheckpoint creates a new snapshot checkpoint.
func NewSnapshotCheckpoint() *SnapshotCheckpoint {
	return &SnapshotCheckpoint{
		Complete:        false,
		SegmentProgress: make(map[int]*SegmentState),
	}
}

// SegmentStartKey returns the starting key for a segment, or nil if starting from the beginning.
func (c *SnapshotCheckpoint) SegmentStartKey(segment int) map[string]dynamodbtypes.AttributeValue {
	if c == nil {
		return nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if state, exists := c.SegmentProgress[segment]; exists && !state.Complete {
		return state.LastKey
	}
	return nil
}

// SegmentComplete returns true if the given segment has already finished scanning.
func (c *SnapshotCheckpoint) SegmentComplete(segment int) bool {
	if c == nil {
		return false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if state, ok := c.SegmentProgress[segment]; ok {
		return state.Complete
	}
	return false
}

// IsComplete returns true if the snapshot is complete.
func (c *SnapshotCheckpoint) IsComplete() bool {
	if c == nil {
		return false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Complete
}

// MarkSegmentComplete marks a segment as complete.
func (c *SnapshotCheckpoint) MarkSegmentComplete(segment int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.SegmentProgress[segment] == nil {
		c.SegmentProgress[segment] = &SegmentState{}
	}
	c.SegmentProgress[segment].Complete = true
}

// MarkComplete marks the entire snapshot as complete.
func (c *SnapshotCheckpoint) MarkComplete() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Complete = true
}
