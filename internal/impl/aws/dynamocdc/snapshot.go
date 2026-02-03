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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// SnapshotScanner performs parallel scanning of a DynamoDB table
type SnapshotScanner struct {
	client             *dynamodb.Client
	table              string
	segments           int
	batchSize          int
	throttle           time.Duration
	checkpointer       *Checkpointer
	checkpointInterval int // Checkpoint every N batches (0 = every batch)
	log                *service.Logger

	// Callbacks
	onBatch            func(ctx context.Context, items []map[string]types.AttributeValue, segment int) error
	onProgress         func(segment, totalSegments int, recordsRead int64)
	onCheckpointFailed func(segment int, err error)
	onSegmentComplete  func(segment int, duration time.Duration, recordsRead int64)

	// State tracking
	segmentsActive atomic.Int32
}

// SnapshotScannerConfig holds configuration for snapshot scanning
type SnapshotScannerConfig struct {
	Client             *dynamodb.Client
	Table              string
	Segments           int
	BatchSize          int
	Throttle           time.Duration
	Checkpointer       *Checkpointer
	CheckpointInterval int // Checkpoint every N batches (default: 10)
	Logger             *service.Logger
}

// NewSnapshotScanner creates a new snapshot scanner
func NewSnapshotScanner(conf SnapshotScannerConfig) *SnapshotScanner {
	checkpointInterval := conf.CheckpointInterval
	if checkpointInterval == 0 {
		checkpointInterval = 10 // Default: checkpoint every 10 batches
	}

	return &SnapshotScanner{
		client:             conf.Client,
		table:              conf.Table,
		segments:           conf.Segments,
		batchSize:          conf.BatchSize,
		throttle:           conf.Throttle,
		checkpointer:       conf.Checkpointer,
		checkpointInterval: checkpointInterval,
		log:                conf.Logger,
	}
}

// SetBatchCallback sets the callback for processing batches of items
func (s *SnapshotScanner) SetBatchCallback(fn func(ctx context.Context, items []map[string]types.AttributeValue, segment int) error) {
	s.onBatch = fn
}

// SetProgressCallback sets the callback for progress updates
func (s *SnapshotScanner) SetProgressCallback(fn func(segment, totalSegments int, recordsRead int64)) {
	s.onProgress = fn
}

// SetCheckpointFailedCallback sets the callback for checkpoint failures
func (s *SnapshotScanner) SetCheckpointFailedCallback(fn func(segment int, err error)) {
	s.onCheckpointFailed = fn
}

// SetSegmentCompleteCallback sets the callback for segment completion with duration tracking
func (s *SnapshotScanner) SetSegmentCompleteCallback(fn func(segment int, duration time.Duration, recordsRead int64)) {
	s.onSegmentComplete = fn
}

// GetActiveSegments returns the current number of active scan segments
func (s *SnapshotScanner) GetActiveSegments() int {
	return int(s.segmentsActive.Load())
}

// Scan performs the snapshot scan, optionally resuming from a checkpoint
func (s *SnapshotScanner) Scan(ctx context.Context, resume *SnapshotCheckpoint) error {
	if s.onBatch == nil {
		return errors.New("batch callback must be set before scanning")
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(s.segments) // Limit concurrent goroutines

	s.log.Infof("Starting snapshot scan with %d segments", s.segments)

	// Start a goroutine for each segment
	for segment := 0; segment < s.segments; segment++ {
		segmentID := segment
		startKey := resume.GetSegmentStartKey(segmentID)

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

// scanSegment scans a single segment of the table
func (s *SnapshotScanner) scanSegment(ctx context.Context, segment int, startKey map[string]types.AttributeValue) error {
	s.segmentsActive.Add(1)
	defer s.segmentsActive.Add(-1)

	startTime := time.Now()
	s.log.Debugf("Starting scan for segment %d", segment)

	var (
		lastEvaluatedKey = startKey
		recordsRead      int64
		batchCount       = 0
		throttleTicker   = time.NewTicker(s.throttle)
		firstRequest     = true
	)
	defer throttleTicker.Stop()

	// Initialize backoff for throttling errors
	boff := backoff.NewExponentialBackOff()
	boff.InitialInterval = 200 * time.Millisecond
	boff.MaxInterval = 5 * time.Second
	boff.MaxElapsedTime = 0 // Never give up

	for {
		// Check for cancellation
		select {
		case <-ctx.Done():
			s.log.Debugf("Segment %d cancelled after %d records", segment, recordsRead)
			return ctx.Err()
		default:
		}

		// Throttle between requests (except first request)
		if !firstRequest {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-throttleTicker.C:
				// Proceed with request
			}
		}
		firstRequest = false

		// Perform scan request
		result, err := s.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(s.table),
			Limit:             aws.Int32(int32(s.batchSize)),
			Segment:           aws.Int32(int32(segment)),
			TotalSegments:     aws.Int32(int32(s.segments)),
			ExclusiveStartKey: lastEvaluatedKey,
			// Use eventually consistent reads (cheaper and faster)
			ConsistentRead: aws.Bool(false),
		})
		if err != nil {
			// Check if throttled
			if isThrottlingError(err) {
				wait := boff.NextBackOff()
				s.log.Warnf("Segment %d throttled, backing off for %v", segment, wait)
				time.Sleep(wait)
				continue
			}
			return fmt.Errorf("scan failed for segment %d: %w", segment, err)
		}

		// Success - reset backoff
		boff.Reset()

		// Process batch if we got items
		if len(result.Items) > 0 {
			if err := s.onBatch(ctx, result.Items, segment); err != nil {
				return fmt.Errorf("failed to process batch for segment %d: %w", segment, err)
			}
			recordsRead += int64(len(result.Items))
			batchCount++
		}

		// Update checkpoint periodically (not after every batch)
		shouldCheckpoint := s.checkpointer != nil && (batchCount%s.checkpointInterval == 0 || result.LastEvaluatedKey == nil)
		if shouldCheckpoint {
			if err := s.checkpointer.UpdateSnapshotProgress(ctx, segment, result.LastEvaluatedKey, recordsRead); err != nil {
				// Log warning but continue - checkpoint is best-effort
				// Worst case: we rescan from earlier point on restart
				s.log.Warnf("Failed to update checkpoint for segment %d: %v", segment, err)
				// Notify about checkpoint failure for monitoring
				if s.onCheckpointFailed != nil {
					s.onCheckpointFailed(segment, err)
				}
			} else {
				s.log.Debugf("Checkpointed segment %d at %d records (%d batches)", segment, recordsRead, batchCount)
			}
		}

		// Report progress
		if s.onProgress != nil {
			s.onProgress(segment, s.segments, recordsRead)
		}

		// Check if segment complete
		lastEvaluatedKey = result.LastEvaluatedKey
		if lastEvaluatedKey == nil {
			duration := time.Since(startTime)
			s.log.Infof("Segment %d completed: %d records read in %v", segment, recordsRead, duration)

			// Report segment completion with duration for metrics
			if s.onSegmentComplete != nil {
				s.onSegmentComplete(segment, duration, recordsRead)
			}
			return nil
		}
	}
}

// isThrottlingError checks if an error is due to AWS throttling
func isThrottlingError(err error) bool {
	if err == nil {
		return false
	}
	var limitErr *types.LimitExceededException
	var provisionedErr *types.ProvisionedThroughputExceededException
	return errors.As(err, &limitErr) || errors.As(err, &provisionedErr)
}

// SnapshotCheckpoint holds the progress of a snapshot scan
type SnapshotCheckpoint struct {
	Complete        bool
	SegmentProgress map[int]*SegmentState
	mu              sync.RWMutex
}

// SegmentState holds the state of a single scan segment
type SegmentState struct {
	LastKey     map[string]types.AttributeValue
	RecordsRead int64
	Complete    bool
}

// NewSnapshotCheckpoint creates a new snapshot checkpoint
func NewSnapshotCheckpoint() *SnapshotCheckpoint {
	return &SnapshotCheckpoint{
		Complete:        false,
		SegmentProgress: make(map[int]*SegmentState),
	}
}

// GetSegmentStartKey returns the starting key for a segment (nil if starting from beginning)
func (c *SnapshotCheckpoint) GetSegmentStartKey(segment int) map[string]types.AttributeValue {
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

// IsComplete returns true if the snapshot is complete
func (c *SnapshotCheckpoint) IsComplete() bool {
	if c == nil {
		return false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Complete
}

// MarkSegmentComplete marks a segment as complete
func (c *SnapshotCheckpoint) MarkSegmentComplete(segment int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.SegmentProgress[segment] == nil {
		c.SegmentProgress[segment] = &SegmentState{}
	}
	c.SegmentProgress[segment].Complete = true
}

// MarkComplete marks the entire snapshot as complete
func (c *SnapshotCheckpoint) MarkComplete() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Complete = true
}
