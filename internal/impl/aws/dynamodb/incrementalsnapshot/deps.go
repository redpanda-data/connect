// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"context"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Progress is a table's durable snapshot progress, as read from the
// checkpoint store. It is opaque to this package beyond these checks: the
// backfill hands it back to Deps.Scan to resume from.
type Progress interface {
	// IsComplete reports whether the whole snapshot has completed.
	IsComplete() bool
	// HasSegmentProgress reports whether any scan segment has recorded
	// progress, that is whether a backfill has started.
	HasSegmentProgress() bool
}

// ProgressStore is one table's snapshot checkpoint store. The DynamoDB
// input's per-table checkpointer satisfies it as it is.
type ProgressStore[P Progress] interface {
	// SnapshotProgress reads the table's snapshot progress.
	SnapshotProgress(ctx context.Context) (P, error)
	// MarkSnapshotComplete durably records the snapshot as complete.
	MarkSnapshotComplete(ctx context.Context) error
	// ResetSnapshotProgress discards the table's snapshot progress, so the
	// next backfill starts from the beginning.
	ResetSnapshotProgress(ctx context.Context) error
	// MarkSnapshotRequested durably records that a snapshot was requested.
	MarkSnapshotRequested(ctx context.Context) error
	// SnapshotRequested reports whether a snapshot request is recorded.
	SnapshotRequested(ctx context.Context) (bool, error)
}

// Deps supplies the side-effecting operations a Backfill needs for one
// table, so this package carries no DynamoDB client, checkpointer or message
// channel of its own.
type Deps[P Progress] interface {
	ProgressStore[P]

	// CDCCheckpointStale reports whether the table's stream checkpoint points
	// at data the stream has already trimmed.
	CDCCheckpointStale(ctx context.Context) (bool, error)

	// NewEmitter returns the emitter for one backfill run. Each run gets its
	// own, so a failed run's unacked pages cannot hold up the next one.
	NewEmitter() Emitter

	// Scan reads the table from resume, calling hooks as it goes, and
	// returns once every segment has finished or ctx ends.
	Scan(ctx context.Context, resume P, hooks ScanHooks) error
}

// Emitter sends one backfill run's released pages downstream and tracks
// their acknowledgements, persisting each segment's position only once
// everything before it is acked.
type Emitter interface {
	// Emit sends a released page's items. cursor is the page's scan resume
	// position, persisted once the items are acked.
	Emit(ctx context.Context, items []Item, segment int, cursor Item) error
	// SealSegment records segment as complete behind its in-flight pages.
	SealSegment(ctx context.Context, segment int) error
	// WaitAcked blocks until every emitted page is acked or nacked, or ctx
	// ends.
	WaitAcked(ctx context.Context) error
	// FlushCompleted re-drives segment completion writes that failed
	// earlier.
	FlushCompleted(ctx context.Context) error
}

// ScanHooks are the callbacks Deps.Scan runs while it reads a table. Each
// matches the SnapshotScanner callback of the same name.
type ScanHooks struct {
	// BeforeRequest runs immediately before every Scan request of a segment,
	// retries included.
	BeforeRequest func(segment int)
	// Batch receives a page of items; lastKey is the scan position after it.
	Batch func(ctx context.Context, items []Item, segment int, lastKey Item) error
	// SegmentSealed runs once a segment has emitted its last page.
	SegmentSealed func(ctx context.Context, segment int) error
	// SegmentComplete runs once a segment's scan has finished.
	SegmentComplete func(segment int, duration time.Duration, recordsRead int64)
}

// Metrics are the input's metrics a Backfill updates.
type Metrics struct {
	// WindowHeld is the number of live items held in the window.
	WindowHeld *service.MetricGauge
	// WindowDropped counts held items dropped by a stream touch.
	WindowDropped *service.MetricCounter
	// WindowWait times a page from its read to its release.
	WindowWait *service.MetricTimer
	// SegmentDuration times each segment's scan.
	SegmentDuration *service.MetricTimer
	// CheckpointFailures counts failed checkpoint writes.
	CheckpointFailures *service.MetricCounter
}
