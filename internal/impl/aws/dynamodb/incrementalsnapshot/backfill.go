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
	"errors"
	"fmt"
	"time"

	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// defaultReleaseTick bounds how long a releasable page waits when no progress
// nudge arrives. It is only a fallback poll interval, because releases are
// normally triggered by stream progress nudges, so changing it alters release
// latency by at most one tick.
const defaultReleaseTick = 250 * time.Millisecond

// Backfill is one table's incremental backfill. P is the checkpoint store's
// snapshot progress type.
type Backfill[P Progress] struct {
	// Table names the table in logs and errors.
	Table string
	// KeySchema is the table's primary key schema, used to key scanned
	// items in the window.
	KeySchema []dynamodbtypes.KeySchemaElement
	// State is the table's window and shard progress, shared with its
	// readers.
	State   *State
	Deps    Deps[P]
	Metrics Metrics
	Logger  *service.Logger
	// ReleaseTick overrides the release poll interval, for tests. Zero
	// means the 250ms default.
	ReleaseTick time.Duration
}

// Run scans the table page by page through its window, releasing each page
// once no stream record can still supersede it out of order, and marks the
// snapshot complete once every released item is acked.
func (b *Backfill[P]) Run(ctx context.Context) (retErr error) {
	progress, err := b.Deps.SnapshotProgress(ctx)
	if err != nil {
		return fmt.Errorf("getting snapshot progress for table %s: %w", b.Table, err)
	}
	if progress.IsComplete() {
		b.Logger.Infof("Incremental snapshot of table %s already complete", b.Table)
		return nil
	}

	emitter := b.Deps.NewEmitter()
	win := b.State.window
	hooks := ScanHooks{
		BeforeRequest: win.Begin,
		Batch: func(ctx context.Context, items []Item, segment int, lastKey Item) error {
			high := time.Now()
			keys := make([]string, len(items))
			for i, it := range items {
				k, ok := WindowKeyFromItem(it, b.KeySchema)
				if !ok {
					return fmt.Errorf("table %s: scanned item has no usable primary key", b.Table)
				}
				keys[i] = k
			}
			released, dropped := win.Hold(segment, items, keys, high, lastKey)
			if dropped > 0 {
				b.Metrics.WindowDropped.Incr(int64(dropped))
			}
			b.Metrics.WindowHeld.Set(int64(win.HeldItems()))
			b.State.Poke()
			select {
			case <-released:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		SegmentSealed: func(ctx context.Context, segment int) error {
			win.Abort(segment)
			if err := emitter.SealSegment(ctx, segment); err != nil {
				b.Metrics.CheckpointFailures.Incr(1)
				b.Logger.Warnf("Failed to persist completion for snapshot segment %d of table %s (will retry once all acks drain): %v", segment, b.Table, err)
			}
			return nil
		},
		SegmentComplete: func(_ int, duration time.Duration, _ int64) {
			b.Metrics.SegmentDuration.Timing(duration.Nanoseconds())
		},
	}

	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	releaseErr := make(chan error, 1)
	go func() {
		tick := b.ReleaseTick
		if tick == 0 {
			tick = defaultReleaseTick
		}
		ticker := time.NewTicker(tick)
		defer ticker.Stop()
		for {
			select {
			case <-scanCtx.Done():
				releaseErr <- nil
				return
			case <-ticker.C:
			case <-b.State.nudge:
			}
			err := win.ReleaseReady(b.State.CanRelease, func(p ReleasedPage) error {
				b.Metrics.WindowWait.Timing(time.Since(p.High).Nanoseconds())
				return emitter.Emit(scanCtx, p.Items, p.Segment, p.Cursor)
			})
			b.Metrics.WindowHeld.Set(int64(win.HeldItems()))
			if err != nil {
				releaseErr <- err
				cancel()
				return
			}
		}
	}()

	scanErr := b.Deps.Scan(scanCtx, progress, hooks)
	cancel()
	rerr := <-releaseErr
	// The scanner and the release loop have both stopped, so nothing races
	// the reset. A failed run must not leave a held page behind: the next run
	// reuses this window, and its Begin would panic on it or its release loop
	// would send it through the new emitter.
	defer func() {
		if retErr != nil {
			win.Reset()
			b.Metrics.WindowHeld.Set(0)
		}
	}()
	if rerr != nil && !errors.Is(rerr, context.Canceled) {
		return fmt.Errorf("releasing snapshot pages for table %s: %w", b.Table, rerr)
	}
	if scanErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("incremental snapshot scan for table %s: %w", b.Table, scanErr)
	}

	if err := emitter.WaitAcked(ctx); err != nil {
		return err
	}
	if err := emitter.FlushCompleted(ctx); err != nil {
		b.Metrics.CheckpointFailures.Incr(1)
		b.Logger.Warnf("Failed to re-drive snapshot completion writes for table %s: %v", b.Table, err)
	}
	if err := b.Deps.MarkSnapshotComplete(ctx); err != nil {
		return fmt.Errorf("marking snapshot complete for table %s: %w", b.Table, err)
	}
	b.Logger.Infof("Incremental snapshot of table %s complete", b.Table)
	return nil
}

// Prepare resets a completed snapshot whose stream checkpoint is stale (the
// stream trimmed changes the backfill no longer covers), so the table is
// backfilled again. It reports skip=true when the snapshot is complete and
// still valid, and reset=true when it just reset a stale snapshot.
// signalOnly is true when signal_table_name is set, so backfills run only
// on request.
func (b *Backfill[P]) Prepare(ctx context.Context, signalOnly bool) (skip, reset bool, err error) {
	progress, err := b.Deps.SnapshotProgress(ctx)
	if err != nil {
		return false, false, fmt.Errorf("getting snapshot progress for table %s: %w", b.Table, err)
	}
	if !progress.IsComplete() {
		return false, false, nil
	}
	stale, err := b.Deps.CDCCheckpointStale(ctx)
	if err != nil {
		b.Logger.Warnf("Failed to check CDC checkpoint staleness for table %s, keeping the completed snapshot: %v", b.Table, err)
		return true, false, nil
	}
	if !stale {
		return true, false, nil
	}
	b.Logger.Warnf("CDC checkpoint for table %s is stale (stream data trimmed), re-running its incremental snapshot", b.Table)
	// In signal-only mode the reset table is queued only in memory, so the
	// request is made durable first: a crash before the first segment
	// checkpoint must not leave the table waiting for a signal.
	if signalOnly {
		if err := b.Deps.MarkSnapshotRequested(ctx); err != nil {
			return false, false, fmt.Errorf("recording snapshot request for table %s: %w", b.Table, err)
		}
	}
	if err := b.Deps.ResetSnapshotProgress(ctx); err != nil {
		return false, false, fmt.Errorf("resetting snapshot progress for table %s: %w", b.Table, err)
	}
	return false, true, nil
}

// SignalOnlyPending reports whether a table whose snapshot is not complete
// must be backfilled without waiting for a signal: either a backfill is in
// progress or a requested one has not completed. A failed read is logged
// and the table is not queued.
func (b *Backfill[P]) SignalOnlyPending(ctx context.Context) bool {
	progress, err := b.Deps.SnapshotProgress(ctx)
	if err != nil {
		b.Logger.Errorf("Incremental snapshot of table %s not started, it will be retried on the next connect: getting snapshot progress: %v", b.Table, err)
		return false
	}
	if progress.IsComplete() {
		return false
	}
	if progress.HasSegmentProgress() {
		return true
	}
	requested, err := b.Deps.SnapshotRequested(ctx)
	if err != nil {
		b.Logger.Errorf("Incremental snapshot of table %s not started, it will be retried on the next connect: %v", b.Table, err)
		return false
	}
	if !requested {
		b.Logger.Debugf("Incremental snapshot of table %s waits for a snapshot-execute signal", b.Table)
	}
	return requested
}
