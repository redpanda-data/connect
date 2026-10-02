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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

// incrementalState is one table's incremental snapshot machinery: the page
// window and the shard progress tracker. Readers and shard refresh call its
// methods; every method is a no-op on a nil receiver, so tables outside
// snapshot_mode: incremental pay nothing.
type incrementalState struct {
	window   *incrementalWindow
	progress *shardProgress
	margin   time.Duration
	// nudge wakes the release loop early after progress moves.
	nudge chan struct{}
}

func newIncrementalState(margin, idleGrace time.Duration) *incrementalState {
	return &incrementalState{
		window:   newIncrementalWindow(),
		progress: newShardProgress(margin, idleGrace, time.Now),
		margin:   margin,
		nudge:    make(chan struct{}, 1),
	}
}

func (s *incrementalState) poke() {
	select {
	case s.nudge <- struct{}{}:
	default:
	}
}

// TouchRecords drops every held or in-flight snapshot item the records'
// keys touch. Readers call it before enqueueing the records.
func (s *incrementalState) TouchRecords(records []streamstypes.Record) int {
	if s == nil {
		return 0
	}
	dropped := 0
	for _, r := range records {
		if r.Dynamodb == nil {
			continue
		}
		if key, ok := windowKeyFromStream(r.Dynamodb.Keys); ok {
			dropped += s.window.Touch(key)
		}
	}
	return dropped
}

// ObserveRecords advances the shard to the newest record time. Readers call
// it after the records have been enqueued.
func (s *incrementalState) ObserveRecords(shardID string, records []streamstypes.Record) {
	if s == nil {
		return
	}
	for _, r := range records {
		if r.Dynamodb != nil && r.Dynamodb.ApproximateCreationDateTime != nil {
			s.progress.Observe(shardID, *r.Dynamodb.ApproximateCreationDateTime)
		}
	}
	s.poke()
}

func (s *incrementalState) ObserveIdle(shardID string, pollStart time.Time) {
	if s == nil {
		return
	}
	s.progress.ObserveIdle(shardID, pollStart)
	s.poke()
}

func (s *incrementalState) Exhausted(shardID string) {
	if s == nil {
		return
	}
	s.progress.Exhausted(shardID)
}

func (s *incrementalState) Register(shardID string) {
	if s == nil {
		return
	}
	s.progress.Register(shardID)
}

func (s *incrementalState) RefreshDone(described []string) {
	if s == nil {
		return
	}
	s.progress.RefreshDone(described)
	s.poke()
}

// canRelease reports whether a page read up to high may be released.
func (s *incrementalState) canRelease(high time.Time) bool {
	return s.progress.CaughtUpPast(high.Add(time.Second + s.margin))
}

// releaseTick bounds how long a releasable page waits when no progress
// nudge arrives. It is only a fallback poll interval, because releases are
// normally triggered by stream progress nudges, so changing it alters release
// latency by at most one tick. It is a var only so tests can shorten it.
var releaseTick = 250 * time.Millisecond

// backfillTarget is one table's incremental backfill inputs.
type backfillTarget struct {
	table        string
	keySchema    []dynamodbtypes.KeySchemaElement
	checkpointer *Checkpointer
	state        *incrementalState
}

// runIncrementalBackfill scans tgt.table page by page through its window,
// releasing each page once no stream record can still supersede it out of
// order, and marks the snapshot complete once every released item is acked.
func (d *dynamoDBCDCInput) runIncrementalBackfill(ctx context.Context, tgt backfillTarget) (retErr error) {
	progress, err := tgt.checkpointer.SnapshotProgress(ctx)
	if err != nil {
		return fmt.Errorf("getting snapshot progress for table %s: %w", tgt.table, err)
	}
	if progress.IsComplete() {
		d.log.Infof("Incremental snapshot of table %s already complete", tgt.table)
		return nil
	}

	tracker := newSnapshotAckTracker(tgt.checkpointer, defaultSnapshotCheckpointBatchInterval, d.log)
	ackGate := new(sync.WaitGroup)
	scanner := NewSnapshotScanner(SnapshotScannerConfig{
		Client:         d.dynamoClient,
		Table:          tgt.table,
		Segments:       d.conf.snapshot.segments,
		BatchSize:      d.conf.snapshot.batchSize,
		Throttle:       d.conf.snapshot.throttle,
		ConsistentRead: true,
		Logger:         d.log,
	})
	win := tgt.state.window
	scanner.SetBeforeRequestCallback(win.Begin)
	scanner.SetBatchCallback(func(ctx context.Context, items DynamoItems, segment int, lastKey map[string]dynamodbtypes.AttributeValue) error {
		high := time.Now()
		keys := make([]string, len(items))
		for i, it := range items {
			k, ok := windowKeyFromItem(it, tgt.keySchema)
			if !ok {
				return fmt.Errorf("table %s: scanned item has no usable primary key", tgt.table)
			}
			keys[i] = k
		}
		released, dropped := win.Hold(segment, items, keys, high, lastKey)
		if dropped > 0 {
			d.metrics.snapshotWindowDropped.Incr(int64(dropped))
		}
		d.metrics.snapshotWindowHeld.Set(int64(win.HeldItems()))
		tgt.state.poke()
		select {
		case <-released:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	scanner.SetSegmentSealedCallback(func(ctx context.Context, segment int) error {
		win.Abort(segment)
		if err := tracker.SealSegment(ctx, segment); err != nil {
			d.metrics.checkpointFailures.Incr(1)
			d.log.Warnf("Failed to persist completion for snapshot segment %d of table %s (will retry once all acks drain): %v", segment, tgt.table, err)
		}
		return nil
	})
	scanner.SetSegmentCompleteCallback(func(_ int, duration time.Duration, _ int64) {
		d.metrics.snapshotSegmentDuration.Timing(duration.Nanoseconds())
	})

	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	releaseErr := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(releaseTick)
		defer ticker.Stop()
		for {
			select {
			case <-scanCtx.Done():
				releaseErr <- nil
				return
			case <-ticker.C:
			case <-tgt.state.nudge:
			}
			err := win.ReleaseReady(tgt.state.canRelease, func(p releasedPage) error {
				d.metrics.snapshotWindowWait.Timing(time.Since(p.High).Nanoseconds())
				return d.handleSnapshotBatch(scanCtx, p.Items, p.Segment, tgt.table, p.LastKey, tracker, ackGate)
			})
			d.metrics.snapshotWindowHeld.Set(int64(win.HeldItems()))
			if err != nil {
				releaseErr <- err
				cancel()
				return
			}
		}
	}()

	scanErr := scanner.Scan(scanCtx, progress)
	cancel()
	rerr := <-releaseErr
	// The scanner and the release loop have both stopped, so nothing races
	// the reset. A failed run must not leave a held page behind: the next run
	// reuses this window, and its Begin would panic on it or its release loop
	// would send it through the new tracker.
	defer func() {
		if retErr != nil {
			win.Reset()
			d.metrics.snapshotWindowHeld.Set(0)
		}
	}()
	if rerr != nil && !errors.Is(rerr, context.Canceled) {
		return fmt.Errorf("releasing snapshot pages for table %s: %w", tgt.table, rerr)
	}
	if scanErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("incremental snapshot scan for table %s: %w", tgt.table, scanErr)
	}

	if err := waitAckGate(ctx, ackGate); err != nil {
		return err
	}
	if err := tracker.FlushCompleted(ctx); err != nil {
		d.metrics.checkpointFailures.Incr(1)
		d.log.Warnf("Failed to re-drive snapshot completion writes for table %s: %v", tgt.table, err)
	}
	if err := tgt.checkpointer.MarkSnapshotComplete(ctx); err != nil {
		return fmt.Errorf("marking snapshot complete for table %s: %w", tgt.table, err)
	}
	d.log.Infof("Incremental snapshot of table %s complete", tgt.table)
	return nil
}

// requireNewImage rejects stream views whose records carry no new image:
// the incremental window drops a snapshot item on the promise that the
// stream will deliver the item's current value.
func requireNewImage(tableName string, spec *dynamodbtypes.StreamSpecification) error {
	if spec == nil || !aws.ToBool(spec.StreamEnabled) {
		return fmt.Errorf("table %s: snapshot_mode incremental requires a stream", tableName)
	}
	switch spec.StreamViewType {
	case dynamodbtypes.StreamViewTypeNewImage, dynamodbtypes.StreamViewTypeNewAndOldImages:
		return nil
	default:
		return fmt.Errorf("table %s: snapshot_mode incremental requires stream view NEW_IMAGE or NEW_AND_OLD_IMAGES, got %s", tableName, spec.StreamViewType)
	}
}

// prepareIncrementalBackfill resets a completed snapshot whose stream
// checkpoint is stale (the stream trimmed changes the backfill no longer
// covers), so the table is backfilled again. It reports skip=true when the
// snapshot is complete and still valid.
func (d *dynamoDBCDCInput) prepareIncrementalBackfill(ctx context.Context, tgt backfillTarget, streamArn *string) (bool, error) {
	progress, err := tgt.checkpointer.SnapshotProgress(ctx)
	if err != nil {
		return false, fmt.Errorf("getting snapshot progress for table %s: %w", tgt.table, err)
	}
	if !progress.IsComplete() {
		return false, nil
	}
	stale, err := d.isCDCCheckpointStaleFor(ctx, tgt.checkpointer, streamArn)
	if err != nil {
		d.log.Warnf("Failed to check CDC checkpoint staleness for table %s, keeping the completed snapshot: %v", tgt.table, err)
		return true, nil
	}
	if !stale {
		return true, nil
	}
	d.log.Warnf("CDC checkpoint for table %s is stale (stream data trimmed), re-running its incremental snapshot", tgt.table)
	if err := tgt.checkpointer.ResetSnapshotProgress(ctx); err != nil {
		return false, fmt.Errorf("resetting snapshot progress for table %s: %w", tgt.table, err)
	}
	return false, nil
}

// connectIncrementalSingle starts CDC readers, then backfills the table in
// the background through the incremental window.
func (d *dynamoDBCDCInput) connectIncrementalSingle(ctx context.Context, tableName string) error {
	if d.incremental == nil {
		// Tag discovery is multi-table by config, so no state was built at
		// init, but a single discovered table routes here. Readers have not
		// started yet, so nothing races this write.
		d.incremental = newIncrementalState(d.conf.snapshot.watermarkMargin, d.conf.snapshot.idleShardGrace)
	}
	tgt := backfillTarget{table: tableName, keySchema: d.keySchema, checkpointer: d.checkpointer, state: d.incremental}
	skip, err := d.prepareIncrementalBackfill(ctx, tgt, d.streamArn)
	if err != nil {
		return err
	}
	// Registered before the coordinator starts, so it cannot close msgChan
	// while the backfill may still send on it. The coordinator cancels the
	// backfill before it waits, so a coordinator that stops early (a panic)
	// cannot wait forever on a page its stopped readers would never release.
	var backfillCtx context.Context
	var backfillCancel context.CancelFunc
	if !skip {
		d.msgSenders.Add(1)
		d.snapshot.startTime = time.Now()
		backfillCtx, backfillCancel = context.WithCancel(context.Background())
		d.backfillCancel = backfillCancel
	}
	if err := d.connectCDCOnly(ctx); err != nil {
		if !skip {
			backfillCancel()
			d.msgSenders.Done()
		}
		return err
	}
	if skip {
		return nil
	}
	d.snapshot.state.Store(snapshotStateInProgress)
	d.metrics.snapshotState.Set(int64(snapshotStateInProgress))
	d.startBackgroundWorker("incremental snapshot", func(ctx context.Context) {
		defer d.msgSenders.Done()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		defer backfillCancel()
		stop := context.AfterFunc(backfillCtx, cancel)
		defer stop()
		if err := d.runIncrementalBackfill(ctx, tgt); err != nil {
			if ctx.Err() != nil {
				d.log.Infof("Incremental snapshot of table %s interrupted by shutdown; it resumes from acknowledged progress on the next run", tableName)
				return
			}
			// Streaming carries on: the failure is not surfaced through
			// ReadBatch, which would stop CDC delivery without ever
			// reconnecting. The backfill is retried on the next connect.
			d.log.Errorf("Incremental snapshot of table %s failed, it will be retried on the next connect (the table is still streamed): %v", tableName, err)
			d.snapshot.state.Store(snapshotStateFailed)
			d.metrics.snapshotState.Set(int64(snapshotStateFailed))
			return
		}
		d.snapshot.endTime = time.Now()
		d.snapshot.state.Store(snapshotStateComplete)
		d.metrics.snapshotState.Set(int64(snapshotStateComplete))
	})
	return nil
}

// backfillQueue orders multi-table incremental backfills, one table at a
// time, deduplicating tables already waiting.
type backfillQueue struct {
	mu      sync.Mutex
	pending []string
	queued  map[string]struct{}
	signal  chan struct{}
}

func newBackfillQueue() *backfillQueue {
	return &backfillQueue{queued: map[string]struct{}{}, signal: make(chan struct{}, 1)}
}

// Push queues table unless it is already waiting. It never blocks.
func (q *backfillQueue) Push(table string) {
	q.mu.Lock()
	if _, ok := q.queued[table]; !ok {
		q.queued[table] = struct{}{}
		q.pending = append(q.pending, table)
	}
	q.mu.Unlock()
	select {
	case q.signal <- struct{}{}:
	default:
	}
}

// Pop returns the oldest queued table, blocking until one is pushed. It
// reports false once ctx ends.
func (q *backfillQueue) Pop(ctx context.Context) (string, bool) {
	for {
		q.mu.Lock()
		if len(q.pending) > 0 {
			t := q.pending[0]
			q.pending = q.pending[1:]
			delete(q.queued, t)
			q.mu.Unlock()
			return t, true
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", false
		case <-q.signal:
		}
	}
}

// Len returns the number of tables waiting.
func (q *backfillQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

// prepareTableBackfill decides whether a multi-table incremental table needs
// a backfill: it checks the stream view and resets a completed snapshot
// whose stream checkpoint is stale. It must run before the table's
// coordinator starts: once readers checkpoint, the shards that prove the
// checkpoint stale age out and the trimmed changes are never backfilled. A
// table that fails either step is logged and not queued; streaming it is
// unaffected, and the next connect retries it.
func (d *dynamoDBCDCInput) prepareTableBackfill(ctx context.Context, table string, ts *tableStream) bool {
	if ts.incremental == nil {
		return false
	}
	if err := requireNewImage(table, ts.streamSpec); err != nil {
		d.log.Errorf("Skipping incremental snapshot: %v (the table is still streamed)", err)
		return false
	}
	streamArn := ts.streamArn
	skip, err := d.prepareIncrementalBackfill(ctx, ts.backfillTarget(), &streamArn)
	if err != nil {
		d.log.Errorf("Incremental snapshot of table %s not started, it will be retried on the next connect: %v", table, err)
		return false
	}
	return !skip
}

// backfillTarget returns the table's incremental backfill inputs.
func (ts *tableStream) backfillTarget() backfillTarget {
	return backfillTarget{table: ts.tableName, keySchema: ts.keySchema, checkpointer: ts.checkpointer, state: ts.incremental}
}

// runBackfillQueue backfills queued tables one at a time until ctx ends.
// Tables are queued only once prepareTableBackfill has found them in need of
// a backfill. A failed backfill is logged and retried on the next connect;
// streaming the table is unaffected.
func (d *dynamoDBCDCInput) runBackfillQueue(ctx context.Context) {
	for {
		d.metrics.backfillTablesPending.Set(int64(d.backfills.Len()))
		table, ok := d.backfills.Pop(ctx)
		if !ok {
			return
		}
		d.metrics.backfillTablesPending.Set(int64(d.backfills.Len()))

		d.mu.RLock()
		ts := d.tableStreams[table]
		d.mu.RUnlock()
		if ts == nil || ts.incremental == nil {
			continue
		}
		if !d.backfillTable(ctx, table, ts) {
			return
		}
	}
}

// backfillTable runs one queued table's incremental backfill. The backfill
// is abandoned if the table's coordinator stops: its shard progress can no
// longer advance, so a held page would never release and the serial queue
// would wedge. It reports false once ctx ends.
func (d *dynamoDBCDCInput) backfillTable(ctx context.Context, table string, ts *tableStream) bool {
	tableCtx, cancel := context.WithCancel(ctx)
	watchDone := make(chan struct{})
	// Cancel before waiting, so the watcher always exits with this turn.
	defer func() {
		cancel()
		<-watchDone
	}()
	go func() {
		defer close(watchDone)
		select {
		case <-ts.coordinatorDone:
			cancel()
		case <-tableCtx.Done():
		}
	}()

	abandoned := func() bool {
		if ctx.Err() != nil {
			return false
		}
		select {
		case <-ts.coordinatorDone:
			d.log.Warnf("Incremental snapshot of table %s abandoned: its shard coordinator stopped; it will be retried on the next connect", table)
			return true
		default:
			return false
		}
	}

	if err := d.runIncrementalBackfill(tableCtx, ts.backfillTarget()); err != nil {
		if ctx.Err() != nil {
			d.log.Infof("Incremental snapshot of table %s interrupted by shutdown; it resumes from acknowledged progress on the next run", table)
			return false
		}
		if !abandoned() {
			d.log.Errorf("Incremental snapshot of table %s failed, it will be retried on the next connect: %v", table, err)
		}
	}
	return true
}
