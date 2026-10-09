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
	"sync"
	"time"

	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	incsnapshot "github.com/redpanda-data/connect/v4/internal/impl/aws/dynamodb/incrementalsnapshot"
)

// backfillTarget is one table's incremental backfill inputs.
type backfillTarget struct {
	table        string
	keySchema    []dynamodbtypes.KeySchemaElement
	checkpointer *Checkpointer
	state        *incsnapshot.State
}

// incrementalBackfill binds tgt to this input as an incsnapshot.Backfill.
// streamArn is the table's stream, used only by Prepare's staleness check.
func (d *dynamoDBCDCInput) incrementalBackfill(tgt backfillTarget, streamArn *string) *incsnapshot.Backfill[*SnapshotCheckpoint] {
	return &incsnapshot.Backfill[*SnapshotCheckpoint]{
		Table:     tgt.table,
		KeySchema: tgt.keySchema,
		State:     tgt.state,
		Deps:      &backfillDeps{Checkpointer: tgt.checkpointer, d: d, table: tgt.table, streamArn: streamArn},
		Metrics: incsnapshot.Metrics{
			WindowHeld:         d.metrics.snapshotWindowHeld,
			WindowDropped:      d.metrics.snapshotWindowDropped,
			WindowWait:         d.metrics.snapshotWindowWait,
			SegmentDuration:    d.metrics.snapshotSegmentDuration,
			CheckpointFailures: d.metrics.checkpointFailures,
		},
		Logger:      d.log,
		ReleaseTick: d.releaseTick,
	}
}

// runIncrementalBackfill runs tgt's backfill: see incsnapshot.Backfill.Run.
func (d *dynamoDBCDCInput) runIncrementalBackfill(ctx context.Context, tgt backfillTarget) error {
	// streamArn is deliberately nil: only Prepare reads it, and Run never does.
	return d.incrementalBackfill(tgt, nil).Run(ctx)
}

// prepareIncrementalBackfill readies tgt's backfill: see
// incsnapshot.Backfill.Prepare.
func (d *dynamoDBCDCInput) prepareIncrementalBackfill(ctx context.Context, tgt backfillTarget, streamArn *string) (skip, reset bool, err error) {
	return d.incrementalBackfill(tgt, streamArn).Prepare(ctx, d.conf.signalTable != "")
}

// backfillDeps is incsnapshot.Deps for one table. The embedded Checkpointer
// is the table's progress store.
type backfillDeps struct {
	*Checkpointer
	d         *dynamoDBCDCInput
	table     string
	streamArn *string
}

var _ incsnapshot.Deps[*SnapshotCheckpoint] = (*backfillDeps)(nil)

func (b *backfillDeps) CDCCheckpointStale(ctx context.Context) (bool, error) {
	return b.d.isCDCCheckpointStaleFor(ctx, b.Checkpointer, b.streamArn)
}

func (b *backfillDeps) NewEmitter() incsnapshot.Emitter {
	return &snapshotEmitter{
		d:       b.d,
		table:   b.table,
		tracker: newSnapshotAckTracker(b.Checkpointer, defaultSnapshotCheckpointBatchInterval, b.d.log),
		ackGate: new(sync.WaitGroup),
	}
}

func (b *backfillDeps) Scan(ctx context.Context, resume *SnapshotCheckpoint, hooks incsnapshot.ScanHooks) error {
	scanner := NewSnapshotScanner(SnapshotScannerConfig{
		Client:         b.d.dynamoClient,
		Table:          b.table,
		Segments:       b.d.conf.snapshot.segments,
		BatchSize:      b.d.conf.snapshot.batchSize,
		Throttle:       b.d.conf.snapshot.throttle,
		ConsistentRead: true,
		Logger:         b.d.log,
	})
	scanner.SetBeforeRequestCallback(hooks.BeforeRequest)
	scanner.SetBatchCallback(hooks.Batch)
	scanner.SetSegmentSealedCallback(hooks.SegmentSealed)
	scanner.SetSegmentCompleteCallback(hooks.SegmentComplete)
	return scanner.Scan(ctx, resume)
}

// snapshotEmitter is incsnapshot.Emitter for one backfill run: released
// pages go through handleSnapshotBatch with the run's own ack tracker and
// ack gate.
type snapshotEmitter struct {
	d       *dynamoDBCDCInput
	table   string
	tracker *snapshotAckTracker
	ackGate *sync.WaitGroup
}

func (e *snapshotEmitter) Emit(ctx context.Context, items []incsnapshot.Item, segment int, cursor incsnapshot.Item) error {
	return e.d.handleSnapshotBatch(ctx, items, segment, e.table, cursor, e.tracker, e.ackGate)
}

func (e *snapshotEmitter) SealSegment(ctx context.Context, segment int) error {
	return e.tracker.SealSegment(ctx, segment)
}

func (e *snapshotEmitter) WaitAcked(ctx context.Context) error {
	return waitAckGate(ctx, e.ackGate)
}

func (e *snapshotEmitter) FlushCompleted(ctx context.Context) error {
	return e.tracker.FlushCompleted(ctx)
}

// connectIncrementalSingle starts CDC readers, then backfills the table in
// the background through the incremental window.
func (d *dynamoDBCDCInput) connectIncrementalSingle(ctx context.Context, tableName string) error {
	if d.incremental == nil {
		// Tag discovery is multi-table by config, so no state was built at
		// init, but a single discovered table routes here. Readers have not
		// started yet, so nothing races this write.
		d.incremental = incsnapshot.NewState(d.conf.snapshot.watermarkMargin, d.conf.snapshot.idleShardGrace)
	}
	tgt := backfillTarget{table: tableName, keySchema: d.keySchema, checkpointer: d.checkpointer, state: d.incremental}
	skip, _, err := d.prepareIncrementalBackfill(ctx, tgt, d.streamArn)
	if err != nil {
		return err
	}
	// Registered before the coordinator starts, so it cannot close msgChan
	// while the backfill may still send on it. The coordinator cancels the
	// backfill before it waits, so a coordinator that stops early (a panic)
	// cannot wait forever on a page its stopped readers would never release.
	var (
		backfillCtx    context.Context
		backfillCancel context.CancelFunc
	)
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

// prepareTableBackfill decides whether a multi-table incremental table needs
// a backfill: it checks the stream view and resets a completed snapshot
// whose stream checkpoint is stale. It must run before the table's
// coordinator starts: once readers checkpoint, the shards that prove the
// checkpoint stale age out and the trimmed changes are never backfilled. A
// table that fails either step is logged and not queued; streaming it is
// unaffected, and the next connect retries it. With signal_table_name set,
// backfills are signal-only: see signalOnlyBackfillPending.
func (d *dynamoDBCDCInput) prepareTableBackfill(ctx context.Context, table string, ts *tableStream) bool {
	if ts.incremental == nil {
		return false
	}
	if err := incsnapshot.RequireNewImage(table, ts.streamSpec); err != nil {
		d.log.Errorf("Skipping incremental snapshot: %v (the table is still streamed)", err)
		return false
	}
	streamArn := ts.streamArn
	skip, reset, err := d.prepareIncrementalBackfill(ctx, ts.backfillTarget(), &streamArn)
	if err != nil {
		d.log.Errorf("Incremental snapshot of table %s not started, it will be retried on the next connect: %v", table, err)
		return false
	}
	if skip {
		return false
	}
	// A stale reset is data-loss recovery, so it runs without a signal.
	if d.conf.signalTable == "" || reset {
		return true
	}
	return d.signalOnlyBackfillPending(ctx, table, ts.checkpointer)
}

// signalOnlyBackfillPending reports whether table must be backfilled
// without waiting for a signal: see incsnapshot.Backfill.SignalOnlyPending.
// A failed read is logged and the table is not queued, as in
// prepareTableBackfill.
func (d *dynamoDBCDCInput) signalOnlyBackfillPending(ctx context.Context, table string, cp *Checkpointer) bool {
	// State, KeySchema and streamArn are deliberately unset: SignalOnlyPending reads only Table and the checkpointer.
	return d.incrementalBackfill(backfillTarget{table: table, checkpointer: cp}, nil).SignalOnlyPending(ctx)
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
