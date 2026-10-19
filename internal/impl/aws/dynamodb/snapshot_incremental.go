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
	"github.com/cenkalti/backoff/v4"

	incsnapshot "github.com/redpanda-data/connect/v4/internal/impl/aws/dynamodb/incrementalsnapshot"
)

// Retry pacing for a failed incremental backfill. These are deliberately not
// config fields: they pace recovery from an unexpected failure (a scan,
// release or checkpoint error that outlived the AWS SDK's own retries), not
// throughput, so there is nothing for a user to tune. The table keeps
// streaming while it waits, and the retry resumes from acknowledged
// progress, so the only cost of a long wait is a later backfill. The cap
// keeps a persistent failure from re-scanning the table often while still
// retrying within minutes once the cause clears.
const (
	backfillRetryInitialInterval = 30 * time.Second
	backfillRetryMultiplier      = 2
	backfillRetryMaxInterval     = 10 * time.Minute
)

// newBackfillRetryBackoff returns the backoff between retries of one
// table's failed incremental backfill. It never gives up.
func (d *dynamoDBCDCInput) newBackfillRetryBackoff() backoff.BackOff {
	if d.backfillRetryBackoff != nil {
		return d.backfillRetryBackoff()
	}
	boff := backoff.NewExponentialBackOff()
	boff.InitialInterval = backfillRetryInitialInterval
	boff.Multiplier = backfillRetryMultiplier
	boff.MaxInterval = backfillRetryMaxInterval
	boff.MaxElapsedTime = 0 // Never give up
	// No jitter: retries are rare and per table, so there is no herd to
	// spread, and the documented waits stay exact.
	boff.RandomizationFactor = 0
	boff.Reset()
	return boff
}

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
	// A failed prepare fails Connect, which benthos retries, so in
	// single-table mode the next connect re-runs it.
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
		if !d.runIncrementalSingleUntilDone(ctx, tgt) {
			d.log.Infof("Incremental snapshot of table %s interrupted by shutdown; it resumes from acknowledged progress on the next run", tableName)
			return
		}
		d.snapshot.endTime = time.Now()
		d.snapshot.state.Store(snapshotStateComplete)
		d.metrics.snapshotState.Set(int64(snapshotStateComplete))
	})
	return nil
}

// runIncrementalSingleUntilDone runs the single-table backfill, retrying a
// failed run with backoff until one succeeds. Streaming carries on
// throughout: the failure is not surfaced through ReadBatch, which would
// stop CDC delivery without ever reconnecting. Each retry runs Prepare
// again, so it resumes from the acknowledged checkpoint (or resets a
// snapshot that has gone stale meanwhile). It reports false once ctx ends.
func (d *dynamoDBCDCInput) runIncrementalSingleUntilDone(ctx context.Context, tgt backfillTarget) bool {
	boff := d.newBackfillRetryBackoff()
	err := d.runIncrementalBackfill(ctx, tgt)
	for err != nil {
		if ctx.Err() != nil {
			return false
		}
		wait := boff.NextBackOff()
		d.log.Errorf("Incremental snapshot of table %s failed, retrying in %v: %v", tgt.table, wait, err)
		d.snapshot.state.Store(snapshotStateFailed)
		d.metrics.snapshotState.Set(int64(snapshotStateFailed))
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		d.snapshot.state.Store(snapshotStateInProgress)
		d.metrics.snapshotState.Set(int64(snapshotStateInProgress))
		var skip bool
		if skip, _, err = d.prepareIncrementalBackfill(ctx, tgt, d.streamArn); err == nil && !skip {
			err = d.runIncrementalBackfill(ctx, tgt)
		}
	}
	return true
}

// prepareTableBackfill decides whether a multi-table incremental table needs
// a backfill: it checks the stream view and resets a completed or
// in-progress snapshot whose stream checkpoint is stale. It must run before
// the table's coordinator starts: once readers checkpoint, the shards that
// prove the checkpoint stale age out and the trimmed changes are never
// backfilled. A table whose stream view rules out a backfill is logged and
// reported as not needing one; a failed checkpoint read or reset is returned
// for prepareOrRetryTableBackfill to retry. With signal_table_name set,
// backfills are signal-only: see signalOnlyBackfillPending.
func (d *dynamoDBCDCInput) prepareTableBackfill(ctx context.Context, table string, ts *tableStream) (bool, error) {
	if ts.incremental == nil {
		return false, nil
	}
	if err := incsnapshot.RequireNewImage(table, ts.streamSpec); err != nil {
		d.log.Errorf("Skipping incremental snapshot: %v (the table is still streamed)", err)
		return false, nil
	}
	streamArn := ts.streamArn
	skip, reset, err := d.prepareIncrementalBackfill(ctx, ts.backfillTarget(), &streamArn)
	if err != nil {
		return false, err
	}
	if skip {
		return false, nil
	}
	// A stale reset is data-loss recovery, so it runs without a signal.
	if d.conf.signalTable == "" || reset {
		return true, nil
	}
	return d.signalOnlyBackfillPending(ctx, table, ts.checkpointer)
}

// prepareOrRetryTableBackfill is prepareTableBackfill for connect and
// discovery in multi-table mode, which never reconnects: a failed prepare is
// logged and retried in the background after the table's backoff until it
// succeeds or retryCtx ends, and a retry that finds the table in need of a
// backfill queues it. d.backfills must already exist. It reports whether
// the caller should queue the table now.
//
// ctx bounds the synchronous first attempt (Connect's context at connect
// time), so Connect's cancellation and deadline still apply to its AWS
// calls. retryCtx outlives it and bounds the background retries; it must
// end when the input stops.
//
// The table is prepare-pending from its publication in d.tableStreams
// (see initializeTableStream), or else from the first attempt, until an
// attempt succeeds, so runBackfillQueue never runs its backfill alongside
// a prepare (whose stale reset deletes the rows a live run writes). A push the queue skipped
// meanwhile, such as a snapshot-execute signal's, is honoured once the
// prepare succeeds.
func (d *dynamoDBCDCInput) prepareOrRetryTableBackfill(ctx, retryCtx context.Context, table string, ts *tableStream) bool {
	d.backfillRetries.beginPrepare(table)
	need, err := d.prepareTableBackfill(ctx, table, ts)
	if err == nil {
		return d.backfillRetries.endPrepare(table) || need
	}
	// ctx bounds only this synchronous attempt. If it ended (Connect was
	// cancelled or timed out) but the input is still running, the retry
	// still has to be scheduled, or the table would stay prepare-pending
	// and its signals deferred forever.
	if retryCtx.Err() != nil {
		return false
	}
	d.retryInBackground(retryCtx, d.logPrepareRetry(table, err), func(ctx context.Context) (time.Duration, bool) {
		need, err := d.prepareTableBackfill(ctx, table, ts)
		if err != nil {
			if ctx.Err() != nil {
				return 0, true
			}
			return d.logPrepareRetry(table, err), false
		}
		if d.backfillRetries.endPrepare(table) || need {
			d.backfills.Push(table)
		}
		return 0, true
	})
	return false
}

// logPrepareRetry logs table's failed prepare and returns its retry delay.
func (d *dynamoDBCDCInput) logPrepareRetry(table string, err error) time.Duration {
	wait := d.backfillRetries.next(table, d.newBackfillRetryBackoff)
	d.log.Errorf("Incremental snapshot of table %s not started, retrying in %v (the table is still streamed): %v", table, wait, err)
	return wait
}

// signalOnlyBackfillPending reports whether table must be backfilled
// without waiting for a signal: see incsnapshot.Backfill.SignalOnlyPending.
// A failed read is returned, as in prepareTableBackfill.
func (d *dynamoDBCDCInput) signalOnlyBackfillPending(ctx context.Context, table string, cp *Checkpointer) (bool, error) {
	// State, KeySchema and streamArn are deliberately unset: SignalOnlyPending reads only Table and the checkpointer.
	return d.incrementalBackfill(backfillTarget{table: table, checkpointer: cp}, nil).SignalOnlyPending(ctx)
}

// backfillRetries holds the backoff of each multi-table incremental table
// whose last prepare or backfill failed, and the tables whose prepare has
// not yet succeeded. Its mutex is a leaf lock: nothing else is taken or
// called while it is held.
type backfillRetries struct {
	mu    sync.Mutex
	boffs map[string]backoff.BackOff
	// preparing holds the prepare-pending tables. A value is true once
	// runBackfillQueue has skipped a push of the table while it was pending.
	preparing map[string]bool
}

// next returns table's next retry delay, starting a backoff from newBoff on
// its first failure.
func (r *backfillRetries) next(table string, newBoff func() backoff.BackOff) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	boff, exists := r.boffs[table]
	if !exists {
		if r.boffs == nil {
			r.boffs = map[string]backoff.BackOff{}
		}
		boff = newBoff()
		r.boffs[table] = boff
	}
	return boff.NextBackOff()
}

// reset forgets table's backoff once a backfill succeeds.
func (r *backfillRetries) reset(table string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.boffs, table)
}

// beginPrepare marks table prepare-pending.
func (r *backfillRetries) beginPrepare(table string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.preparing == nil {
		r.preparing = map[string]bool{}
	}
	if _, exists := r.preparing[table]; !exists {
		r.preparing[table] = false
	}
}

// endPrepare clears table's prepare-pending mark and backoff once its
// prepare succeeds. It reports whether a push was skipped meanwhile.
func (r *backfillRetries) endPrepare(table string) (skipped bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	skipped = r.preparing[table]
	delete(r.preparing, table)
	delete(r.boffs, table)
	return skipped
}

// skipIfPreparing reports whether table is prepare-pending, recording the
// skipped push so endPrepare can honour it.
func (r *backfillRetries) skipIfPreparing(table string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.preparing[table]; !exists {
		return false
	}
	r.preparing[table] = true
	return true
}

// retryInBackground calls attempt once wait has passed, and again after
// each delay it returns, until it reports done or ctx ends or the input
// soft-stops. The goroutine is a background worker, so the input's shutdown
// waits for it and none outlives the input.
func (d *dynamoDBCDCInput) retryInBackground(ctx context.Context, wait time.Duration, attempt func(context.Context) (next time.Duration, done bool)) {
	d.backgroundWorkers.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				d.log.Errorf("Background worker incremental snapshot retry panicked: %v", r)
			}
		}()
		ctx, cancel := d.shutSig.SoftStopCtx(ctx)
		defer cancel()
		timer := time.NewTimer(wait)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			if ctx.Err() != nil {
				return
			}
			next, done := attempt(ctx)
			if done {
				return
			}
			timer.Reset(next)
		}
	})
}

// backfillTarget returns the table's incremental backfill inputs.
func (ts *tableStream) backfillTarget() backfillTarget {
	return backfillTarget{table: ts.tableName, keySchema: ts.keySchema, checkpointer: ts.checkpointer, state: ts.incremental}
}

// runBackfillQueue backfills queued tables one at a time until ctx ends.
// Tables are queued only once prepareTableBackfill has found them in need of
// a backfill. A failed backfill does not hold up the queue: the table is
// pushed back after its own backoff, and the next table starts at once.
// Streaming the table is unaffected. No delayed push happens once ctx ends.
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
		// Its prepare re-pushes the table once it succeeds.
		if d.backfillRetries.skipIfPreparing(table) {
			d.log.Debugf("Incremental snapshot of table %s deferred until its prepare succeeds", table)
			continue
		}
		outcome, err := d.backfillTable(ctx, table, ts)
		switch outcome {
		case backfillStopped:
			return
		case backfillFailed:
			wait := d.backfillRetries.next(table, d.newBackfillRetryBackoff)
			d.log.Errorf("Incremental snapshot of table %s failed, retrying in %v (the table is still streamed): %v", table, wait, err)
			d.retryInBackground(ctx, wait, func(context.Context) (time.Duration, bool) {
				d.backfills.Push(table)
				return 0, true
			})
		default:
			d.backfillRetries.reset(table)
		}
	}
}

// backfillOutcome is how one queued table's backfill turn ended.
type backfillOutcome int

const (
	// backfillDone: the backfill completed, or was abandoned because its
	// coordinator stopped. Either way it is not retried in-process.
	backfillDone backfillOutcome = iota
	// backfillFailed: the backfill failed and should be retried.
	backfillFailed
	// backfillStopped: ctx ended.
	backfillStopped
)

// backfillTable runs one queued table's incremental backfill. The backfill
// is abandoned if the table's coordinator stops: its shard progress can no
// longer advance, so a held page would never release and the serial queue
// would wedge. On backfillFailed it returns the failure, for the caller to
// log with its retry delay.
func (d *dynamoDBCDCInput) backfillTable(ctx context.Context, table string, ts *tableStream) (backfillOutcome, error) {
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
			return backfillStopped, nil
		}
		if abandoned() {
			return backfillDone, nil
		}
		return backfillFailed, err
	}
	return backfillDone, nil
}
