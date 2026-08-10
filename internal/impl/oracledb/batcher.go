// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

// batchPublisher is responsible processing individual events into a batch and flushing
// them to the pipeline using service.Batcher.
type batchPublisher struct {
	batcher   *service.Batcher
	batcherMu sync.Mutex

	checkpoint *checkpoint.Capped[replication.SCN]
	msgChan    chan asyncMessage
	cacheSCN   func(ctx context.Context, scn replication.SCN) error
	schemas    *schemaCache

	// snapshotAckWG counts published snapshot batches that have not yet been
	// acknowledged downstream. The snapshot->streaming handoff blocks on it so
	// the post-snapshot SCN is never persisted while snapshot rows are in flight.
	snapshotAckWG sync.WaitGroup
	// snapshotNackErr records the first snapshot batch nack. auto_replay_nacks
	// is user-toggleable, so a nack can be terminal: the gate must fail rather
	// than let the post-snapshot SCN persist over undelivered rows.
	snapshotNackMu  sync.Mutex
	snapshotNackErr error

	log     *service.Logger
	shutSig *shutdown.Signaller
}

// newBatchPublisher creates an instance of batchPublisher.
func newBatchPublisher(batcher *service.Batcher, checkpoint *checkpoint.Capped[replication.SCN], logger *service.Logger) *batchPublisher {
	b := &batchPublisher{
		batcher:    batcher,
		checkpoint: checkpoint,
		msgChan:    make(chan asyncMessage),
		log:        logger,
		shutSig:    shutdown.NewSignaller(),
	}
	go b.loop()
	return b
}

// loop creates a long-running process that periodically flushes batches by configured interval.
// lifted from internal/impl/kafka/franz_reader_ordered.go
func (p *batchPublisher) loop() {
	defer p.shutSig.TriggerHasStopped()

	// No need to loop when there's no batcher for async writes.
	if p.batcher == nil {
		return
	}

	var flushBatch <-chan time.Time
	var flushBatchTicker *time.Ticker
	adjustTimedFlush := func() {
		if flushBatch != nil || p.batcher == nil {
			return
		}

		// UntilNext reads the batcher's internal state, which concurrent
		// Publish calls mutate under batcherMu — take the same lock.
		p.batcherMu.Lock()
		tNext, exists := p.batcher.UntilNext()
		p.batcherMu.Unlock()
		if !exists {
			if flushBatchTicker != nil {
				flushBatchTicker.Stop()
				flushBatchTicker = nil
			}
			return
		}

		if flushBatchTicker != nil {
			flushBatchTicker.Reset(tNext)
		} else {
			flushBatchTicker = time.NewTicker(tNext)
		}
		flushBatch = flushBatchTicker.C
	}

	// hardStopCtx survives a soft stop so that an in-flight sendTracked send can
	// complete before the loop exits. Only a hard stop (triggered by Close)
	// cancels it, which is the forced-shutdown last resort.
	hardStopCtx, done := p.shutSig.HardStopCtx(context.Background())
	defer done()

	for {
		adjustTimedFlush()
		select {
		case <-flushBatch:
			var (
				tracked  *trackedBatch
				trackErr error
			)

			// Wrap this in a closure to make locking/unlocking easier. Track
			// happens under the same lock as the flush so the checkpoint
			// sequence matches flush order.
			func() {
				p.batcherMu.Lock()
				defer p.batcherMu.Unlock()

				flushBatch = nil
				if tNext, exists := p.batcher.UntilNext(); !exists || tNext > 1 {
					// This can happen if a pushed message triggered a batch before
					// the last known flush period. In this case we simply enter the
					// loop again which readjusts our flush batch timer.
					return
				}

				var sendBatch service.MessageBatch
				if sendBatch, _ = p.batcher.Flush(hardStopCtx); len(sendBatch) == 0 {
					return
				}
				tracked, trackErr = p.trackBatchLocked(hardStopCtx, sendBatch)
			}()
			if trackErr != nil {
				return
			}

			if tracked != nil {
				if err := p.sendTracked(hardStopCtx, tracked); err != nil {
					return
				}
			}
		case <-p.shutSig.SoftStopChan():
			return
		}
	}
}

// Publish turns the provided message into a service.Message before batching and
// flushing them based on batch size or time elapsed.
func (b *batchPublisher) Publish(ctx context.Context, m *replication.MessageEvent) error {
	// Resolve schema first — needed both for metadata and value coercion.
	var schemaAny any
	if b.schemas != nil {
		table := replication.UserTable{Schema: m.Schema, Name: m.Table}
		if m.ColumnMeta != nil {
			b.schemas.seedFromColumnMeta(table, m.ColumnMeta)
		}
		eventKeys := mapKeys(m.Data)
		s, typeInfo, sErr := b.schemas.schemaForEvent(ctx, table, eventKeys)
		if sErr != nil {
			b.log.Warnf("Failed to refresh schema for %s.%s: %v", m.Schema, m.Table, sErr)
		}
		schemaAny = s

		// Coerce streaming values to match snapshot types. Snapshot events
		// already have correct Go types from sql.Scan; only streaming events
		// (where LogMiner SQL_REDO quotes all INSERT values) need coercion.
		if m.Operation != replication.MessageOperationRead && typeInfo != nil {
			if dataMap, ok := m.Data.(map[string]any); ok {
				coerceStreamingValues(dataMap, typeInfo, b.log)
				// Oracle LogMiner omits NULL-valued columns from SQL_REDO, so
				// restore them explicitly to keep streaming records consistent
				// with snapshot records (which always include every column).
				for colName := range typeInfo.colTypes {
					if _, exists := dataMap[colName]; !exists {
						dataMap[colName] = nil
					}
				}
			}
		}
	}

	data, err := json.Marshal(m.Data)
	if err != nil {
		return fmt.Errorf("marshalling message: %w", err)
	}

	msg := service.NewMessage(data)
	msg.MetaSet("database_schema", m.Schema)
	msg.MetaSet("table_name", m.Table)
	msg.MetaSet("operation", m.Operation.String())
	if m.TransactionID != "" {
		msg.MetaSet("transaction_id", m.TransactionID)
	}
	if m.SCN.IsValid() {
		msg.MetaSet("scn", m.SCN.String())
	}
	if !m.Timestamp.IsZero() {
		// upcon connection go-ora automatically queries the server's timezone and stores
		// it in conn.dbServerTimeZone so it can convert the redo log timestamp
		// from database-local time to UTC
		msg.MetaSet("source_ts_ms", strconv.FormatInt(m.Timestamp.UnixMilli(), 10))
	}
	if m.CheckpointSCN.IsValid() {
		msg.MetaSet("checkpoint_scn", m.CheckpointSCN.String())
	}
	if !m.CommitTimestamp.IsZero() {
		msg.MetaSet("commit_ts_ms", strconv.FormatInt(m.CommitTimestamp.UnixMilli(), 10))
	}

	if schemaAny != nil {
		msg.MetaSetImmut("schema", service.ImmutableAny{V: schemaAny})
	}

	// Flush and Track must be atomic: Track order defines the checkpoint
	// sequence, so another flusher (the timed-flush loop) must not interleave
	// between our flush and our Track. Only the channel send happens outside
	// the lock.
	var tracked *trackedBatch
	b.batcherMu.Lock()
	if b.batcher.Add(msg) {
		var flushedBatch []*service.Message
		if flushedBatch, err = b.batcher.Flush(ctx); err == nil && len(flushedBatch) > 0 {
			tracked, err = b.trackBatchLocked(ctx, flushedBatch)
		}
	}
	b.batcherMu.Unlock()
	if err != nil {
		return fmt.Errorf("flushing batch due to reaching count limit: %w", err)
	}

	// If a batch was flushed, publish it outside the lock
	if tracked != nil {
		if err := b.sendTracked(ctx, tracked); err != nil {
			return fmt.Errorf("publishing flushed batch: %w", err)
		}
	}

	return nil
}

// trackedBatch pairs a ready-to-send asyncMessage with the bookkeeping needed
// to roll back its snapshot-gate slot if the send fails.
type trackedBatch struct {
	msg        asyncMessage
	isSnapshot bool
}

// trackBatchLocked registers the batch with the ordered checkpoint tracker and
// builds its ack function. It MUST be called with batcherMu held: Track order
// defines the checkpoint sequence, so it has to match flush order exactly.
func (b *batchPublisher) trackBatchLocked(ctx context.Context, batch service.MessageBatch) (*trackedBatch, error) {
	lastMsg := batch[len(batch)-1]

	// ensure we don't checkpoint snapshot batches
	isSnapshotBatch := false
	if op, ok := lastMsg.MetaGet("operation"); ok && op == replication.MessageOperationRead.String() {
		isSnapshotBatch = true
	}

	var checkpointSCN replication.SCN
	// Prefer checkpoint_scn. It accounts for open transactions.
	// Use scn if checkpoint_scn is absent.
	// Snapshot rows never carry checkpoint_scn.
	// All rows in one snapshot run share the same scn, captured once in Snapshot.Prepare().
	// So the fallback always selects that shared scn for snapshot batches.
	scnKey := "checkpoint_scn"
	if _, ok := lastMsg.MetaGet(scnKey); !ok {
		scnKey = "scn"
	}
	if scn, ok := lastMsg.MetaGet(scnKey); ok {
		var parseErr error
		checkpointSCN, parseErr = replication.ParseSCN(scn)
		if parseErr != nil {
			return nil, fmt.Errorf("parsing checkpoint SCN: %w", parseErr)
		}
	}

	resolveFn, err := b.checkpoint.Track(ctx, checkpointSCN, int64(len(batch)))
	if err != nil {
		return nil, fmt.Errorf("tracking SCN checkpoint for batch: %w", err)
	}
	if isSnapshotBatch {
		b.snapshotAckWG.Add(1)
	}
	return &trackedBatch{
		isSnapshot: isSnapshotBatch,
		msg: asyncMessage{
			msg: batch,
			ackFn: func(ctx context.Context, err error) error {
				if isSnapshotBatch {
					defer b.snapshotAckWG.Done()
				}
				if err != nil {
					// auto_replay_nacks is user-toggleable, so a nack can be
					// terminal. Never resolve: the checkpoint stays pinned
					// before this batch so nothing can be persisted past its
					// undelivered rows. Snapshot nacks additionally fail the
					// handoff gate so the post-snapshot SCN is not persisted.
					if isSnapshotBatch {
						b.recordSnapshotNack(err)
					}
					b.log.Errorf("Batch rejected downstream (snapshot=%v, checkpoint SCN %d): the checkpoint is now pinned before this batch and the input will stall once checkpoint_limit is reached, unless the batch is redelivered (auto_replay_nacks) or the pipeline restarts: %v", isSnapshotBatch, checkpointSCN, err)
					return err
				}
				scn := resolveFn()
				if scn == nil || !scn.IsValid() {
					return nil
				}
				if isSnapshotBatch && *scn <= checkpointSCN {
					// Resolved value is this snapshot batch's own shared SCN (or older) —
					// nothing new to persist, and persisting it would be premature.
					return nil
				}
				return b.cacheSCN(ctx, *scn)
			},
		},
	}, nil
}

func (b *batchPublisher) recordSnapshotNack(err error) {
	b.snapshotNackMu.Lock()
	defer b.snapshotNackMu.Unlock()
	if b.snapshotNackErr == nil {
		b.snapshotNackErr = err
	}
}

// resetSnapshotGate clears any nack recorded by a previous snapshot attempt so
// the gate reflects only the current run: the publisher outlives reconnects,
// and a stale error would fail every retry even after a clean re-run. The
// WaitGroup is deliberately left untouched — batches from a previous attempt
// that are still in flight can yet be acked or nacked, and both must keep
// counting.
func (b *batchPublisher) resetSnapshotGate() {
	b.snapshotNackMu.Lock()
	defer b.snapshotNackMu.Unlock()
	b.snapshotNackErr = nil
}

// sendTracked hands a tracked batch to ReadBatch. Must be called WITHOUT
// batcherMu held (the send blocks until consumed). A failed send releases the
// batch's snapshot-gate slot.
func (b *batchPublisher) sendTracked(ctx context.Context, tracked *trackedBatch) error {
	select {
	case b.msgChan <- tracked.msg:
		return nil
	case <-ctx.Done():
		if tracked.isSnapshot {
			b.snapshotAckWG.Done()
		}
		return ctx.Err()
	}
}

// waitSnapshotAcks blocks until every published snapshot batch has been
// acknowledged or nacked downstream, or until ctx is cancelled (the escape
// prevents a stalled downstream from wedging shutdown). Any nack fails the
// gate: with auto_replay_nacks disabled a nack is terminal, so the
// post-snapshot SCN must not be persisted and the snapshot must re-run.
func (b *batchPublisher) waitSnapshotAcks(ctx context.Context) error {
	drained := make(chan struct{})
	go func() {
		// May outlive this call if ctx fires first; bounded by process lifetime.
		b.snapshotAckWG.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		b.snapshotNackMu.Lock()
		defer b.snapshotNackMu.Unlock()
		if b.snapshotNackErr != nil {
			return fmt.Errorf("snapshot batch was rejected downstream: %w", b.snapshotNackErr)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// mapKeys extracts the keys from a map for use in drift detection.
func mapKeys(data any) []string {
	m, ok := data.(map[string]any)
	if !ok {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func (b *batchPublisher) msgs() <-chan asyncMessage {
	return b.msgChan
}

// flushCurrent flushes any partial batch still held by the batcher and
// publishes it, leaving the publisher loop running. Used at the
// snapshot->streaming handoff so every snapshot row is published (and can be
// awaited via waitSnapshotAcks) before the post-snapshot SCN is persisted.
func (b *batchPublisher) flushCurrent(ctx context.Context) error {
	if b.batcher == nil {
		return nil
	}
	var tracked *trackedBatch
	b.batcherMu.Lock()
	remaining, err := b.batcher.Flush(ctx)
	if err == nil && len(remaining) > 0 {
		tracked, err = b.trackBatchLocked(ctx, remaining)
	}
	b.batcherMu.Unlock()
	if err != nil || tracked == nil {
		return err
	}
	return b.sendTracked(ctx, tracked)
}

// FlushRemaining stops the loop goroutine and then flushes any partial batch
// still held in the batcher, blocking until it is consumed by ReadBatch.
func (b *batchPublisher) FlushRemaining(ctx context.Context) error {
	if b.batcher == nil {
		return nil
	}
	b.shutSig.TriggerSoftStop()
	<-b.shutSig.HasStoppedChan()
	return b.flushCurrent(ctx)
}

// Close signals the publisher's loop goroutine to stop and waits for it to exit.
// TriggerHardStop cancels the HardStopCtx used by the flush loop, unblocking any
// send that is waiting on msgChan when no consumer is left.
func (b *batchPublisher) Close() {
	b.shutSig.TriggerSoftStop()
	b.shutSig.TriggerHardStop()
	<-b.shutSig.HasStoppedChan()
	if b.batcher != nil {
		_ = b.batcher.Close(context.Background())
	}
}
