// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/mssqlserver/replication"
)

// batchPublisher is responsible processing individual events into a batch and flushing
// them to the pipeline using service.Batcher.
type batchPublisher struct {
	batcher   *service.Batcher
	batcherMu sync.Mutex

	// tableSchemas caches the computed common schema for each table. No
	// invalidation is needed because MSSQL CDC capture instances are immutable:
	// an ALTER TABLE requires creating a new capture instance, which the input
	// won't discover until it restarts (at which point a fresh batchPublisher
	// with an empty cache is created).
	tableSchemas   map[string]any
	tableSchemasMu sync.RWMutex

	checkpoint *checkpoint.Capped[replication.LSN]
	msgChan    chan asyncMessage
	log        *service.Logger
	cacheLSN   func(ctx context.Context, lsn replication.LSN) error
	shutSig    *shutdown.Signaller

	// snapshotAckWG counts published snapshot batches that have not yet been
	// acknowledged downstream. The snapshot->streaming handoff blocks on it so
	// the post-snapshot LSN is never persisted while snapshot rows are in flight.
	snapshotAckWG sync.WaitGroup
	// snapshotNackErr records the first snapshot batch nack. auto_replay_nacks
	// is user-toggleable, so a nack can be terminal: the gate must fail rather
	// than let the post-snapshot LSN persist over undelivered rows.
	snapshotNackMu  sync.Mutex
	snapshotNackErr error

	// pendingCheckpointLSN mirrors the CheckpointLSN of the most recently
	// added message: the start LSN of the last transaction whose rows are all
	// published, the only value safe to persist as a resume position. Guarded
	// by batcherMu, so at flush time it always belongs to the flushed batch's
	// last message.
	pendingCheckpointLSN replication.LSN
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

// newBatchPublisher creates an instance of batchPublisher.
func newBatchPublisher(batcher *service.Batcher, checkpoint *checkpoint.Capped[replication.LSN], logger *service.Logger) *batchPublisher {
	b := &batchPublisher{
		batcher:      batcher,
		checkpoint:   checkpoint,
		log:          logger,
		msgChan:      make(chan asyncMessage),
		shutSig:      shutdown.NewSignaller(),
		tableSchemas: make(map[string]any),
	}
	go b.loop()
	return b
}

// loop creates a long-running process that periodically flushes batches by configured interval.
// lifted from internal/impl/kafka/franz_reader_ordered.go.
func (p *batchPublisher) loop() {
	defer func() {
		if p.batcher != nil {
			p.batcher.Close(context.Background())
		}
		p.shutSig.TriggerHasStopped()
	}()

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

	closeAtLeisureCtx, done := p.shutSig.SoftStopCtx(context.Background())
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
				if sendBatch, _ = p.batcher.Flush(closeAtLeisureCtx); len(sendBatch) == 0 {
					return
				}
				tracked, trackErr = p.trackBatchLocked(closeAtLeisureCtx, sendBatch)
			}()
			if trackErr != nil {
				return
			}

			if tracked != nil {
				if err := p.sendTracked(closeAtLeisureCtx, tracked); err != nil {
					return
				}
			}
		case <-p.shutSig.SoftStopChan():
			return
		}
	}
}

// getOrComputeTableSchema returns the cached schema for tableName. If not yet
// cached and colTypes is non-empty, it computes and caches the schema from the
// provided column metadata.
func (b *batchPublisher) getOrComputeTableSchema(tableName string, colNames []string, colTypes []*sql.ColumnType) any {
	b.tableSchemasMu.RLock()
	if s, ok := b.tableSchemas[tableName]; ok {
		b.tableSchemasMu.RUnlock()
		return s
	}
	b.tableSchemasMu.RUnlock()

	if len(colTypes) == 0 {
		return nil
	}

	s := columnTypesToSchema(tableName, colNames, colTypes)
	b.tableSchemasMu.Lock()
	b.tableSchemas[tableName] = s
	b.tableSchemasMu.Unlock()
	return s
}

// Publish turns the provided message into a service.Message before batching and
// flushing them based on batch size or time elapsed.
func (b *batchPublisher) Publish(ctx context.Context, m replication.MessageEvent) error {
	data, err := json.Marshal(m.Data)
	if err != nil {
		return fmt.Errorf("failure to marshal message: %w", err)
	}

	msg := service.NewMessage(data)
	msg.MetaSet("database_schema", m.Schema)
	msg.MetaSet("table", m.Table)
	msg.MetaSet("operation", m.Operation)
	if len(m.LSN) != 0 {
		msg.MetaSet("lsn", string(m.LSN))
	}
	if s := b.getOrComputeTableSchema(m.Table, m.ColumnNames, m.ColumnTypes); s != nil {
		msg.MetaSetImmut("schema", service.ImmutableAny{V: s})
	}

	// Flush and Track must be atomic: Track order defines the checkpoint
	// sequence, so another flusher (the timed-flush loop) must not interleave
	// between our flush and our Track. Only the channel send happens outside
	// the lock.
	var tracked *trackedBatch
	b.batcherMu.Lock()
	b.pendingCheckpointLSN = m.CheckpointLSN
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
	// Checkpoint only the pending checkpoint LSN: the last transaction whose
	// rows are all published. The row's own lsn must never be persisted — all
	// rows of a transaction share a start LSN and resume is exclusive (> lsn),
	// so persisting it mid-transaction would skip the transaction's remaining
	// rows on restart. Snapshot rows never carry one; we don't track those.
	checkpointLSN := []byte(b.pendingCheckpointLSN)

	// Snapshot batches are tracked so the snapshot->streaming handoff can block
	// until they are acknowledged downstream (see waitSnapshotAcks).
	isSnapshotBatch := false
	if op, ok := lastMsg.MetaGet("operation"); ok && op == replication.MessageOperationRead.String() {
		isSnapshotBatch = true
	}

	resolveFn, err := b.checkpoint.Track(ctx, checkpointLSN, int64(len(batch)))
	if err != nil {
		return nil, fmt.Errorf("tracking LSN checkpoint for batch: %w", err)
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
					// handoff gate so the post-snapshot LSN is not persisted.
					if isSnapshotBatch {
						b.recordSnapshotNack(err)
					}
					return err
				}
				lsn := resolveFn()
				if lsn != nil && len(*lsn) != 0 {
					return b.cacheLSN(ctx, *lsn)
				}
				return nil
			},
		},
	}, nil
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
// post-snapshot LSN must not be persisted and the snapshot must re-run.
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

// CheckpointWindow registers an empty marker slot carrying lsn with the
// ordered tracker, after flushing any partial batch belonging to the window.
// The marker resolves immediately, so lsn is persisted as soon as every batch
// published before it has been acked — giving the stream an exact resume
// position at each drained polling window instead of lagging one transaction
// behind (which would re-deliver the final transaction of a burst on every
// restart).
func (b *batchPublisher) CheckpointWindow(ctx context.Context, lsn replication.LSN) error {
	// Flush buffered rows first: they belong to the window, so the marker
	// must be tracked after them.
	if err := b.flushCurrent(ctx); err != nil {
		return fmt.Errorf("flushing window remainder: %w", err)
	}

	b.batcherMu.Lock()
	resolveFn, err := b.checkpoint.Track(ctx, lsn, 1)
	b.batcherMu.Unlock()
	if err != nil {
		return fmt.Errorf("tracking window checkpoint: %w", err)
	}
	// Resolve the marker immediately: if everything before it is already
	// acked this persists lsn now; otherwise the last outstanding ack's
	// resolve will surface it.
	if resolved := resolveFn(); resolved != nil && len(*resolved) != 0 {
		return b.cacheLSN(ctx, *resolved)
	}
	return nil
}

// flushCurrent flushes any partial batch still held by the batcher and
// publishes it, leaving the publisher loop running. Used at the
// snapshot->streaming handoff so every snapshot row is published (and can be
// awaited via waitSnapshotAcks) before the post-snapshot LSN is persisted.
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

func (b *batchPublisher) msgs() <-chan asyncMessage {
	return b.msgChan
}
