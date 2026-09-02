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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/mssqlserver/replication"
)

// batchPublisher is responsible processing individual events into a batch and flushing
// them to the pipeline using service.Batcher.
type batchPublisher struct {
	batcher *service.Batcher
	// batcherMu guards only the batcher's buffer (Add/Flush/UntilNext) and
	// the pendingCheckpointLSN/buffered bookkeeping.
	batcherMu sync.Mutex
	// Flush tickets keep the checkpoint sequence exact without a lock held
	// across Track: each flush (and CheckpointWindow marker) takes a ticket
	// under batcherMu - atomically with the Flush, so the user's batching
	// policy stays exact - and Track+send admission happens in ticket order.
	// Track can block on checkpoint_limit under downstream backpressure;
	// only the admitted ticket holder (and flushers queued behind it) waits,
	// while Publish calls keep buffering and the timed-flush ticker keeps
	// reading UntilNext. Admission is cancellable: an abandoned ticket is
	// skipped when its turn comes, so a graceful stop unwinds queued
	// flushers instead of wedging them behind a parked send.
	ticketMu   sync.Mutex
	nextTicket uint64                   // next ticket to hand out; guarded by batcherMu
	admitted   uint64                   // next ticket allowed to Track+send; guarded by ticketMu
	waiters    map[uint64]chan struct{} // parked admit calls; guarded by ticketMu
	abandoned  map[uint64]struct{}      // cancelled tickets to skip; guarded by ticketMu
	// sealed refuses all further admissions (guarded by ticketMu): set when
	// an abandoned ticket owned a flushed batch. Admission is strictly
	// ordered, so at that moment nothing after the dropped rows has been
	// tracked - sealing guarantees nothing ever is, so no ack can persist an
	// LSN past them before Connect rebuilds the poisoned publisher.
	sealed bool
	// stopping is set by the input's Close BEFORE any cancellation
	// propagates, so sendTracked can distinguish the expected
	// graceful-shutdown unwind (debug) from a send that fails while the
	// pipeline is meant to be live (warn) - relying on the publisher's own
	// shutSig alone is racy on the streaming path.
	stopping atomic.Bool
	// closed marks the batcher as torn down (guarded by batcherMu): the
	// flush loop's deferred batcher.Close races in-flight Publish calls
	// otherwise, and the batcher is not goroutine-safe.
	closed bool
	// poisoned is set when a tracked batch could not be handed to ReadBatch:
	// its checkpoint slot can never resolve, so this publisher can never
	// checkpoint past it. Connect rebuilds a poisoned publisher.
	poisoned atomic.Bool

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
	// persistMu serializes resolve+persist pairs. The ordered tracker hands
	// out monotonically increasing frontiers, but ack functions and
	// CheckpointWindow run on different goroutines: without a shared critical
	// section around resolveFn()+cacheLSN, two persists can land out of order
	// and regress the cached resume position.
	persistMu sync.Mutex
	// pendingCheckpointLSN mirrors the CheckpointLSN of the most recently
	// added message (or a stronger drained-window LSN, see CheckpointWindow):
	// the start LSN of the last transaction whose rows are all published, the
	// only value safe to persist as a resume position. Guarded by batcherMu,
	// so at flush time it always belongs to the flushed batch's last message.
	pendingCheckpointLSN replication.LSN
	// buffered counts messages currently held by the batcher (guarded by
	// batcherMu). CheckpointWindow uses it to decide between deferring the
	// window checkpoint to the buffered batch and registering a marker.
	buffered int
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
	b.waiters = make(map[uint64]chan struct{})
	b.abandoned = make(map[uint64]struct{})
	go b.loop()
	return b
}

// takeTicketLocked hands out the next flush ticket. MUST be called with
// batcherMu held, atomically with the Flush that produced the batch, so
// ticket order is exactly flush order.
func (b *batchPublisher) takeTicketLocked() uint64 {
	t := b.nextTicket
	b.nextTicket++
	return t
}

// errQueueSealed refuses admission after an abandoned ticket dropped a
// flushed batch: nothing may be tracked past that gap until Connect rebuilds
// the poisoned publisher.
var errQueueSealed = errors.New("publisher flush queue sealed after an abandoned batch; reconnecting rebuilds the publisher")

// admit blocks until it is ticket's turn to Track+send, or ctx is cancelled.
// On success, pair with release. On cancellation the ticket is marked
// abandoned - release skips it when its turn comes - and the caller must NOT
// release it. ownsRows declares whether the ticket holds a non-empty flushed
// batch: such an abandon seals and poisons IN THE SAME critical section that
// records the abandonment, because the moment abandoned[ticket] is visible,
// a release from the previous holder may skip it and admit the next ticket -
// sealing any later would let that ticket track, deliver, and ack past the
// dropped rows before the seal lands. Row-less abandons (barrier tickets,
// window markers) skip benignly.
func (b *batchPublisher) admit(ctx context.Context, ticket uint64, ownsRows bool) error {
	b.ticketMu.Lock()
	if b.sealed {
		b.ticketMu.Unlock()
		return errQueueSealed
	}
	if b.admitted == ticket {
		b.ticketMu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	b.waiters[ticket] = ch
	b.ticketMu.Unlock()

	wake := func() error {
		b.ticketMu.Lock()
		defer b.ticketMu.Unlock()
		if b.sealed {
			return errQueueSealed
		}
		return nil
	}

	select {
	case <-ch:
		return wake()
	case <-ctx.Done():
		b.ticketMu.Lock()
		select {
		case <-ch:
			// Woken between cancellation and the lock: either admitted
			// normally (caller owns the release) or the queue was sealed.
			sealed := b.sealed
			b.ticketMu.Unlock()
			if sealed {
				return errQueueSealed
			}
			return nil
		default:
		}
		delete(b.waiters, ticket)
		b.abandoned[ticket] = struct{}{}
		if ownsRows {
			b.sealLocked()
		}
		b.ticketMu.Unlock()
		if ownsRows {
			b.poisoned.Store(true)
		}
		return ctx.Err()
	}
}

// sealLocked marks the queue sealed and wakes every waiter (they observe the
// seal and refuse). Caller must hold ticketMu.
func (b *batchPublisher) sealLocked() {
	b.sealed = true
	for t, ch := range b.waiters {
		close(ch)
		delete(b.waiters, t)
	}
}

// sealQueue permanently refuses further admissions and poisons the publisher:
// called when flushed-but-untracked rows were dropped (a failed Flush or
// trackBatch), so no later batch can be tracked (and therefore no ack can
// persist a position) past the dropped rows before Connect rebuilds. Safe to
// call while holding batcherMu: the established order is batcherMu before
// ticketMu, never the reverse.
func (b *batchPublisher) sealQueue() {
	b.ticketMu.Lock()
	b.sealLocked()
	b.ticketMu.Unlock()
	b.poisoned.Store(true)
}

// release passes the sequence to the next live ticket, skipping abandoned
// ones. Every ADMITTED ticket must be released exactly once, error paths
// included, or the sequence wedges.
func (b *batchPublisher) release() {
	b.ticketMu.Lock()
	b.admitted++
	for {
		if _, ok := b.abandoned[b.admitted]; !ok {
			break
		}
		delete(b.abandoned, b.admitted)
		b.admitted++
	}
	if ch, ok := b.waiters[b.admitted]; ok {
		close(ch)
		delete(b.waiters, b.admitted)
	}
	b.ticketMu.Unlock()
}

// loop creates a long-running process that periodically flushes batches by configured interval.
// lifted from internal/impl/kafka/franz_reader_ordered.go.
func (p *batchPublisher) loop() {
	defer func() {
		if p.batcher != nil {
			// The batcher is not goroutine-safe and in-flight Publish calls
			// may still be mutating it under batcherMu when a shutdown stops
			// this loop: close it under the same lock, and mark it closed so
			// later flush paths refuse instead of touching a closed batcher.
			p.batcherMu.Lock()
			p.closed = true
			_ = p.batcher.Close(context.Background())
			p.batcherMu.Unlock()
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
			flushBatch = nil
			if err := func() error {
				p.batcherMu.Lock()
				if tNext, exists := p.batcher.UntilNext(); !exists || tNext > 1 {
					// This can happen if a pushed message triggered a batch before
					// the last known flush period. In this case we simply enter the
					// loop again which readjusts our flush batch timer.
					p.batcherMu.Unlock()
					return nil
				}
				sendBatch, flushErr := p.batcher.Flush(closeAtLeisureCtx)
				var (
					checkpointLSN []byte
					ticket        uint64
				)
				if flushErr == nil {
					// Any successful Flush drains the buffer - including an
					// empty result when batching.processors filtered the
					// whole batch. Resetting only on non-empty flushes left
					// buffered stale, making CheckpointWindow defer window
					// checkpoints to a flush that had already happened.
					p.buffered = 0
				}
				if flushErr == nil && len(sendBatch) > 0 {
					checkpointLSN = []byte(p.pendingCheckpointLSN)
					ticket = p.takeTicketLocked()
				}
				if flushErr != nil {
					// Defensive: the current benthos Batcher.Flush never
					// assigns its error return (processor failures surface as
					// errored messages), so this branch is unreachable today -
					// but the signature declares the error, and if a future
					// version does fail here the drained rows were never
					// tracked. Seal BEFORE releasing batcherMu: in the gap
					// after the unlock another flusher could take the next
					// ticket and be admitted past the dropped rows.
					p.sealQueue()
				}
				p.batcherMu.Unlock()
				if flushErr != nil {
					return fmt.Errorf("flushing timed batch: %w", flushErr)
				}
				if len(sendBatch) == 0 {
					return nil
				}

				return p.dispatch(closeAtLeisureCtx, ticket, sendBatch, checkpointLSN)
			}(); err != nil {
				if p.stopping.Load() || p.shutSig.IsSoftStopSignalled() {
					// Expected when a shutdown cancels an in-flight flush:
					// the session is being torn down for good, not rebuilt.
					p.log.Debugf("Flush loop exiting during shutdown: %v", err)
					return
				}
				// With period-only batching this loop is the ONLY flusher, so
				// its death must be loud and recoverable: every live error
				// path has already poisoned the publisher, and ReadBatch
				// observes the stop below and returns ErrNotConnected so the
				// framework reconnects and Connect rebuilds. Without that, a
				// processor error here silently stalled the pipeline until
				// SQL Server's CDC retention walked past the checkpoint LSN.
				p.log.Errorf("Flush loop stopping after error; the session will reconnect, rebuild the publisher, and re-read from the last durable LSN: %v", err)
				return
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

	// Add and Flush are atomic under batcherMu so the user's batching policy
	// stays exact, and the flush ticket taken in the same critical section
	// pins this batch's position in the checkpoint sequence. Track+send then
	// run outside batcherMu in ticket order: a Track blocked on
	// checkpoint_limit stalls only the ticket queue, never concurrent
	// buffering or the timed-flush ticker.
	var (
		flushedBatch  service.MessageBatch
		checkpointLSN []byte
		ticket        uint64
	)
	b.batcherMu.Lock()
	if b.closed {
		b.batcherMu.Unlock()
		return context.Canceled
	}
	b.pendingCheckpointLSN = m.CheckpointLSN
	if b.batcher.Add(msg) {
		if flushedBatch, err = b.batcher.Flush(ctx); err == nil {
			// Any successful Flush drains the buffer, even one emptied by
			// batching.processors filtering - see the timed-flush path.
			b.buffered = 0
		}
		if err == nil && len(flushedBatch) > 0 {
			checkpointLSN = []byte(b.pendingCheckpointLSN)
			ticket = b.takeTicketLocked()
		}
	} else {
		b.buffered++
	}
	if err != nil {
		// The failed Flush drained rows that were never tracked. Seal BEFORE
		// releasing batcherMu: in the gap after the unlock another flusher
		// could flush, take the next ticket, and be admitted past the
		// dropped rows.
		b.sealQueue()
	}
	b.batcherMu.Unlock()
	if err != nil {
		return fmt.Errorf("flushing batch due to reaching count limit: %w", err)
	}
	if len(flushedBatch) == 0 {
		return nil
	}

	return b.dispatch(ctx, ticket, flushedBatch, checkpointLSN)
}

// trackedBatch pairs a ready-to-send asyncMessage with the bookkeeping needed
// to roll back its snapshot-gate slot if the send fails.
type trackedBatch struct {
	msgs       asyncMessage
	isSnapshot bool
}

// dispatch admits the flush ticket, tracks the batch, and hands it to
// ReadBatch, applying the shared failure actions: a cancelled rows-owning
// admission seals inside admit itself, and a track failure seals here since
// the rows already left the batcher while the deferred release lets later
// tickets proceed. A ticket with no batch (flushCurrent's barrier) passes
// through the empty skip after admission.
func (b *batchPublisher) dispatch(ctx context.Context, ticket uint64, batch service.MessageBatch, checkpointLSN []byte) error {
	if err := b.admit(ctx, ticket, len(batch) > 0); err != nil {
		return err
	}
	defer b.release()
	if len(batch) == 0 {
		return nil
	}
	tracked, err := b.trackBatch(ctx, batch, checkpointLSN)
	if err != nil {
		// The rows left the batcher but were never tracked, and the deferred
		// release lets later tickets proceed: seal so nothing can be tracked
		// (and persisted) past the gap.
		b.sealQueue()
		return err
	}
	return b.sendTracked(ctx, tracked)
}

// trackBatch registers the batch with the ordered checkpoint tracker and
// builds its ack function. It MUST be called by the admitted ticket holder:
// Track order defines the checkpoint sequence, so it has to match flush
// (ticket) order exactly. Track may block on checkpoint_limit, which is why
// batcherMu must NOT be held here. checkpointLSN is the pendingCheckpointLSN captured under
// batcherMu at flush time: the last transaction whose rows are all published
// (a row's own lsn must never be persisted — all rows of a transaction share
// a start LSN and resume is exclusive (> lsn), so persisting it
// mid-transaction would skip the transaction's remaining rows on restart;
// snapshot rows never carry one).
func (b *batchPublisher) trackBatch(ctx context.Context, batch service.MessageBatch, checkpointLSN []byte) (*trackedBatch, error) {
	lastMsg := batch[len(batch)-1]

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
		msgs: asyncMessage{
			msg: batch,
			// Nacks resolve like acks: they are replayed by auto_replay_nacks
			// (the default), and disabling that is a documented opt-in to DROP
			// rejected messages, so the checkpoint must advance past them
			// rather than pin the tracker. The drop is logged - it is the one
			// place rows become unrecoverable by design.
			ackFn: func(ctx context.Context, ackErr error) error {
				if isSnapshotBatch {
					defer b.snapshotAckWG.Done()
				}
				if ackErr != nil {
					b.log.Warnf("Dropping batch of %d messages rejected downstream (snapshot=%v, checkpoint LSN %X): auto_replay_nacks is disabled, so the checkpoint advances past the dropped rows: %v", len(batch), isSnapshotBatch, checkpointLSN, ackErr)
				}
				b.persistMu.Lock()
				defer b.persistMu.Unlock()
				lsn := resolveFn()
				if lsn != nil && len(*lsn) != 0 {
					return b.cacheLSN(ctx, *lsn)
				}
				return nil
			},
		},
	}, nil
}

// sendTracked hands a tracked batch to ReadBatch. Must be called by the
// admitted ticket holder, never under batcherMu: the send blocks until
// consumed. A failed send releases the batch's snapshot-gate slot and poisons
// the publisher.
func (b *batchPublisher) sendTracked(ctx context.Context, tracked *trackedBatch) error {
	select {
	case b.msgChan <- tracked.msgs:
		return nil
	case <-ctx.Done():
		if tracked.isSnapshot {
			b.snapshotAckWG.Done()
		}
		// The batch's checkpoint slot is registered but its ackFn will never
		// run, so the tracker is permanently pinned before this batch: mark
		// the publisher poisoned so Connect rebuilds it with a fresh tracker.
		// Resolving the slot here instead would be unsafe - another flusher
		// may already have delivered a later-tracked batch, and its ack would
		// then persist an LSN past these undelivered rows.
		if b.stopping.Load() || b.shutSig.IsSoftStopSignalled() {
			// Expected on a graceful stop: nothing drains msgChan once
			// ReadBatch stops, and Close cancels this send. Not a fault.
			b.log.Debugf("Batch of %d messages undelivered at shutdown; its rows re-read from the last durable LSN on the next run", len(tracked.msgs.msg))
		} else {
			b.log.Warnf("Batch of %d messages could not be handed to the pipeline; the publisher is marked for rebuild and its rows re-read from the last durable LSN on reconnect", len(tracked.msgs.msg))
		}
		b.poisoned.Store(true)
		return ctx.Err()
	}
}

// waitSnapshotAcks blocks until every published snapshot batch has been
// acknowledged (or nacked) downstream, or until ctx is cancelled. Nacked
// batches release the gate too: redelivery is owned by auto_replay_nacks,
// and disabling that is a documented opt-in to drop rejections. The ctx
// escape prevents a permanently-failing downstream from wedging shutdown.
func (b *batchPublisher) waitSnapshotAcks(ctx context.Context) error {
	drained := make(chan struct{})
	go func() {
		// May outlive this call if ctx fires first; bounded by process lifetime.
		b.snapshotAckWG.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CheckpointWindow records that every transaction up to and including lsn is
// fully published (a polling window drained), giving the stream an exact
// resume position instead of lagging one transaction behind (which would
// re-deliver the final transaction of a burst on every restart).
//
// The user's batching policy stays in charge of batch sizes: if rows from the
// window are still buffered, the window-end LSN simply becomes their batch's
// checkpoint payload (safe, and stronger than the last row's transaction
// boundary). Only when the batcher is empty is an immediately-resolved marker
// slot registered, so lsn persists once every published batch is acked.
func (b *batchPublisher) CheckpointWindow(ctx context.Context, lsn replication.LSN) error {
	b.batcherMu.Lock()
	if b.closed {
		b.batcherMu.Unlock()
		return context.Canceled
	}
	if b.buffered > 0 {
		b.pendingCheckpointLSN = lsn
		b.batcherMu.Unlock()
		return nil
	}
	ticket := b.takeTicketLocked()
	b.batcherMu.Unlock()

	// The marker joins the checkpoint sequence like any flush: it takes a
	// ticket so no later flush can Track ahead of it, and Track runs outside
	// batcherMu (it may block on checkpoint_limit).
	// The marker owns no rows: an abandoned marker drops nothing, so no
	// seal is needed - the next drained window re-marks.
	if err := b.admit(ctx, ticket, false); err != nil {
		return err
	}
	defer b.release()
	resolveFn, err := b.checkpoint.Track(ctx, lsn, 1)
	if err != nil {
		return fmt.Errorf("tracking window checkpoint: %w", err)
	}
	// Resolve the marker immediately: if everything before it is already
	// acked this persists lsn now; otherwise the last outstanding ack's
	// resolve will surface it.
	b.persistMu.Lock()
	defer b.persistMu.Unlock()
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
	b.batcherMu.Lock()
	if b.closed {
		b.batcherMu.Unlock()
		return context.Canceled
	}
	remaining, err := b.batcher.Flush(ctx)
	var checkpointLSN []byte
	if err == nil {
		// Any successful Flush drains the buffer, even one emptied by
		// batching.processors filtering.
		b.buffered = 0
	}
	if err == nil && len(remaining) > 0 {
		checkpointLSN = []byte(b.pendingCheckpointLSN)
	}
	// The ticket is taken unconditionally - even when the batcher is empty -
	// so that admission below doubles as a sequence barrier: another flusher
	// (the timed loop) may already hold the final snapshot rows while parked
	// in checkpoint.Track, before it has counted them on the snapshot ack
	// gate. Being admitted proves every earlier flush has finished
	// trackBatch+send, so once flushCurrent returns the gate counts every
	// published snapshot batch and waitSnapshotAcks cannot release early.
	ticket := b.takeTicketLocked()
	if err != nil {
		// The failed Flush may have drained rows that were never tracked.
		// Seal BEFORE releasing batcherMu: in the gap after the unlock
		// another flusher could flush, take the next ticket, and be admitted
		// past the dropped rows.
		b.sealQueue()
	}
	b.batcherMu.Unlock()
	if err != nil {
		// The seal is already applied under batcherMu; return the real flush
		// error rather than letting admit's sealed refusal mask it (the
		// operator needs the batching.processors failure, not the seal).
		return err
	}
	return b.dispatch(ctx, ticket, remaining, checkpointLSN)
}

func (b *batchPublisher) msgs() <-chan asyncMessage {
	return b.msgChan
}

// close stops the publisher's flush-loop goroutine and waits for it to exit.
// Used before a poisoned publisher is replaced; in-flight ack functions keep
// working against the abandoned tracker.
func (b *batchPublisher) close() {
	b.shutSig.TriggerSoftStop()
	<-b.shutSig.HasStoppedChan()
}
