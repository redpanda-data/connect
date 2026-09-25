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
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
	"github.com/redpanda-data/connect/v4/internal/replication/ticket"
)

// batchPublisher is responsible processing individual events into a batch and flushing
// them to the pipeline using service.Batcher.
type batchPublisher struct {
	batcher *service.Batcher
	// batcherMu guards only the batcher's buffer (Add/Flush/UntilNext).
	batcherMu sync.Mutex
	// Flush tickets keep the checkpoint sequence exact without a lock held
	// across Track: each flush takes a ticket under batcherMu - atomically
	// with the Flush, so the user's batching policy stays exact - and
	// Track+send admission happens in ticket order. Track can block on
	// checkpoint_limit under downstream backpressure; only the admitted
	// ticket holder (and flushers queued behind it) waits, while Publish
	// calls keep buffering and the timed-flush ticker keeps reading
	// UntilNext. Admission is cancellable: an abandoned ticket is skipped
	// when its turn comes, so a graceful stop unwinds queued flushers
	// instead of wedging them behind a send parked under hardStopCtx.
	// A ticket that owns a flushed batch seals the queue when it is
	// abandoned. Admission is strictly ordered, so at that moment nothing
	// after the dropped rows has been tracked. The seal guarantees nothing
	// ever is, so no ack can persist an SCN past them before Connect
	// rebuilds the poisoned publisher (see poisoned). A drop path (a failed
	// Flush or trackBatch) also seals the queue, for the same reason.
	queue ticket.Lock
	// closed marks the batcher as torn down (guarded by batcherMu): Close's
	// batcher.Close races in-flight Publish calls otherwise, and the batcher
	// is not goroutine-safe.
	closed bool
	// stopping is set by the input's Close BEFORE any cancellation
	// propagates, so sendTracked can distinguish the expected
	// graceful-shutdown unwind (debug) from a send that fails while the
	// pipeline is meant to be live (warn) - the publisher's own shutSig is
	// triggered too late on the streaming path to make that call.
	stopping atomic.Bool
	// sendFailed is set when a tracked batch could not be handed to
	// ReadBatch: its checkpoint slot can never resolve.
	sendFailed atomic.Bool

	checkpoint *checkpoint.Capped[replication.SCN]
	msgChan    chan asyncMessage
	cacheSCN   func(ctx context.Context, scn replication.SCN) error
	schemas    *schemaCache

	// snapshotAckWG counts published snapshot batches that have not yet been
	// acknowledged downstream. The snapshot->streaming handoff blocks on it so
	// the post-snapshot SCN is never persisted while snapshot rows are in flight.
	snapshotAckWG sync.WaitGroup
	log           *service.Logger
	shutSig       *shutdown.Signaller
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

// poisoned reports whether this publisher can never checkpoint again, so
// Connect must rebuild it. This is true when rows were dropped (the flush
// queue is sealed) or when a checkpoint slot can never resolve (a failed
// send).
func (b *batchPublisher) poisoned() bool {
	return b.queue.Sealed() || b.sendFailed.Load()
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
				sendBatch, flushErr := p.batcher.Flush(hardStopCtx)
				var ticket uint64
				if flushErr == nil && len(sendBatch) > 0 {
					ticket = p.queue.Take()
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
					p.queue.Seal()
				}
				p.batcherMu.Unlock()
				if flushErr != nil {
					return fmt.Errorf("flushing timed batch: %w", flushErr)
				}
				if len(sendBatch) == 0 {
					return nil
				}

				return p.dispatch(hardStopCtx, ticket, sendBatch)
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
				// Oracle's redo retention walked past the checkpoint SCN.
				p.log.Errorf("Flush loop stopping after error; the session will reconnect, rebuild the publisher, and re-read from the last durable SCN: %v", err)
				return
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
	if m.Username != "" {
		msg.MetaSet("username", m.Username)
	}

	if schemaAny != nil {
		msg.MetaSetImmut("schema", service.ImmutableAny{V: schemaAny})
	}

	// Add and Flush are atomic under batcherMu so the user's batching policy
	// stays exact, and the flush ticket taken in the same critical section
	// pins this batch's position in the checkpoint sequence. Track+send then
	// run outside batcherMu in ticket order: a Track blocked on
	// checkpoint_limit stalls only the ticket queue, never concurrent
	// buffering or the timed-flush ticker.
	var (
		flushedBatch service.MessageBatch
		ticket       uint64
	)
	b.batcherMu.Lock()
	if b.closed {
		b.batcherMu.Unlock()
		return context.Canceled
	}
	if b.batcher.Add(msg) {
		if flushedBatch, err = b.batcher.Flush(ctx); err == nil && len(flushedBatch) > 0 {
			ticket = b.queue.Take()
		}
	}
	if err != nil {
		// The failed Flush drained rows that were never tracked. Seal BEFORE
		// releasing batcherMu: in the gap after the unlock another flusher
		// could flush, take the next ticket, and be admitted past the
		// dropped rows.
		b.queue.Seal()
	}
	b.batcherMu.Unlock()
	if err != nil {
		return fmt.Errorf("flushing batch due to reaching count limit: %w", err)
	}
	if len(flushedBatch) == 0 {
		return nil
	}

	return b.dispatch(ctx, ticket, flushedBatch)
}

// trackedBatch pairs a ready-to-send asyncMessage with the bookkeeping needed
// to roll back its snapshot-gate slot if the send fails.
type trackedBatch struct {
	msgs       asyncMessage
	isSnapshot bool
}

// dispatch admits the flush ticket, tracks the batch, and hands it to
// ReadBatch, applying the shared failure actions: a cancelled rows-owning
// admission seals inside queue.Acquire itself, and a track failure seals here since
// the rows already left the batcher while the deferred release lets later
// tickets proceed. A ticket with no batch (flushCurrent's barrier) passes
// through the empty skip after admission.
func (b *batchPublisher) dispatch(ctx context.Context, tkt uint64, batch service.MessageBatch) error {
	if err := b.queue.Acquire(ctx, tkt, len(batch) > 0); err != nil {
		if errors.Is(err, ticket.ErrSealed) {
			return fmt.Errorf("publisher flush queue sealed after an abandoned batch; reconnecting rebuilds the publisher: %w", err)
		}
		return err
	}
	defer b.queue.Release()
	if len(batch) == 0 {
		return nil
	}
	tracked, err := b.trackBatch(ctx, batch)
	if err != nil {
		// The rows left the batcher but were never tracked, and the deferred
		// release lets later tickets proceed: seal so nothing can be tracked
		// (and persisted) past the gap.
		b.queue.Seal()
		return err
	}
	return b.sendTracked(ctx, tracked)
}

// trackBatch registers the batch with the ordered checkpoint tracker and
// builds its ack function. It MUST be called by the admitted ticket holder:
// Track order defines the checkpoint sequence, so it has to match flush
// (ticket) order exactly. Track may block on checkpoint_limit, which is why
// batcherMu must NOT be held here.
func (b *batchPublisher) trackBatch(ctx context.Context, batch service.MessageBatch) (*trackedBatch, error) {
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
					b.log.Warnf("Dropping batch of %d messages rejected downstream (snapshot=%v, checkpoint SCN %s): auto_replay_nacks is disabled, so the checkpoint advances past the dropped rows: %v", len(batch), isSnapshotBatch, checkpointSCN, ackErr)
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

// sendTracked hands a tracked batch to ReadBatch. Must be called by the
// admitted ticket holder, never under batcherMu: the send blocks until
// consumed. A failed send releases the batch's snapshot-gate slot and sets
// sendFailed, which poisons the publisher.
func (b *batchPublisher) sendTracked(ctx context.Context, tracked *trackedBatch) error {
	select {
	case b.msgChan <- tracked.msgs:
		return nil
	case <-ctx.Done():
		if tracked.isSnapshot {
			b.snapshotAckWG.Done()
		}
		// The batch's checkpoint slot is registered but its ackFn will never
		// run, so the tracker is permanently pinned before this batch: set
		// sendFailed so Connect rebuilds the publisher with a fresh tracker.
		// Resolving the slot here instead would be unsafe - another flusher
		// may already have delivered a later-tracked batch, and its ack would
		// then persist an SCN past these undelivered rows.
		if b.stopping.Load() || b.shutSig.IsSoftStopSignalled() {
			// Expected on a graceful stop: nothing drains msgChan once
			// ReadBatch stops, and Close cancels this send. Not a fault.
			b.log.Debugf("Batch of %d messages undelivered at shutdown; its rows re-read from the last durable SCN on the next run", len(tracked.msgs.msg))
		} else {
			b.log.Warnf("Batch of %d messages could not be handed to the pipeline; the publisher is marked for rebuild and its rows re-read from the last durable SCN on reconnect", len(tracked.msgs.msg))
		}
		b.sendFailed.Store(true)
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
	b.batcherMu.Lock()
	if b.closed {
		b.batcherMu.Unlock()
		return context.Canceled
	}
	remaining, err := b.batcher.Flush(ctx)
	// The ticket is taken unconditionally - even when the batcher is empty -
	// so that admission below doubles as a sequence barrier: another flusher
	// (the timed loop) may already hold the final snapshot rows while parked
	// in checkpoint.Track, before it has counted them on the snapshot ack
	// gate. Being admitted proves every earlier flush has finished
	// trackBatch+send, so once flushCurrent returns the gate counts every
	// published snapshot batch and waitSnapshotAcks cannot release early.
	ticket := b.queue.Take()
	if err != nil {
		// The failed Flush may have drained rows that were never tracked.
		// Seal BEFORE releasing batcherMu: in the gap after the unlock
		// another flusher could flush, take the next ticket, and be admitted
		// past the dropped rows.
		b.queue.Seal()
	}
	b.batcherMu.Unlock()
	if err != nil {
		// The seal is already applied under batcherMu; return the real flush
		// error rather than letting the sealed refusal of Acquire mask it (the
		// operator needs the batching.processors failure, not the seal).
		return err
	}
	return b.dispatch(ctx, ticket, remaining)
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
		// The batcher is not goroutine-safe and session goroutines may still
		// be inside Publish: close it under batcherMu and mark it closed so
		// later flush paths refuse instead of touching a closed batcher.
		b.batcherMu.Lock()
		b.closed = true
		_ = b.batcher.Close(context.Background())
		b.batcherMu.Unlock()
	}
}
