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

// batchPublisher processes individual events into batches and flushes them
// to the pipeline using service.Batcher.
//
// Streaming (CDC) rows and snapshot rows are kept on entirely separate
// paths: streaming rows go through Publish, are tracked against the ordered
// SCN checkpoint tracker, and are delivered via msgs() to ReadBatch.
// Snapshot rows go through PublishSnapshot, are never registered with the
// checkpoint tracker, and are delivered via snapshotMsgs() to
// oracleDBCDCInput.SnapshotReadBatch. The framework-level SnapshotAsync ack
// barrier (see input_oracledb_cdc.go) is what now guarantees every snapshot
// batch is settled before the post-snapshot SCN is persisted, so this type
// no longer needs its own ack-counting gate for the snapshot phase.
type batchPublisher struct {
	batcher   *service.Batcher
	batcherMu sync.Mutex

	snapshotBatcher   *service.Batcher
	snapshotBatcherMu sync.Mutex

	checkpoint      *checkpoint.Capped[replication.SCN]
	msgChan         chan asyncMessage
	snapshotMsgChan chan asyncMessage

	cacheSCN func(ctx context.Context, scn replication.SCN) error
	schemas  *schemaCache

	log     *service.Logger
	shutSig *shutdown.Signaller
}

// newBatchPublisher creates an instance of batchPublisher.
func newBatchPublisher(batcher, snapshotBatcher *service.Batcher, checkpoint *checkpoint.Capped[replication.SCN], logger *service.Logger) *batchPublisher {
	b := &batchPublisher{
		batcher:         batcher,
		snapshotBatcher: snapshotBatcher,
		checkpoint:      checkpoint,
		msgChan:         make(chan asyncMessage),
		snapshotMsgChan: make(chan asyncMessage),
		log:             logger,
		shutSig:         shutdown.NewSignaller(),
	}
	go b.loop()
	return b
}

// loop creates a long-running process that periodically flushes the
// streaming batcher by configured interval. There's no equivalent timed
// flush for the snapshot batcher: SnapshotReadBatch drives it synchronously
// and flushes its trailing partial batch itself once the snapshot's row
// producer finishes (see flushSnapshotRemaining).
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

	// hardStopCtx survives a soft stop so that an in-flight send can
	// complete before the loop exits. Only a hard stop (triggered by Close)
	// cancels it, which is the forced-shutdown last resort.
	hardStopCtx, done := p.shutSig.HardStopCtx(context.Background())
	defer done()

	for {
		adjustTimedFlush()
		select {
		case <-flushBatch:
			var (
				tracked  asyncMessage
				hasMsg   bool
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
				hasMsg = trackErr == nil
			}()
			if trackErr != nil {
				return
			}

			if hasMsg {
				if err := p.sendStreaming(hardStopCtx, tracked); err != nil {
					return
				}
			}
		case <-p.shutSig.SoftStopChan():
			return
		}
	}
}

// buildMessage converts a replication.MessageEvent into a service.Message,
// applying schema resolution/coercion and metadata common to both streaming
// and snapshot rows.
func (b *batchPublisher) buildMessage(ctx context.Context, m *replication.MessageEvent) (*service.Message, error) {
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
		return nil, fmt.Errorf("marshalling message: %w", err)
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
	return msg, nil
}

// Publish turns the provided streaming (CDC) event into a service.Message
// before batching and flushing based on batch size or time elapsed. Every
// flushed batch is registered with the ordered SCN checkpoint tracker.
func (b *batchPublisher) Publish(ctx context.Context, m *replication.MessageEvent) error {
	msg, err := b.buildMessage(ctx, m)
	if err != nil {
		return err
	}

	// Flush and Track must be atomic: Track order defines the checkpoint
	// sequence, so another flusher (the timed-flush loop) must not interleave
	// between our flush and our Track. Only the channel send happens outside
	// the lock.
	var (
		tracked asyncMessage
		hasMsg  bool
	)
	b.batcherMu.Lock()
	if b.batcher.Add(msg) {
		var flushedBatch []*service.Message
		if flushedBatch, err = b.batcher.Flush(ctx); err == nil && len(flushedBatch) > 0 {
			tracked, err = b.trackBatchLocked(ctx, flushedBatch)
			hasMsg = err == nil
		}
	}
	b.batcherMu.Unlock()
	if err != nil {
		return fmt.Errorf("flushing batch due to reaching count limit: %w", err)
	}

	// If a batch was flushed, publish it outside the lock
	if hasMsg {
		if err := b.sendStreaming(ctx, tracked); err != nil {
			return fmt.Errorf("publishing flushed batch: %w", err)
		}
	}

	return nil
}

// trackBatchLocked registers a streaming batch with the ordered checkpoint
// tracker and builds its ack function. It MUST be called with batcherMu
// held: Track order defines the checkpoint sequence, so it has to match
// flush order exactly.
func (b *batchPublisher) trackBatchLocked(ctx context.Context, batch service.MessageBatch) (asyncMessage, error) {
	lastMsg := batch[len(batch)-1]

	var checkpointSCN replication.SCN
	// Prefer checkpoint_scn. It accounts for open transactions.
	// Use scn if checkpoint_scn is absent.
	scnKey := "checkpoint_scn"
	if _, ok := lastMsg.MetaGet(scnKey); !ok {
		scnKey = "scn"
	}
	if scn, ok := lastMsg.MetaGet(scnKey); ok {
		var parseErr error
		checkpointSCN, parseErr = replication.ParseSCN(scn)
		if parseErr != nil {
			return asyncMessage{}, fmt.Errorf("parsing checkpoint SCN: %w", parseErr)
		}
	}

	resolveFn, err := b.checkpoint.Track(ctx, checkpointSCN, int64(len(batch)))
	if err != nil {
		return asyncMessage{}, fmt.Errorf("tracking SCN checkpoint for batch: %w", err)
	}
	return asyncMessage{
		msg: batch,
		// The ack error is deliberately ignored: nacks are replayed by
		// auto_replay_nacks (the default), and disabling that is a
		// documented opt-in to DROP rejected messages, so the checkpoint
		// must advance past them rather than pin the tracker.
		ackFn: func(ctx context.Context, _ error) error {
			scn := resolveFn()
			if scn == nil || !scn.IsValid() {
				return nil
			}
			return b.cacheSCN(ctx, *scn)
		},
	}, nil
}

// sendStreaming hands a tracked streaming batch to ReadBatch. Must be called
// WITHOUT batcherMu held (the send blocks until consumed).
func (b *batchPublisher) sendStreaming(ctx context.Context, msg asyncMessage) error {
	select {
	case b.msgChan <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PublishSnapshot turns the provided snapshot row into a service.Message
// before batching and flushing based on batch size or time elapsed.
// Snapshot batches are never registered with the SCN checkpoint tracker:
// the SnapshotAsync ack barrier in AsyncReader (driven by
// oracleDBCDCInput.SnapshotReadBatch/SnapshotComplete) guarantees every
// batch is settled downstream before the post-snapshot SCN is persisted, so
// there's nothing here that needs ordering against the checkpoint.
func (b *batchPublisher) PublishSnapshot(ctx context.Context, m *replication.MessageEvent) error {
	msg, err := b.buildMessage(ctx, m)
	if err != nil {
		return err
	}

	var flushed service.MessageBatch
	b.snapshotBatcherMu.Lock()
	if b.snapshotBatcher.Add(msg) {
		flushed, err = b.snapshotBatcher.Flush(ctx)
	}
	b.snapshotBatcherMu.Unlock()
	if err != nil {
		return fmt.Errorf("flushing snapshot batch: %w", err)
	}

	if len(flushed) > 0 {
		if err := b.sendSnapshot(ctx, flushed); err != nil {
			return fmt.Errorf("publishing flushed snapshot batch: %w", err)
		}
	}
	return nil
}

// sendSnapshot hands a flushed snapshot batch to SnapshotReadBatch. Must be
// called WITHOUT snapshotBatcherMu held (the send blocks until consumed).
func (b *batchPublisher) sendSnapshot(ctx context.Context, batch service.MessageBatch) error {
	msg := asyncMessage{
		msg: batch,
		// No-op regardless of ack/nack: there's no checkpoint state tied to
		// a snapshot batch to resolve. A nack is replayed by
		// AutoRetryNacksBatched (via the framework's independent snapshot
		// retry list) exactly like a nacked streaming batch would be;
		// disabling auto_replay_nacks is the documented opt-in to drop it
		// instead. Either way this ackFn has nothing left to do.
		ackFn: func(context.Context, error) error { return nil },
	}
	select {
	case b.snapshotMsgChan <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// flushSnapshotRemaining flushes any partial batch still held by the
// snapshot batcher and publishes it. Called by SnapshotReadBatch once the
// snapshot's row producer (replication.Snapshot.Read) has returned, so the
// final trailing partial batch reaches the pipeline before signalling
// component.ErrSnapshotComplete.
func (b *batchPublisher) flushSnapshotRemaining(ctx context.Context) error {
	b.snapshotBatcherMu.Lock()
	remaining, err := b.snapshotBatcher.Flush(ctx)
	b.snapshotBatcherMu.Unlock()
	if err != nil || len(remaining) == 0 {
		return err
	}
	return b.sendSnapshot(ctx, remaining)
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

func (b *batchPublisher) snapshotMsgs() <-chan asyncMessage {
	return b.snapshotMsgChan
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
	if b.snapshotBatcher != nil {
		_ = b.snapshotBatcher.Close(context.Background())
	}
}
