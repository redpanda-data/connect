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

		tNext, exists := p.batcher.UntilNext()
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
			var sendBatch service.MessageBatch

			// Wrap this in a closure to make locking/unlocking easier.
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

				if sendBatch, _ = p.batcher.Flush(closeAtLeisureCtx); len(sendBatch) == 0 {
					return
				}
			}()

			if len(sendBatch) > 0 {
				if err := p.publishBatch(closeAtLeisureCtx, sendBatch); err != nil {
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

	if schemaAny != nil {
		msg.MetaSetImmut("schema", service.ImmutableAny{V: schemaAny})
	}

	var flushedBatch []*service.Message
	b.batcherMu.Lock()
	if b.batcher.Add(msg) {
		flushedBatch, err = b.batcher.Flush(ctx)
	}
	b.batcherMu.Unlock()
	if err != nil {
		return fmt.Errorf("flushing batch due to reaching count limit: %w", err)
	}

	// If a batch was flushed, publish it outside the lock
	if len(flushedBatch) > 0 {
		if err := b.publishBatch(ctx, flushedBatch); err != nil {
			return fmt.Errorf("publishing flushed batch: %w", err)
		}
	}

	return nil
}

func (b *batchPublisher) publishBatch(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	lastMsg := batch[len(batch)-1]
	var checkpointSCN replication.SCN
	// Prefer checkpoint_scn (which accounts for open transactions) otherwise fall back to scn.
	// Snapshot records don't have an scn so we don't track those.
	scnKey := "checkpoint_scn"
	if _, ok := lastMsg.MetaGet(scnKey); !ok {
		scnKey = "scn"
	}
	if scn, ok := lastMsg.MetaGet(scnKey); ok {
		var parseErr error
		checkpointSCN, parseErr = replication.ParseSCN(scn)
		if parseErr != nil {
			return fmt.Errorf("parsing checkpoint SCN: %w", parseErr)
		}
	}

	resolveFn, err := b.checkpoint.Track(ctx, checkpointSCN, int64(len(batch)))
	if err != nil {
		return fmt.Errorf("tracking SCN checkpoint for batch: %w", err)
	}
	msg := asyncMessage{
		msg: batch,
		ackFn: func(ctx context.Context, _ error) error {
			scn := resolveFn()
			if scn != nil && scn.IsValid() {
				return b.cacheSCN(ctx, *scn)
			}
			return nil
		},
	}
	select {
	case b.msgChan <- msg:
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

// Close signals the publisher's loop goroutine to stop and waits for it to exit.
func (b *batchPublisher) Close() {
	b.shutSig.TriggerSoftStop()
	<-b.shutSig.HasStoppedChan()
}
