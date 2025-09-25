// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlserver

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/sqlserver/replication"
)

// batchPublisher is responsible processing individual events into a batch and flushing them to the pipeline using service.Batcher.
type batchPublisher struct {
	batcher   *service.Batcher
	batcherMu sync.Mutex

	checkpoint *checkpoint.Capped[replication.LSN]
	msgChan    chan asyncMessage
	logger     *service.Logger
	cacheLSN   func(ctx context.Context, lsn replication.LSN) error
	shutSig    *shutdown.Signaller
}

// newBatchPublisher creates an instance of batchPublisher.
func newBatchPublisher(batcher *service.Batcher, checkpoint *checkpoint.Capped[replication.LSN], logger *service.Logger) *batchPublisher {
	b := &batchPublisher{
		batcher:    batcher,
		checkpoint: checkpoint,
		logger:     logger,
		msgChan:    make(chan asyncMessage),
		shutSig:    shutdown.NewSignaller(),
	}
	go b.loop()
	return b
}

// loop creates a long-running process that periodically flushes batches by configured interval.
// inspired by internal/impl/kafka/franz_reader_ordered.go
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

// Publish turns the provided message into a service.Message before batching and flushing them based on batch size or time elapsed.
func (b *batchPublisher) Publish(ctx context.Context, m replication.MessageEvent) error {
	data, err := json.Marshal(m.Data)
	if err != nil {
		return fmt.Errorf("failure to marshal message: %w", err)
	}
	msg := service.NewMessage(data)
	msg.MetaSet("table", m.Table)
	if len(m.LSN) != 0 {
		msg.MetaSet("start_lsn", string(m.LSN))
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
	var checkpointLSN []byte
	// snapshot records don't have a start_lsn as we don't track those
	if lsn, ok := lastMsg.MetaGet("start_lsn"); ok {
		checkpointLSN = replication.LSN(lsn)
	}

	resolveFn, err := b.checkpoint.Track(ctx, checkpointLSN, int64(len(batch)))
	if err != nil {
		return fmt.Errorf("failed to track LSN checkpoint for batch: %w", err)
	}
	msg := asyncMessage{
		msg: batch,
		ackFn: func(ctx context.Context, _ error) error {
			lsn := resolveFn()
			if lsn != nil && len(*lsn) != 0 {
				return b.cacheLSN(ctx, *lsn)
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

func (b *batchPublisher) msgs() <-chan asyncMessage {
	return b.msgChan
}
