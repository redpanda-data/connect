package sqlserver

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/sqlserver/replication"
)

type batchPublisher struct {
	batcher   *service.Batcher
	batcherMu sync.Mutex

	checkpoint *checkpoint.Capped[replication.LSN]
	msgChan    chan asyncMessage
	logger     *service.Logger
	cacheLSN   func(ctx context.Context, lsn replication.LSN) error
}

// newBatchPublisher creates a batchPublisher which is responsible for generating capturing messages for the purpose of batching
func newBatchPublisher(batcher *service.Batcher, checkpoint *checkpoint.Capped[replication.LSN], logger *service.Logger) *batchPublisher {
	b := &batchPublisher{
		batcher:    batcher,
		checkpoint: checkpoint,
		logger:     logger,
		msgChan:    make(chan asyncMessage),
	}
	return b
}

// startBatchFlusher periodically flushes the batch
func (b *batchPublisher) startBatchFlusher(ctx context.Context) {
	go func() {
		// user a Timer instead of a Ticker so we can reset it.
		var timer *time.Timer
		defer func() {
			if b.batcher != nil {
				b.batcher.Close(context.Background())
			}
			if timer != nil {
				timer.Stop()
			}
		}()

		// not needed if batcher does not exist
		if b.batcher == nil {
			return
		}

		for {
			d, ok := b.batcher.UntilNext()
			if !ok {
				// No flush scheduled, wait for cancellation
				select {
				case <-ctx.Done():
					return
				default:
					time.Sleep(50 * time.Millisecond) // cheap idle sleep
					continue
				}
			}

			if timer == nil {
				timer = time.NewTimer(d)
			} else {
				if !timer.Stop() {
					// Safely drain the channel
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(d)
			}

			// flush batch
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				// lock batcher from being added to whilst we flush it
				b.batcherMu.Lock()
				msgBatch, err := b.batcher.Flush(ctx)
				b.batcherMu.Unlock()
				if err != nil {
					b.logger.Errorf("timed flush batch error: %v", err)
					return
				}
				if err := b.publishBatch(ctx, b.checkpoint, msgBatch); err != nil {
					b.logger.Errorf("failed to flush periodic batch: %v", err)
				}
			}
		}
	}()
}

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
		if err := b.publishBatch(ctx, b.checkpoint, flushedBatch); err != nil {
			return fmt.Errorf("publishing flushed batch: %w", err)
		}
	}

	return nil
}

func (b *batchPublisher) publishBatch(ctx context.Context, checkpointer *checkpoint.Capped[replication.LSN], batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	lastMsg := batch[len(batch)-1]
	var checkpointLSN []byte
	// snapshot records don't have a start_lsn as we don't track those
	if lsn, ok := lastMsg.MetaGet("start_lsn"); ok {
		checkpointLSN = replication.LSN(lsn)
	}

	resolveFn, err := checkpointer.Track(ctx, checkpointLSN, int64(len(batch)))
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
