// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// watermarkRecorder is a test seam for spannerCDCReader.updateWatermark.
type watermarkRecorder struct {
	mu     sync.Mutex
	writes []time.Time
}

func (w *watermarkRecorder) update(_ context.Context, _ string, ts time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes = append(w.writes, ts)
	return nil
}

func (w *watermarkRecorder) recorded() []time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]time.Time(nil), w.writes...)
}

func newTestSpannerReader(t *testing.T) (*spannerCDCReader, *spannerPartitionBatcher, *watermarkRecorder) {
	t.Helper()

	r := newSpannerCDCReader(spannerCDCInputConfig{}, service.BatchPolicy{Count: 1}, service.MockResources())
	rec := &watermarkRecorder{}
	r.updateWatermark = rec.update

	// Drain emitted messages so emit's channel send never blocks; acks are
	// driven explicitly by the tests via the returned ack.Once.
	go func() {
		for {
			select {
			case <-t.Context().Done():
				return
			case <-r.resCh:
			}
		}
	}()

	batcher, _, err := r.batcher.forPartition("p1")
	require.NoError(t, err)
	return r, batcher, rec
}

func testBatch() service.MessageBatch {
	return service.MessageBatch{service.NewMessage([]byte("{}"))}
}

func TestSpannerEmitOrderedWatermarks(t *testing.T) {
	t1 := time.Date(2026, 8, 6, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)
	t3 := t2.Add(time.Second)

	t.Run("out-of-order acks never advance past unacked batches", func(t *testing.T) {
		ctx := t.Context()
		r, batcher, rec := newTestSpannerReader(t)

		ack1, err := r.emit(ctx, batcher, "p1", testBatch(), t1)
		require.NoError(t, err)
		ack2, err := r.emit(ctx, batcher, "p1", testBatch(), t2)
		require.NoError(t, err)

		// Acking only the LATER batch must not write anything: its records
		// are durable but the earlier batch's are not. (The pre-fix code
		// wrote t2 here - the data-loss window.)
		require.NoError(t, ack2.Ack(ctx, nil))
		require.Empty(t, rec.recorded(), "watermark must not advance past a still-unacked earlier batch")

		// Acking the earlier batch resolves the full prefix: one write, t2.
		require.NoError(t, ack1.Ack(ctx, nil))
		require.Equal(t, []time.Time{t2}, rec.recorded())
	})

	t.Run("in-order acks advance incrementally", func(t *testing.T) {
		ctx := t.Context()
		r, batcher, rec := newTestSpannerReader(t)

		ack1, err := r.emit(ctx, batcher, "p1", testBatch(), t1)
		require.NoError(t, err)
		ack2, err := r.emit(ctx, batcher, "p1", testBatch(), t2)
		require.NoError(t, err)

		require.NoError(t, ack1.Ack(ctx, nil))
		require.NoError(t, ack2.Ack(ctx, nil))
		require.Equal(t, []time.Time{t1, t2}, rec.recorded())
	})

	t.Run("zero-watermark batches carry the last safe watermark forward", func(t *testing.T) {
		ctx := t.Context()
		r, batcher, rec := newTestSpannerReader(t)

		ack1, err := r.emit(ctx, batcher, "p1", testBatch(), t1)
		require.NoError(t, err)
		require.NoError(t, ack1.Ack(ctx, nil))
		require.Equal(t, []time.Time{t1}, rec.recorded())

		// b2 is a mid-record flush (zero watermark), b3 completes a record.
		ack2, err := r.emit(ctx, batcher, "p1", testBatch(), time.Time{})
		require.NoError(t, err)
		ack3, err := r.emit(ctx, batcher, "p1", testBatch(), t3)
		require.NoError(t, err)

		// Acking b3 alone must not advance past t1 (b2 is unacked; a repeat
		// write of the current safe watermark t1 is fine and idempotent).
		// The pre-fix code wrote t3 here - the data-loss window.
		require.NoError(t, ack3.Ack(ctx, nil))
		for _, w := range rec.recorded() {
			require.False(t, w.After(t1), "watermark advanced past an unacked batch: %v", w)
		}
		// Acking b2 resolves the full prefix through b3.
		require.NoError(t, ack2.Ack(ctx, nil))
		writes := rec.recorded()
		require.Equal(t, t3, writes[len(writes)-1])
	})

	t.Run("zero-watermark batch acked alone re-writes only the safe watermark", func(t *testing.T) {
		ctx := t.Context()
		r, batcher, rec := newTestSpannerReader(t)

		ack1, err := r.emit(ctx, batcher, "p1", testBatch(), t1)
		require.NoError(t, err)
		ack2, err := r.emit(ctx, batcher, "p1", testBatch(), time.Time{})
		require.NoError(t, err)

		require.NoError(t, ack1.Ack(ctx, nil))
		require.NoError(t, ack2.Ack(ctx, nil))

		// The carried-forward value is t1; writes never regress and never
		// mention a timestamp past the last completed record.
		writes := rec.recorded()
		require.NotEmpty(t, writes)
		for _, w := range writes {
			require.Equal(t, t1, w)
		}
	})
}

func TestSpannerConnectResetsPartitionBatchers(t *testing.T) {
	r, batcher, _ := newTestSpannerReader(t)
	_ = batcher

	// Same token returns the cached batcher before reset...
	_, existed, err := r.batcher.forPartition("p1")
	require.NoError(t, err)
	require.True(t, existed)

	// ...and a fresh one after the reset performed on (re)connect.
	r.resetPartitionBatchers()
	_, existed, err = r.batcher.forPartition("p1")
	require.NoError(t, err)
	require.False(t, existed, "reconnect must not reuse stale partition batcher state")
}
