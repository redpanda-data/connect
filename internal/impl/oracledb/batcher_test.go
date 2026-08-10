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
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

func TestPublishBatch(t *testing.T) {
	t.Run("snapshot batches never persist via ackFn", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		msg := publishAndReceive(t, ctx, publisher, snapshotEvent(100))
		msg1 := publishAndReceive(t, ctx, publisher, snapshotEvent(100))

		require.NoError(t, msg.ackFn(ctx, nil))
		require.Empty(t, cachedSCNs(), "cacheSCN must not be called after acking only the first snapshot batch")

		require.NoError(t, msg1.ackFn(ctx, nil))
		require.Empty(t, cachedSCNs(), "cacheSCN must not be called after acking all snapshot batches")
	})

	t.Run("streaming batch still persists via ackFn", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		msg := publishAndReceive(t, ctx, publisher, streamingEvent(200))
		require.NoError(t, msg.ackFn(ctx, nil))

		scns := cachedSCNs()
		require.Len(t, scns, 1, "expected cacheSCN to be called exactly once for a streaming batch")
		require.Equal(t, replication.SCN(200), scns[0])
	})

	t.Run("mixed snapshot and streaming batch persists", func(t *testing.T) {
		ctx := t.Context()
		// Count=2 groups the snapshot and streaming events into one batch.
		publisher, cachedSCNs := newTestBatchPublisherWithCount(t, 2)

		got := make(chan asyncMessage, 1)
		go func() { got <- <-publisher.msgs() }()
		require.NoError(t, publisher.Publish(ctx, snapshotEvent(100)))
		require.NoError(t, publisher.Publish(ctx, streamingEvent(300)))

		var msg asyncMessage
		select {
		case msg = <-got:
			require.Len(t, msg.msg, 2)
		case <-time.After(5 * time.Second):
			t.Fatal("mixed batch was never published")
		}
		require.NoError(t, msg.ackFn(ctx, nil))

		scns := cachedSCNs()
		require.Len(t, scns, 1, "expected cacheSCN to be called exactly once for a batch whose last message is streaming")
		require.Equal(t, replication.SCN(300), scns[0], "expected the streaming SCN from the last message, not the leftover snapshot SCN")
	})

	t.Run("streaming SCN survives an out-of-order snapshot ack", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		snapshotMsg := publishAndReceive(t, ctx, publisher, snapshotEvent(100))
		streamingMsg := publishAndReceive(t, ctx, publisher, streamingEvent(200))

		require.NoError(t, streamingMsg.ackFn(ctx, nil))
		require.Empty(t, cachedSCNs(), "cacheSCN must not be called while the snapshot batch is still the unresolved head")
		require.NoError(t, snapshotMsg.ackFn(ctx, nil))

		scns := cachedSCNs()
		require.Len(t, scns, 1, "expected cacheSCN to be called exactly once when the snapshot batch resolves the streaming SCN")
		require.Equal(t, replication.SCN(200), scns[0], "expected the streaming batch's SCN to survive the out-of-order snapshot ack")
	})

	t.Run("a nacked batch pins the checkpoint", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		b1 := publishAndReceive(t, ctx, publisher, streamingEvent(200))
		b2 := publishAndReceive(t, ctx, publisher, streamingEvent(300))

		// Nack b1: with auto_replay_nacks disabled this is terminal, so b2's
		// ack must not persist anything past the undelivered b1.
		nackErr := errors.New("downstream failure")
		require.ErrorIs(t, b1.ackFn(ctx, nackErr), nackErr)
		require.NoError(t, b2.ackFn(ctx, nil))

		require.Empty(t, cachedSCNs(), "a checkpoint must never be persisted past a nacked batch")
	})
}

func TestSnapshotAckGate(t *testing.T) {
	t.Run("blocks until the snapshot batch is acked", func(t *testing.T) {
		ctx := t.Context()
		publisher, _ := newTestBatchPublisher(t)

		msg := publishAndReceive(t, ctx, publisher, snapshotEvent(100))

		done := make(chan error, 1)
		go func() { done <- publisher.waitSnapshotAcks(ctx) }()

		select {
		case err := <-done:
			t.Fatalf("waitSnapshotAcks returned before the snapshot batch was acked: %v", err)
		case <-time.After(100 * time.Millisecond):
		}

		require.NoError(t, msg.ackFn(ctx, nil))
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("waitSnapshotAcks did not return after the snapshot batch was acked")
		}
	})

	t.Run("a nack releases the gate but fails it", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		msg := publishAndReceive(t, ctx, publisher, snapshotEvent(100))
		nackErr := errors.New("downstream failure")
		require.ErrorIs(t, msg.ackFn(ctx, nackErr), nackErr)

		// auto_replay_nacks is user-toggleable, so a nack can be terminal:
		// the gate must report it so the post-snapshot SCN is not persisted
		// and the snapshot re-runs on restart.
		err := publisher.waitSnapshotAcks(ctx)
		require.ErrorIs(t, err, nackErr)
		require.Empty(t, cachedSCNs())
	})

	t.Run("streaming batches do not hold the gate", func(t *testing.T) {
		ctx := t.Context()
		publisher, _ := newTestBatchPublisher(t)

		// Published but never acked: must not block the gate.
		publishAndReceive(t, ctx, publisher, streamingEvent(200))

		require.NoError(t, publisher.waitSnapshotAcks(ctx))
	})

	t.Run("a nack fails only the snapshot attempt it belongs to", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		// Run 1: a snapshot batch is nacked; the gate fails.
		msg := publishAndReceive(t, ctx, publisher, snapshotEvent(100))
		nackErr := errors.New("downstream failure")
		require.ErrorIs(t, msg.ackFn(ctx, nackErr), nackErr)
		require.ErrorIs(t, publisher.waitSnapshotAcks(ctx), nackErr)

		// Run 2 (reconnect reuses the publisher): the gate is reset, the
		// re-run snapshot acks cleanly, and the gate must pass — a stale
		// run-1 error here would livelock the input re-snapshotting forever.
		publisher.resetSnapshotGate()
		msg2 := publishAndReceive(t, ctx, publisher, snapshotEvent(100))
		require.NoError(t, msg2.ackFn(ctx, nil))
		require.NoError(t, publisher.waitSnapshotAcks(ctx))
		require.Empty(t, cachedSCNs())
	})

	t.Run("context cancellation escapes the gate", func(t *testing.T) {
		publisher, _ := newTestBatchPublisher(t)

		ctx, cancel := context.WithCancel(t.Context())
		publishAndReceive(t, ctx, publisher, snapshotEvent(100))

		done := make(chan error, 1)
		go func() { done <- publisher.waitSnapshotAcks(ctx) }()
		cancel()

		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("waitSnapshotAcks did not return after context cancellation")
		}
	})
}

func TestFlushCurrent(t *testing.T) {
	ctx := t.Context()
	// Count=100 keeps published events buffered in the batcher until flushed.
	publisher, _ := newTestBatchPublisherWithCount(t, 100)

	publishEvent := func(v int) {
		t.Helper()
		e := snapshotEvent(100)
		e.Data = map[string]any{"a": v}
		require.NoError(t, publisher.Publish(ctx, e))
	}
	receive := func(failMsg string) {
		t.Helper()
		got := make(chan asyncMessage, 1)
		go func() { got <- <-publisher.msgs() }()
		require.NoError(t, publisher.flushCurrent(ctx))
		select {
		case m := <-got:
			require.Len(t, m.msg, 1)
		case <-time.After(5 * time.Second):
			t.Fatal(failMsg)
		}
	}

	publishEvent(1)
	receive("flushCurrent did not publish the buffered partial batch")

	// The loop must still be alive after flushCurrent (unlike FlushRemaining):
	// a second publish+flush must work identically.
	publishEvent(2)
	receive("publisher loop no longer functional after flushCurrent")
}

func TestTerminalNack(t *testing.T) {
	t.Run("invokes onTerminalNack so the input can restart", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		var got atomic.Value
		publisher.onTerminalNack = func(err error) { got.Store(err) }

		am := publishAndReceive(t, ctx, publisher, streamingEvent(200))
		nackErr := errors.New("downstream failure")
		require.ErrorIs(t, am.ackFn(ctx, nackErr), nackErr)

		stored, _ := got.Load().(error)
		require.ErrorIs(t, stored, nackErr)
		require.Empty(t, cachedSCNs())
	})

	t.Run("a sealed publisher neither persists nor restarts", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		restarted := false
		publisher.onTerminalNack = func(error) { restarted = true }

		am1 := publishAndReceive(t, ctx, publisher, streamingEvent(200))
		am2 := publishAndReceive(t, ctx, publisher, streamingEvent(300))
		publisher.seal()

		// Late ack from a replaced session: must not persist.
		require.NoError(t, am1.ackFn(ctx, nil))
		require.Empty(t, cachedSCNs(), "a sealed publisher must not persist checkpoints")

		// Late nack: must not trigger a restart of the new session.
		require.Error(t, am2.ackFn(ctx, errors.New("late failure")))
		require.False(t, restarted, "a sealed publisher must not trigger restarts")
	})
}

// TestTrackOrderUnderConcurrentFlush stresses the two concurrent flushers (the
// count-triggered flush in Publish and the timed-flush loop) and asserts the
// persisted checkpoint never regresses when batches are acked in delivery
// order. Before Track was moved under the batcher mutex, the two flushers
// could interleave between flush and Track, registering batches with the
// ordered tracker in the wrong order and persisting a regressing SCN. Run with
// -race to also catch the underlying data race structurally.
func TestTrackOrderUnderConcurrentFlush(t *testing.T) {
	ctx := t.Context()
	logger := service.NewLoggerFromSlog(slog.Default())
	cp := checkpoint.NewCapped[replication.SCN](1000)

	// Count 2 + a tiny period keeps both flush paths active concurrently.
	batcher, err := (service.BatchPolicy{Count: 2, Period: "1ms"}).NewBatcher(service.MockResources())
	require.NoError(t, err)

	publisher := newBatchPublisher(batcher, cp, logger)
	t.Cleanup(publisher.Close)

	var (
		mu        sync.Mutex
		persisted []replication.SCN
	)
	publisher.cacheSCN = func(_ context.Context, scn replication.SCN) error {
		mu.Lock()
		defer mu.Unlock()
		persisted = append(persisted, scn)
		return nil
	}

	// Consumer: ack every batch immediately, in delivery order.
	consumerDone := make(chan struct{})
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	go func() {
		defer close(consumerDone)
		for {
			select {
			case m := <-publisher.msgs():
				_ = m.ackFn(ctx, nil)
			case <-consumerCtx.Done():
				return
			}
		}
	}()

	const events = 500
	for i := range events {
		require.NoError(t, publisher.Publish(ctx, &replication.MessageEvent{
			Schema:        "S",
			Table:         "T",
			Operation:     replication.MessageOperationInsert,
			CheckpointSCN: replication.SCN(i + 1),
			Data:          map[string]any{"i": i},
		}))
	}
	require.NoError(t, publisher.flushCurrent(ctx))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(persisted) > 0 && persisted[len(persisted)-1] == replication.SCN(events)
	}, 10*time.Second, 10*time.Millisecond, "final SCN was never persisted")
	stopConsumer()
	<-consumerDone

	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < len(persisted); i++ {
		require.GreaterOrEqual(t, persisted[i], persisted[i-1],
			"persisted checkpoint regressed at index %d: %v", i, persisted)
	}
}

// newTestBatchPublisher builds a publisher whose batcher flushes on every
// published event (count=1), so tests drive the production
// Publish->trackBatchLocked->sendTracked path directly.
func newTestBatchPublisher(t *testing.T) (*batchPublisher, func() []replication.SCN) {
	t.Helper()
	return newTestBatchPublisherWithCount(t, 1)
}

func newTestBatchPublisherWithCount(t *testing.T, count int) (*batchPublisher, func() []replication.SCN) {
	t.Helper()

	logger := service.NewLoggerFromSlog(slog.Default())
	cp := checkpoint.NewCapped[replication.SCN](100)

	batcher, err := (service.BatchPolicy{Count: count}).NewBatcher(service.MockResources())
	require.NoError(t, err)

	publisher := newBatchPublisher(batcher, cp, logger)
	t.Cleanup(publisher.Close)

	var (
		mu         sync.Mutex
		cachedSCNs []replication.SCN
	)
	publisher.cacheSCN = func(_ context.Context, scn replication.SCN) error {
		mu.Lock()
		defer mu.Unlock()
		cachedSCNs = append(cachedSCNs, scn)
		return nil
	}

	cachedSCNsFn := func() []replication.SCN {
		mu.Lock()
		defer mu.Unlock()
		return append([]replication.SCN(nil), cachedSCNs...)
	}

	return publisher, cachedSCNsFn
}

func snapshotEvent(scn replication.SCN) *replication.MessageEvent {
	return &replication.MessageEvent{
		Schema:    "S",
		Table:     "T",
		Operation: replication.MessageOperationRead,
		SCN:       scn,
		Data:      map[string]any{"a": 1},
	}
}

func streamingEvent(checkpointSCN replication.SCN) *replication.MessageEvent {
	return &replication.MessageEvent{
		Schema:        "S",
		Table:         "T",
		Operation:     replication.MessageOperationInsert,
		CheckpointSCN: checkpointSCN,
		Data:          map[string]any{"a": 1},
	}
}

// publishAndReceive publishes a single event through the production Publish
// path (count=1 batcher: every event flushes, tracks, and sends immediately)
// and returns the delivered asyncMessage.
func publishAndReceive(t *testing.T, ctx context.Context, publisher *batchPublisher, event *replication.MessageEvent) asyncMessage {
	t.Helper()
	go func() {
		_ = publisher.Publish(ctx, event)
	}()
	return <-publisher.msgs()
}
