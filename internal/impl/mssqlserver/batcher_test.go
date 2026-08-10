// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/mssqlserver/replication"
)

func TestSnapshotAckGate(t *testing.T) {
	t.Run("blocks until the snapshot batch is acked", func(t *testing.T) {
		ctx := t.Context()
		publisher, _ := newTestBatchPublisher(t)

		msg := publishAndReceive(t, ctx, publisher, snapshotEvent())

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
		publisher, cachedLSNs := newTestBatchPublisher(t)

		msg := publishAndReceive(t, ctx, publisher, snapshotEvent())
		nackErr := errors.New("downstream failure")
		require.ErrorIs(t, msg.ackFn(ctx, nackErr), nackErr)

		// auto_replay_nacks is user-toggleable, so a nack can be terminal:
		// the gate must report it so the post-snapshot LSN is not persisted
		// and the snapshot re-runs on restart.
		err := publisher.waitSnapshotAcks(ctx)
		require.ErrorIs(t, err, nackErr)
		require.Empty(t, cachedLSNs())
	})

	t.Run("streaming batches do not hold the gate", func(t *testing.T) {
		ctx := t.Context()
		publisher, _ := newTestBatchPublisher(t)

		// Published but never acked: must not block the gate.
		publishAndReceive(t, ctx, publisher, streamingEvent("00000030", ""))

		require.NoError(t, publisher.waitSnapshotAcks(ctx))
	})

	t.Run("a nack fails only the snapshot attempt it belongs to", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedLSNs := newTestBatchPublisher(t)

		// Run 1: a snapshot batch is nacked; the gate fails.
		msg := publishAndReceive(t, ctx, publisher, snapshotEvent())
		nackErr := errors.New("downstream failure")
		require.ErrorIs(t, msg.ackFn(ctx, nackErr), nackErr)
		require.ErrorIs(t, publisher.waitSnapshotAcks(ctx), nackErr)

		// Run 2 (reconnect reuses the publisher): the gate is reset, the
		// re-run snapshot acks cleanly, and the gate must pass — a stale
		// run-1 error here would livelock the input re-snapshotting forever.
		publisher.resetSnapshotGate()
		msg2 := publishAndReceive(t, ctx, publisher, snapshotEvent())
		require.NoError(t, msg2.ackFn(ctx, nil))
		require.NoError(t, publisher.waitSnapshotAcks(ctx))
		require.Empty(t, cachedLSNs())
	})

	t.Run("context cancellation escapes the gate", func(t *testing.T) {
		publisher, _ := newTestBatchPublisher(t)

		ctx, cancel := context.WithCancel(t.Context())
		publishAndReceive(t, ctx, publisher, snapshotEvent())

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

	publishEvent := func() {
		t.Helper()
		require.NoError(t, publisher.Publish(ctx, snapshotEvent()))
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

	publishEvent()
	receive("flushCurrent did not publish the buffered partial batch")

	// The loop must still be alive after flushCurrent: a second
	// publish+flush must work identically.
	publishEvent()
	receive("publisher loop no longer functional after flushCurrent")
}

func TestCheckpointSelection(t *testing.T) {
	t.Run("persists the transaction boundary, never the row's own lsn", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedLSNs := newTestBatchPublisher(t)

		am := publishAndReceive(t, ctx, publisher, streamingEvent("00000042", "00000041"))
		require.NoError(t, am.ackFn(ctx, nil))

		lsns := cachedLSNs()
		require.Len(t, lsns, 1)
		require.Equal(t, "00000041", string(lsns[0]),
			"the checkpoint must be the last fully-published transaction boundary, not the row's own LSN")
	})

	t.Run("no boundary yet (first transaction) persists nothing", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedLSNs := newTestBatchPublisher(t)

		am := publishAndReceive(t, ctx, publisher, streamingEvent("00000042", ""))
		require.NoError(t, am.ackFn(ctx, nil))

		require.Empty(t, cachedLSNs(),
			"a batch ending mid-transaction (no prior complete transaction) must not persist any LSN")
	})

	t.Run("a nacked batch pins the checkpoint", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedLSNs := newTestBatchPublisher(t)

		b1 := publishAndReceive(t, ctx, publisher, streamingEvent("00000042", "00000041"))
		b2 := publishAndReceive(t, ctx, publisher, streamingEvent("00000043", "00000042"))

		// Nack b1: with auto_replay_nacks disabled this is terminal, so b2's
		// ack must not persist anything past the undelivered b1.
		nackErr := errors.New("downstream failure")
		require.ErrorIs(t, b1.ackFn(ctx, nackErr), nackErr)
		require.NoError(t, b2.ackFn(ctx, nil))

		require.Empty(t, cachedLSNs(),
			"a checkpoint must never be persisted past a nacked batch")
	})
}

func TestCheckpointWindow(t *testing.T) {
	t.Run("persists immediately when all prior batches are acked", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedLSNs := newTestBatchPublisher(t)

		am := publishAndReceive(t, ctx, publisher, streamingEvent("00000042", ""))
		require.NoError(t, am.ackFn(ctx, nil))
		require.Empty(t, cachedLSNs(), "mid-transaction batch must not persist anything on its own")

		require.NoError(t, publisher.CheckpointWindow(ctx, replication.LSN("00000042")))

		lsns := cachedLSNs()
		require.Len(t, lsns, 1)
		require.Equal(t, "00000042", string(lsns[0]),
			"a drained window must checkpoint its exact end position")
	})

	t.Run("waits for outstanding acks before surfacing", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedLSNs := newTestBatchPublisher(t)

		am := publishAndReceive(t, ctx, publisher, streamingEvent("00000042", ""))
		require.NoError(t, publisher.CheckpointWindow(ctx, replication.LSN("00000042")))
		require.Empty(t, cachedLSNs(), "the window end must not persist while its batches are unacked")

		require.NoError(t, am.ackFn(ctx, nil))
		lsns := cachedLSNs()
		require.Len(t, lsns, 1)
		require.Equal(t, "00000042", string(lsns[0]))
	})

	t.Run("a nacked batch pins the window checkpoint", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedLSNs := newTestBatchPublisher(t)

		am := publishAndReceive(t, ctx, publisher, streamingEvent("00000042", ""))
		require.NoError(t, publisher.CheckpointWindow(ctx, replication.LSN("00000042")))

		nackErr := errors.New("downstream failure")
		require.ErrorIs(t, am.ackFn(ctx, nackErr), nackErr)
		require.Empty(t, cachedLSNs(), "the window end must never persist past a nacked batch")
	})
}

func TestCheckpointWindowDefersToBufferedBatch(t *testing.T) {
	ctx := t.Context()
	// Count=100 keeps published events buffered: the window checkpoint must
	// ride on the eventual batch instead of forcing a flush (which would
	// override the user's batching policy).
	publisher, cachedLSNs := newTestBatchPublisherWithCount(t, 100)

	require.NoError(t, publisher.Publish(ctx, streamingEvent("00000042", "")))
	require.NoError(t, publisher.CheckpointWindow(ctx, replication.LSN("00000042")))
	require.Empty(t, cachedLSNs(), "a deferred window checkpoint must not persist before its batch is acked")

	// No batch may have been force-flushed by CheckpointWindow.
	select {
	case m := <-publisher.msgs():
		t.Fatalf("CheckpointWindow force-flushed a batch of %d messages, overriding the batching policy", len(m.msg))
	case <-time.After(100 * time.Millisecond):
	}

	// When the batch eventually flushes and acks, it carries the window LSN.
	got := make(chan asyncMessage, 1)
	go func() { got <- <-publisher.msgs() }()
	require.NoError(t, publisher.flushCurrent(ctx))
	var am asyncMessage
	select {
	case am = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("buffered batch was never flushed")
	}
	require.NoError(t, am.ackFn(ctx, nil))

	lsns := cachedLSNs()
	require.Len(t, lsns, 1)
	require.Equal(t, "00000042", string(lsns[0]), "the drained-window LSN must ride on the buffered batch's checkpoint")
}

func TestTerminalNack(t *testing.T) {
	t.Run("invokes onTerminalNack so the input can restart", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedLSNs := newTestBatchPublisher(t)

		var got atomic.Value
		publisher.onTerminalNack = func(err error) { got.Store(err) }

		am := publishAndReceive(t, ctx, publisher, streamingEvent("00000042", "00000041"))
		nackErr := errors.New("downstream failure")
		require.ErrorIs(t, am.ackFn(ctx, nackErr), nackErr)

		stored, _ := got.Load().(error)
		require.ErrorIs(t, stored, nackErr)
		require.Empty(t, cachedLSNs())
	})

	t.Run("a sealed publisher neither persists nor restarts", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedLSNs := newTestBatchPublisher(t)

		restarted := false
		publisher.onTerminalNack = func(error) { restarted = true }

		am1 := publishAndReceive(t, ctx, publisher, streamingEvent("00000042", "00000041"))
		am2 := publishAndReceive(t, ctx, publisher, streamingEvent("00000043", "00000042"))
		publisher.seal()

		// Late ack from a replaced session: must not persist.
		require.NoError(t, am1.ackFn(ctx, nil))
		require.Empty(t, cachedLSNs(), "a sealed publisher must not persist checkpoints")

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
// ordered tracker in the wrong order and persisting a regressing LSN. Run with
// -race to also catch the underlying data race structurally.
func TestTrackOrderUnderConcurrentFlush(t *testing.T) {
	ctx := t.Context()
	logger := service.NewLoggerFromSlog(slog.Default())
	cp := checkpoint.NewCapped[replication.LSN](1000)

	// Count 2 + a tiny period keeps both flush paths active concurrently.
	batcher, err := (service.BatchPolicy{Count: 2, Period: "1ms"}).NewBatcher(service.MockResources())
	require.NoError(t, err)

	publisher := newBatchPublisher(batcher, cp, logger)
	t.Cleanup(func() { publisher.shutSig.TriggerSoftStop() })

	var (
		mu        sync.Mutex
		persisted []replication.LSN
	)
	publisher.cacheLSN = func(_ context.Context, lsn replication.LSN) error {
		mu.Lock()
		defer mu.Unlock()
		persisted = append(persisted, lsn)
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
		// %08d keeps lexicographic order == numeric order, like real LSNs.
		lsn := fmt.Sprintf("%08d", i)
		require.NoError(t, publisher.Publish(ctx, streamingEvent(lsn, lsn)))
	}
	require.NoError(t, publisher.flushCurrent(ctx))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(persisted) > 0 && string(persisted[len(persisted)-1]) == fmt.Sprintf("%08d", events-1)
	}, 10*time.Second, 10*time.Millisecond, "final LSN was never persisted")
	stopConsumer()
	<-consumerDone

	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < len(persisted); i++ {
		require.GreaterOrEqual(t, string(persisted[i]), string(persisted[i-1]),
			"persisted checkpoint regressed at index %d: %v", i, persisted)
	}
}

// newTestBatchPublisher builds a publisher whose batcher flushes on every
// published event (count=1), so tests drive the production
// Publish->trackBatchLocked->sendTracked path directly.
func newTestBatchPublisher(t *testing.T) (*batchPublisher, func() []replication.LSN) {
	t.Helper()
	return newTestBatchPublisherWithCount(t, 1)
}

func newTestBatchPublisherWithCount(t *testing.T, count int) (*batchPublisher, func() []replication.LSN) {
	t.Helper()

	logger := service.NewLoggerFromSlog(slog.Default())
	cp := checkpoint.NewCapped[replication.LSN](100)

	batcher, err := (service.BatchPolicy{Count: count}).NewBatcher(service.MockResources())
	require.NoError(t, err)

	publisher := newBatchPublisher(batcher, cp, logger)
	t.Cleanup(func() { publisher.shutSig.TriggerSoftStop() })

	var (
		mu         sync.Mutex
		cachedLSNs []replication.LSN
	)
	publisher.cacheLSN = func(_ context.Context, lsn replication.LSN) error {
		mu.Lock()
		defer mu.Unlock()
		cachedLSNs = append(cachedLSNs, lsn)
		return nil
	}

	cachedLSNsFn := func() []replication.LSN {
		mu.Lock()
		defer mu.Unlock()
		return append([]replication.LSN(nil), cachedLSNs...)
	}

	return publisher, cachedLSNsFn
}

func snapshotEvent() replication.MessageEvent {
	return replication.MessageEvent{
		Schema:    "dbo",
		Table:     "t",
		Operation: replication.MessageOperationRead.String(),
		Data:      map[string]any{"a": 1},
	}
}

func streamingEvent(lsn, checkpointLSN string) replication.MessageEvent {
	return replication.MessageEvent{
		Schema:        "dbo",
		Table:         "t",
		Operation:     replication.MessageOperationInsert.String(),
		LSN:           replication.LSN(lsn),
		CheckpointLSN: replication.LSN(checkpointLSN),
		Data:          map[string]any{"a": 1},
	}
}

// publishAndReceive publishes a single event through the production Publish
// path (count=1 batcher: every event flushes, tracks, and sends immediately)
// and returns the delivered asyncMessage.
func publishAndReceive(t *testing.T, ctx context.Context, publisher *batchPublisher, event replication.MessageEvent) asyncMessage {
	t.Helper()
	go func() {
		_ = publisher.Publish(ctx, event)
	}()
	return <-publisher.msgs()
}
