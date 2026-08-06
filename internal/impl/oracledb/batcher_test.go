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

		msg := publishAndReceive(t, ctx, publisher, service.MessageBatch{newSnapshotMessage("100")})
		msg1 := publishAndReceive(t, ctx, publisher, service.MessageBatch{newSnapshotMessage("100")})

		require.NoError(t, msg.ackFn(ctx, nil))
		require.Empty(t, cachedSCNs(), "cacheSCN must not be called after acking only the first snapshot batch")

		require.NoError(t, msg1.ackFn(ctx, nil))
		require.Empty(t, cachedSCNs(), "cacheSCN must not be called after acking all snapshot batches")
	})

	t.Run("streaming batch still persists via ackFn", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		batch := service.MessageBatch{newStreamingMessage("200")}
		msg := publishAndReceive(t, ctx, publisher, batch)
		require.NoError(t, msg.ackFn(ctx, nil))

		scns := cachedSCNs()
		require.Len(t, scns, 1, "expected cacheSCN to be called exactly once for a streaming batch")
		require.Equal(t, replication.SCN(200), scns[0])
	})

	t.Run("mixed snapshot and streaming batch persists", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		batch := service.MessageBatch{
			newSnapshotMessage("100"),
			newStreamingMessage("300"),
		}
		msg := publishAndReceive(t, ctx, publisher, batch)
		require.NoError(t, msg.ackFn(ctx, nil))

		scns := cachedSCNs()
		require.Len(t, scns, 1, "expected cacheSCN to be called exactly once for a batch whose last message is streaming")
		require.Equal(t, replication.SCN(300), scns[0], "expected the streaming SCN from the last message, not the leftover snapshot SCN")
	})

	t.Run("streaming SCN survives an out-of-order snapshot ack", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		snapshotMsg := publishAndReceive(t, ctx, publisher, service.MessageBatch{newSnapshotMessage("100")})
		streamingMsg := publishAndReceive(t, ctx, publisher, service.MessageBatch{newStreamingMessage("200")})

		require.NoError(t, streamingMsg.ackFn(ctx, nil))
		require.Empty(t, cachedSCNs(), "cacheSCN must not be called while the snapshot batch is still the unresolved head")
		require.NoError(t, snapshotMsg.ackFn(ctx, nil))

		scns := cachedSCNs()
		require.Len(t, scns, 1, "expected cacheSCN to be called exactly once when the snapshot batch resolves the streaming SCN")
		require.Equal(t, replication.SCN(200), scns[0], "expected the streaming batch's SCN to survive the out-of-order snapshot ack")
	})
}

func TestSnapshotAckGate(t *testing.T) {
	t.Run("blocks until the snapshot batch is acked", func(t *testing.T) {
		ctx := t.Context()
		publisher, _ := newTestBatchPublisher(t)

		msg := publishAndReceive(t, ctx, publisher, service.MessageBatch{newSnapshotMessage("100")})

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

	t.Run("a nack also releases the gate", func(t *testing.T) {
		ctx := t.Context()
		publisher, _ := newTestBatchPublisher(t)

		msg := publishAndReceive(t, ctx, publisher, service.MessageBatch{newSnapshotMessage("100")})
		require.NoError(t, msg.ackFn(ctx, errors.New("downstream failure")))

		require.NoError(t, publisher.waitSnapshotAcks(ctx))
	})

	t.Run("streaming batches do not hold the gate", func(t *testing.T) {
		ctx := t.Context()
		publisher, _ := newTestBatchPublisher(t)

		// Published but never acked: must not block the gate.
		publishAndReceive(t, ctx, publisher, service.MessageBatch{newStreamingMessage("200")})

		require.NoError(t, publisher.waitSnapshotAcks(ctx))
	})

	t.Run("context cancellation escapes the gate", func(t *testing.T) {
		publisher, _ := newTestBatchPublisher(t)

		ctx, cancel := context.WithCancel(t.Context())
		publishAndReceive(t, ctx, publisher, service.MessageBatch{newSnapshotMessage("100")})

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
	logger := service.NewLoggerFromSlog(slog.Default())
	cp := checkpoint.NewCapped[replication.SCN](100)

	batcher, err := (service.BatchPolicy{Count: 100}).NewBatcher(service.MockResources())
	require.NoError(t, err)

	publisher := newBatchPublisher(batcher, cp, logger)
	t.Cleanup(publisher.Close)
	publisher.cacheSCN = func(context.Context, replication.SCN) error { return nil }

	publishEvent := func(v int) {
		t.Helper()
		require.NoError(t, publisher.Publish(ctx, &replication.MessageEvent{
			Schema:    "S",
			Table:     "T",
			Operation: replication.MessageOperationRead,
			Data:      map[string]any{"a": v},
			SCN:       replication.SCN(100),
		}))
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

	// Count=100 keeps a single event buffered in the batcher until flushed.
	publishEvent(1)
	receive("flushCurrent did not publish the buffered partial batch")

	// The loop must still be alive after flushCurrent (unlike FlushRemaining):
	// a second publish+flush must work identically.
	publishEvent(2)
	receive("publisher loop no longer functional after flushCurrent")
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

func newTestBatchPublisher(t *testing.T) (*batchPublisher, func() []replication.SCN) {
	t.Helper()

	logger := service.NewLoggerFromSlog(slog.Default())
	cp := checkpoint.NewCapped[replication.SCN](100)

	publisher := newBatchPublisher(nil, cp, logger)
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

func newSnapshotMessage(scn string) *service.Message {
	msg := service.NewMessage([]byte("{}"))
	msg.MetaSet("operation", replication.MessageOperationRead.String())
	msg.MetaSet("scn", scn)
	return msg
}

func newStreamingMessage(checkpointSCN string) *service.Message {
	msg := service.NewMessage([]byte("{}"))
	msg.MetaSet("operation", replication.MessageOperationInsert.String())
	msg.MetaSet("checkpoint_scn", checkpointSCN)
	return msg
}

func publishAndReceive(t *testing.T, ctx context.Context, publisher *batchPublisher, batch service.MessageBatch) asyncMessage {
	t.Helper()
	go func() {
		_ = publisher.publishBatch(ctx, batch)
	}()
	return <-publisher.msgs()
}
