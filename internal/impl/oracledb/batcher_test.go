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

func TestPublish(t *testing.T) {
	t.Run("streaming batch persists via ackFn", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		msg := publishAndReceive(t, ctx, publisher, streamingEvent(200))
		require.NoError(t, msg.ackFn(ctx, nil))

		scns := cachedSCNs()
		require.Len(t, scns, 1, "expected cacheSCN to be called exactly once for a streaming batch")
		require.Equal(t, replication.SCN(200), scns[0])
	})

	t.Run("SCN persists in delivery order regardless of ack order", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		first := publishAndReceive(t, ctx, publisher, streamingEvent(100))
		second := publishAndReceive(t, ctx, publisher, streamingEvent(200))

		require.NoError(t, second.ackFn(ctx, nil))
		require.Empty(t, cachedSCNs(), "cacheSCN must not be called while an earlier batch is still the unresolved head")

		require.NoError(t, first.ackFn(ctx, nil))
		scns := cachedSCNs()
		require.Len(t, scns, 1, "expected cacheSCN to be called exactly once once the head resolves")
		require.Equal(t, replication.SCN(200), scns[0], "expected the later batch's SCN to survive the out-of-order ack")
	})

	t.Run("a nack resolves too: auto_replay_nacks off is an opt-in drop", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		b1 := publishAndReceive(t, ctx, publisher, streamingEvent(200))
		b2 := publishAndReceive(t, ctx, publisher, streamingEvent(300))

		// A nacked batch is deleted per the auto_replay_nacks contract: its
		// slot resolves so the stream continues past it instead of pinning
		// the tracker and back-pressuring forever.
		require.NoError(t, b1.ackFn(ctx, errors.New("downstream failure")))
		require.NoError(t, b2.ackFn(ctx, nil))

		scns := cachedSCNs()
		require.NotEmpty(t, scns, "the checkpoint must continue advancing past a dropped batch")
		require.Equal(t, replication.SCN(300), scns[len(scns)-1])
	})
}

func TestPublishSnapshot(t *testing.T) {
	t.Run("snapshot batches never persist via ackFn", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		msg := publishSnapshotAndReceive(t, ctx, publisher, snapshotEvent(100))
		msg1 := publishSnapshotAndReceive(t, ctx, publisher, snapshotEvent(100))

		require.NoError(t, msg.ackFn(ctx, nil))
		require.NoError(t, msg1.ackFn(ctx, nil))
		require.Empty(t, cachedSCNs(), "cacheSCN must never be called for a snapshot batch: SnapshotComplete owns persisting the post-snapshot SCN")
	})

	t.Run("a nack is also a no-op: replay is owned by AutoRetryNacksBatched", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		msg := publishSnapshotAndReceive(t, ctx, publisher, snapshotEvent(100))
		require.NoError(t, msg.ackFn(ctx, errors.New("downstream failure")))
		require.Empty(t, cachedSCNs())
	})

	t.Run("streaming batches are delivered separately from snapshot batches", func(t *testing.T) {
		ctx := t.Context()
		publisher, _ := newTestBatchPublisher(t)

		go func() { _ = publisher.Publish(ctx, streamingEvent(200)) }()
		select {
		case <-publisher.snapshotMsgs():
			t.Fatal("a streaming batch must never be delivered on the snapshot channel")
		case m := <-publisher.msgs():
			require.Len(t, m.msg, 1)
		case <-time.After(5 * time.Second):
			t.Fatal("streaming batch was never published")
		}
	})
}

func TestFlushSnapshotRemaining(t *testing.T) {
	ctx := t.Context()
	// Count=100 keeps published events buffered in the snapshot batcher
	// until explicitly flushed.
	publisher, _ := newTestBatchPublisherWithCount(t, 100)

	publishEvent := func(v int) {
		t.Helper()
		e := snapshotEvent(100)
		e.Data = map[string]any{"a": v}
		require.NoError(t, publisher.PublishSnapshot(ctx, e))
	}
	receive := func(failMsg string) {
		t.Helper()
		got := make(chan asyncMessage, 1)
		go func() { got <- <-publisher.snapshotMsgs() }()
		require.NoError(t, publisher.flushSnapshotRemaining(ctx))
		select {
		case m := <-got:
			require.Len(t, m.msg, 1)
		case <-time.After(5 * time.Second):
			t.Fatal(failMsg)
		}
	}

	publishEvent(1)
	receive("flushSnapshotRemaining did not publish the buffered partial batch")

	// The loop and snapshot batcher must still be usable afterwards: a
	// second publish+flush must work identically.
	publishEvent(2)
	receive("snapshot batcher no longer functional after flushSnapshotRemaining")
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
	policy := service.BatchPolicy{Count: 2, Period: "1ms"}
	batcher, err := policy.NewBatcher(service.MockResources())
	require.NoError(t, err)
	snapshotBatcher, err := policy.NewBatcher(service.MockResources())
	require.NoError(t, err)

	publisher := newBatchPublisher(batcher, snapshotBatcher, cp, logger)
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

// newTestBatchPublisher builds a publisher whose batchers flush on every
// published event (count=1), so tests drive the production
// Publish/PublishSnapshot->trackBatchLocked->send path directly.
func newTestBatchPublisher(t *testing.T) (*batchPublisher, func() []replication.SCN) {
	t.Helper()
	return newTestBatchPublisherWithCount(t, 1)
}

func newTestBatchPublisherWithCount(t *testing.T, count int) (*batchPublisher, func() []replication.SCN) {
	t.Helper()

	logger := service.NewLoggerFromSlog(slog.Default())
	cp := checkpoint.NewCapped[replication.SCN](100)

	policy := service.BatchPolicy{Count: count}
	batcher, err := policy.NewBatcher(service.MockResources())
	require.NoError(t, err)
	snapshotBatcher, err := policy.NewBatcher(service.MockResources())
	require.NoError(t, err)

	publisher := newBatchPublisher(batcher, snapshotBatcher, cp, logger)
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

// publishAndReceive publishes a single streaming event through the
// production Publish path (count=1 batcher: every event flushes, tracks, and
// sends immediately) and returns the delivered asyncMessage.
func publishAndReceive(t *testing.T, ctx context.Context, publisher *batchPublisher, event *replication.MessageEvent) asyncMessage {
	t.Helper()
	go func() {
		_ = publisher.Publish(ctx, event)
	}()
	return <-publisher.msgs()
}

// publishSnapshotAndReceive is publishAndReceive's snapshot-phase
// counterpart: it drives the production PublishSnapshot path and returns
// the delivered asyncMessage from the snapshot channel.
func publishSnapshotAndReceive(t *testing.T, ctx context.Context, publisher *batchPublisher, event *replication.MessageEvent) asyncMessage {
	t.Helper()
	go func() {
		_ = publisher.PublishSnapshot(ctx, event)
	}()
	return <-publisher.snapshotMsgs()
}
