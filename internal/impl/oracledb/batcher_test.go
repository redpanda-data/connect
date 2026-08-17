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

	t.Run("a nack also releases the gate", func(t *testing.T) {
		ctx := t.Context()
		publisher, _ := newTestBatchPublisher(t)

		msg := publishAndReceive(t, ctx, publisher, snapshotEvent(100))
		// Nacks count as settled: replay is owned by auto_replay_nacks, and
		// disabling it is a documented opt-in to drop rejections.
		require.NoError(t, msg.ackFn(ctx, errors.New("downstream failure")))
		require.NoError(t, publisher.waitSnapshotAcks(ctx))
	})

	t.Run("streaming batches do not hold the gate", func(t *testing.T) {
		ctx := t.Context()
		publisher, _ := newTestBatchPublisher(t)

		// Published but never acked: must not block the gate.
		publishAndReceive(t, ctx, publisher, streamingEvent(200))

		require.NoError(t, publisher.waitSnapshotAcks(ctx))
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

// TestPublishBuffersWhileTrackBlocked verifies that a flusher blocked in
// checkpoint.Track (checkpoint_limit reached, nothing acked) waits only in
// the ticket queue: other Publish calls must still be able to buffer rows
// instead of freezing on batcherMu behind the blocked Track.
func TestPublishBuffersWhileTrackBlocked(t *testing.T) {
	ctx := t.Context()
	logger := service.NewLoggerFromSlog(slog.Default())
	// Capacity for exactly one 2-row batch: the second flush blocks in Track.
	cp := checkpoint.NewCapped[replication.SCN](2)

	batcher, err := (service.BatchPolicy{Count: 2}).NewBatcher(service.MockResources())
	require.NoError(t, err)
	publisher := newBatchPublisher(batcher, cp, logger)
	publisher.cacheSCN = func(context.Context, replication.SCN) error { return nil }
	t.Cleanup(publisher.Close)

	// Batch 1 fills the tracker to capacity; consume it but do not ack. The
	// flushing Publish blocks on the unbuffered channel send until the batch
	// is consumed, so it runs on its own goroutine.
	require.NoError(t, publisher.Publish(ctx, streamingEvent(100)))
	firstPublished := make(chan error, 1)
	go func() { firstPublished <- publisher.Publish(ctx, streamingEvent(101)) }()
	first := <-publisher.msgs()
	require.NoError(t, <-firstPublished)

	// Batch 2 flushes and blocks in Track (capacity exhausted).
	blocked := make(chan error, 1)
	go func() {
		blocked <- func() error {
			if err := publisher.Publish(ctx, streamingEvent(200)); err != nil {
				return err
			}
			return publisher.Publish(ctx, streamingEvent(201))
		}()
	}()

	// Give the flusher time to reach Track and park there.
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-blocked:
		t.Fatalf("expected the second batch's flusher to block in Track, but it returned: %v", err)
	default:
	}

	// The key assertion: a concurrent Publish that only buffers (no flush due)
	// completes promptly even though a flusher is parked in Track.
	buffered := make(chan error, 1)
	go func() { buffered <- publisher.Publish(ctx, streamingEvent(300)) }()
	select {
	case err := <-buffered:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("a buffering Publish froze behind a Track blocked on checkpoint_limit")
	}

	// Ack batch 1 to release the blocked flusher and drain.
	require.NoError(t, first.ackFn(ctx, nil))
	go func() {
		for m := range publisher.msgs() {
			_ = m.ackFn(ctx, nil)
		}
	}()
	select {
	case err := <-blocked:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("blocked flusher never released after the ack freed tracker capacity")
	}
}

// TestFlushCurrentBarriersParkedFlusher encodes the snapshot-handoff crash
// window: the timed-flush loop can hold the final snapshot rows while parked
// in checkpoint.Track (before counting them on the snapshot ack gate). The
// handoff's flushCurrent must not return - and waitSnapshotAcks must not
// release - until that parked flusher has registered and delivered its batch,
// otherwise the post-snapshot SCN persists ahead of undelivered rows.
func TestFlushCurrentBarriersParkedFlusher(t *testing.T) {
	ctx := t.Context()
	logger := service.NewLoggerFromSlog(slog.Default())
	// Capacity for exactly one 2-row batch: the second flush parks in Track.
	cp := checkpoint.NewCapped[replication.SCN](2)

	batcher, err := (service.BatchPolicy{Count: 2}).NewBatcher(service.MockResources())
	require.NoError(t, err)
	publisher := newBatchPublisher(batcher, cp, logger)
	publisher.cacheSCN = func(context.Context, replication.SCN) error { return nil }
	t.Cleanup(publisher.Close)

	// Snapshot batch 1 fills the tracker; consume it but do not ack (WG=1).
	require.NoError(t, publisher.Publish(ctx, snapshotEvent(100)))
	firstPublished := make(chan error, 1)
	go func() { firstPublished <- publisher.Publish(ctx, snapshotEvent(100)) }()
	first := <-publisher.msgs()
	require.NoError(t, <-firstPublished)

	// Snapshot batch 2 flushes, takes its ticket, and parks in Track - the
	// exact state the timed-flush loop can be in at the handoff.
	parked := make(chan error, 1)
	go func() {
		parked <- func() error {
			if err := publisher.Publish(ctx, snapshotEvent(100)); err != nil {
				return err
			}
			return publisher.Publish(ctx, snapshotEvent(100))
		}()
	}()
	time.Sleep(100 * time.Millisecond)

	// The handoff: flushCurrent sees an empty batcher but must still barrier
	// behind the parked flusher's ticket.
	flushed := make(chan error, 1)
	go func() { flushed <- publisher.flushCurrent(ctx) }()
	select {
	case err := <-flushed:
		t.Fatalf("flushCurrent returned (%v) while a flusher holding snapshot rows was still parked in Track: the ack gate does not yet count those rows", err)
	case <-time.After(300 * time.Millisecond):
	}

	// Ack batch 1: the parked flusher tracks, counts, and delivers batch 2.
	require.NoError(t, first.ackFn(ctx, nil))
	second := <-publisher.msgs()
	require.NoError(t, <-parked)
	require.NoError(t, <-flushed)

	// The gate must now hold for batch 2: it is published but un-acked.
	gate := make(chan error, 1)
	go func() { gate <- publisher.waitSnapshotAcks(ctx) }()
	select {
	case err := <-gate:
		t.Fatalf("waitSnapshotAcks returned (%v) with a published snapshot batch still un-acked", err)
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, second.ackFn(ctx, nil))
	select {
	case err := <-gate:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("gate never released after the final ack")
	}
}

// TestFailedSendPoisonsPublisher verifies that a tracked batch that cannot be
// handed to ReadBatch marks the publisher poisoned: its checkpoint slot can
// never resolve, so Connect must rebuild the publisher rather than reuse a
// permanently pinned tracker.
func TestFailedSendPoisonsPublisher(t *testing.T) {
	publisher, _ := newTestBatchPublisher(t)

	sendCtx, cancel := context.WithCancel(t.Context())
	cancel() // nobody consumes msgs(): the send can only fail

	err := publisher.Publish(sendCtx, streamingEvent(100))
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, publisher.poisoned.Load(),
		"a failed send orphans its tracker slot; the publisher must be marked for rebuild")
}

// newTestBatchPublisher builds a publisher whose batcher flushes on every
// published event (count=1), so tests drive the production
// Publish->trackBatch->sendTracked path directly.
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
