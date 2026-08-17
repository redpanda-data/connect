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

	t.Run("a nack also releases the gate", func(t *testing.T) {
		ctx := t.Context()
		publisher, _ := newTestBatchPublisher(t)

		msg := publishAndReceive(t, ctx, publisher, snapshotEvent())
		// Nacks count as settled: replay is owned by auto_replay_nacks, and
		// disabling it is a documented opt-in to drop rejections.
		require.NoError(t, msg.ackFn(ctx, errors.New("downstream failure")))
		require.NoError(t, publisher.waitSnapshotAcks(ctx))
	})

	t.Run("streaming batches do not hold the gate", func(t *testing.T) {
		ctx := t.Context()
		publisher, _ := newTestBatchPublisher(t)

		// Published but never acked: must not block the gate.
		publishAndReceive(t, ctx, publisher, streamingEvent("00000030", ""))

		require.NoError(t, publisher.waitSnapshotAcks(ctx))
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

	t.Run("a nack resolves too: auto_replay_nacks off is an opt-in drop", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedLSNs := newTestBatchPublisher(t)

		b1 := publishAndReceive(t, ctx, publisher, streamingEvent("00000042", "00000041"))
		b2 := publishAndReceive(t, ctx, publisher, streamingEvent("00000043", "00000042"))

		// A nacked batch is deleted per the auto_replay_nacks contract: its
		// slot resolves so the stream continues past it instead of pinning
		// the tracker and back-pressuring forever.
		require.NoError(t, b1.ackFn(ctx, errors.New("downstream failure")))
		require.NoError(t, b2.ackFn(ctx, nil))

		lsns := cachedLSNs()
		require.NotEmpty(t, lsns, "the checkpoint must continue advancing past a dropped batch")
		require.Equal(t, "00000042", string(lsns[len(lsns)-1]))
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

	t.Run("a nacked batch settles the window checkpoint too", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedLSNs := newTestBatchPublisher(t)

		am := publishAndReceive(t, ctx, publisher, streamingEvent("00000042", ""))
		require.NoError(t, publisher.CheckpointWindow(ctx, replication.LSN("00000042")))

		// A nacked batch is deleted per the auto_replay_nacks contract, so
		// the window checkpoint behind it still persists.
		require.NoError(t, am.ackFn(ctx, errors.New("downstream failure")))
		lsns := cachedLSNs()
		require.NotEmpty(t, lsns)
		require.Equal(t, "00000042", string(lsns[len(lsns)-1]))
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

// TestPersistOrderUnderConcurrentAcksAndWindows locks in that the cached
// resume position never regresses when batch acks (pipeline goroutines) and
// CheckpointWindow markers (stream goroutine) persist concurrently: the
// resolve+persist pair must be a single critical section, otherwise two
// persists can land out of order.
func TestPersistOrderUnderConcurrentAcksAndWindows(t *testing.T) {
	ctx := t.Context()
	logger := service.NewLoggerFromSlog(slog.Default())
	cp := checkpoint.NewCapped[replication.LSN](1000)

	batcher, err := (service.BatchPolicy{Count: 1}).NewBatcher(service.MockResources())
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

	// Consumer: ack every batch on its own goroutine so acks complete out of
	// order relative to each other and to the window markers.
	var ackWG sync.WaitGroup
	consumerDone := make(chan struct{})
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	go func() {
		defer close(consumerDone)
		for {
			select {
			case m := <-publisher.msgs():
				ackWG.Go(func() {
					_ = m.ackFn(ctx, nil)
				})
			case <-consumerCtx.Done():
				return
			}
		}
	}()

	const events = 400
	for i := range events {
		lsn := fmt.Sprintf("%08d", i)
		require.NoError(t, publisher.Publish(ctx, streamingEvent(lsn, lsn)))
		// A drained polling window ends every 10 rows; its end LSN persists
		// via an immediately-resolved marker racing the in-flight acks.
		if i%10 == 9 {
			require.NoError(t, publisher.CheckpointWindow(ctx, replication.LSN(lsn)))
		}
	}

	finalLSN := fmt.Sprintf("%08d", events-1)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(persisted) > 0 && string(persisted[len(persisted)-1]) == finalLSN
	}, 10*time.Second, 10*time.Millisecond, "final LSN was never persisted")
	stopConsumer()
	<-consumerDone
	ackWG.Wait()

	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < len(persisted); i++ {
		require.GreaterOrEqual(t, string(persisted[i]), string(persisted[i-1]),
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
	cp := checkpoint.NewCapped[replication.LSN](2)

	batcher, err := (service.BatchPolicy{Count: 2}).NewBatcher(service.MockResources())
	require.NoError(t, err)
	publisher := newBatchPublisher(batcher, cp, logger)
	publisher.cacheLSN = func(context.Context, replication.LSN) error { return nil }
	t.Cleanup(func() { publisher.shutSig.TriggerSoftStop() })

	// Batch 1 fills the tracker to capacity; consume it but do not ack. The
	// flushing Publish blocks on the unbuffered channel send until the batch
	// is consumed, so it runs on its own goroutine.
	require.NoError(t, publisher.Publish(ctx, streamingEvent("00000001", "00000001")))
	firstPublished := make(chan error, 1)
	go func() { firstPublished <- publisher.Publish(ctx, streamingEvent("00000002", "00000002")) }()
	first := <-publisher.msgs()
	require.NoError(t, <-firstPublished)

	// Batch 2 flushes and blocks in Track (capacity exhausted).
	blocked := make(chan error, 1)
	go func() {
		blocked <- func() error {
			if err := publisher.Publish(ctx, streamingEvent("00000003", "00000003")); err != nil {
				return err
			}
			return publisher.Publish(ctx, streamingEvent("00000004", "00000004"))
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
	go func() { buffered <- publisher.Publish(ctx, streamingEvent("00000005", "00000005")) }()
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
// otherwise the post-snapshot LSN persists ahead of undelivered rows.
func TestFlushCurrentBarriersParkedFlusher(t *testing.T) {
	ctx := t.Context()
	logger := service.NewLoggerFromSlog(slog.Default())
	// Capacity for exactly one 2-row batch: the second flush parks in Track.
	cp := checkpoint.NewCapped[replication.LSN](2)

	batcher, err := (service.BatchPolicy{Count: 2}).NewBatcher(service.MockResources())
	require.NoError(t, err)
	publisher := newBatchPublisher(batcher, cp, logger)
	publisher.cacheLSN = func(context.Context, replication.LSN) error { return nil }
	t.Cleanup(func() { publisher.shutSig.TriggerSoftStop() })

	// Snapshot batch 1 fills the tracker; consume it but do not ack (gate=1).
	require.NoError(t, publisher.Publish(ctx, snapshotEvent()))
	firstPublished := make(chan error, 1)
	go func() { firstPublished <- publisher.Publish(ctx, snapshotEvent()) }()
	first := <-publisher.msgs()
	require.NoError(t, <-firstPublished)

	// Snapshot batch 2 flushes, takes its ticket, and parks in Track - the
	// exact state the timed-flush loop can be in at the handoff.
	parked := make(chan error, 1)
	go func() {
		parked <- func() error {
			if err := publisher.Publish(ctx, snapshotEvent()); err != nil {
				return err
			}
			return publisher.Publish(ctx, snapshotEvent())
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

	err := publisher.Publish(sendCtx, streamingEvent("00000001", "00000001"))
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, publisher.poisoned.Load(),
		"a failed send orphans its tracker slot; the publisher must be marked for rebuild")
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
