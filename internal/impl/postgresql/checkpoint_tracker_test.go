// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pgstream

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/incrementalsnapshot"
)

// TestCheckpointTrackerPreventsClobberFromRowlessSentinel reproduces the
// deadlock/data-loss scenario in commitIncrementalSnapshotCheckpoint: a
// row-less incremental snapshot checkpoint (LSN=nil) is tracked right after
// a real batch and resolved synchronously, without waiting on that batch's
// own ack. Without checkpointTracker's merge, the sentinel's nil LSN would
// wipe out the still-pending batch's real LSN.
func TestCheckpointTrackerPreventsClobberFromRowlessSentinel(t *testing.T) {
	lsn := "1/AAAA"
	state := []byte("snapshot-state")

	tracker := newCheckpointTracker(10, new(atomic.Uint64))

	resolveBatch, err := tracker.Track(context.Background(), incrementalsnapshot.CheckpointOffset{LSN: &lsn}, 1)
	require.NoError(t, err)

	resolveSentinel, err := tracker.Track(context.Background(), incrementalsnapshot.CheckpointOffset{IncSnapshotState: state}, 0)
	require.NoError(t, err)

	// The sentinel resolves immediately, ahead of the still-pending batch;
	// nothing is visible yet since the batch hasn't resolved.
	assert.Nil(t, resolveSentinel())

	// The batch resolving afterward must still carry its own LSN *and* the
	// snapshot state spliced onto it - neither field lost.
	maxOffset := resolveBatch()
	require.NotNil(t, maxOffset)
	assert.Equal(t, &lsn, maxOffset.LSN)
	assert.Equal(t, state, maxOffset.IncSnapshotState)
}

// TestCheckpointTrackerPreservesPendingStateAcrossLaterBatch is the mirror
// scenario: a later batch with no new snapshot state (because
// pendingIncrementalState was already consumed by the earlier batch)
// resolves before its still-pending predecessor, which is still carrying
// unflushed snapshot state.
func TestCheckpointTrackerPreservesPendingStateAcrossLaterBatch(t *testing.T) {
	lsnA := "1/AAAA"
	lsnB := "1/BBBB"
	stateA := []byte("state-a")

	tracker := newCheckpointTracker(10, new(atomic.Uint64))

	resolveA, err := tracker.Track(context.Background(), incrementalsnapshot.CheckpointOffset{LSN: &lsnA, IncSnapshotState: stateA}, 1)
	require.NoError(t, err)

	resolveB, err := tracker.Track(context.Background(), incrementalsnapshot.CheckpointOffset{LSN: &lsnB}, 1)
	require.NoError(t, err)

	// B resolves first, while A is still pending; nothing visible yet.
	assert.Nil(t, resolveB())

	maxOffset := resolveA()
	require.NotNil(t, maxOffset)
	assert.Equal(t, &lsnB, maxOffset.LSN, "B's own LSN must still surface once A resolves")
	assert.Equal(t, stateA, maxOffset.IncSnapshotState, "A's pending snapshot state must not be lost even though B resolved first")
}

// TestCommitCheckpointSkipsRedundantStatePersist verifies commitCheckpoint
// only writes to the checkpoint cache when the incremental snapshot state
// actually changed, since checkpointTracker.Track now carries the
// last-known state forward onto every checkpoint (including ones that
// didn't themselves advance it) to prevent the clobber above.
func TestCommitCheckpointSkipsRedundantStatePersist(t *testing.T) {
	const cacheName = "inc_snapshot_cache"
	mgr := service.MockResources(service.MockResourcesOptAddCache(cacheName))

	p := &pgStreamInput{
		mgr:                           mgr,
		incSnapshotCheckpointCache:    cacheName,
		incSnapshotCheckpointCacheKey: "key",
	}

	ctx := context.Background()
	stateA := []byte("state-a")

	// offset.LSN is nil throughout this test, so commitCheckpoint never
	// touches pgStream - passing nil is safe.
	require.NoError(t, p.commitCheckpoint(ctx, nil, incrementalsnapshot.CheckpointOffset{IncSnapshotState: stateA, Seq: 1}))

	got, err := p.loadCachedIncSnapshotStateBytes(ctx)
	require.NoError(t, err)
	assert.Equal(t, stateA, got)

	// Delete the cache entry directly, bypassing commitCheckpoint, so a
	// re-write would be observable.
	require.NoError(t, mgr.AccessCache(ctx, cacheName, func(c service.Cache) {
		require.NoError(t, c.Delete(ctx, "key"))
	}))

	// Re-committing the *same* state must be a no-op: checkpointTracker
	// would carry stateA forward onto every later checkpoint even when
	// nothing new happened, so commitCheckpoint must recognise it's
	// unchanged and skip the redundant cache write.
	require.NoError(t, p.commitCheckpoint(ctx, nil, incrementalsnapshot.CheckpointOffset{IncSnapshotState: stateA, Seq: 2}))
	_, err = p.loadCachedIncSnapshotStateBytes(ctx)
	require.ErrorIs(t, err, service.ErrKeyNotFound, "unchanged state must not be re-persisted")

	// A genuinely new state must still be persisted.
	stateB := []byte("state-b")
	require.NoError(t, p.commitCheckpoint(ctx, nil, incrementalsnapshot.CheckpointOffset{IncSnapshotState: stateB, Seq: 3}))
	got, err = p.loadCachedIncSnapshotStateBytes(ctx)
	require.NoError(t, err)
	assert.Equal(t, stateB, got)
}

// loadCachedIncSnapshotStateBytes is a small test helper reading the raw
// cache bytes directly, sidestepping loadCachedIncSnapshotState's JSON
// unmarshal (which requires a valid incrementalsnapshot.State payload).
func (p *pgStreamInput) loadCachedIncSnapshotStateBytes(ctx context.Context) ([]byte, error) {
	var (
		val  []byte
		cErr error
	)
	if err := p.mgr.AccessCache(ctx, p.incSnapshotCheckpointCache, func(c service.Cache) {
		val, cErr = c.Get(ctx, p.incSnapshotCheckpointCacheKey)
	}); err != nil {
		return nil, err
	}
	return val, cErr
}

// TestTrackAssignsIncreasingSeq checks that each tracked offset gets the
// next Seq.
func TestTrackAssignsIncreasingSeq(t *testing.T) {
	lsnA := "1/AAAA"
	lsnB := "1/BBBB"

	tracker := newCheckpointTracker(10, new(atomic.Uint64))

	resolveA, err := tracker.Track(context.Background(), incrementalsnapshot.CheckpointOffset{LSN: &lsnA}, 1)
	require.NoError(t, err)
	offsetA := resolveA()
	require.NotNil(t, offsetA)
	assert.Equal(t, uint64(1), offsetA.Seq)

	resolveB, err := tracker.Track(context.Background(), incrementalsnapshot.CheckpointOffset{LSN: &lsnB}, 1)
	require.NoError(t, err)
	offsetB := resolveB()
	require.NotNil(t, offsetB)
	assert.Equal(t, uint64(2), offsetB.Seq)
}

// TestCommitCheckpointRejectsOlderState covers the race between concurrent
// acknowledgements: their writes can land in either order, and an older one
// landing last makes a restart re-deliver chunks. A lock alone does not fix
// this, because the calls can take it in either order.
func TestCommitCheckpointRejectsOlderState(t *testing.T) {
	const cacheName = "inc_snapshot_cache"
	mgr := service.MockResources(service.MockResourcesOptAddCache(cacheName))

	p := &pgStreamInput{
		mgr:                           mgr,
		incSnapshotCheckpointCache:    cacheName,
		incSnapshotCheckpointCacheKey: "key",
	}

	ctx := context.Background()
	older := []byte("state-1")
	newer := []byte("state-2")

	// Newer acknowledgement lands first.
	require.NoError(t, p.commitCheckpoint(ctx, nil, incrementalsnapshot.CheckpointOffset{IncSnapshotState: newer, Seq: 2}))

	// Older one arrives after, and must not write.
	require.NoError(t, p.commitCheckpoint(ctx, nil, incrementalsnapshot.CheckpointOffset{IncSnapshotState: older, Seq: 1}))

	got, err := p.loadCachedIncSnapshotStateBytes(ctx)
	require.NoError(t, err)
	assert.Equal(t, newer, got, "an older checkpoint must not replace a newer one")
}

// TestCommitCheckpointConcurrentAcksNeverRegress runs many acknowledgements
// concurrently. The cache must end on the newest state. Run with -race.
func TestCommitCheckpointConcurrentAcksNeverRegress(t *testing.T) {
	const (
		cacheName = "inc_snapshot_cache"
		acks      = 64
	)
	mgr := service.MockResources(service.MockResourcesOptAddCache(cacheName))

	p := &pgStreamInput{
		mgr:                           mgr,
		incSnapshotCheckpointCache:    cacheName,
		incSnapshotCheckpointCacheKey: "key",
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 1; i <= acks; i++ {
		wg.Add(1)
		go func(seq uint64) {
			defer wg.Done()
			state := fmt.Appendf(nil, "state-%03d", seq)
			assert.NoError(t, p.commitCheckpoint(ctx, nil, incrementalsnapshot.CheckpointOffset{
				IncSnapshotState: state,
				Seq:              seq,
			}))
		}(uint64(i))
	}
	wg.Wait()

	got, err := p.loadCachedIncSnapshotStateBytes(ctx)
	require.NoError(t, err)
	assert.Equal(t, fmt.Appendf(nil, "state-%03d", acks), got)
}

// TestCheckpointsPersistAcrossTrackerReplacement covers the reconnect path.
// Connect builds a fresh checkpointTracker each time, but the
// lastPersisted fields live on the input. A Seq counter owned by the tracker
// would restart at 0, fail the "not newer" guard against the pre-reconnect
// high-water mark, and silently stop persisting for the life of the process.
func TestCheckpointsPersistAcrossTrackerReplacement(t *testing.T) {
	const cacheName = "inc_snapshot_cache"
	mgr := service.MockResources(service.MockResourcesOptAddCache(cacheName))

	p := &pgStreamInput{
		mgr:                           mgr,
		incSnapshotCheckpointCache:    cacheName,
		incSnapshotCheckpointCacheKey: "key",
	}

	// offset.LSN stays nil throughout, so commitCheckpoint never touches
	// pgStream and passing nil for it is safe.
	ctx := context.Background()

	// First connection: track and commit a few checkpoints.
	first := newCheckpointTracker(10, &p.incSnapshotSeq)
	for i := range 5 {
		state := fmt.Appendf(nil, "state-a-%d", i)
		resolve, err := first.Track(ctx, incrementalsnapshot.CheckpointOffset{IncSnapshotState: state}, 1)
		require.NoError(t, err)
		offset := resolve()
		require.NotNil(t, offset)
		require.NoError(t, p.commitCheckpoint(ctx, nil, *offset))
	}

	got, err := p.loadCachedIncSnapshotStateBytes(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("state-a-4"), got)

	// Reconnect: a new tracker, the same input.
	second := newCheckpointTracker(10, &p.incSnapshotSeq)
	state := []byte("state-b-0")
	resolve, err := second.Track(ctx, incrementalsnapshot.CheckpointOffset{IncSnapshotState: state}, 1)
	require.NoError(t, err)
	offset := resolve()
	require.NotNil(t, offset)
	assert.Greater(t, offset.Seq, uint64(5), "Seq must continue past the first connection")
	require.NoError(t, p.commitCheckpoint(ctx, nil, *offset))

	got, err = p.loadCachedIncSnapshotStateBytes(ctx)
	require.NoError(t, err)
	assert.Equal(t, state, got, "checkpoints must keep persisting after a reconnect")
}
