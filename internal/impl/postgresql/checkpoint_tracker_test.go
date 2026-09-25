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

	incsnapshot "github.com/redpanda-data/connect/v4/internal/impl/postgresql/incrementalsnapshot"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream"
	replincsnapshot "github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

func TestCheckpointTrackerPreventsClobberFromRowlessSentinel(t *testing.T) {
	lsn := "1/AAAA"
	state := []byte("snapshot-state")

	tracker := newCheckpointTracker(10, new(atomic.Uint64))

	resolveBatch, err := tracker.Track(t.Context(), checkpointOffset{lsn: &lsn}, 1)
	require.NoError(t, err)

	resolveSentinel, err := tracker.Track(t.Context(), checkpointOffset{incSnapshotState: state}, 0)
	require.NoError(t, err)

	// The sentinel resolves immediately, ahead of the still-pending batch;
	// nothing is visible yet since the batch hasn't resolved.
	assert.Nil(t, resolveSentinel())

	// The batch resolving afterward must still carry its own LSN *and* the
	// snapshot state spliced onto it - neither field lost.
	maxOffset := resolveBatch()
	require.NotNil(t, maxOffset)
	assert.Equal(t, &lsn, maxOffset.lsn)
	assert.Equal(t, state, maxOffset.incSnapshotState)
}

func TestCheckpointTrackerPreservesPendingStateAcrossLaterBatch(t *testing.T) {
	lsnA := "1/AAAA"
	lsnB := "1/BBBB"
	stateA := []byte("state-a")

	tracker := newCheckpointTracker(10, new(atomic.Uint64))

	resolveA, err := tracker.Track(t.Context(), checkpointOffset{lsn: &lsnA, incSnapshotState: stateA}, 1)
	require.NoError(t, err)

	resolveB, err := tracker.Track(t.Context(), checkpointOffset{lsn: &lsnB}, 1)
	require.NoError(t, err)

	// B resolves first, while A is still pending; nothing visible yet.
	assert.Nil(t, resolveB())

	maxOffset := resolveA()
	require.NotNil(t, maxOffset)
	assert.Equal(t, &lsnB, maxOffset.lsn, "B's own LSN must still surface once A resolves")
	assert.Equal(t, stateA, maxOffset.incSnapshotState, "A's pending snapshot state must not be lost even though B resolved first")
}

func TestCommitCheckpointSkipsRedundantStatePersist(t *testing.T) {
	const cacheName = "inc_snapshot_cache"
	mgr := service.MockResources(service.MockResourcesOptAddCache(cacheName))

	p := &pgStreamInput{
		mgr:                           mgr,
		incSnapshotCheckpointCache:    cacheName,
		incSnapshotCheckpointCacheKey: "key",
	}

	ctx := t.Context()
	stateA := []byte("state-a")

	// offset.lsn is nil throughout this test, so commitCheckpoint never
	// touches pgStream - passing nil is safe.
	require.NoError(t, p.commitCheckpoint(ctx, nil, checkpointOffset{incSnapshotState: stateA, seq: 1}))

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
	require.NoError(t, p.commitCheckpoint(ctx, nil, checkpointOffset{incSnapshotState: stateA, seq: 2}))
	_, err = p.loadCachedIncSnapshotStateBytes(ctx)
	require.ErrorIs(t, err, service.ErrKeyNotFound, "unchanged state must not be re-persisted")

	// A genuinely new state must still be persisted.
	stateB := []byte("state-b")
	require.NoError(t, p.commitCheckpoint(ctx, nil, checkpointOffset{incSnapshotState: stateB, seq: 3}))
	got, err = p.loadCachedIncSnapshotStateBytes(ctx)
	require.NoError(t, err)
	assert.Equal(t, stateB, got)
}

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

	resolveA, err := tracker.Track(t.Context(), checkpointOffset{lsn: &lsnA}, 1)
	require.NoError(t, err)
	offsetA := resolveA()
	require.NotNil(t, offsetA)
	assert.Equal(t, uint64(1), offsetA.seq)

	resolveB, err := tracker.Track(t.Context(), checkpointOffset{lsn: &lsnB}, 1)
	require.NoError(t, err)
	offsetB := resolveB()
	require.NotNil(t, offsetB)
	assert.Equal(t, uint64(2), offsetB.seq)
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

	ctx := t.Context()
	older := []byte("state-1")
	newer := []byte("state-2")

	// Newer acknowledgement lands first.
	require.NoError(t, p.commitCheckpoint(ctx, nil, checkpointOffset{incSnapshotState: newer, seq: 2}))

	// Older one arrives after, and must not write.
	require.NoError(t, p.commitCheckpoint(ctx, nil, checkpointOffset{incSnapshotState: older, seq: 1}))

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

	ctx := t.Context()
	var wg sync.WaitGroup
	for i := 1; i <= acks; i++ {
		wg.Add(1)
		go func(seq uint64) {
			defer wg.Done()
			state := fmt.Appendf(nil, "state-%03d", seq)
			assert.NoError(t, p.commitCheckpoint(ctx, nil, checkpointOffset{
				incSnapshotState: state,
				seq:              seq,
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

	// offset.lsn stays nil throughout, so commitCheckpoint never touches
	// pgStream and passing nil for it is safe.
	ctx := t.Context()

	// First connection: track and commit a few checkpoints.
	first := newCheckpointTracker(10, &p.checkpointSeq)
	for i := range 5 {
		state := fmt.Appendf(nil, "state-a-%d", i)
		resolve, err := first.Track(ctx, checkpointOffset{incSnapshotState: state}, 1)
		require.NoError(t, err)
		offset := resolve()
		require.NotNil(t, offset)
		require.NoError(t, p.commitCheckpoint(ctx, nil, *offset))
	}

	got, err := p.loadCachedIncSnapshotStateBytes(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("state-a-4"), got)

	// Reconnect: a new tracker, the same input.
	second := newCheckpointTracker(10, &p.checkpointSeq)
	state := []byte("state-b-0")
	resolve, err := second.Track(ctx, checkpointOffset{incSnapshotState: state}, 1)
	require.NoError(t, err)
	offset := resolve()
	require.NotNil(t, offset)
	assert.Greater(t, offset.seq, uint64(5), "Seq must continue past the first connection")
	require.NoError(t, p.commitCheckpoint(ctx, nil, *offset))

	got, err = p.loadCachedIncSnapshotStateBytes(ctx)
	require.NoError(t, err)
	assert.Equal(t, state, got, "checkpoints must keep persisting after a reconnect")
}

func TestLoadCachedIncSnapshotStateRejectsForeignVersion(t *testing.T) {
	// A checkpoint this build cannot interpret must fail loudly, and the
	// error must name the way out.
	const cacheName = "inc_snapshot_cache"
	mgr := service.MockResources(service.MockResourcesOptAddCache(cacheName))

	p := &pgStreamInput{
		mgr:                           mgr,
		incSnapshotCheckpointCache:    cacheName,
		incSnapshotCheckpointCacheKey: "key",
	}

	require.NoError(t, p.saveIncrementalSnapshotState(t.Context(), []byte(`{"version":99,"last_sent_pk":[42]}`)))

	_, err := p.loadCachedIncSnapshotState(t.Context())
	require.ErrorIs(t, err, replincsnapshot.ErrUnsupportedStateVersion)
	assert.ErrorContains(t, err, "checkpoint_cache_key")
}

func TestCommitCheckpointHoldsAckWhenStateWriteFails(t *testing.T) {
	// No cache is registered under this name, so the state write fails.
	p := &pgStreamInput{
		mgr:                           service.MockResources(),
		incSnapshotCheckpointCache:    "missing_cache",
		incSnapshotCheckpointCacheKey: "key",
	}

	lsn := "0/1000"
	err := p.commitCheckpoint(t.Context(), nil, checkpointOffset{
		lsn:              &lsn,
		incSnapshotState: []byte("state"),
		seq:              1,
	})
	require.Error(t, err, "the failed state write must surface")

	// The failure must not be recorded as persisted either, or the retry
	// would skip it as unchanged.
	assert.Zero(t, p.lastPersistedIncSnapshotSeq)
	assert.Nil(t, p.lastPersistedIncSnapshotState)
}

func TestCommitCheckpointAcksWhenThereIsNoState(t *testing.T) {
	p := &pgStreamInput{mgr: service.MockResources()}
	require.NoError(t, p.commitCheckpoint(t.Context(), nil, checkpointOffset{seq: 1}))
}

func TestFlushBatchLSNFromMixedBatch(t *testing.T) {
	change := func(lsn string) *service.Message {
		msg := service.NewMessage([]byte(`{}`))
		msg.MetaSet("lsn", lsn)
		return msg
	}
	// A backfill read, which carries no lsn.
	read := func() *service.Message { return service.NewMessage([]byte(`{}`)) }

	for _, test := range []struct {
		name     string
		enabled  bool
		batch    service.MessageBatch
		wantLSN  string
		wantNone bool
	}{
		{
			name:    "mixed batch ending on a read reaches past the tail",
			enabled: true,
			batch:   service.MessageBatch{change("1/AAAA"), read(), change("1/BBBB"), read()},
			// The batch is in stream order, so the last message carrying an
			// lsn holds the greatest one in the batch.
			wantLSN: "1/BBBB",
		},
		{
			name:    "mixed batch ending on a change uses that change",
			enabled: true,
			batch:   service.MessageBatch{change("1/AAAA"), read(), change("1/BBBB")},
			wantLSN: "1/BBBB",
		},
		{
			name:     "a batch of reads alone has no lsn to checkpoint",
			enabled:  true,
			batch:    service.MessageBatch{read(), read()},
			wantNone: true,
		},
		{
			name:    "with the snapshot disabled the last message is enough",
			enabled: false,
			batch:   service.MessageBatch{change("1/AAAA"), change("1/BBBB")},
			wantLSN: "1/BBBB",
		},
		{
			// The blocking snapshot drains the batcher and waits for its
			// acknowledgements before streaming starts, so with the snapshot
			// disabled no batch mixes the two and this batch cannot occur.
			// Asserted only to record that the cheap path is in force.
			name:     "with the snapshot disabled a trailing read wins",
			enabled:  false,
			batch:    service.MessageBatch{change("1/AAAA"), read()},
			wantNone: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := &pgStreamInput{
				streamConfig: &pglogicalstream.Config{
					IncrementalSnapshot: incsnapshot.Cfg{Enabled: test.enabled},
				},
				// Buffered, so flushBatch's send completes without a reader.
				msgChan: make(chan asyncMessage, 1),
			}
			tracker := newCheckpointTracker(10, new(atomic.Uint64))

			// pgStream is only reached through the acknowledgement, which
			// this test never runs, and blockingSnapshotComplete keeps
			// flushBatch off the snapshot-barrier bookkeeping.
			require.NoError(t, p.flushBatch(t.Context(), nil, tracker, test.batch, nil, true))

			if test.wantNone {
				assert.Nil(t, tracker.last.lsn, "a batch with no lsn must checkpoint none")
				return
			}
			require.NotNil(t, tracker.last.lsn)
			assert.Equal(t, test.wantLSN, *tracker.last.lsn)
		})
	}
}
