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
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

// recordingCache is a minimal service.Cache capturing Set calls.
type recordingCache struct {
	service.Cache
	mu   sync.Mutex
	sets [][]byte
}

func (c *recordingCache) Set(_ context.Context, _ string, value []byte, _ *time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sets = append(c.sets, append([]byte(nil), value...))
	return nil
}

func (c *recordingCache) recorded() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.sets...)
}

func newTestInput(t *testing.T) (*oracleDBCDCInput, *recordingCache) {
	t.Helper()
	cache := &recordingCache{}
	o := &oracleDBCDCInput{
		cfg:             Config{SCNCacheKey: "scn"},
		res:             service.MockResources(),
		log:             service.NewLoggerFromSlog(slog.Default()),
		stopSig:         shutdown.NewSignaller(),
		cpCache:         cache,
		batching:        service.BatchPolicy{Count: 1},
		checkpointLimit: 8,
	}
	batcher, err := o.batching.NewBatcher(o.res)
	require.NoError(t, err)
	pub := newBatchPublisher(batcher, checkpoint.NewCapped[replication.SCN](8), o.log)
	pub.cacheSCN = o.cacheSCN
	o.publisher.Store(pub)
	t.Cleanup(func() { o.publisher.Load().Close() })
	return o, cache
}

// TestCacheSCNMonotonicGuard locks in the persist guard: advancing writes
// land, equal and regressing writes are silently skipped - a stale ack from
// an abandoned publisher generation must never move the durable resume
// position backwards.
func TestCacheSCNMonotonicGuard(t *testing.T) {
	o, cache := newTestInput(t)
	ctx := t.Context()

	require.NoError(t, o.cacheSCN(ctx, replication.SCN(10)))
	require.NoError(t, o.cacheSCN(ctx, replication.SCN(20)), "an advancing SCN must persist")
	require.NoError(t, o.cacheSCN(ctx, replication.SCN(20)), "an equal SCN is a no-op, not an error")
	require.NoError(t, o.cacheSCN(ctx, replication.SCN(15)), "a regressing SCN is a no-op, not an error")

	got := cache.recorded()
	require.Len(t, got, 2, "only the two advancing writes may reach the cache")

	require.Error(t, o.cacheSCN(ctx, replication.InvalidSCN), "an invalid SCN is rejected")
}

// TestRebuildPublisherIfPoisoned proves the rebuild actually swaps
// generations: the old publisher is closed, the new one is distinct with a
// fresh tracker wired to cacheSCN, and a late ack from the OLD generation
// cannot regress the durable position past the guard.
func TestRebuildPublisherIfPoisoned(t *testing.T) {
	o, cache := newTestInput(t)
	ctx := t.Context()

	old := o.publisher.Load()

	// Not poisoned: same generation back.
	same, err := o.rebuildPublisherIfPoisoned()
	require.NoError(t, err)
	require.Same(t, old, same)

	// Deliver a batch on the old generation but hold its ack (late ack). The
	// flushing Publish blocks on the unbuffered channel until consumed.
	oldPublished := make(chan error, 1)
	go func() { oldPublished <- old.Publish(ctx, streamingEvent(10)) }()
	oldMsg := <-old.msgs()
	require.NoError(t, <-oldPublished)

	// Poison and rebuild.
	old.poisoned.Store(true)
	rebuilt, err := o.rebuildPublisherIfPoisoned()
	require.NoError(t, err)
	require.NotSame(t, old, rebuilt, "a poisoned publisher must be replaced")
	require.Same(t, rebuilt, o.publisher.Load(), "the stored pointer must be the new generation")
	select {
	case <-old.shutSig.HasStoppedChan():
	default:
		t.Fatal("the old generation's flush loop must be stopped by the rebuild")
	}

	// The new generation persists progress normally.
	newPublished := make(chan error, 1)
	go func() { newPublished <- rebuilt.Publish(ctx, streamingEvent(30)) }()
	newMsg := <-rebuilt.msgs()
	require.NoError(t, <-newPublished)
	require.NoError(t, newMsg.ackFn(ctx, nil))

	// The old generation's late ack resolves into its abandoned tracker and
	// must be a no-op on the durable position (monotonic guard).
	require.NoError(t, oldMsg.ackFn(ctx, nil))
	got := cache.recorded()
	require.NotEmpty(t, got)
	require.Equal(t, replication.SCN(30).Bytes(), got[len(got)-1], "a late ack from the abandoned generation must not regress the cache")
}

// TestReadBatchReconnectsOnPoisonedLoopDeath encodes the silent-stall
// finding: with period-only batching the timed-flush loop is the only
// flusher, and when it dies after poisoning the publisher nothing else can
// trigger the reconnect that rebuilds it - ReadBatch must observe the stop
// and return ErrNotConnected. A DELIBERATE stop (FlushRemaining at the
// snapshot-only handoff, publisher not poisoned) must instead keep draining
// so the final flushed batch is still delivered.
func TestReadBatchReconnectsOnPoisonedLoopDeath(t *testing.T) {
	t.Run("poisoned loop death reconnects", func(t *testing.T) {
		o, _ := newTestInput(t)
		pub := o.publisher.Load()
		pub.poisoned.Store(true)
		pub.shutSig.TriggerSoftStop()
		require.Eventually(t, func() bool {
			select {
			case <-pub.shutSig.HasStoppedChan():
				return true
			default:
				return false
			}
		}, 5*time.Second, time.Millisecond)

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_, _, err := o.ReadBatch(ctx)
		require.ErrorIs(t, err, service.ErrNotConnected,
			"a poisoned publisher whose flush loop died must force a reconnect, not stall silently")
	})

	t.Run("deliberate stop keeps draining the final batch", func(t *testing.T) {
		o, _ := newTestInput(t)
		pub := o.publisher.Load()
		// FlushRemaining's shape: stop the loop (not poisoned), then flush the
		// final batch, which blocks on msgs() until ReadBatch consumes it.
		pub.shutSig.TriggerSoftStop()
		require.Eventually(t, func() bool {
			select {
			case <-pub.shutSig.HasStoppedChan():
				return true
			default:
				return false
			}
		}, 5*time.Second, time.Millisecond)

		flushed := make(chan error, 1)
		go func() {
			if err := pub.Publish(t.Context(), streamingEvent(10)); err != nil {
				flushed <- err
				return
			}
			flushed <- nil
		}()

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		batch, ackFn, err := o.ReadBatch(ctx)
		require.NoError(t, err, "a deliberately stopped loop must not force a reconnect while the final batch is undelivered")
		require.Len(t, batch, 1)
		require.NoError(t, ackFn(ctx, nil))
		require.NoError(t, <-flushed)
	})
}
