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
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/mssqlserver/replication"
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

func newTestInput(t *testing.T) (*sqlServerCDCInput, *recordingCache) {
	t.Helper()
	cache := &recordingCache{}
	i := &sqlServerCDCInput{
		cfg:             &config{lsnCacheKey: "lsn"},
		res:             service.MockResources(),
		log:             service.NewLoggerFromSlog(slog.Default()),
		stopSig:         shutdown.NewSignaller(),
		cpCache:         cache,
		batching:        service.BatchPolicy{Count: 1},
		checkpointLimit: 8,
	}
	batcher, err := i.batching.NewBatcher(i.res)
	require.NoError(t, err)
	pub := newBatchPublisher(batcher, checkpoint.NewCapped[replication.LSN](8), i.log)
	pub.cacheLSN = i.cacheLSN
	i.publisher.Store(pub)
	t.Cleanup(func() { i.publisher.Load().shutSig.TriggerSoftStop() })
	return i, cache
}

// TestCacheLSNMonotonicGuard locks in the persist guard: advancing writes
// land, equal and regressing writes are silently skipped - a stale ack from
// an abandoned publisher generation must never move the durable resume
// position backwards.
func TestCacheLSNMonotonicGuard(t *testing.T) {
	i, cache := newTestInput(t)
	ctx := t.Context()

	require.NoError(t, i.cacheLSN(ctx, replication.LSN("00000010")))
	require.NoError(t, i.cacheLSN(ctx, replication.LSN("00000020")), "an advancing LSN must persist")
	require.NoError(t, i.cacheLSN(ctx, replication.LSN("00000020")), "an equal LSN is a no-op, not an error")
	require.NoError(t, i.cacheLSN(ctx, replication.LSN("00000015")), "a regressing LSN is a no-op, not an error")

	got := cache.recorded()
	require.Len(t, got, 2, "only the two advancing writes may reach the cache")
	require.Equal(t, "00000010", string(got[0]))
	require.Equal(t, "00000020", string(got[1]))

	require.Error(t, i.cacheLSN(ctx, nil), "an empty LSN is rejected")
}

// TestRebuildPublisherIfPoisoned proves the rebuild actually swaps
// generations: the old publisher is closed, the new one is a distinct
// publisher with a fresh tracker wired to cacheLSN, and a late ack from the
// OLD generation cannot regress the durable position past the guard.
func TestRebuildPublisherIfPoisoned(t *testing.T) {
	i, cache := newTestInput(t)
	ctx := t.Context()

	old := i.publisher.Load()

	// Not poisoned: same generation back.
	same, err := i.rebuildPublisherIfPoisoned()
	require.NoError(t, err)
	require.Same(t, old, same)

	// Deliver a batch on the old generation but hold its ack (late ack). The
	// flushing Publish blocks on the unbuffered channel until consumed.
	oldPublished := make(chan error, 1)
	go func() { oldPublished <- old.Publish(ctx, streamingEvent("00000010", "00000010")) }()
	oldMsg := <-old.msgs()
	require.NoError(t, <-oldPublished)

	// Poison and rebuild.
	old.poisoned.Store(true)
	rebuilt, err := i.rebuildPublisherIfPoisoned()
	require.NoError(t, err)
	require.NotSame(t, old, rebuilt, "a poisoned publisher must be replaced")
	require.Same(t, rebuilt, i.publisher.Load(), "the stored pointer must be the new generation")
	select {
	case <-old.shutSig.HasStoppedChan():
	default:
		t.Fatal("the old generation's flush loop must be stopped by the rebuild")
	}

	// The new generation persists progress normally.
	newPublished := make(chan error, 1)
	go func() { newPublished <- rebuilt.Publish(ctx, streamingEvent("00000030", "00000030")) }()
	newMsg := <-rebuilt.msgs()
	require.NoError(t, <-newPublished)
	require.NoError(t, newMsg.ackFn(ctx, nil))

	// The old generation's late ack resolves into its abandoned tracker and
	// must be a no-op on the durable position (monotonic guard).
	require.NoError(t, oldMsg.ackFn(ctx, nil))
	got := cache.recorded()
	require.Equal(t, "00000030", string(got[len(got)-1]), "a late ack from the abandoned generation must not regress the cache")
	for _, v := range got {
		require.NotEqual(t, "00000010", string(v), "the stale LSN must never have been persisted after the newer one")
	}
}
