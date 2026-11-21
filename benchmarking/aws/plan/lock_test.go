// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package plan

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestLock(api S3API, now *time.Time) Lock {
	l := NewS3Lock(api, "bucket")
	l.(interface{ SetClock(func() time.Time) }).SetClock(func() time.Time { return *now })
	return l
}

func TestLockTwoAcquirersOneWins(t *testing.T) {
	api := newFakeS3()
	now := time.Unix(1700000000, 0)
	a, b := newTestLock(api, &now), newTestLock(api, &now)
	ctx := context.Background()
	okA, err := a.Acquire(ctx, "a", time.Hour)
	require.NoError(t, err)
	okB, err := b.Acquire(ctx, "b", time.Hour)
	require.NoError(t, err)
	require.True(t, okA)
	require.False(t, okB)
	require.Contains(t, string(api.objs["locks/shared-stack"]), "\"holder\":\"a\"")
}

func TestLockConcurrentFreeOneWins(t *testing.T) {
	api := newFakeS3()
	now := time.Unix(1700000000, 0)
	var wins int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := newTestLock(api, &now).Acquire(context.Background(), "h"+string(rune('a'+i)), time.Hour)
			require.NoError(t, err)
			if ok {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 1, wins)
}

func TestLockExpiredTakenOver(t *testing.T) {
	api := newFakeS3()
	now := time.Unix(1700000000, 0)
	ctx := context.Background()
	ok, err := newTestLock(api, &now).Acquire(ctx, "old", time.Hour)
	require.NoError(t, err)
	require.True(t, ok)

	now = now.Add(30 * time.Minute)
	ok, err = newTestLock(api, &now).Acquire(ctx, "new", time.Hour)
	require.NoError(t, err)
	require.False(t, ok, "not yet expired")

	now = now.Add(31 * time.Minute)
	ok, err = newTestLock(api, &now).Acquire(ctx, "new", time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	require.Contains(t, string(api.objs["locks/shared-stack"]), "\"holder\":\"new\"")
}

func TestLockTwoContendersOnExpiredLockOneWins(t *testing.T) {
	api := newFakeS3()
	now := time.Unix(1700000000, 0)
	ctx := context.Background()
	ok, err := newTestLock(api, &now).Acquire(ctx, "old", time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	now = now.Add(2 * time.Hour)

	// Contender A reads the expired lock; before it can replace it, B reads
	// and replaces it. A's If-Match on the stale ETag must then fail.
	var bWon bool
	fired := false
	api.afterGet = map[string]func(){"locks/shared-stack": func() {
		if fired {
			return
		}
		fired = true
		var err error
		bWon, err = newTestLock(api, &now).Acquire(ctx, "b", time.Hour)
		require.NoError(t, err)
	}}
	aWon, err := newTestLock(api, &now).Acquire(ctx, "a", time.Hour)
	require.NoError(t, err)
	require.True(t, bWon)
	require.False(t, aWon)
	require.Contains(t, string(api.objs["locks/shared-stack"]), "\"holder\":\"b\"")
}

func TestLockReleaseByNonHolderIsNoOp(t *testing.T) {
	api := newFakeS3()
	now := time.Unix(1700000000, 0)
	l := newTestLock(api, &now)
	ctx := context.Background()
	ok, err := l.Acquire(ctx, "a", time.Hour)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, l.Release(ctx, "intruder"))
	require.Contains(t, api.objs, "locks/shared-stack")

	require.NoError(t, l.Release(ctx, "a"))
	require.NotContains(t, api.objs, "locks/shared-stack")

	require.NoError(t, l.Release(ctx, "a"), "releasing an absent lock is fine")
	ok, err = l.Acquire(ctx, "b", time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
}
