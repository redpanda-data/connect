// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package ticket

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const waitTimeout = 5 * time.Second

// acquireAsync starts Acquire in a goroutine and returns its result channel.
func acquireAsync(ctx context.Context, l *Lock, t uint64, sealOnAbandon bool) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- l.Acquire(ctx, t, sealOnAbandon) }()
	return ch
}

func requireResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(waitTimeout):
		require.FailNow(t, "Acquire did not return")
		return nil
	}
}

func requireBlocked(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		require.FailNow(t, "Acquire returned before its turn", "err: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestTakeIsSequential(t *testing.T) {
	var l Lock
	for want := range uint64(5) {
		assert.Equal(t, want, l.Take())
	}
}

func TestAcquireFIFO(t *testing.T) {
	var l Lock
	t0, t1, t2 := l.Take(), l.Take(), l.Take()

	// Start the later tickets first: the order of arrival must not matter.
	r2 := acquireAsync(t.Context(), &l, t2, false)
	r1 := acquireAsync(t.Context(), &l, t1, false)
	requireBlocked(t, r1)
	requireBlocked(t, r2)

	require.NoError(t, l.Acquire(t.Context(), t0, false))
	requireBlocked(t, r1)
	l.Release()

	require.NoError(t, requireResult(t, r1))
	requireBlocked(t, r2)
	l.Release()

	require.NoError(t, requireResult(t, r2))
	l.Release()
}

func TestReleaseSkipsAbandonedTicket(t *testing.T) {
	var l Lock
	t0, t1, t2 := l.Take(), l.Take(), l.Take()
	require.NoError(t, l.Acquire(t.Context(), t0, false))

	ctx1, cancel1 := context.WithCancel(t.Context())
	r1 := acquireAsync(ctx1, &l, t1, false)
	r2 := acquireAsync(t.Context(), &l, t2, false)
	requireBlocked(t, r1)
	cancel1()
	require.ErrorIs(t, requireResult(t, r1), context.Canceled)
	assert.False(t, l.Sealed(), "an abandon without sealOnAbandon must not seal")

	l.Release()
	require.NoError(t, requireResult(t, r2), "the abandoned ticket must be skipped")
	l.Release()
}

func TestAbandonSeals(t *testing.T) {
	var l Lock
	t0, t1, t2 := l.Take(), l.Take(), l.Take()
	require.NoError(t, l.Acquire(t.Context(), t0, false))

	r2 := acquireAsync(t.Context(), &l, t2, false)
	ctx1, cancel1 := context.WithCancel(t.Context())
	r1 := acquireAsync(ctx1, &l, t1, true)
	requireBlocked(t, r1)
	cancel1()
	require.ErrorIs(t, requireResult(t, r1), context.Canceled)
	assert.True(t, l.Sealed())

	// The seal wakes the waiter behind the abandoned ticket before any
	// Release, so it cannot pass the gap.
	require.ErrorIs(t, requireResult(t, r2), ErrSealed)
	l.Release()
}

func TestSealWakesEveryWaiter(t *testing.T) {
	var l Lock
	t0 := l.Take()
	require.NoError(t, l.Acquire(t.Context(), t0, false))

	results := make([]<-chan error, 3)
	for i := range results {
		results[i] = acquireAsync(t.Context(), &l, l.Take(), false)
	}
	for _, r := range results {
		requireBlocked(t, r)
	}

	l.Seal()
	for _, r := range results {
		require.ErrorIs(t, requireResult(t, r), ErrSealed)
	}
	l.Release()
}

func TestAcquireAfterSeal(t *testing.T) {
	var l Lock
	t0 := l.Take()
	l.Seal()
	assert.True(t, l.Sealed())
	require.ErrorIs(t, l.Acquire(t.Context(), t0, false), ErrSealed,
		"a seal refuses even the ticket whose turn it is")
}

func TestAcquireCancelledAfterWake(t *testing.T) {
	// A cancelled context is not an abandon when the ticket is already
	// admitted: the caller owns the turn and must Release it.
	var l Lock
	t0 := l.Take()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, l.Acquire(ctx, t0, true))
	assert.False(t, l.Sealed())
	l.Release()

	t1 := l.Take()
	require.NoError(t, l.Acquire(t.Context(), t1, false))
	l.Release()
}
