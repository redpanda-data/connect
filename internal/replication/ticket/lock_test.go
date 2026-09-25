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
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
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

// waitParked waits until exactly n Acquire calls are parked. A parked call
// returns only after a Release or a Seal wakes it, or after its context is
// cancelled.
func waitParked(t *testing.T, l *Lock, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return len(l.waiters) == n
	}, waitTimeout, time.Millisecond)
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
	waitParked(t, &l, 2)

	require.NoError(t, l.Acquire(t.Context(), t0, false))
	l.Release()

	require.NoError(t, requireResult(t, r1))
	waitParked(t, &l, 1)
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
	waitParked(t, &l, 2)
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
	waitParked(t, &l, 2)
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
	waitParked(t, &l, len(results))

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

func TestReleaseSkipsConsecutiveAbandonedTickets(t *testing.T) {
	var l Lock
	t0 := l.Take()
	require.NoError(t, l.Acquire(t.Context(), t0, false))

	ctx, cancel := context.WithCancel(t.Context())
	abandoned := make([]<-chan error, 3)
	for i := range abandoned {
		abandoned[i] = acquireAsync(ctx, &l, l.Take(), false)
	}
	last := acquireAsync(t.Context(), &l, l.Take(), false)
	waitParked(t, &l, 4)

	cancel()
	for _, r := range abandoned {
		require.ErrorIs(t, requireResult(t, r), context.Canceled)
	}
	waitParked(t, &l, 1)

	l.Release()
	require.NoError(t, requireResult(t, last), "one Release must skip every abandoned ticket in a row")
	l.mu.Lock()
	assert.Empty(t, l.abandoned, "skipped tickets must be removed")
	l.mu.Unlock()
	l.Release()
}

func TestSealWhileHolding(t *testing.T) {
	// The holder seals, then releases: the publisher does this when Track
	// fails after the turn was given.
	var l Lock
	t0 := l.Take()
	require.NoError(t, l.Acquire(t.Context(), t0, false))
	next := acquireAsync(t.Context(), &l, l.Take(), false)
	waitParked(t, &l, 1)

	l.Seal()
	require.ErrorIs(t, requireResult(t, next), ErrSealed)
	l.Release()

	require.ErrorIs(t, l.Acquire(t.Context(), l.Take(), false), ErrSealed,
		"a ticket taken after the seal must be refused")
}

// TestAcquireConcurrent runs many takers with random cancellation. It checks
// that only one goroutine has the turn at a time, that turns come in ticket
// order, and that the sequence never stops. It also exercises the race where
// a cancellation and a wake happen at the same time.
func TestAcquireConcurrent(t *testing.T) {
	for range 20 {
		runConcurrentRound(t, 32)
	}
}

// taker is one goroutine of runConcurrentRound.
type taker struct {
	ticket      uint64
	cancellable bool  // a cancel at a random time is scheduled
	err         error // result of Acquire
}

// turnLog records the turns in the order they happen.
type turnLog struct {
	holders atomic.Int32
	mu      sync.Mutex
	order   []uint64
}

// hold records a turn. It fails the test if another taker has the turn at
// the same time.
func (r *turnLog) hold(t *testing.T, ticket uint64) {
	if n := r.holders.Add(1); n != 1 {
		t.Errorf("ticket %d: %d holders at the same time", ticket, n)
	}
	r.mu.Lock()
	r.order = append(r.order, ticket)
	r.mu.Unlock()
	runtime.Gosched() // Let the other takers run while we have the turn.
	r.holders.Add(-1)
}

func runConcurrentRound(t *testing.T, n int) {
	t.Helper()
	var (
		l     Lock
		turns turnLog
		wg    sync.WaitGroup
	)

	// Start the takers. About one in three is cancelled at a random time.
	takers := make([]taker, n)
	for i := range takers {
		tk := &takers[i]
		tk.ticket = l.Take()
		ctx, cancel := context.WithCancel(t.Context())
		if rand.N(3) == 0 {
			tk.cancellable = true
			go func() {
				time.Sleep(rand.N(200 * time.Microsecond))
				cancel()
			}()
		}
		wg.Go(func() {
			defer cancel()
			if tk.err = l.Acquire(ctx, tk.ticket, false); tk.err != nil {
				return
			}
			turns.hold(t, tk.ticket)
			l.Release()
		})
	}
	requireWait(t, &wg, "the ticket sequence stopped")

	// A taker that was not cancelled must get its turn. A cancelled taker
	// can get its turn or abandon it.
	for _, tk := range takers {
		switch {
		case !tk.cancellable:
			require.NoError(t, tk.err, "ticket %d was never cancelled", tk.ticket)
		case tk.err != nil:
			require.ErrorIs(t, tk.err, context.Canceled)
		}
	}
	require.IsIncreasing(t, turns.order, "turns must come in ticket order")

	// Every ticket was served or skipped, and nothing is left behind.
	l.mu.Lock()
	defer l.mu.Unlock()
	assert.Equal(t, uint64(n), l.serving)
	assert.Empty(t, l.waiters)
	assert.Empty(t, l.abandoned)
}

// requireWait waits for wg, and fails the test after waitTimeout.
func requireWait(t *testing.T, wg *sync.WaitGroup, msg string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(waitTimeout):
		require.FailNow(t, msg)
	}
}
