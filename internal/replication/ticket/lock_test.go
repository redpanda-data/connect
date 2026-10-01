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
	"errors"
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

var errTestSeal = errors.New("test seal")

// waitAsync starts Wait in a goroutine and returns its result channel.
func waitAsync(ctx context.Context, tkt Ticket) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- tkt.Wait(ctx) }()
	return ch
}

func requireResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(waitTimeout):
		require.FailNow(t, "Wait did not return")
		return nil
	}
}

// waitParked waits until exactly n Wait calls are parked. A parked call
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

// takeFor draws a ticket with TakeSkippable if skippable is true, and with
// Take if it is false.
func takeFor(l *Lock, skippable bool) Ticket {
	if skippable {
		return l.TakeSkippable()
	}
	return l.Take()
}

func TestTakeIsSequential(t *testing.T) {
	var l Lock
	assert.Equal(t, uint64(0), l.Take().n)
	assert.Equal(t, uint64(1), l.TakeSkippable().n)
	assert.Equal(t, uint64(2), l.Take().n)
}

func TestWaitFIFO(t *testing.T) {
	var l Lock
	t0, t1, t2 := l.Take(), l.Take(), l.Take()

	// Start the later tickets first: the order of arrival must not matter.
	r2 := waitAsync(t.Context(), t2)
	r1 := waitAsync(t.Context(), t1)
	waitParked(t, &l, 2)

	require.NoError(t, t0.Wait(t.Context()))
	t0.Release()

	require.NoError(t, requireResult(t, r1))
	waitParked(t, &l, 1)
	t1.Release()

	require.NoError(t, requireResult(t, r2))
	t2.Release()
}

func TestReleaseSkipsAbandonedTicket(t *testing.T) {
	var l Lock
	t0, t1, t2 := l.Take(), l.TakeSkippable(), l.Take()
	require.NoError(t, t0.Wait(t.Context()))

	ctx1, cancel1 := context.WithCancel(t.Context())
	r1 := waitAsync(ctx1, t1)
	r2 := waitAsync(t.Context(), t2)
	waitParked(t, &l, 2)
	cancel1()
	require.ErrorIs(t, requireResult(t, r1), context.Canceled)
	assert.NoError(t, l.Err(), "an abandon of a skippable ticket must not seal")

	t0.Release()
	require.NoError(t, requireResult(t, r2), "the abandoned ticket must be skipped")
	t2.Release()
}

func TestAbandonSeals(t *testing.T) {
	var l Lock
	t0, t1, t2 := l.Take(), l.Take(), l.Take()
	require.NoError(t, t0.Wait(t.Context()))

	r2 := waitAsync(t.Context(), t2)
	ctx1, cancel1 := context.WithCancel(t.Context())
	r1 := waitAsync(ctx1, t1)
	waitParked(t, &l, 2)
	cancel1()
	require.ErrorIs(t, requireResult(t, r1), context.Canceled)
	assert.ErrorIs(t, l.Err(), ErrSealed)

	// The seal wakes the waiter behind the abandoned ticket before any
	// Release, so it cannot pass the gap.
	require.ErrorIs(t, requireResult(t, r2), ErrSealed)
	t0.Release()
}

func TestSealWakesEveryWaiter(t *testing.T) {
	var l Lock
	t0 := l.Take()
	require.NoError(t, t0.Wait(t.Context()))

	results := make([]<-chan error, 3)
	for i := range results {
		results[i] = waitAsync(t.Context(), l.Take())
	}
	waitParked(t, &l, len(results))

	l.Seal(errTestSeal)
	for _, r := range results {
		require.ErrorIs(t, requireResult(t, r), ErrSealed)
	}
	t0.Release()
}

func TestWaitAfterSeal(t *testing.T) {
	var l Lock
	t0 := l.Take()
	l.Seal(errTestSeal)
	assert.ErrorIs(t, l.Err(), ErrSealed)
	require.ErrorIs(t, t0.Wait(t.Context()), ErrSealed,
		"a seal refuses even the ticket whose turn it is")
}

func TestWaitCancelledAfterWake(t *testing.T) {
	// A cancelled context is not an abandon when it is already the turn of
	// the ticket: the caller holds the turn and must Release it.
	var l Lock
	t0 := l.Take()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, t0.Wait(ctx))
	assert.NoError(t, l.Err())
	t0.Release()

	t1 := l.Take()
	require.NoError(t, t1.Wait(t.Context()))
	t1.Release()
}

func TestReleaseSkipsConsecutiveAbandonedTickets(t *testing.T) {
	var l Lock
	t0 := l.Take()
	require.NoError(t, t0.Wait(t.Context()))

	ctx, cancel := context.WithCancel(t.Context())
	abandoned := make([]<-chan error, 3)
	for i := range abandoned {
		abandoned[i] = waitAsync(ctx, l.TakeSkippable())
	}
	t4 := l.Take()
	last := waitAsync(t.Context(), t4)
	waitParked(t, &l, 4)

	cancel()
	for _, r := range abandoned {
		require.ErrorIs(t, requireResult(t, r), context.Canceled)
	}
	waitParked(t, &l, 1)

	t0.Release()
	require.NoError(t, requireResult(t, last), "one Release must skip every abandoned ticket in a row")
	l.mu.Lock()
	assert.Empty(t, l.abandoned, "skipped tickets must be removed")
	l.mu.Unlock()
	t4.Release()
}

func TestSealWhileHolding(t *testing.T) {
	// The holder seals, then releases: the publisher does this when Track
	// fails after the turn was given.
	var l Lock
	t0 := l.Take()
	require.NoError(t, t0.Wait(t.Context()))
	next := waitAsync(t.Context(), l.Take())
	waitParked(t, &l, 1)

	l.Seal(errTestSeal)
	require.ErrorIs(t, requireResult(t, next), ErrSealed)
	t0.Release()

	require.ErrorIs(t, l.Take().Wait(t.Context()), ErrSealed,
		"a ticket taken after the seal must be refused")
}

func TestReleaseBeforeTurn(t *testing.T) {
	tests := []struct {
		name       string
		skippable  bool
		wantSealed bool
	}{
		{
			name:       "take seals",
			skippable:  false,
			wantSealed: true,
		},
		{
			name:       "take skippable is skipped",
			skippable:  true,
			wantSealed: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var l Lock
			t0 := l.Take()
			t1 := takeFor(&l, tc.skippable)
			t2 := l.TakeSkippable()
			require.NoError(t, t0.Wait(t.Context()))

			// t1 gives up before its turn, without a Wait.
			t1.Release()
			assert.Equal(t, tc.wantSealed, l.Err() != nil)

			t0.Release()
			err := t2.Wait(t.Context())
			if tc.wantSealed {
				require.ErrorIs(t, err, ErrSealed)
				return
			}
			require.NoError(t, err, "the released ticket must be skipped")
			t2.Release()
		})
	}
}

func TestReleaseAtTurnWithoutWait(t *testing.T) {
	// It is the turn of t0, but its Wait was never called. A Release here
	// must abandon t0, not end its turn as if it held the turn: for a ticket
	// from Take, the work of t0 is lost.
	tests := []struct {
		name       string
		skippable  bool
		wantSealed bool
	}{
		{
			name:       "take seals",
			skippable:  false,
			wantSealed: true,
		},
		{
			name:       "take skippable is skipped",
			skippable:  true,
			wantSealed: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var l Lock
			t0 := takeFor(&l, tc.skippable)
			t1 := l.TakeSkippable()

			t0.Release()
			assert.Equal(t, tc.wantSealed, l.Err() != nil)

			err := t1.Wait(t.Context())
			if tc.wantSealed {
				require.ErrorIs(t, err, ErrSealed)
				return
			}
			require.NoError(t, err)
			t1.Release()
		})
	}
}

func TestSecondReleaseDoesNothing(t *testing.T) {
	var l Lock
	t0, t1, t2 := l.Take(), l.Take(), l.Take()
	require.NoError(t, t0.Wait(t.Context()))
	t0.Release()
	require.NoError(t, t1.Wait(t.Context()))
	r2 := waitAsync(t.Context(), t2)
	waitParked(t, &l, 1)

	// A second Release of t0 must not end the turn of t1.
	t0.Release()
	l.mu.Lock()
	assert.Equal(t, t1.n, l.serving)
	assert.True(t, l.held)
	l.mu.Unlock()
	waitParked(t, &l, 1)

	t1.Release()
	require.NoError(t, requireResult(t, r2))
	t2.Release()
	assert.NoError(t, l.Err())
}

func TestReleaseAfterCancelledWait(t *testing.T) {
	var l Lock
	t0, t1, t2 := l.Take(), l.TakeSkippable(), l.Take()
	require.NoError(t, t0.Wait(t.Context()))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, t1.Wait(ctx), context.Canceled)
	// t1 is already abandoned, so this Release does nothing.
	t1.Release()
	assert.NoError(t, l.Err())

	t0.Release()
	require.NoError(t, t2.Wait(t.Context()))
	t2.Release()
	// t1 was skipped before this Release, so it also does nothing.
	t1.Release()

	l.mu.Lock()
	defer l.mu.Unlock()
	assert.Equal(t, uint64(3), l.serving)
	assert.Empty(t, l.abandoned)
	assert.NoError(t, l.err)
}

func TestWaitAfterRelease(t *testing.T) {
	tests := []struct {
		name    string
		release func(t *testing.T, tkt Ticket)
	}{
		{
			name: "after the turn",
			release: func(t *testing.T, tkt Ticket) {
				require.NoError(t, tkt.Wait(t.Context()))
				tkt.Release()
			},
		},
		{
			name: "before the turn",
			release: func(_ *testing.T, tkt Ticket) {
				tkt.Release()
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var l Lock
			t0 := l.TakeSkippable()
			tc.release(t, t0)
			// The Wait must return an error at once. If it parks, nothing
			// ever wakes it.
			require.ErrorContains(t, t0.Wait(t.Context()), "already released")
		})
	}
}

// TestWaitConcurrent runs many takers with random cancellation, and some
// takers that release without a Wait. It checks that only one goroutine has
// the turn at a time, that turns come in ticket order, and that the sequence
// never stops. It also exercises the race where a cancellation and a wake
// happen at the same time.
func TestWaitConcurrent(t *testing.T) {
	for range 20 {
		runConcurrentRound(t, 32)
	}
}

// taker is one goroutine of runConcurrentRound.
type taker struct {
	ticket      Ticket
	cancellable bool  // a cancel at a random time is scheduled
	noWait      bool  // the taker releases its ticket without a Wait
	err         error // result of Wait
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

	// Start the takers. About one in three is cancelled at a random time,
	// and about one in eight releases its ticket without a Wait.
	takers := make([]taker, n)
	for i := range takers {
		tk := &takers[i]
		tk.ticket = l.TakeSkippable()
		tk.noWait = rand.N(8) == 0
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
			defer tk.ticket.Release()
			if tk.noWait {
				return
			}
			if tk.err = tk.ticket.Wait(ctx); tk.err != nil {
				return
			}
			turns.hold(t, tk.ticket.n)
		})
	}
	requireWait(t, &wg, "the ticket sequence stopped")

	// A taker that was not cancelled must get its turn. A cancelled taker
	// can get its turn or abandon it.
	for _, tk := range takers {
		switch {
		case tk.noWait:
		case !tk.cancellable:
			require.NoError(t, tk.err, "ticket %d was never cancelled", tk.ticket.n)
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
	assert.NoError(t, l.err)
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

func TestSealCause(t *testing.T) {
	var l Lock
	t0 := l.Take()
	l.Seal(errTestSeal)

	err := l.Err()
	require.ErrorIs(t, err, ErrSealed)
	require.ErrorIs(t, err, errTestSeal)
	assert.EqualError(t, err, "ticket lock sealed: test seal")
	assert.Equal(t, err, t0.Wait(t.Context()), "Wait must return the seal error")
}

func TestFirstSealCauseWins(t *testing.T) {
	var l Lock
	l.Seal(errTestSeal)
	l.Seal(errors.New("second seal"))
	assert.EqualError(t, l.Err(), "ticket lock sealed: test seal")
}

func TestSealNilCause(t *testing.T) {
	var l Lock
	l.Seal(nil)
	assert.Equal(t, ErrSealed, l.Err())
}

func TestAbandonSealCause(t *testing.T) {
	t.Run("cancelled wait", func(t *testing.T) {
		var l Lock
		t0, t1 := l.Take(), l.Take()
		require.NoError(t, t0.Wait(t.Context()))

		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(errors.New("shutting down"))
		require.ErrorIs(t, t1.Wait(ctx), context.Canceled)
		assert.EqualError(t, l.Err(), "ticket lock sealed: ticket 1 abandoned: shutting down")
		t0.Release()
	})
	t.Run("release before the turn", func(t *testing.T) {
		var l Lock
		t0, t1 := l.Take(), l.Take()
		require.NoError(t, t0.Wait(t.Context()))

		t1.Release()
		assert.EqualError(t, l.Err(), "ticket lock sealed: ticket 1 released before its turn")
		t0.Release()
	})
}
