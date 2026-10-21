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
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const waitTimeout = 5 * time.Second

var errTestSeal = errors.New("test seal")

// waitAsync starts Wait in a goroutine and returns its result channel.
//
// A test that must know that the Wait parked runs in a synctest bubble, and
// calls synctest.Wait after waitAsync. synctest.Wait returns when every other
// goroutine of the bubble is durably blocked. A Wait is durably blocked only
// when it parks on woken: a goroutine that waits for Lock.mu is not.
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

// newLock returns a Lock that checks its invariants when the test ends.
func newLock(t *testing.T) *Lock {
	t.Helper()
	l := &Lock{}
	t.Cleanup(func() { checkInvariants(t, l) })
	return l
}

// checkInvariants checks the invariants that the godoc of Lock and node
// state. They hold each time mu is free, also while a Wait is parked.
func checkInvariants(t *testing.T, l *Lock) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.head == nil {
		assert.Nil(t, l.tail, "an empty queue has no tail")
		return
	}
	assert.Contains(t, []state{turn, held}, l.head.state, "head must have the turn")
	var last *node
	for i, nd := 0, l.head; nd != nil; i, nd = i+1, nd.next {
		if nd != l.head {
			assert.Contains(t, []state{waiting, done}, nd.state, "queue position %d is after head", i)
		}
		if l.err != nil {
			assert.NotEqual(t, waiting, nd.state, "queue position %d: a sealed lock has no waiting ticket", i)
		}
		if nd.woken != nil {
			closed := false
			select {
			case <-nd.woken:
				closed = true
			default:
			}
			assert.Equal(t, nd.state != waiting, closed, "queue position %d: woken is closed if and only if it left waiting", i)
		}
		last = nd
	}
	assert.Same(t, last, l.tail, "tail is the last node")
}

// queued checks the invariants of l, and returns the tickets in its queue,
// from the head. Each ticket is given as its index in tickets, or -1 if it is
// not in tickets. Tickets that are done leave the queue when the turn passes
// them.
func queued(t *testing.T, l *Lock, tickets ...Ticket) []int {
	t.Helper()
	checkInvariants(t, l)
	l.mu.Lock()
	defer l.mu.Unlock()
	var is []int
	for nd := l.head; nd != nil; nd = nd.next {
		is = append(is, slices.IndexFunc(tickets, func(tk Ticket) bool { return tk.nd == nd }))
	}
	return is
}

func TestWaitFIFO(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newLock(t)
		t0, t1, t2 := l.Take(), l.Take(), l.Take()

		// Start the later tickets first: the order of arrival must not matter.
		r2 := waitAsync(t.Context(), t2)
		r1 := waitAsync(t.Context(), t1)
		synctest.Wait()

		require.NoError(t, t0.Wait(t.Context()))
		t0.Release()

		require.NoError(t, requireResult(t, r1))
		synctest.Wait()
		t1.Release()

		require.NoError(t, requireResult(t, r2))
		t2.Release()
	})
}

func TestReleaseSkipsAbandonedTicket(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newLock(t)
		t0, t1, t2 := l.Take(), l.TakeSkippable(), l.Take()
		require.NoError(t, t0.Wait(t.Context()))

		ctx1, cancel1 := context.WithCancel(t.Context())
		r1 := waitAsync(ctx1, t1)
		r2 := waitAsync(t.Context(), t2)
		synctest.Wait()
		cancel1()
		require.ErrorIs(t, requireResult(t, r1), context.Canceled)
		assert.NoError(t, l.Err(), "an abandon of a skippable ticket must not seal")

		t0.Release()
		require.NoError(t, requireResult(t, r2), "the abandoned ticket must be skipped")
		t2.Release()
	})
}

func TestAbandonSeals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newLock(t)
		t0, t1, t2 := l.Take(), l.Take(), l.Take()
		require.NoError(t, t0.Wait(t.Context()))

		r2 := waitAsync(t.Context(), t2)
		ctx1, cancel1 := context.WithCancel(t.Context())
		r1 := waitAsync(ctx1, t1)
		synctest.Wait()
		cancel1()
		require.ErrorIs(t, requireResult(t, r1), context.Canceled)
		assert.ErrorIs(t, l.Err(), ErrSealed)

		// The seal wakes the waiter behind the abandoned ticket before any
		// Release, so it cannot pass the gap.
		err := requireResult(t, r2)
		require.ErrorIs(t, err, ErrSealed)
		// The context of t2 is not cancelled. If the seal error matched
		// context.Canceled, a caller would report the lost batch as a clean
		// shutdown.
		assert.NotErrorIs(t, err, context.Canceled)
		t0.Release()
	})
}

func TestSealWakesEveryWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newLock(t)
		t0 := l.Take()
		require.NoError(t, t0.Wait(t.Context()))

		results := make([]<-chan error, 3)
		for i := range results {
			results[i] = waitAsync(t.Context(), l.Take())
		}
		synctest.Wait()

		l.Seal(errTestSeal)
		for _, r := range results {
			require.ErrorIs(t, requireResult(t, r), ErrSealed)
		}
		t0.Release()
	})
}

func TestWaitAfterSeal(t *testing.T) {
	l := newLock(t)
	t0 := l.Take()
	l.Seal(errTestSeal)
	assert.ErrorIs(t, l.Err(), ErrSealed)
	require.ErrorIs(t, t0.Wait(t.Context()), ErrSealed,
		"a seal refuses even the ticket whose turn it is")
}

func TestWaitCancelledAtTurn(t *testing.T) {
	// A cancelled context is not an abandon when it is already the turn of
	// the ticket: the caller holds the turn and must Release it.
	l := newLock(t)
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

func TestWaitWokenByReleaseOfCopy(t *testing.T) {
	// A Release on a copy abandons a skippable ticket while its Wait is
	// parked. The lock is not sealed, so the parked Wait wakes up to a done
	// ticket, and must fail without a turn.
	synctest.Test(t, func(t *testing.T) {
		l := newLock(t)
		t0, t1 := l.Take(), l.TakeSkippable()
		require.NoError(t, t0.Wait(t.Context()))

		r1 := waitAsync(t.Context(), t1)
		synctest.Wait()
		cp := t1
		cp.Release()
		require.EqualError(t, requireResult(t, r1), "ticket turn is already over")
		assert.NoError(t, l.Err(), "an abandon of a skippable ticket must not seal")

		t0.Release()
		assert.Empty(t, queued(t, l), "the turn must skip the abandoned ticket")
	})
}

func TestWaitWakeAndCancelAtOnce(t *testing.T) {
	// The wake of a parked Wait and the cancel of its context happen in one
	// critical section. The parked Wait then sees both channels ready, and
	// select picks one at random. The wake came first, so Wait must return
	// its result on both paths, and must not abandon the ticket.
	tests := []struct {
		name    string
		wake    func(l *Lock)
		wantErr error
	}{
		{
			name:    "turn",
			wake:    func(l *Lock) { l.finishLocked(l.head, nil) },
			wantErr: nil,
		},
		{
			name:    "seal",
			wake:    func(l *Lock) { l.sealLocked(errTestSeal) },
			wantErr: ErrSealed,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// Each round has about one chance in two to take the cancel path.
				for range 100 {
					l := newLock(t)
					t0, t1 := l.Take(), l.Take()
					require.NoError(t, t0.Wait(t.Context()))
					ctx, cancel := context.WithCancel(t.Context())
					r1 := waitAsync(ctx, t1)
					synctest.Wait()

					l.mu.Lock()
					cancel()
					tc.wake(l)
					l.mu.Unlock()

					err := requireResult(t, r1)
					if tc.wantErr == nil {
						require.NoError(t, err, "the turn came before the cancel")
						require.NoError(t, l.Err(), "a ticket that got its turn must not seal the lock")
						t1.Release()
					} else {
						require.ErrorIs(t, err, tc.wantErr, "the seal came before the cancel")
						t0.Release()
					}
				}
			})
		})
	}
}

func TestReleaseSkipsConsecutiveAbandonedTickets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newLock(t)
		t0 := l.Take()
		require.NoError(t, t0.Wait(t.Context()))

		ctx, cancel := context.WithCancel(t.Context())
		abandoned := make([]<-chan error, 3)
		for i := range abandoned {
			abandoned[i] = waitAsync(ctx, l.TakeSkippable())
		}
		t4 := l.Take()
		last := waitAsync(t.Context(), t4)
		synctest.Wait()

		cancel()
		for _, r := range abandoned {
			require.ErrorIs(t, requireResult(t, r), context.Canceled)
		}
		synctest.Wait()

		t0.Release()
		require.NoError(t, requireResult(t, last), "one Release must skip every abandoned ticket in a row")
		assert.Equal(t, []int{0}, queued(t, l, t4), "skipped tickets must be removed")
		t4.Release()
	})
}

func TestSealWhileHolding(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The holder seals, then releases: the publisher does this when Track
		// fails after the turn was given.
		l := newLock(t)
		t0 := l.Take()
		require.NoError(t, t0.Wait(t.Context()))
		next := waitAsync(t.Context(), l.Take())
		synctest.Wait()

		l.Seal(errTestSeal)
		require.ErrorIs(t, requireResult(t, next), ErrSealed)
		t0.Release()

		require.ErrorIs(t, l.Take().Wait(t.Context()), ErrSealed,
			"a ticket taken after the seal must be refused")
	})
}

// TestReleaseInEachState releases a ticket in each of its states, and then
// a second time. head holds the turn, tkt is the ticket under test, and
// next is a skippable ticket behind it.
func TestReleaseInEachState(t *testing.T) {
	tests := []struct {
		name      string
		skippable bool
		// sealFirst seals the lock before tkt is taken.
		sealFirst bool
		// prepare moves tkt to the state under test. It returns the result
		// of a parked Wait of tkt, or nil. A nil prepare leaves tkt waiting.
		prepare    func(t *testing.T, l *Lock, head, tkt Ticket) <-chan error
		wantSealed bool
		// wantQueued is the queue after the releases of tkt, before head
		// ends its turn: 0 is head, 1 is tkt, and 2 is next.
		wantQueued []int
	}{
		{
			name:       "waiting, take seals",
			skippable:  false,
			wantSealed: true,
			wantQueued: []int{0, 1, 2},
		},
		{
			name:       "waiting, take skippable is skipped",
			skippable:  true,
			wantSealed: false,
			wantQueued: []int{0, 1, 2},
		},
		{
			name:       "parked, take seals",
			skippable:  false,
			prepare:    parkWait,
			wantSealed: true,
			wantQueued: []int{0, 1, 2},
		},
		{
			name:       "parked, take skippable is skipped",
			skippable:  true,
			prepare:    parkWait,
			wantSealed: false,
			wantQueued: []int{0, 1, 2},
		},
		{
			// It is the turn of tkt, but its Wait was never called. Release
			// must abandon tkt, not end its turn: the work of tkt is lost.
			name:       "turn without Wait, take seals",
			skippable:  false,
			prepare:    giveTurn,
			wantSealed: true,
			wantQueued: nil,
		},
		{
			name:       "turn without Wait, take skippable is skipped",
			skippable:  true,
			prepare:    giveTurn,
			wantSealed: false,
			wantQueued: []int{2},
		},
		{
			name:      "held",
			skippable: false,
			prepare: func(t *testing.T, _ *Lock, head, tkt Ticket) <-chan error {
				head.Release()
				require.NoError(t, tkt.Wait(t.Context()))
				require.EqualError(t, tkt.Wait(t.Context()), "ticket already holds its turn",
					"a second Wait must fail and keep the turn")
				return nil
			},
			wantSealed: false,
			wantQueued: []int{2},
		},
		{
			name:      "done by a cancelled Wait",
			skippable: true,
			prepare: func(t *testing.T, _ *Lock, _, tkt Ticket) <-chan error {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				require.ErrorIs(t, tkt.Wait(ctx), context.Canceled)
				return nil
			},
			wantSealed: false,
			wantQueued: []int{0, 1, 2},
		},
		{
			// A caller defers Release, so each ticket refused after a seal
			// is released. These releases must not put the ticket in the
			// queue.
			name:       "done by a seal before the take",
			skippable:  false,
			sealFirst:  true,
			wantSealed: true,
			wantQueued: []int{0},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := newLock(t)
				head := l.Take()
				require.NoError(t, head.Wait(t.Context()))
				if tc.sealFirst {
					l.Seal(errTestSeal)
				}
				var tkt Ticket
				if tc.skippable {
					tkt = l.TakeSkippable()
				} else {
					tkt = l.Take()
				}
				next := l.TakeSkippable()
				var parked <-chan error
				if tc.prepare != nil {
					parked = tc.prepare(t, l, head, tkt)
				}

				tkt.Release()
				if parked != nil {
					// If the parked Wait stays parked, nothing wakes it later:
					// the lock skips tkt when its turn comes.
					err := requireResult(t, parked)
					require.Error(t, err, "Release must wake the parked Wait")
					assert.Equal(t, tc.wantSealed, errors.Is(err, ErrSealed))
				}
				// A second Release must do nothing. If it ended a turn, next
				// would lose its turn and its Wait would fail below.
				tkt.Release()
				assert.Equal(t, tc.wantSealed, l.Err() != nil)
				assert.Equal(t, tc.wantQueued, queued(t, l, head, tkt, next))
				require.Error(t, requireResult(t, waitAsync(t.Context(), tkt)),
					"a Wait after Release must fail at once")

				head.Release()
				err := next.Wait(t.Context())
				if tc.wantSealed {
					require.ErrorIs(t, err, ErrSealed)
				} else {
					require.NoError(t, err, "the turn must pass tkt")
					next.Release()
				}
				assert.Empty(t, queued(t, l))
			})
		})
	}
}

// parkWait parks a Wait of tkt, and returns its result.
func parkWait(t *testing.T, _ *Lock, _, tkt Ticket) <-chan error {
	r := waitAsync(t.Context(), tkt)
	synctest.Wait()
	return r
}

// giveTurn ends the turn of head, so that tkt gets the turn.
func giveTurn(_ *testing.T, _ *Lock, head, _ Ticket) <-chan error {
	head.Release()
	return nil
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
	order   []int
}

// hold records a turn. It fails the test if another taker has the turn at
// the same time.
func (r *turnLog) hold(t *testing.T, ticket int) {
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
	l := newLock(t)
	var (
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
			turns.hold(t, i)
		})
	}
	requireWait(t, &wg, "the ticket sequence stopped")

	// A taker that was not cancelled must get its turn. A cancelled taker
	// can get its turn or abandon it.
	for i, tk := range takers {
		switch {
		case tk.noWait:
		case !tk.cancellable:
			require.NoError(t, tk.err, "ticket %d was never cancelled", i)
		case tk.err != nil:
			require.ErrorIs(t, tk.err, context.Canceled)
		}
	}
	require.IsIncreasing(t, turns.order, "turns must come in ticket order")

	// Every ticket was served or skipped, and nothing is left behind.
	assert.Empty(t, queued(t, l))
	for i, tk := range takers {
		assert.Equal(t, done, tk.ticket.nd.state, "ticket %d", i)
	}
	assert.NoError(t, l.Err())
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
	tests := []struct {
		name string
		// causes are given to Seal, one call each, in order.
		causes  []error
		wantErr string
	}{
		{
			name: "cause in the message",
			causes: []error{
				errTestSeal,
			},
			wantErr: "ticket lock sealed: test seal",
		},
		{
			name: "first cause wins",
			causes: []error{
				errTestSeal,
				errors.New("second seal"),
			},
			wantErr: "ticket lock sealed: test seal",
		},
		{
			name: "nil cause",
			causes: []error{
				nil,
			},
			wantErr: "ticket lock sealed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := newLock(t)
			t0 := l.Take()
			for _, cause := range tc.causes {
				l.Seal(cause)
			}

			err := l.Err()
			require.ErrorIs(t, err, ErrSealed)
			assert.EqualError(t, err, tc.wantErr)
			for _, cause := range tc.causes {
				assert.NotErrorIs(t, err, cause, "the cause must be in the message only")
			}
			assert.Equal(t, err, t0.Wait(t.Context()), "Wait must return the seal error")
		})
	}
}

func TestAbandonSealCause(t *testing.T) {
	t.Run("cancelled wait", func(t *testing.T) {
		l := newLock(t)
		t0, t1 := l.Take(), l.Take()
		require.NoError(t, t0.Wait(t.Context()))

		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(errors.New("shutting down"))
		require.ErrorIs(t, t1.Wait(ctx), context.Canceled)
		assert.EqualError(t, l.Err(), "ticket lock sealed: ticket abandoned: shutting down")
		t0.Release()
	})
	t.Run("release before the turn", func(t *testing.T) {
		l := newLock(t)
		t0, t1 := l.Take(), l.Take()
		require.NoError(t, t0.Wait(t.Context()))

		t1.Release()
		assert.EqualError(t, l.Err(), "ticket lock sealed: ticket released before its turn")
		t0.Release()
	})
}
