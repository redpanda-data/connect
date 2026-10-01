// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package ticket provides a cancellable, sealable ticket lock. A caller
// draws a ticket, and then waits until it is the ticket's turn. Turns are
// given in ticket order (Mellor-Crummey and Scott 1991, "Algorithms for
// scalable synchronization on shared-memory multiprocessors"; the sequencer
// of Reed and Kanodia 1979).
//
// CDC publishers use it to keep the checkpoint sequence equal to the flush
// order without holding a lock across a call that can block.
package ticket

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrSealed identifies the error of a sealed Lock. The error from Wait and
// Lock.Err wraps ErrSealed and also the cause given to Seal. Use errors.Is
// to test for it.
var ErrSealed = errors.New("ticket lock sealed")

// Lock is a FIFO ticket lock. It works like the "take a number" counter at
// a bakery: each caller takes a numbered ticket, and waits until the "now
// serving" display shows its number. Turns come strictly in ticket order.
//
// Two extras make it fit for CDC publishers:
//
//   - A caller can give up its ticket, for example on shutdown. The ticket
//     is then "abandoned". When its turn comes, the lock skips it, so the
//     tickets behind it do not wait forever.
//   - The lock can be "sealed". A sealed lock refuses all further turns.
//     Use this when the work of a ticket was lost, and no later ticket may
//     pass that gap.
//
// The caller states at take time what an abandon does. With Take, an
// abandon seals the lock. With TakeSkippable, the lock skips the ticket.
//
// Why not a plain sync.Mutex? A mutex does not hand out turns in a fixed
// order, and a wait for it cannot be cancelled. Here the order is the whole
// point, as the typical use below shows.
//
// Typical use: goroutines flush batches from a shared batcher. The
// checkpoint must track the batches in the order they were flushed. Each
// flusher does:
//
//  1. Lock the batcher, flush, call Take, unlock the batcher. Take happens
//     under the batcher lock, so ticket order is the same as flush order.
//  2. Defer Ticket.Release. It is safe in every state of the ticket.
//  3. Call Ticket.Wait to wait for its turn. If Wait returns an error,
//     stop.
//  4. Track and send the batch. This step can block.
//
// The batcher lock is free during step 4, so other goroutines can continue
// to add and flush while one flusher is blocked.
//
// If a flushed batch is dropped, for example because Track failed, seal the
// lock. The checkpoint then cannot move past the dropped rows. A sealed Lock
// stays sealed: build a new one (the publisher is rebuilt).
//
// The zero value is ready to use. A Lock must not be copied after first use.
type Lock struct {
	// mu guards all the fields below.
	mu sync.Mutex
	// next is the number on the next ticket that a take hands out.
	next uint64
	// serving is the "now serving" display: the number of the ticket whose
	// turn it is.
	serving uint64
	// held is true from the moment Wait returns nil for the ticket at
	// serving, until the Release of that ticket. Release reads it to choose
	// between the end of a turn and an abandon. serving alone is not
	// enough: a ticket can get its turn before its Wait is called, and a
	// Release on that path must abandon the ticket, not end its turn.
	held bool
	// waiters holds one channel for each parked Wait, by ticket number.
	// advanceLocked closes the channel when it is that ticket's turn, and
	// sealLocked closes all of them.
	waiters map[uint64]chan struct{}
	// abandoned holds the numbers of the tickets that were abandoned before
	// their turn. advanceLocked skips each one when its turn comes, and
	// removes it.
	abandoned map[uint64]struct{}
	// err is nil until the first seal. Then it holds ErrSealed joined with
	// the seal cause, and it does not change again. Wait and Err return it.
	// A non-nil err refuses all further turns.
	err error
}

// Take draws the next ticket. If the ticket is abandoned, the lock is
// sealed: no later ticket ever gets a turn. Use Take when the work of the
// ticket must not be lost silently, for example a flushed batch that the
// checkpoint must not pass. If you are not sure, use Take: an unnecessary
// seal costs a rebuild, but an incorrect skip can lose data.
//
// Call Take inside the same critical section as the work that the ticket
// orders. Ticket order is then equal to the order of that work. If you call
// it after that critical section, another goroutine can do its work and
// take a ticket in between, and the two get their turns in the wrong order.
//
// You can hold that caller lock while you call Take, TakeSkippable or Seal.
// This cannot deadlock, because Wait and Release never take the caller
// lock.
func (l *Lock) Take() Ticket {
	return l.take(true)
}

// TakeSkippable draws the next ticket. If the ticket is abandoned, the lock
// skips it and the sequence continues. Use it when nothing is lost if the
// turn of the ticket never happens, for example a ticket that only waits
// for all earlier tickets to finish.
//
// The same critical section rule as for Take applies.
func (l *Lock) TakeSkippable() Ticket {
	return l.take(false)
}

func (l *Lock) take(sealOnAbandon bool) Ticket {
	l.mu.Lock()
	defer l.mu.Unlock()
	t := Ticket{l: l, n: l.next, sealOnAbandon: sealOnAbandon}
	l.next++
	return t
}

// Seal refuses all further turns, for good. cause says why, for example
// the error of a failed Track. Wait and Err then return an error that wraps
// both ErrSealed and cause. A nil cause is allowed: the error is then
// ErrSealed alone.
//
// Only the first seal sets the cause. Later calls do nothing, so the error
// always shows the first failure.
//
// Every parked Wait wakes up and returns the seal error, and every later
// Wait returns it at once. A holder whose Wait already returned nil keeps
// its turn, and must still call Release. For example, a holder whose Track
// fails seals the lock, and then releases its turn as usual.
//
// You can call Seal while you hold the caller lock of Take.
func (l *Lock) Seal(cause error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sealLocked(cause)
}

// Err returns nil if the lock is not sealed. If it is sealed, Err returns
// the error that wraps ErrSealed and the first seal cause.
func (l *Lock) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// sealLocked seals the lock and wakes every parked Wait. Each one sees err
// and returns it. If the lock is already sealed, sealLocked does nothing.
// The caller must hold mu.
func (l *Lock) sealLocked(cause error) {
	if l.err != nil {
		return
	}
	l.err = ErrSealed
	if cause != nil {
		l.err = fmt.Errorf("%w: %w", ErrSealed, cause)
	}
	for t, ch := range l.waiters {
		close(ch)
		delete(l.waiters, t)
	}
}

// abandonLocked marks ticket t as abandoned. If t is a Take ticket, it also
// seals the lock, with cause as the seal cause. If it is already the turn
// of t, the turn goes at once to the next ticket. The caller must hold mu.
//
// The seal happens in the same critical section that marks t as abandoned.
// It cannot happen later: as soon as t is marked, the holder before t can
// call Release, skip t, and give the turn to t+1. A seal after that is too
// late, because t+1 already passed the gap.
func (l *Lock) abandonLocked(t Ticket, cause error) {
	if t.sealOnAbandon {
		l.sealLocked(cause)
	}
	if l.serving == t.n {
		l.advanceLocked()
		return
	}
	if l.abandoned == nil {
		l.abandoned = make(map[uint64]struct{})
	}
	l.abandoned[t.n] = struct{}{}
}

// advanceLocked gives the turn to the next ticket that is not abandoned,
// and wakes it if it is parked. The caller must hold mu.
func (l *Lock) advanceLocked() {
	l.held = false
	l.serving++

	// Skip the abandoned tickets. Nobody waits for them, so if we give one
	// of them the turn, nobody releases it and the sequence stops. For
	// example, if 3 and 4 are abandoned and 2 releases, the turn goes to 5.
	for {
		if _, ok := l.abandoned[l.serving]; !ok {
			break
		}
		delete(l.abandoned, l.serving)
		l.serving++
	}

	// Wake the new holder if it is parked. If it is not parked yet, that is
	// fine: its Wait sees serving == t and returns at once.
	if ch, ok := l.waiters[l.serving]; ok {
		close(ch)
		delete(l.waiters, l.serving)
	}
}

// Ticket is one place in the turn order of a Lock. Take and TakeSkippable
// hand it out. The holder calls Wait to get its turn, and Release when it
// is done with the ticket.
//
// A Ticket is a small value. You can copy it, but all copies are the same
// ticket and get one turn only. The zero Ticket is not valid.
type Ticket struct {
	// l is the Lock that issued the ticket.
	l *Lock
	// n is the place of the ticket in the turn order. Wait and Release
	// compare it with l.serving to find the state of the ticket.
	n uint64
	// sealOnAbandon is true for a Take ticket. abandonLocked reads it: if it
	// is true, the abandon seals l. The lock cannot find this value itself,
	// because only the taker knows if the work of the ticket can be lost.
	sealOnAbandon bool
}

// Wait waits for the turn of t. It returns when one of these happens
// first:
//
//   - It is the turn of t. Wait returns nil, and t holds the turn. Call
//     Release when you are done.
//   - ctx is cancelled. Wait abandons t and returns ctx.Err(). For a Take
//     ticket, the abandon also seals the lock, with the context cause in
//     the seal cause.
//   - The lock is sealed. Wait returns the seal error (see Lock.Err).
//
// If it is already the turn of t, Wait returns nil and does not look at
// ctx. After an error, Release is still safe to call, and does nothing.
//
// Call Wait at most once for each ticket. A Wait after the Release of t
// returns an error at once.
func (t Ticket) Wait(ctx context.Context) error {
	l := t.l
	l.mu.Lock()
	if l.err != nil {
		l.mu.Unlock()
		return l.err
	}

	// The turn of t already passed, or t is abandoned: nobody can wake a
	// parked Wait, so refuse at once.
	if _, abandoned := l.abandoned[t.n]; abandoned || t.n < l.serving {
		l.mu.Unlock()
		return fmt.Errorf("ticket %d is already released", t.n)
	}

	// It's our turn, good to go.
	if l.serving == t.n {
		l.held = true
		l.mu.Unlock()
		return nil
	}

	// It's not our turn yet, so we need to "park".
	// Parking here means registering a channel that advanceLocked closes when
	// it's our turn, or sealLocked closes when the lock is sealed.
	if l.waiters == nil {
		l.waiters = make(map[uint64]chan struct{})
	}
	ch := make(chan struct{})
	l.waiters[t.n] = ch
	l.mu.Unlock()

	// Blocking section: wait for our turn or the lock to be sealed.
	select {
	case <-ch:
		// We were woken up. Check why: a seal, or our turn.
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.wokenLocked()
	case <-ctx.Done():
		// We were cancelled, but we can be too late to abandon. Between
		// ctx.Done and l.mu.Lock, a Release can give us the turn, or a Seal
		// can close ch. So check ch again, now that we hold mu.
		l.mu.Lock()
		defer l.mu.Unlock()
		select {
		case <-ch:
			// The wake came first. If it was our turn, we hold it and
			// return nil, the same as when the turn is ours at the start.
			return l.wokenLocked()
		default:
		}

		// Nobody woke us, so we abandon the ticket.
		delete(l.waiters, t.n)
		l.abandonLocked(t, fmt.Errorf("ticket %d abandoned: %w", t.n, context.Cause(ctx)))
		return ctx.Err()
	}
}

// wokenLocked is the result of a parked Wait after its channel was closed.
// The channel is closed by a seal, or because it is the turn of the ticket.
// The caller must hold mu.
func (l *Lock) wokenLocked() error {
	if l.err != nil {
		return l.err
	}
	l.held = true
	return nil
}

// Release ends the use of t. Call it once for each ticket, also on error
// paths. The best place is a defer right after the take. What Release does
// depends on the state of t:
//
//   - Wait returned nil, so t holds the turn: Release gives the turn to the
//     next ticket that is not abandoned.
//   - Wait was not called, or did not return nil: Release abandons t. The
//     lock skips t when its turn comes. For a Take ticket, the abandon also
//     seals the lock, because the work of t is lost.
//   - t was already released, abandoned or skipped: Release does nothing.
//
// If a ticket is never released, no later ticket ever gets a turn.
func (t Ticket) Release() {
	l := t.l
	l.mu.Lock()
	defer l.mu.Unlock()

	// The turn of t already passed: t was released or skipped.
	if t.n < l.serving {
		return
	}
	// t holds the turn: end it.
	if t.n == l.serving && l.held {
		l.advanceLocked()
		return
	}
	// t was abandoned by a cancelled Wait.
	if _, ok := l.abandoned[t.n]; ok {
		return
	}
	// t never held the turn. Abandon it.
	l.abandonLocked(t, fmt.Errorf("ticket %d released before its turn", t.n))
}
