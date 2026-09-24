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
	"sync"
)

// ErrSealed is returned by Acquire after the lock is sealed.
var ErrSealed = errors.New("ticket lock sealed")

// Lock is a FIFO ticket lock. A waiter can abandon its ticket, and the lock
// then skips that ticket when its turn comes. A sealed lock refuses all
// further turns.
//
// Typical use: goroutines flush batches from a shared batcher, and the
// checkpoint must track the batches in flush order. Each flusher does:
//
//  1. Lock the batcher, flush, call Take, unlock the batcher.
//  2. Call Acquire to wait for its turn.
//  3. Track and send the batch. This step can block.
//  4. Call Release.
//
// The batcher lock is free during step 3, so other goroutines can continue
// to add and flush. If a flushed batch is dropped, seal the lock: no later
// batch can then be tracked past the gap.
//
// The zero value is ready to use. A Lock must not be copied after first use.
type Lock struct {
	// mu guards all the fields below.
	mu sync.Mutex
	// next is the ticket that the next Take hands out.
	next uint64
	// serving is the ticket whose turn it is.
	serving uint64
	// waiters holds one channel per parked Acquire call, by ticket.
	// Close the channel to wake the waiter.
	waiters map[uint64]chan struct{}
	// abandoned holds the tickets whose Acquire was cancelled. Release
	// skips them.
	abandoned map[uint64]struct{}
	// sealed refuses all further turns. It is never cleared.
	sealed bool
}

// Take draws the next ticket. Call it inside the same critical section as
// the work that the ticket orders (for example, a batch flush), so that
// ticket order is equal to the order of that work. That caller lock can be
// held while Take runs: Acquire and Release never take it.
func (l *Lock) Take() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	t := l.next
	l.next++
	return t
}

// Acquire blocks until it is ticket t's turn, until ctx is cancelled, or
// until the lock is sealed. On success, the caller must call Release exactly
// once. If it is already t's turn, Acquire returns nil and does not check ctx.
//
// On cancellation, Acquire marks t abandoned and returns ctx.Err(). The
// caller must NOT call Release, because Release skips t when its turn comes.
//
// sealOnAbandon declares that an abandon leaves a gap that no later ticket
// may pass (for example, the ticket holds a flushed batch that is now
// dropped). The seal then happens IN THE SAME critical section that records
// the abandonment: the moment the abandonment is visible, a Release from the
// previous holder can skip t and admit the next ticket. A seal after that
// would let the next ticket pass the gap before the seal lands. Without
// sealOnAbandon (for example, a barrier ticket with no batch), an abandoned
// ticket is skipped and the sequence continues.
func (l *Lock) Acquire(ctx context.Context, t uint64, sealOnAbandon bool) error {
	// Fast path: refuse a sealed lock, or return at once if it is our turn.
	l.mu.Lock()
	if l.sealed {
		l.mu.Unlock()
		return ErrSealed
	}
	if l.serving == t {
		l.mu.Unlock()
		return nil
	}
	// Park: register a channel that Release (our turn) or Seal closes.
	if l.waiters == nil {
		l.waiters = make(map[uint64]chan struct{})
	}
	ch := make(chan struct{})
	l.waiters[t] = ch
	l.mu.Unlock()

	select {
	case <-ch:
		// Woken: it is our turn, or the lock is sealed.
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.sealed {
			return ErrSealed
		}
		return nil
	case <-ctx.Done():
		// Cancelled: a Release or a Seal can close ch at the same time, so
		// check ch again under the lock before we abandon.
		l.mu.Lock()
		defer l.mu.Unlock()
		select {
		case <-ch:
			// Woken between the cancellation and the lock: either it is
			// t's turn (the caller owns the Release) or the lock is sealed.
			if l.sealed {
				return ErrSealed
			}
			return nil
		default:
		}
		// Abandon: Release skips t from now on. Seal in this same critical
		// section if the caller asked (see the doc comment).
		delete(l.waiters, t)
		if l.abandoned == nil {
			l.abandoned = make(map[uint64]struct{})
		}
		l.abandoned[t] = struct{}{}
		if sealOnAbandon {
			l.sealLocked()
		}
		return ctx.Err()
	}
}

// Release gives the turn to the next ticket that is not abandoned. Every
// ticket for which Acquire returned nil must be released exactly once,
// error paths included, or the sequence stops.
func (l *Lock) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.serving++
	for {
		if _, ok := l.abandoned[l.serving]; !ok {
			break
		}
		delete(l.abandoned, l.serving)
		l.serving++
	}
	if ch, ok := l.waiters[l.serving]; ok {
		close(ch)
		delete(l.waiters, l.serving)
	}
}

// Seal permanently refuses all further turns and wakes every waiter, which
// then returns ErrSealed. A holder whose Acquire already returned nil keeps
// its turn. Seal is safe to call with the caller lock of Take held.
func (l *Lock) Seal() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sealLocked()
}

// Sealed reports whether the lock is sealed.
func (l *Lock) Sealed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sealed
}

func (l *Lock) sealLocked() {
	l.sealed = true
	for t, ch := range l.waiters {
		close(ch)
		delete(l.waiters, t)
	}
}
