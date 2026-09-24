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

// Lock is a FIFO ticket lock. It works like the "take a number" counter at
// a bakery: each caller takes a numbered ticket, and waits until the "now
// serving" display shows its number. Turns come strictly in ticket order.
//
// Two extras make it fit for CDC publishers:
//
//   - A waiter can give up, for example on shutdown. We say it "abandons"
//     its ticket. When the ticket's turn comes, the lock skips it, so the
//     tickets behind it do not wait forever.
//   - The lock can be "sealed". A sealed lock refuses all further turns.
//     Use this when a ticket's work was lost, and no later ticket may pass
//     that gap.
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
//  2. Call Acquire to wait for its turn. If ctx is cancelled here, the
//     ticket is abandoned: stop, and do NOT call Release.
//  3. Track and send the batch. This step can block.
//  4. Call Release, also on error paths.
//
// The batcher lock is free during step 3, so other goroutines can continue
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
	// next is the number on the next ticket that Take hands out.
	next uint64
	// serving is the "now serving" display: the ticket whose turn it is.
	serving uint64
	// waiters holds one channel for each parked Acquire, by ticket. Release
	// closes the channel when it is that ticket's turn, and Seal closes all
	// of them.
	waiters map[uint64]chan struct{}
	// abandoned holds the tickets whose Acquire was cancelled before their
	// turn. Release skips each one when its turn comes, and removes it.
	abandoned map[uint64]struct{}
	// sealed refuses all further turns. It is never cleared.
	sealed bool
}

// Take draws the next ticket.
//
// Call Take inside the same critical section as the work that the ticket
// orders, for example a batch flush. Ticket order is then equal to the order
// of that work. If you call Take after that critical section, another
// goroutine can flush and take a ticket in between, and the two batches then
// get their turns in the wrong order.
//
// You can hold that caller lock while you call Take or Seal. This cannot
// deadlock, because Acquire and Release never take the caller lock.
func (l *Lock) Take() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	t := l.next
	l.next++
	return t
}

// Acquire waits for the turn of ticket t. It returns when one of these
// happens first:
//
//   - It is t's turn. Acquire returns nil, and the caller now holds the turn.
//     The caller must call Release exactly once when it is done.
//   - ctx is cancelled. Acquire marks t as "abandoned" and returns ctx.Err().
//     The caller must NOT call Release. When t's turn comes, Release skips
//     t, so the tickets behind t do not wait forever.
//   - The lock is sealed. Acquire returns ErrSealed.
//
// If it is already t's turn, Acquire returns nil and does not look at ctx.
//
// sealOnAbandon is for a ticket that must not be skipped. For example, t
// holds a flushed batch. If we skip t, the next batch gets its turn, is
// tracked and acked, and the checkpoint moves past rows that were never
// sent. With sealOnAbandon, an abandon seals the lock instead, so no later
// ticket ever gets a turn.
//
// The seal happens in the same critical section that marks t as abandoned.
// It cannot happen later: as soon as t is marked, the holder before t can
// call Release, skip t, and give the turn to t+1. A seal after that is too
// late, because t+1 already passed the gap.
//
// Without sealOnAbandon (for example, a barrier ticket with no batch), an
// abandoned ticket is skipped and the sequence continues.
func (l *Lock) Acquire(ctx context.Context, t uint64, sealOnAbandon bool) error {
	// Lock is sealed, return immediately with ErrSealed.
	l.mu.Lock()
	if l.sealed {
		l.mu.Unlock()
		return ErrSealed
	}

	// It's our turn, good to go.
	if l.serving == t {
		l.mu.Unlock()
		return nil
	}

	// It's not our turn yet, so we need to "park".
	// Parking here means registering a channel that Release closes when it's
	// our turn, or Seal closes when the lock is sealed.
	if l.waiters == nil {
		l.waiters = make(map[uint64]chan struct{})
	}
	ch := make(chan struct{})
	l.waiters[t] = ch
	l.mu.Unlock()

	// Blocking section: wait for our turn or the lock to be sealed.
	select {
	case <-ch:
		// We were woken up. Check why: a Seal, or our turn.
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.sealed {
			return ErrSealed
		}
		return nil
	case <-ctx.Done():
		// We were cancelled, but we can be too late to abandon. Between
		// ctx.Done and l.mu.Lock, a Release can give us the turn, or a Seal
		// can close ch. So check ch again, now that we hold mu.
		l.mu.Lock()
		defer l.mu.Unlock()
		select {
		case <-ch:
			// The wake came first. If it was a Release, we hold the turn:
			// return nil, and the caller must Release it. If we abandon here
			// instead, nobody releases our turn, and no later ticket ever
			// gets one.
			if l.sealed {
				return ErrSealed
			}
			return nil
		default:
		}

		// Nobody woke us, so we "abandon" the ticket. Abandoning here means
		// removing our channel and adding t to the abandoned set. When t's
		// turn comes, Release skips it and gives the turn to the next ticket.
		delete(l.waiters, t)
		if l.abandoned == nil {
			l.abandoned = make(map[uint64]struct{})
		}
		l.abandoned[t] = struct{}{}

		// The caller says t must not be skipped: seal now, while we still
		// hold mu. See the Acquire doc for why this cannot wait.
		if sealOnAbandon {
			l.sealLocked()
		}
		return ctx.Err()
	}
}

// Release ends the current turn and gives the turn to the next ticket that
// is not abandoned. Call it exactly once for each Acquire that returned nil,
// also on error paths. If a turn is never released, no later ticket ever
// gets a turn.
func (l *Lock) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Our turn is done, so the next ticket is up.
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
	// fine: its Acquire sees serving == t and returns at once.
	if ch, ok := l.waiters[l.serving]; ok {
		close(ch)
		delete(l.waiters, l.serving)
	}
}

// Seal refuses all further turns, for good. Every parked Acquire wakes up
// and returns ErrSealed, and every later Acquire returns ErrSealed at once.
//
// A holder whose Acquire already returned nil keeps its turn, and must
// still call Release. For example, a holder whose Track fails seals the
// lock, and then releases its turn as usual.
//
// You can call Seal while you hold the caller lock of Take.
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

// sealLocked seals the lock and wakes every parked Acquire. Each one sees
// sealed and returns ErrSealed. The caller must hold mu.
func (l *Lock) sealLocked() {
	l.sealed = true
	for t, ch := range l.waiters {
		close(ch)
		delete(l.waiters, t)
	}
}
