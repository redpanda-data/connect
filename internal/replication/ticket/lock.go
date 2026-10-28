// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package ticket provides a cancellable, sealable ticket lock. A caller
// draws a ticket, and then waits until it is the ticket's turn. Turns are
// given in ticket order.
//
// CDC publishers use it to keep the checkpoint sequence equal to the flush
// order without holding a lock across a call that can block.
package ticket

import (
	"errors"
	"fmt"
	"sync"
)

// ErrSealed marks a sealed Lock. After Seal, the error from Wait and Lock.Err
// wraps ErrSealed. Use errors.Is to test for it.
var ErrSealed = errors.New("ticket lock sealed")

// Lock is a FIFO ticket lock. Each caller takes a ticket, and the lock gives
// the turns strictly in ticket order. A caller can abandon its ticket (see
// Ticket), and the lock can be sealed, which refuses all further turns.
//
// Typical use: goroutines flush batches from a shared batcher, and the
// checkpoint must track the batches in flush order. Each flusher does:
//
//  1. Lock the batcher, flush, call Take, unlock the batcher.
//  2. Defer Ticket.Release.
//  3. Call Ticket.Wait. If it returns an error, stop.
//  4. Track and send the batch. This step can block, but the batcher lock
//     is free, so other goroutines can continue to flush.
//
// If a flushed batch is dropped, for example because Track failed, seal the
// lock, so that the checkpoint cannot move past the dropped rows. A sealed
// Lock stays sealed: build a new one.
//
// The zero value is ready to use. A Lock must not be copied after first use.
type Lock struct {
	// mu guards all the fields below, and the state of every node.
	mu sync.Mutex
	// head and tail are the ends of the queue of the tickets that are not
	// done, in ticket order. head has the turn, so its state is turn or held,
	// and never done. The nodes after head are waiting or done. Both are nil
	// if the queue is empty: the next take then gets the turn at once.
	head, tail *node
	// err is nil until the first seal. Then it holds ErrSealed, with the
	// seal cause in the message, and it does not change again. Wait and Err
	// return it. A non-nil err refuses all further turns.
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
	nd := &node{sealOnAbandon: sealOnAbandon}
	switch {
	case l.err != nil:
		// A sealed lock never gives a turn, so the ticket is done at once.
		// It does not go in the queue, so its Release has nothing to undo.
		nd.state = done
	case l.tail == nil:
		nd.state = turn
		l.head, l.tail = nd, nd
	default:
		// A ticket that starts in waiting gets its wake channel now, so the
		// field never changes and a parked Wait can read it without mu.
		nd.woken = make(chan struct{})
		l.tail.next = nd
		l.tail = nd
	}
	return Ticket{l: l, nd: nd}
}

// Seal refuses all further turns, for good. cause says why, for example the
// error of a failed Track. Only the first call has an effect.
//
// After Seal, Wait and Err return an error that wraps ErrSealed, with the
// text of cause in its message (ErrSealed alone if cause is nil). A parked
// Wait wakes up with this error. A holder whose Wait already returned nil
// keeps its turn, and must still call Release.
//
// The error does not wrap cause, because cause is often the context error of
// one goroutine. If the seal error matched context.Canceled, other goroutines
// could take a lost batch for a clean shutdown. Each caller must decide on
// shutdown from its own context.
func (l *Lock) Seal(cause error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sealLocked(cause)
}

// Err returns nil if the lock is not sealed. If it is sealed, Err returns
// the error that wraps ErrSealed, with the first seal cause in its message.
func (l *Lock) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// sealLocked seals the lock with cause. The caller must hold mu.
func (l *Lock) sealLocked(cause error) {
	// Only the first seal has an effect, so the error shows the first
	// failure.
	if l.err != nil {
		return
	}

	// A non-nil err is the seal. Wait returns it instead of a turn, and take
	// makes each new ticket done at once.
	l.err = ErrSealed
	if cause != nil {
		// %v, not %w: see Seal.
		l.err = fmt.Errorf("%w: %v", ErrSealed, cause)
	}

	// head keeps its state: a holder keeps its turn (see Seal). The tickets
	// after head can never get a turn, so make them done. setLocked wakes
	// their parked Waits. They stay in the queue until head finishes, and
	// finishLocked skips them then, as it skips an abandoned ticket.
	if l.head == nil {
		return
	}
	for nd := l.head.next; nd != nil; nd = nd.next {
		nd.setLocked(done)
	}
}

// finishLocked moves nd to done. Release and a cancelled Wait call it.
//
// If nd is finished before reaching held (while still in waiting or turn), it
// is considered abandoned. For a ticket obtained via Take (as opposed to
// TakeSkippable), this abandon seals the lock with cause to prevent
// subsequent tickets from proceeding past missing work.
//
// If nd is at the head of the queue, finishLocked advances the queue to the
// next ticket that is not done and gives it the turn. Otherwise, nd stays in
// the queue as done, and is skipped when its turn comes.
//
// The seal happens in the same critical section that makes nd done. It
// cannot happen later: as soon as nd is done, an earlier holder can call
// Release, skip nd, and give the turn to the ticket after it. A seal after
// that is too late, because that ticket already passed the gap.
//
// The caller must hold mu.
func (l *Lock) finishLocked(nd *node, cause error) {
	switch nd.state {
	case done:
		return
	case waiting, turn:
		// Abandon: the caller gave up before its turn was held.
		if nd.sealOnAbandon {
			l.sealLocked(cause)
		}
	case held:
		// Normal end of the turn. Nothing is lost, so do not seal.
	}
	nd.setLocked(done)
	if l.head != nd {
		return
	}

	// Advance to the next active waiter, skipping any tickets already marked
	// done. Because no goroutine waits for or releases an abandoned ticket,
	// giving it the turn would deadlock the lock.
	next := nd.next
	for next != nil && next.state == done {
		next = next.next
	}
	l.head = next
	if next == nil {
		l.tail = nil
		return
	}
	next.setLocked(turn)
}
