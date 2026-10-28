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
	"fmt"
)

// Ticket is one place in the turn order of a Lock. Take and TakeSkippable
// hand it out. The holder calls Wait to get its turn, and Release when it
// is done with the ticket.
//
// A ticket goes through these states:
//
//   - Waiting: an earlier ticket still has the turn.
//   - Turn: it is the turn of the ticket, but Wait has not returned nil
//     yet. For example, the caller has not called Wait.
//   - Held: Wait returned nil. The caller does its work.
//   - Done: Release ended the turn, the ticket was abandoned, or the lock
//     was sealed before the ticket got its turn.
//
// A ticket is abandoned when the caller gives up before Held: its Wait is
// cancelled, or it calls Release first. The lock skips an abandoned ticket.
// For a ticket obtained via Take (as opposed to TakeSkippable), the abandon
// also seals the lock.
//
// A Ticket is a small value. You can copy it, but all copies are the same
// ticket and get one turn only. The zero Ticket is not valid.
type Ticket struct {
	// l is the Lock that issued the ticket.
	l *Lock
	// nd holds the state of the ticket. Copies of the Ticket share it.
	nd *node
}

// Wait waits for the turn of t. It returns when one of these happens
// first:
//
//   - It is the turn of t. Wait returns nil, and t holds the turn. Call
//     Release when you are done.
//   - ctx is cancelled. Wait abandons t and returns ctx.Err(). For a ticket
//     obtained via Take (as opposed to TakeSkippable), the abandon also
//     seals the lock, with the context cause in the message of the seal
//     error.
//   - The lock is sealed. Wait returns the seal error (see Lock.Err).
//
// If it is already the turn of t, Wait returns nil and does not look at
// ctx. After an error, Release is still safe to call, and does nothing.
//
// Call Wait at most once for each ticket. A second Wait, or a Wait after
// Release, returns an error and does not change the turn.
func (t Ticket) Wait(ctx context.Context) error {
	l, nd := t.l, t.nd
	l.mu.Lock()
	defer l.mu.Unlock()

	// Park: it is not the turn of t yet. Wait outside mu until setLocked
	// closes woken. A sealed lock has no waiting tickets, so a parked Wait
	// always has a reason to wake up.
	if nd.state == waiting {
		// Release mu while parked. A turn or a seal closes woken, or ctx is
		// cancelled.
		l.mu.Unlock()
		select {
		case <-nd.woken:
		case <-ctx.Done():
		}

		// Resuming: re-acquire the mu lock.
		l.mu.Lock()

		// The state should have moved on from "waiting" before being woken up.
		// If not, it means the context was canceled before a turn or seal arrived.
		if nd.state == waiting {
			l.finishLocked(nd, fmt.Errorf("ticket abandoned: %w", context.Cause(ctx)))
			return ctx.Err()
		}
	}

	// Sealing takes precedence over ticket state. If the lock is sealed, return
	// the seal error even if this ticket would otherwise have its turn.
	if l.err != nil {
		return l.err
	}

	switch nd.state {
	case turn:
		nd.setLocked(held)
		return nil
	case held:
		// A second Wait, or a Wait on a copy of t.
		return errors.New("ticket already holds its turn")
	default:
		// done: the turn of the ticket is already over, but the caller still
		// called Wait on it. The state cannot be waiting here: a parked Wait
		// that is still waiting after it wakes returns above. The switch keeps
		// default instead of case done, because Go needs a terminating
		// statement.
		return errors.New("ticket turn is already over")
	}
}

// Release ends the use of t. If t is held, Release gives the turn to the
// next ticket. Before that, Release abandons t (see Ticket). After that, it
// does nothing. Call it for each ticket, also on error paths: a defer right
// after the take is best. If a ticket is never released, no later ticket
// ever gets a turn.
func (t Ticket) Release() {
	l := t.l
	l.mu.Lock()
	defer l.mu.Unlock()
	l.finishLocked(t.nd, errors.New("ticket released before its turn"))
}

// state is the state of a ticket. See Ticket for what each state means.
// The state only moves forward, and can skip states, for example waiting
// to done on an abandon.
type state uint8

const (
	// waiting indicates that an earlier ticket in the queue currently holds
	// the turn. The ticket is parked and waiting for notification via woken.
	waiting state = iota

	// turn indicates that this ticket is at the head of the queue and eligible
	// to proceed, but Wait has not yet returned nil (the caller has not yet
	// entered its critical section).
	turn

	// held indicates that Wait returned nil and the caller is actively
	// holding the turn.
	//
	// It is distinct from turn so that Release can differentiate between
	// normal completion and abandonment. If Release is called while still in
	// turn, the caller never ran its critical work, so the ticket is treated
	// as abandoned (sealing the lock for tickets obtained via Take). In held,
	// Release represents normal completion and passes the turn without
	// sealing.
	held

	// done indicates that the ticket's lifecycle has ended. This occurs after
	// a normal Release from held, an abandonment (cancelled Wait or early
	// Release), or when the lock is sealed.
	done
)

// node is the queue entry of one ticket. take creates it, and the Lock
// links it in its queue. Wait and Release read and change its state. All
// copies of a Ticket share one node, so they are the same ticket.
type node struct {
	// sealOnAbandon is true for tickets obtained via Take (and false for
	// TakeSkippable). finishLocked reads it: if it is true, an abandon seals
	// the lock. The lock cannot find this value itself, because only the taker
	// knows if the work of the ticket can be lost.
	sealOnAbandon bool
	// state is the state of the ticket. Lock.mu guards it. Change it with
	// setLocked only.
	state state
	// woken wakes a parked Wait. take makes it for a ticket that starts in
	// waiting, and it is nil for all other tickets. setLocked closes it when
	// state leaves waiting, so it is closed if and only if state left
	// waiting. It never changes after take, so a parked Wait reads it
	// without mu.
	woken chan struct{}
	// next is the node of the next ticket in the queue, or nil for the
	// tail.
	next *node
}

// setLocked moves nd to state s. It keeps the invariant of node.woken: when
// nd leaves waiting, setLocked closes woken, so a parked Wait wakes up and
// reads the new state. All state changes out of waiting must go through
// setLocked, or a parked Wait stays parked. The caller must hold Lock.mu.
func (nd *node) setLocked(s state) {
	if nd.state == waiting {
		close(nd.woken)
	}
	nd.state = s
}
