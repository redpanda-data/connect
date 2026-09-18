// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pool

import (
	"container/list"
	"context"
)

type (
	// Indexed is essentially a pool where each object in the pool is explicitly retrieved by name.
	Indexed[T any] interface {
		// Acquire gets a named object T out of the pool if available, otherwise will create a new
		// item using the given name.
		// The context can be used to abort waiting for an item to be released, otherwise an error
		// is only ever returned if creating the object in the pool fails.
		Acquire(ctx context.Context, name string) (T, error)
		// Return the object back to the pool to be used.
		Release(name string, item T)
		// Reset all items in the pool
		Reset()
		// Get all the keys in the pool
		Keys() []string
	}

	// indexedEntry tracks one name's item: either idle and available (item
	// holds it, waiters is empty) or checked out (a caller holds it, and
	// anyone else calling Acquire for this name queues in waiters).
	indexedEntry[T any] struct {
		idle    bool
		item    T
		waiters list.List // of *indexedWaiter[T], oldest at Front
	}

	// indexedWaiter is one blocked Acquire call's personal, single-use,
	// buffered-by-one handoff channel. Addressing waiters individually
	// (rather than having them all race to receive from one shared channel)
	// is what makes Release's handoff FIFO instead of arbitrary.
	indexedWaiter[T any] struct {
		ch chan T
	}

	indexedImpl[T any] struct {
		ctor  func(context.Context, string) (T, error)
		items map[string]*indexedEntry[T]
		mu    chan any
	}
)

var _ Indexed[any] = &indexedImpl[any]{}

// NewIndexed creates a new Indexed pool that uses the following constructor to create new items.
func NewIndexed[T any](ctor func(context.Context, string) (T, error)) Indexed[T] {
	i := &indexedImpl[T]{
		ctor:  ctor,
		items: map[string]*indexedEntry[T]{},
		mu:    make(chan any, 1),
	}
	i.mu <- nil
	return i
}

func (p *indexedImpl[T]) lock(ctx context.Context) error {
	select {
	case <-p.mu:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *indexedImpl[T]) unlock() {
	p.mu <- nil
}

// Acquire hands the named item to callers in the order they called Acquire
// (FIFO) whenever it's already checked out. That guarantee matters to
// callers with their own external ordering requirement across acquisitions
// of the same name (internal/impl/snowflake's exactly-once dedup, the
// motivating case: it assumes whoever acquires a channel next holds a
// commit token no earlier than every previous holder's, which plain
// receive-from-a-shared-channel can't promise -- Go gives no ordering
// guarantee among goroutines simply blocked receiving on the same channel).
func (p *indexedImpl[T]) Acquire(ctx context.Context, name string) (item T, err error) {
	if err = p.lock(ctx); err != nil {
		return
	}
	entry, ok := p.items[name]
	if !ok {
		item, err = p.ctor(ctx, name)
		if err == nil {
			p.items[name] = &indexedEntry[T]{}
		}
		p.unlock()
		return item, err
	}
	if entry.idle {
		entry.idle = false
		item = entry.item
		p.unlock()
		return item, nil
	}
	w := &indexedWaiter[T]{ch: make(chan T, 1)}
	elem := entry.waiters.PushBack(w)
	p.unlock()
	select {
	case item = <-w.ch:
		return item, nil
	case <-ctx.Done():
		p.abandonWaiter(name, elem, w)
		return item, ctx.Err()
	}
}

// abandonWaiter removes w from name's wait queue after its Acquire call's
// context was cancelled. Release may have already won the race and handed
// w the item before the removal below runs -- in that case entry.waiters.Remove
// is a harmless no-op (w was already dequeued) and w.ch already has the
// item buffered, so drain it and feed it back through Release to whoever
// should actually get it (the next waiter, or idle) rather than stranding
// it.
//
// The non-blocking drain relies on Release performing its dequeue and its
// send as one atomic step under the pool lock (see Release): whichever of
// the two lock() calls -- this one or Release's -- wins the race decides
// the outcome cleanly. If this one wins, the Remove above is a real
// removal and Release will never target this w.ch at all, so the
// non-blocking receive correctly finds nothing. If Release's wins, both
// its dequeue and its send complete before this lock() call can succeed,
// so the item is unconditionally already sitting in w.ch by the time
// the receive below runs. There is no third outcome where w.ch is
// empty now but a send is still coming -- that gap (send happening
// after, not during, Release's lock hold) is exactly what used to make
// this drain unsound.
func (p *indexedImpl[T]) abandonWaiter(name string, elem *list.Element, w *indexedWaiter[T]) {
	_ = p.lock(context.Background())
	if entry, ok := p.items[name]; ok {
		entry.waiters.Remove(elem)
	}
	p.unlock()
	select {
	case item := <-w.ch:
		p.Release(name, item)
	default:
	}
}

func (p *indexedImpl[T]) Release(name string, item T) {
	_ = p.lock(context.Background())
	entry, ok := p.items[name]
	if !ok {
		// Reset dropped this name while the item was checked out; there's
		// nothing left to release into.
		p.unlock()
		return
	}
	if front := entry.waiters.Front(); front != nil {
		entry.waiters.Remove(front)
		// Send while still holding the lock, not after unlocking. w.ch is
		// buffered by one, so this can never block -- and doing the
		// dequeue and the send as one atomic step under the lock is what
		// makes abandonWaiter's own under-lock check authoritative: if
		// abandonWaiter's lock() call happens first, it removes this
		// waiter before Release (below) ever sees it as front, and this
		// send never fires for that waiter. If Release's lock() call
		// happens first, the removal AND the send both complete before
		// abandonWaiter can acquire the lock, so by the time it checks,
		// the item is unconditionally already sitting in w.ch. Sending
		// after unlocking left a window where Release had dequeued the
		// waiter but not yet sent to it, during which abandonWaiter could
		// observe "already removed" and give up via its non-blocking
		// drain -- stranding the item in w.ch forever and wedging this
		// entry (idle=false, no waiters) until Reset().
		front.Value.(*indexedWaiter[T]).ch <- item
		p.unlock()
		return
	}
	entry.idle = true
	entry.item = item
	p.unlock()
}

func (p *indexedImpl[T]) Reset() {
	_ = p.lock(context.Background())
	clear(p.items)
	p.unlock()
}

func (p *indexedImpl[T]) Keys() []string {
	keys := []string{}
	_ = p.lock(context.Background())
	defer p.unlock()
	for k := range p.items {
		keys = append(keys, k)
	}
	return keys
}
