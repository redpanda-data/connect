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

	// indexedWaiter is one blocked Acquire call's single-use, buffered-by-one
	// handoff channel; addressing waiters individually is what makes the
	// handoff FIFO.
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

// Acquire hands a checked-out item to waiters in the order they called
// Acquire (FIFO). Go gives no ordering guarantee among goroutines blocked on
// one shared channel, and callers with an ordering requirement across
// acquisitions (the snowflake output's exactly-once dedup) need one.
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

// abandonWaiter removes w from name's queue after its Acquire was cancelled.
// If Release already dequeued w and sent it the item, the item is in w.ch
// and is fed back through Release rather than stranded. The non-blocking
// drain is sound because Release dequeues and sends atomically under the
// pool lock: either this lock() won and no send is coming, or Release's won
// and the item is already buffered.
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
		// Reset dropped this name while the item was checked out.
		p.unlock()
		return
	}
	if front := entry.waiters.Front(); front != nil {
		entry.waiters.Remove(front)
		// Send under the lock (the channel is buffered by one, so this
		// cannot block): dequeue+send as one step is what lets
		// abandonWaiter's non-blocking drain be authoritative. Sending
		// after unlocking left a window where a cancelled waiter could
		// see itself already dequeued, find w.ch empty, and give up --
		// stranding the item and wedging the entry until Reset.
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
