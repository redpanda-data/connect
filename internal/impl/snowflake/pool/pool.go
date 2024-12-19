// Copyright 2024 Redpanda Data, Inc.
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
	"context"
	"sync"
	"sync/atomic"
)

type (
	// Capped is an object that reuses existing objects in a manner similar to sync.Pool,
	// but it's more strict than sync.Pool in that it will support a fixed upper bound
	// of items. If the cap has been reached then we will wait for one to become available.
	// Constructing new items is sequential in that only one will be created at a time.
	Capped[T any] interface {
		// Acquire gets an object T out of the pool if available, otherwise will create a new item.
		// The context can be used to abort waiting for an item from the queue, otherwise an error
		// is only ever returned if creating the object in the pool fails.
		Acquire(context.Context) (T, error)
		// TryAcquireExisting will return an item from the pool in a non-blocking manner.
		// if ok returns true, item should be `Release`-d back into the pool when it is
		// done being used.
		TryAcquireExisting() (item T, ok bool)
		// Return the object back to the pool to be used.
		Release(T)
		// Size returns the number of items the pool has *created* (which may be all in use).
		Size() int
		Reset()
	}
	cappedImpl[T any] struct {
		ctor      func(context.Context, int) (T, error)
		queued    chan T
		allocated atomic.Int64
		mu        sync.Mutex
	}
)

var _ Capped[any] = &cappedImpl[any]{}

// NewCapped constructs a new pool that will create up to `capacity` elements using `ctor`.
func NewCapped[T any](capacity int, ctor func(context.Context, int) (T, error)) Capped[T] {
	return &cappedImpl[T]{
		ctor:   ctor,
		queued: make(chan T, capacity),
	}
}

func (p *cappedImpl[T]) Acquire(ctx context.Context) (T, error) {
	item, ok := p.TryAcquireExisting()
	if ok {
		return item, nil
	}
	// lock-free check for the steady state
	if p.Size() >= cap(p.queued) {
		return p.acquireWait(ctx)
	}
	p.mu.Lock()
	// since we grabbed the lock we could have hit our cap
	id := p.Size()
	if id >= cap(p.queued) {
		p.mu.Unlock()
		return p.acquireWait(ctx)
	}
	item, err := p.ctor(ctx, id)
	if err == nil {
		p.allocated.Add(1)
	}
	p.mu.Unlock()
	return item, err
}

func (p *cappedImpl[T]) acquireWait(ctx context.Context) (item T, err error) {
	select {
	case item = <-p.queued:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return
}

func (p *cappedImpl[T]) TryAcquireExisting() (item T, ok bool) {
	select {
	case item = <-p.queued:
		ok = true
	default:
	}
	return
}

func (p *cappedImpl[T]) Release(item T) {
	p.queued <- item
}

func (p *cappedImpl[T]) Size() int {
	return int(p.allocated.Load())
}

func (p *cappedImpl[T]) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allocated.Store(0)
	for {
		select {
		case <-p.queued:
		default:
			return
		}
	}
}
