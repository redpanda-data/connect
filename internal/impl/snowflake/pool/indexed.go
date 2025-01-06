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
	indexedImpl[T any] struct {
		ctor  func(context.Context, string) (T, error)
		items map[string]chan T
		mu    chan any
	}
)

var _ Indexed[any] = &indexedImpl[any]{}

// NewIndexed creates a new Indexed pool that uses the following constructor to create new items.
func NewIndexed[T any](ctor func(context.Context, string) (T, error)) Indexed[T] {
	i := &indexedImpl[T]{
		ctor:  ctor,
		items: map[string]chan T{},
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

func (p *indexedImpl[T]) Acquire(ctx context.Context, name string) (item T, err error) {
	if err = p.lock(ctx); err != nil {
		return
	}
	ch, ok := p.items[name]
	if ok {
		p.unlock()
		select {
		case item := <-ch:
			return item, nil
		case <-ctx.Done():
			return item, ctx.Err()
		}
	}
	item, err = p.ctor(ctx, name)
	if err == nil {
		p.items[name] = make(chan T, 1)
	}
	p.unlock()
	return item, err
}

func (p *indexedImpl[T]) Release(name string, item T) {
	_ = p.lock(context.Background())
	defer p.unlock()
	p.items[name] <- item
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
