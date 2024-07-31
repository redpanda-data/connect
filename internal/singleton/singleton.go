// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package singleton

import (
	"context"
	"sync"
)

// Singleton is a thread-safe type that holds one `T` per process.
//
// Example usage:
//
//	var globalFoo = singleton.New(singleton.Config[*Foo]{
//		Constructor: func (ctx context.Context) (*Foo, error) {
//			return NewFoo(ctx)
//		},
//		Destructor: func (ctx context.Context, foo *Foo) error {
//			return foo.Close(ctx)
//		})
//
// In your setup code:
//
//	foo, ticket, err := globalFoo.Acquire(ctx)
//
// In your teardown code:
//
//	err := globalFoo.Close(ctx, ticket)
type Singleton[T any] struct {
	mu         sync.Mutex
	tickets    map[Ticket]struct{}
	nextTicket Ticket
	cfg        Config[T]
	value      T
}

// Ticket is an opaque type signifying that a singleton's resource is acquired.
type Ticket int

// Config holds the required methods to setup/teardown a `Singleton`.
type Config[T any] struct {
	Constructor func(context.Context) (T, error)
	Destructor  func(context.Context, T) error
}

// New creates a new singleton using the given constructor and destructor to setup and teardown the object.
func New[T any](cfg Config[T]) *Singleton[T] {
	// Don't use 0 as the initial ticket so default values don't mess up the reference counting
	return &Singleton[T]{
		cfg:        cfg,
		tickets:    map[Ticket]struct{}{},
		nextTicket: Ticket(1),
	}
}

// Acquire returns the singleton value, creating it if needed and returning the ticket for close.
//
// If there is no error, any result from `Acquire` should be cached.
//
// There must be a corresponding call to `Close` for each successful call to `Acquire` with the
// returned ticket.
func (s *Singleton[T]) Acquire(ctx context.Context) (val T, t Ticket, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.tickets) == 0 {
		val, err = s.cfg.Constructor(ctx)
		if err != nil {
			return
		}
		s.value = val
	} else {
		val = s.value
	}
	t = s.nextTicket
	s.nextTicket++
	s.tickets[t] = struct{}{}
	return
}

// Close the item behind the singleton using the ticket, and if needed calling the destructor.
//
// This function must be called once for every successful `Acquire` call on the singleton.
//
// This function is safe to call (even concurrently) with the same ticket - subsequent calls will noop.
func (s *Singleton[T]) Close(ctx context.Context, ticket Ticket) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Prevent multiple destructor calls and only call the destructor if the ref count goes to 0.
	if len(s.tickets) == 0 {
		return nil
	}
	delete(s.tickets, ticket)
	if len(s.tickets) == 0 {
		return s.cfg.Destructor(ctx, s.value)
	}
	return nil
}
