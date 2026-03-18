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

package syncx

import (
	"context"
	"math"

	"golang.org/x/sync/semaphore"
)

// RWMutex is similar to sync.RWMutex but Lock and RLock accept a context,
// allowing the caller to give up waiting if the context is canceled. This is
// useful when the mutex is held during IO operations.
//
// Internally it uses a semaphore with weight math.MaxInt64: a write lock
// acquires the full weight for exclusive access, and each read lock acquires
// weight 1, allowing up to math.MaxInt64 concurrent readers.
type RWMutex struct {
	sema *semaphore.Weighted
}

// NewRWMutex returns a new, unlocked RWMutex.
func NewRWMutex() *RWMutex {
	return &RWMutex{sema: semaphore.NewWeighted(math.MaxInt64)}
}

// Lock acquires exclusive (write) access, blocking until the lock is available
// or ctx is canceled. Returns ctx.Err() if the context is canceled before the
// lock is acquired.
func (m *RWMutex) Lock(ctx context.Context) error {
	return m.sema.Acquire(ctx, math.MaxInt64)
}

// TryLock attempts to acquire exclusive (write) access without blocking.
// Returns true if the lock was acquired.
func (m *RWMutex) TryLock() bool {
	return m.sema.TryAcquire(math.MaxInt64)
}

// Unlock releases exclusive (write) access acquired by Lock or TryLock.
func (m *RWMutex) Unlock() {
	m.sema.Release(math.MaxInt64)
}

// RLock acquires shared (read) access, blocking until the lock is available or
// ctx is canceled. Returns ctx.Err() if the context is canceled before the
// lock is acquired.
func (m *RWMutex) RLock(ctx context.Context) error {
	return m.sema.Acquire(ctx, 1)
}

// TryRLock attempts to acquire shared (read) access without blocking.
// Returns true if the lock was acquired.
func (m *RWMutex) TryRLock() bool {
	return m.sema.TryAcquire(1)
}

// RUnlock releases shared (read) access acquired by RLock or TryRLock.
func (m *RWMutex) RUnlock() {
	m.sema.Release(1)
}
