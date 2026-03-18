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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRWMutexLockUnlock(t *testing.T) {
	m := NewRWMutex()
	require.NoError(t, m.Lock(t.Context()))
	m.Unlock()
	// Can re-acquire after unlock.
	require.NoError(t, m.Lock(t.Context()))
	m.Unlock()
}

func TestRWMutexRLockRUnlock(t *testing.T) {
	m := NewRWMutex()
	require.NoError(t, m.RLock(t.Context()))
	m.RUnlock()
	// Can re-acquire after unlock.
	require.NoError(t, m.RLock(t.Context()))
	m.RUnlock()
}

func TestRWMutexConcurrentReaders(t *testing.T) {
	const numReaders = 10
	m := NewRWMutex()

	var wg sync.WaitGroup
	readersHeld := make(chan struct{}, numReaders)

	for range numReaders {
		wg.Go(func() {
			require.NoError(t, m.RLock(t.Context()))
			readersHeld <- struct{}{}
			// Hold long enough for all readers to be inside simultaneously.
			time.Sleep(20 * time.Millisecond)
			m.RUnlock()
		})
	}

	// All readers must be able to hold the lock at the same time.
	for range numReaders {
		select {
		case <-readersHeld:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for readers to acquire lock simultaneously")
		}
	}
	wg.Wait()
}

func TestRWMutexWriterExcludesReaders(t *testing.T) {
	m := NewRWMutex()
	require.NoError(t, m.Lock(t.Context()))

	rLockAcquired := make(chan struct{})
	go func() {
		require.NoError(t, m.RLock(t.Context()))
		close(rLockAcquired)
		m.RUnlock()
	}()

	// Give the goroutine time to block on RLock.
	time.Sleep(20 * time.Millisecond)
	select {
	case <-rLockAcquired:
		t.Fatal("RLock must not be acquired while write lock is held")
	default:
	}

	m.Unlock()

	select {
	case <-rLockAcquired:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for RLock after write unlock")
	}
}

func TestRWMutexTryLock(t *testing.T) {
	tests := []struct {
		name   string
		lockFn func(m *RWMutex)
		want   bool
	}{
		{
			name:   "succeeds when unlocked",
			lockFn: func(_ *RWMutex) {},
			want:   true,
		},
		{
			name:   "fails when write-locked",
			lockFn: func(m *RWMutex) { require.NoError(t, m.Lock(t.Context())) },
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewRWMutex()
			tt.lockFn(m)
			assert.Equal(t, tt.want, m.TryLock())
			if tt.want {
				m.Unlock() // release the TryLock acquisition
			} else {
				m.Unlock() // release the setup lock
			}
		})
	}
}

func TestRWMutexTryRLock(t *testing.T) {
	tests := []struct {
		name   string
		lockFn func(m *RWMutex)
		want   bool
	}{
		{
			name:   "succeeds when unlocked",
			lockFn: func(_ *RWMutex) {},
			want:   true,
		},
		{
			name:   "fails when write-locked",
			lockFn: func(m *RWMutex) { require.NoError(t, m.Lock(t.Context())) },
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewRWMutex()
			tt.lockFn(m)
			got := m.TryRLock()
			assert.Equal(t, tt.want, got)
			if tt.want {
				m.RUnlock()
			} else {
				m.Unlock() // release the setup write lock
			}
		})
	}
}

func TestRWMutexLockCancelledContext(t *testing.T) {
	m := NewRWMutex()
	require.NoError(t, m.Lock(t.Context()))
	defer m.Unlock()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.Error(t, m.Lock(ctx))
}

func TestRWMutexRLockCancelledContext(t *testing.T) {
	m := NewRWMutex()
	require.NoError(t, m.Lock(t.Context()))
	defer m.Unlock()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.Error(t, m.RLock(ctx))
}
