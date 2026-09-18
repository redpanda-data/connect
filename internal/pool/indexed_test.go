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

package pool_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/pool"
)

type bar struct {
	string
}

func TestIndexedAcquire(t *testing.T) {
	var mu sync.Mutex
	created := map[string]bool{}
	p := pool.NewIndexed(func(_ context.Context, name string) (bar, error) {
		mu.Lock()
		created[name] = true
		mu.Unlock()
		return bar{name}, nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	for i := 1; i <= 5; i++ {
		b, err := p.Acquire(ctx, strconv.Itoa(i))
		require.NoError(t, err)
		require.Len(t, created, i)
		p.Release(strconv.Itoa(i), b)
	}
	for i := 1; i <= 5; i++ {
		b, err := p.Acquire(ctx, strconv.Itoa(i))
		require.NoError(t, err)
		require.Len(t, created, 5)
		p.Release(strconv.Itoa(i), b)
	}
	_, err := p.Acquire(ctx, "1")
	require.NoError(t, err)
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()
	_, err = p.Acquire(ctx, "1")
	require.Error(t, err)
}

func TestIndexedCtorCancellation(t *testing.T) {
	p := pool.NewIndexed(func(ctx context.Context, _ string) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	_, err := p.Acquire(ctx, "foo")
	require.Equal(t, context.Canceled, err)
}

// TestIndexedAcquireIsFIFO proves that concurrent Acquire calls for the same
// name, once queued behind the current holder, are handed the item back in
// the order they called Acquire -- not in whatever order Go's runtime
// happens to wake goroutines blocked receiving from a shared channel, which
// carries no such guarantee. A caller that depends on "whoever acquires
// next holds no earlier a claim than I did" (internal/impl/snowflake's
// exactly-once dedup, which compares each acquisition's commit token
// against the channel's last-committed one) breaks silently without this.
func TestIndexedAcquireIsFIFO(t *testing.T) {
	p := pool.NewIndexed(func(context.Context, string) (int, error) { return 0, nil })

	held, err := p.Acquire(t.Context(), "shared")
	require.NoError(t, err)

	const n = 30
	// arrival records the order in which waiters called Acquire, and order
	// the sequence in which they received the item back; both are guarded
	// by mu since every waiter appends from its own goroutine. The
	// assertion is order == arrival, NOT order == 0..n-1: which goroutine
	// wakes from its stagger sleep first is up to the scheduler and timer
	// resolution, and asserting on i pinned that (flaked on a loaded CI
	// runner, where waiter 11's 11ms timer fired before waiter 10's).
	// Recording arrival immediately before the Acquire call leaves only the
	// microseconds between the two lines as a window, against a stagger of
	// several milliseconds between adjacent waiters.
	var mu sync.Mutex
	var arrival, order []int
	release := make(chan struct{}, n)

	for i := range n {
		go func(i int) {
			// Stagger the waiters so they queue one at a time: with all n
			// racing into Acquire together the queue order would be
			// arbitrary even under a correct FIFO implementation, and the
			// test would have nothing meaningful to compare against.
			time.Sleep(time.Duration(i) * 5 * time.Millisecond)
			mu.Lock()
			arrival = append(arrival, i)
			mu.Unlock()
			item, err := p.Acquire(t.Context(), "shared")
			require.NoError(t, err)
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			<-release
			p.Release("shared", item)
		}(i)
	}
	// Let most of the waiters queue before the drain starts. Not
	// load-bearing for correctness: a waiter that arrives after the drain
	// has begun either finds the item idle (and takes it -- still in
	// arrival order, since everyone who arrived before it has already been
	// served) or queues behind whoever is still waiting.
	time.Sleep(time.Duration(n) * 5 * time.Millisecond)
	p.Release("shared", held)
	for range n {
		release <- struct{}{}
	}

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == n
	}, 10*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, arrival, n)
	require.Equal(t, arrival, order, "waiters were handed the item in a different order than they called Acquire")
}
