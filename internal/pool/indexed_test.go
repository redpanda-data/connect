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

	"github.com/stretchr/testify/assert"
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

// TestIndexedAcquireIsFIFO checks that waiters queued behind the current
// holder are handed the item in the order they called Acquire. The snowflake
// output's exactly-once dedup relies on that ordering.
func TestIndexedAcquireIsFIFO(t *testing.T) {
	p := pool.NewIndexed(func(context.Context, string) (int, error) { return 0, nil })

	held, err := p.Acquire(t.Context(), "shared")
	require.NoError(t, err)

	const n = 30
	// Asserting order == arrival (recorded just before each Acquire) rather
	// than order == 0..n-1 keeps the test independent of which goroutine's
	// stagger timer happens to fire first.
	var mu sync.Mutex
	var arrival, order []int
	release := make(chan struct{}, n)

	for i := range n {
		go func(i int) {
			time.Sleep(time.Duration(i) * 5 * time.Millisecond)
			mu.Lock()
			arrival = append(arrival, i)
			mu.Unlock()
			item, err := p.Acquire(t.Context(), "shared")
			if !assert.NoError(t, err) {
				return
			}
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			<-release
			p.Release("shared", item)
		}(i)
	}
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
