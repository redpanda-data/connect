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

package pool_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/pool"
	"github.com/redpanda-data/connect/v4/internal/typed"
)

type foo struct {
	int
}

func TestReuse(t *testing.T) {
	foos := []*foo{{1}, {2}, {3}}
	p := pool.NewCapped(len(foos), func(context.Context, int) (*foo, error) {
		return nil, errors.New("")
	})
	for _, f := range foos {
		p.Release(f)
	}
	for range foos {
		f, ok := p.TryAcquireExisting()
		require.True(t, ok)
		require.Contains(t, foos, f)
		foos = slices.DeleteFunc(foos, func(e *foo) bool {
			return e == f
		})
	}
	require.Empty(t, foos)
	_, ok := p.TryAcquireExisting()
	require.False(t, ok)
}

func TestAcquire(t *testing.T) {
	numCreated := 0
	p := pool.NewCapped(5, func(_ context.Context, id int) (foo, error) {
		require.Equal(t, id, numCreated)
		numCreated++
		return foo{}, nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	for i := 1; i <= 5; i++ {
		_, err := p.Acquire(ctx)
		require.NoError(t, err)
		require.Equal(t, i, numCreated)
		require.Equal(t, i, p.Size())
	}
	errResult := typed.NewAtomicValue[error](nil)
	go func() {
		_, err := p.Acquire(ctx)
		errResult.Store(err)
	}()
	time.Sleep(100 * time.Millisecond)
	// We're still waiting for something
	require.NoError(t, errResult.Load())
	cancel()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Error(c, errResult.Load())
	}, time.Second, time.Millisecond)

	valResult := typed.NewAtomicValue[*foo](nil)
	expected := foo{99}
	go func() {
		val, _ := p.Acquire(t.Context())
		valResult.Store(&val)
	}()
	p.Release(expected)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, &expected, valResult.Load())
	}, time.Second, time.Millisecond)
}

func TestCtorCancellation(t *testing.T) {
	p := pool.NewCapped(5, func(ctx context.Context, _ int) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	_, err := p.Acquire(ctx)
	require.Equal(t, context.Canceled, err)
}

func TestRandomized(t *testing.T) {
	var created atomic.Int64
	p := pool.NewCapped(5, func(_ context.Context, id int) (*foo, error) {
		created.Add(1)
		return &foo{id}, nil
	})
	var wg sync.WaitGroup
	for range 25 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				f, err := p.Acquire(t.Context())
				require.NoError(t, err)
				time.Sleep(time.Millisecond)
				p.Release(f)
			}
		}()
	}
	wg.Wait()
	// Technically possible to only create one if unlikely
	// this test is mostly for -race detection anyways.
	require.Greater(t, int(created.Load()), 1)
	require.LessOrEqual(t, int(created.Load()), 5)
	require.Equal(t, int(created.Load()), p.Size())
	t.Logf("created %d objects in the pool", p.Size())
}
