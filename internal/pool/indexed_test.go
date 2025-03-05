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
	p := pool.NewIndexed(func(ctx context.Context, name string) (bar, error) {
		mu.Lock()
		created[name] = true
		mu.Unlock()
		return bar{name}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
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
	p := pool.NewIndexed(func(ctx context.Context, name string) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	_, err := p.Acquire(ctx, "foo")
	require.Equal(t, context.Canceled, err)
}
