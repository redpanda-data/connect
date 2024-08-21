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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type Foo struct{}

func TestSingleGoroutine(t *testing.T) {
	open := false
	s := New(Config[*Foo]{
		Constructor: func(ctx context.Context) (*Foo, error) {
			if open {
				t.Error("constructor called multiple times")
			}
			open = true
			return &Foo{}, nil
		},
		Destructor: func(ctx context.Context, foo *Foo) error {
			if !open {
				t.Error("destructor called multiple times")
			}
			open = false
			return nil
		},
	})
	require.False(t, open)
	f1, ticket1, err := s.Acquire(context.Background())
	require.NoError(t, err)
	require.True(t, open)
	f2, ticket2, err := s.Acquire(context.Background())
	require.NoError(t, err)
	require.True(t, open)
	require.Same(t, f1, f2)
	require.NoError(t, s.Close(context.Background(), ticket1))
	require.True(t, open)
	require.NoError(t, s.Close(context.Background(), ticket1))
	require.True(t, open)
	require.NoError(t, s.Close(context.Background(), ticket2))
	require.False(t, open)
	require.NoError(t, s.Close(context.Background(), ticket2))
	require.False(t, open)
}

func TestMultipleGoroutines(t *testing.T) {
	open := atomic.Bool{}
	s := New(Config[*Foo]{
		Constructor: func(ctx context.Context) (*Foo, error) {
			if open.Swap(true) {
				t.Error("constructor called multiple times")
			}
			return &Foo{}, nil
		},
		Destructor: func(ctx context.Context, foo *Foo) error {
			if !open.Swap(false) {
				t.Error("destructor called multiple times")
			}
			return nil
		},
	})
	require.False(t, open.Load())
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			f1, ticket1, err := s.Acquire(context.Background())
			require.NoError(t, err)
			require.True(t, open.Load())
			f2, ticket2, err := s.Acquire(context.Background())
			require.NoError(t, err)
			require.True(t, open.Load())
			require.Same(t, f1, f2)
			require.NoError(t, s.Close(context.Background(), ticket1))
			require.True(t, open.Load())
			require.NoError(t, s.Close(context.Background(), ticket1))
			require.True(t, open.Load())
			require.NoError(t, s.Close(context.Background(), ticket2))
			// Nothing to assert, could race with other goroutines
			require.NoError(t, s.Close(context.Background(), ticket2))
		}(i)
	}
	wg.Wait()
	require.False(t, open.Load())
}
