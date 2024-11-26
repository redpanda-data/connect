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

package asyncroutine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type req struct{ i int }
type resp struct{ i int }

func TestBatcherCancellation(t *testing.T) {
	b, err := NewBatcher(3, func(ctx context.Context, reqs []req) (resps []resp, err error) {
		<-ctx.Done()
		err = ctx.Err()
		return
	})
	require.NoError(t, err)

	// test request cancellation
	ctx, cancel := context.WithCancel(context.Background())
	var done atomic.Bool
	go func() {
		_, err := b.Submit(ctx, req{1})
		require.ErrorIs(t, err, context.Canceled)
		done.Store(true)
	}()
	time.Sleep(5 * time.Millisecond)
	require.False(t, done.Load())
	cancel()
	require.Eventually(t, func() bool { return done.Load() }, time.Second, time.Millisecond)

	// test batcher cancellation
	done.Store(false)
	go func() {
		_, err := b.Submit(context.Background(), req{1})
		require.ErrorIs(t, err, context.Canceled)
		done.Store(true)
	}()
	time.Sleep(5 * time.Millisecond)
	require.False(t, done.Load())
	b.Close()
	require.Eventually(t, func() bool { return done.Load() }, time.Second, time.Millisecond)
}

func TestBatching(t *testing.T) {
	batchSize := make(chan int)
	b, err := NewBatcher(3, func(_ context.Context, reqs []req) (resps []resp, err error) {
		batchSize <- len(reqs)
		resps = make([]resp, len(reqs))
		for i, req := range reqs {
			resps[i].i = req.i
		}
		return
	})
	require.NoError(t, err)

	var done, submitted sync.WaitGroup
	done.Add(100)
	submitted.Add(100)
	for i := range 100 {
		go func(i int) {
			submitted.Done()
			resp, err := b.Submit(context.Background(), req{i})
			require.NoError(t, err)
			require.Equal(t, i, resp.i)
			done.Done()
		}(i)
	}
	submitted.Wait()

	// We can't strictly assert anything here without races,
	// but in general we should get *some* batching
	batches := 0
	for batches < 100 {
		size := <-batchSize
		require.Greater(t, size, 0)
		require.LessOrEqual(t, size, 3)
		batches += size
	}
	done.Wait()
	b.Close()
}
