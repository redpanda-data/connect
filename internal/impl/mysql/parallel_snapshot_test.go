// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDistributeTablesToWorkers_CoversEveryTableExactlyOnce(t *testing.T) {
	tables := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

	for _, workers := range []int{1, 2, 3, 4, 8, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			var mu sync.Mutex
			var visited []string

			err := distributeTablesToWorkers(t.Context(), tables, workers, func(_ context.Context, _ int, table string) error {
				mu.Lock()
				visited = append(visited, table)
				mu.Unlock()
				return nil
			})
			require.NoError(t, err)

			sort.Strings(visited)
			expected := append([]string{}, tables...)
			sort.Strings(expected)
			assert.Equal(t, expected, visited, "each table must be visited exactly once")
		})
	}
}

func TestDistributeTablesToWorkers_WorkerCountCappedByTableCount(t *testing.T) {
	tables := []string{"a", "b"}

	var activeWorkers atomic.Int32
	var maxActive atomic.Int32

	err := distributeTablesToWorkers(t.Context(), tables, 16, func(_ context.Context, _ int, _ string) error {
		n := activeWorkers.Add(1)
		for {
			cur := maxActive.Load()
			if n <= cur || maxActive.CompareAndSwap(cur, n) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		activeWorkers.Add(-1)
		return nil
	})
	require.NoError(t, err)
	assert.LessOrEqual(t, int(maxActive.Load()), len(tables), "should never exceed table count, even when workerCount is larger")
}

func TestDistributeTablesToWorkers_SingleWorkerIsSequential(t *testing.T) {
	tables := []string{"a", "b", "c", "d"}

	var mu sync.Mutex
	var inFlight int
	var maxInFlight int

	err := distributeTablesToWorkers(t.Context(), tables, 1, func(_ context.Context, _ int, _ string) error {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, maxInFlight, "workerCount=1 must serialize all reads")
}

func TestDistributeTablesToWorkers_ErrorPropagatesAndCancelsSiblings(t *testing.T) {
	tables := make([]string, 50)
	for i := range tables {
		tables[i] = fmt.Sprintf("t%d", i)
	}

	sentinel := errors.New("boom")
	var calls atomic.Int32

	err := distributeTablesToWorkers(t.Context(), tables, 4, func(ctx context.Context, _ int, table string) error {
		calls.Add(1)
		if table == "t5" {
			return sentinel
		}
		// Block until cancelled so we can observe siblings being cancelled
		// after the sentinel error fires.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
			return nil
		}
	})
	require.ErrorIs(t, err, sentinel)
	// At most every worker got 1 table before cancellation, plus the sentinel.
	// We should not have processed all 50 tables.
	assert.Less(t, int(calls.Load()), len(tables), "error must cancel siblings before all tables are consumed")
}

func TestDistributeTablesToWorkers_ContextCancellationPropagates(t *testing.T) {
	tables := make([]string, 100)
	for i := range tables {
		tables[i] = fmt.Sprintf("t%d", i)
	}

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err := distributeTablesToWorkers(ctx, tables, 4, func(ctx context.Context, _ int, _ string) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return nil
		}
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestDistributeTablesToWorkers_ZeroWorkersRejected(t *testing.T) {
	err := distributeTablesToWorkers(t.Context(), []string{"a"}, 0, func(context.Context, int, string) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), ">= 1")
}

func TestDistributeTablesToWorkers_EmptyTablesIsNoop(t *testing.T) {
	var called atomic.Bool
	err := distributeTablesToWorkers(t.Context(), nil, 4, func(context.Context, int, string) error {
		called.Store(true)
		return nil
	})
	require.NoError(t, err)
	assert.False(t, called.Load(), "readFn must not be called when table list is empty")
}

func TestDistributeTablesToWorkers_WorkerIdxWithinBounds(t *testing.T) {
	tables := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	const workerCount = 3

	var mu sync.Mutex
	seenIdxs := map[int]struct{}{}

	err := distributeTablesToWorkers(t.Context(), tables, workerCount, func(_ context.Context, idx int, _ string) error {
		mu.Lock()
		seenIdxs[idx] = struct{}{}
		mu.Unlock()
		assert.GreaterOrEqual(t, idx, 0)
		assert.Less(t, idx, workerCount)
		return nil
	})
	require.NoError(t, err)
	// Not all worker idxs are guaranteed to fire (fast paths may let one
	// worker drain the whole channel), but every idx we observed must be
	// within [0, workerCount).
	for idx := range seenIdxs {
		assert.GreaterOrEqual(t, idx, 0)
		assert.Less(t, idx, workerCount)
	}
}
