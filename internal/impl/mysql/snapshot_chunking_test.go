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
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// splitIntRange is the pure chunking math. These tests lock down the
// partitioning invariants that the planner and the SQL predicate builder
// both depend on.
func TestSplitIntRange_SingleChunkWhenNLEOne(t *testing.T) {
	for _, n := range []int{0, 1, -3} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			got := splitIntRange(0, 100, n)
			require.Len(t, got, 1)
			assert.Nil(t, got[0].lo, "single chunk must be unbounded below")
			assert.Nil(t, got[0].hi, "single chunk must be unbounded above")
		})
	}
}

func TestSplitIntRange_SingleChunkWhenRangeCollapsed(t *testing.T) {
	// lo == hi (1 row) and lo > hi (empty / reversed) both degenerate to a
	// single unbounded chunk so the worker sees the whole table (possibly
	// empty) without the planner emitting a no-op chunk.
	for _, tc := range []struct{ lo, hi int64 }{
		{lo: 5, hi: 5},
		{lo: 10, hi: 3},
	} {
		t.Run(fmt.Sprintf("lo=%d,hi=%d", tc.lo, tc.hi), func(t *testing.T) {
			got := splitIntRange(tc.lo, tc.hi, 4)
			require.Len(t, got, 1)
			assert.Nil(t, got[0].lo)
			assert.Nil(t, got[0].hi)
		})
	}
}

func TestSplitIntRange_OutermostChunksAreOpenEnded(t *testing.T) {
	// The first chunk must have no lower bound and the last chunk must have
	// no upper bound. This guarantees every row in [MIN, MAX] is covered
	// regardless of endpoint-inclusion decisions and that any row that
	// somehow exists outside [MIN, MAX] is still read (not skipped).
	got := splitIntRange(0, 100, 4)
	require.Len(t, got, 4)
	assert.Nil(t, got[0].lo, "first chunk must be unbounded below")
	assert.NotNil(t, got[0].hi)
	assert.NotNil(t, got[len(got)-1].lo)
	assert.Nil(t, got[len(got)-1].hi, "last chunk must be unbounded above")
}

func TestSplitIntRange_ChunksCoverAllIntegersExactlyOnce(t *testing.T) {
	// Enumerate every integer in the range and confirm that each one belongs
	// to exactly one chunk under the half-open [lo, hi) semantics the SQL
	// predicate builder emits.
	lo, hi := int64(0), int64(50)
	for _, n := range []int{2, 3, 5, 7, 10, 16} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			got := splitIntRange(lo, hi, n)
			require.NotEmpty(t, got)
			for v := lo; v <= hi; v++ {
				covers := 0
				for _, c := range got {
					lower := c.lo == nil || v >= c.lo.(int64)
					upper := c.hi == nil || v < c.hi.(int64)
					if lower && upper {
						covers++
					}
				}
				assert.Equal(t, 1, covers, "value %d must belong to exactly one chunk", v)
			}
		})
	}
}

func TestSplitIntRange_WhenNExceedsSpanStepIsAtLeastOne(t *testing.T) {
	// [0, 3] asked for 10 chunks — span < n. The implementation floors step
	// to 1; the open-ended outer chunks still guarantee total coverage even
	// though some inner chunks may overlap the same pk values. Coverage
	// (every row visited at least once) is what we lock down here.
	got := splitIntRange(0, 3, 10)
	require.NotEmpty(t, got)
	for v := int64(0); v <= 3; v++ {
		covers := 0
		for _, c := range got {
			lower := c.lo == nil || v >= c.lo.(int64)
			upper := c.hi == nil || v < c.hi.(int64)
			if lower && upper {
				covers++
			}
		}
		assert.GreaterOrEqual(t, covers, 1, "value %d must be covered by at least one chunk", v)
	}
}

func TestSplitIntRange_LargeSpanDoesNotOverflow(t *testing.T) {
	// hi-lo near math.MaxInt64 must not overflow int64 arithmetic during
	// step computation — we cast through uint64 to guard against that.
	got := splitIntRange(math.MinInt64/2, math.MaxInt64/2, 8)
	require.Len(t, got, 8)
	assert.Nil(t, got[0].lo)
	assert.Nil(t, got[len(got)-1].hi)
}

// buildChunkPredicate translates chunkBounds to a SQL fragment. These tests
// pin the shape of that fragment so changes to the query surface are obvious.
func TestBuildChunkPredicate_NilReturnsEmpty(t *testing.T) {
	frag, args := buildChunkPredicate(nil)
	assert.Empty(t, frag)
	assert.Nil(t, args)
}

func TestBuildChunkPredicate_BothBoundsPresent(t *testing.T) {
	frag, args := buildChunkPredicate(&chunkBounds{firstPKCol: "id", lowerIncl: int64(10), upperExcl: int64(20)})
	assert.Equal(t, "`id` >= ? AND `id` < ?", frag)
	assert.Equal(t, []any{int64(10), int64(20)}, args)
}

func TestBuildChunkPredicate_OnlyLowerBound(t *testing.T) {
	frag, args := buildChunkPredicate(&chunkBounds{firstPKCol: "id", lowerIncl: int64(10)})
	assert.Equal(t, "`id` >= ?", frag)
	assert.Equal(t, []any{int64(10)}, args)
}

func TestBuildChunkPredicate_OnlyUpperBound(t *testing.T) {
	frag, args := buildChunkPredicate(&chunkBounds{firstPKCol: "id", upperExcl: int64(20)})
	assert.Equal(t, "`id` < ?", frag)
	assert.Equal(t, []any{int64(20)}, args)
}

func TestBuildChunkPredicate_OpenEndedBothSidesReturnsEmpty(t *testing.T) {
	// An "all open" chunk (both bounds nil) degenerates to no predicate —
	// the caller omits the WHERE clause entirely.
	frag, args := buildChunkPredicate(&chunkBounds{firstPKCol: "id"})
	assert.Empty(t, frag)
	assert.Nil(t, args)
}

// distributeWorkToWorkers was generalised from the table-string signature to
// a generic one so work units can share the same fan-out code path. Confirm
// the generic instantiation works for snapshotWorkUnit values.
func TestDistributeWorkToWorkers_SnapshotWorkUnitInstantiation(t *testing.T) {
	units := []snapshotWorkUnit{
		{table: "a"},
		{table: "b", bounds: &chunkBounds{firstPKCol: "id", upperExcl: int64(100)}},
		{table: "b", bounds: &chunkBounds{firstPKCol: "id", lowerIncl: int64(100)}},
	}

	var mu sync.Mutex
	var visited []snapshotWorkUnit
	var workerIdxMax atomic.Int32

	err := distributeWorkToWorkers(t.Context(), units, 2, func(_ context.Context, idx int, u snapshotWorkUnit) error {
		mu.Lock()
		visited = append(visited, u)
		mu.Unlock()
		for {
			cur := workerIdxMax.Load()
			if int32(idx) <= cur || workerIdxMax.CompareAndSwap(cur, int32(idx)) {
				break
			}
		}
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, visited, len(units), "every work unit must be visited exactly once")
	assert.LessOrEqual(t, int(workerIdxMax.Load()), 1, "worker idx must stay within [0, workerCount)")
}
