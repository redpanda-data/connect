// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	for _, tc := range []struct{ lo, hi int64 }{{5, 5}, {10, 3}} {
		t.Run(fmt.Sprintf("lo=%d,hi=%d", tc.lo, tc.hi), func(t *testing.T) {
			got := splitIntRange(tc.lo, tc.hi, 4)
			require.Len(t, got, 1)
			assert.Nil(t, got[0].lo)
			assert.Nil(t, got[0].hi)
		})
	}
}

func TestSplitIntRange_OutermostChunksAreOpenEnded(t *testing.T) {
	got := splitIntRange(0, 100, 4)
	require.Len(t, got, 4)
	assert.Nil(t, got[0].lo, "first chunk must be unbounded below")
	assert.NotNil(t, got[0].hi)
	assert.NotNil(t, got[len(got)-1].lo)
	assert.Nil(t, got[len(got)-1].hi, "last chunk must be unbounded above")
}

func TestSplitIntRange_ChunksCoverAllIntegersExactlyOnce(t *testing.T) {
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

func TestSplitIntRange_WhenNExceedsSpanCoverageHolds(t *testing.T) {
	// Span < n — step is floored to 1. Open-ended outer chunks ensure every
	// row is still covered even if some chunks overlap pk values.
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

func TestSplitIntRange_LargeSpanNoOverflow(t *testing.T) {
	got := splitIntRange(math.MinInt64/2, math.MaxInt64/2, 8)
	require.Len(t, got, 8)
	assert.Nil(t, got[0].lo)
	assert.Nil(t, got[len(got)-1].hi)
}

func TestBuildChunkPredicate_NilReturnsEmpty(t *testing.T) {
	frag, args := buildChunkPredicate(nil)
	assert.Empty(t, frag)
	assert.Nil(t, args)
}

func TestBuildChunkPredicate_BothBounds(t *testing.T) {
	frag, args := buildChunkPredicate(&chunkBounds{firstPKCol: "id", lowerIncl: int64(10), upperExcl: int64(20)})
	assert.Equal(t, "`id` >= ? AND `id` < ?", frag)
	assert.Equal(t, []any{int64(10), int64(20)}, args)
}

func TestBuildChunkPredicate_OnlyLower(t *testing.T) {
	frag, args := buildChunkPredicate(&chunkBounds{firstPKCol: "id", lowerIncl: int64(10)})
	assert.Equal(t, "`id` >= ?", frag)
	assert.Equal(t, []any{int64(10)}, args)
}

func TestBuildChunkPredicate_OnlyUpper(t *testing.T) {
	frag, args := buildChunkPredicate(&chunkBounds{firstPKCol: "id", upperExcl: int64(20)})
	assert.Equal(t, "`id` < ?", frag)
	assert.Equal(t, []any{int64(20)}, args)
}

func TestBuildChunkPredicate_AllNilBoundsReturnsEmpty(t *testing.T) {
	frag, args := buildChunkPredicate(&chunkBounds{firstPKCol: "id"})
	assert.Empty(t, frag)
	assert.Nil(t, args)
}
