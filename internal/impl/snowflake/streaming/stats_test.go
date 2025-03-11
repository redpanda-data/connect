/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package streaming

import (
	"cmp"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming/int128"
)

func TestMergeInt(t *testing.T) {
	s := mergeStats(&statsBuffer{
		minIntVal: int128.FromInt64(-1),
		maxIntVal: int128.FromInt64(4),
		hasData:   true,
	}, &statsBuffer{
		minIntVal: int128.FromInt64(3),
		maxIntVal: int128.FromInt64(5),
		hasData:   true,
	})
	require.Equal(t, &statsBuffer{
		minIntVal: int128.FromInt64(-1),
		maxIntVal: int128.FromInt64(5),
		hasData:   true,
	}, s)
}

func TestMergeReal(t *testing.T) {
	s := mergeStats(&statsBuffer{
		minRealVal: -1.2,
		maxRealVal: 4.5,
		nullCount:  4,
		hasData:    true,
	}, &statsBuffer{
		minRealVal: 3.4,
		maxRealVal: 5.9,
		nullCount:  2,
		hasData:    true,
	})
	require.Equal(t, &statsBuffer{
		minRealVal: -1.2,
		maxRealVal: 5.9,
		nullCount:  6,
		hasData:    true,
	}, s)
}

func TestMergeStr(t *testing.T) {
	s := mergeStats(&statsBuffer{
		minStrVal: []byte("aa"),
		maxStrVal: []byte("bbbb"),
		maxStrLen: 6,
		nullCount: 1,
		hasData:   true,
	}, &statsBuffer{
		minStrVal: []byte("aaaa"),
		maxStrVal: []byte("cccccc"),
		maxStrLen: 24,
		nullCount: 1,
		hasData:   true,
	})
	require.Equal(t, &statsBuffer{
		minStrVal: []byte("aa"),
		maxStrVal: []byte("cccccc"),
		maxStrLen: 24,
		nullCount: 2,
		hasData:   true,
	}, s)
}

func TestRenderFloat(t *testing.T) {
	require.Equal(t, `"NaN"`, string(asJSONNumber(math.NaN())))
	require.Equal(t, `"Infinity"`, string(asJSONNumber(math.Inf(1))))
	require.Equal(t, `"-Infinity"`, string(asJSONNumber(math.Inf(-1))))
	require.Equal(
		t,
		"3.141592653589793",
		string(asJSONNumber(3.141592653589793)),
	)
	require.Equal(
		t,
		"1.7976931348623157e+308",
		string(asJSONNumber(math.MaxFloat64)),
	)
	require.Equal(
		t,
		"-1.7976931348623157e+308",
		string(asJSONNumber(-math.MaxFloat64)),
	)
}

func TestRealTotalOrder(t *testing.T) {
	isSorted := slices.IsSortedFunc([]float64{
		math.Inf(-1),
		-math.MaxFloat64,
		-math.MaxFloat32,
		-1,
		-math.SmallestNonzeroFloat32,
		-math.SmallestNonzeroFloat64,
		math.Copysign(0, -1),
		0,
		math.SmallestNonzeroFloat64,
		math.SmallestNonzeroFloat32,
		1,
		math.MaxFloat32,
		math.MaxFloat64,
		math.Inf(1),
		math.NaN(),
	}, compareDouble)
	require.True(t, isSorted)
}

func BenchmarkRealComparison(b *testing.B) {
	values := []float64{
		math.Inf(-1),
		-math.MaxFloat64,
		-math.MaxFloat32,
		-1,
		-math.SmallestNonzeroFloat32,
		-math.SmallestNonzeroFloat64,
		math.Copysign(0, -1),
		0,
		math.SmallestNonzeroFloat64,
		math.SmallestNonzeroFloat32,
		1,
		math.MaxFloat32,
		math.MaxFloat64,
		math.Inf(1),
		math.NaN(),
	}
	b.Run("JVMSemantics", func(b *testing.B) {
		for range b.N {
			for _, v1 := range values {
				for _, v2 := range values {
					_ = compareDouble(v1, v2)
				}
			}
		}
	})
	b.Run("GoSemantics", func(b *testing.B) {
		for range b.N {
			for _, v1 := range values {
				for _, v2 := range values {
					_ = cmp.Compare(v1, v2)
				}
			}
		}
	})
}
