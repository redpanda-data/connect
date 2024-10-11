/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package int128

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

var MaxInt128 = Bytes([]byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
var MinInt128 = Bytes([]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})

func TestAdd(t *testing.T) {
	require.Equal(t, MinInt128, Add(MaxInt128, Int64(1)))
	require.Equal(t, MaxInt128, Add(MinInt128, Int64(-1)))
	require.Equal(t, Int64(2), Add(Int64(1), Int64(1)))
	require.Equal(
		t,
		Bytes([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE}),
		Add(Uint64(math.MaxUint64), Uint64(math.MaxUint64)),
	)
	require.Equal(
		t,
		Bytes([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}),
		Add(Int64(math.MaxInt64), Int64(1)),
	)
	require.Equal(
		t,
		Bytes([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}),
		Add(Uint64(math.MaxUint64), Int64(1)),
	)
}

func TestSub(t *testing.T) {
	require.Equal(t, MaxInt128, Sub(MinInt128, Int64(1)))
	require.Equal(t, MinInt128, Sub(MaxInt128, Int64(-1)))
	require.Equal(
		t,
		Bytes([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}),
		Sub(Int64(0), Int64(math.MaxInt64)),
	)
	require.Equal(
		t,
		Bytes([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}),
		Sub(Int64(0), Uint64(math.MaxUint64)),
	)
}

func SlowMul(a Int128, b Int128) Int128 {
	delta := Int64(-1)
	deltaFn := Add
	if Less(b, Int64(0)) {
		delta = Int64(1)
		deltaFn = Sub
	}
	r := Int64(0)
	for i := b; i != Int64(0); i = Add(i, delta) {
		r = deltaFn(r, a)
	}
	return r
}

func TestMul(t *testing.T) {
	tc := [][2]Int128{
		{Int64(10), Int64(10)},
		{Int64(1), Int64(10)},
		{Int64(0), Int64(10)},
		{Int64(0), Int64(0)},
		{Int64(math.MaxInt64), Int64(0)},
		{Int64(math.MaxInt64), Int64(1)},
		{Int64(math.MaxInt64), Int64(2)},
		{Int64(math.MaxInt64), Int64(3)},
		{Int64(math.MaxInt64), Int64(4)},
		{Int64(math.MaxInt64), Int64(10)},
		{Uint64(math.MaxUint64), Int64(10)},
		{Uint64(math.MaxUint64), Int64(2)},
		{Uint64(math.MaxUint64), Int64(100)},
		{MaxInt128, Int64(100)},
		{MaxInt128, Int64(10)},
		{MinInt128, Int64(10)},
		{MinInt128, Int64(-1)},
		{MaxInt128, Int64(-1)},
		{Int64(-1), Int64(-1)},
	}
	for _, c := range tc {
		a, b := c[0], c[1]
		expected := SlowMul(a, b)
		actual := Mul(a, b)
		require.Equal(
			t,
			expected,
			actual,
			"%s x %s, got: %s, want: %s",
			a.String(),
			b.String(),
			actual.String(),
			expected.String(),
		)
		actual = Mul(b, a)
		require.Equal(
			t,
			expected,
			actual,
			"%s x %s, got: %s, want: %s",
			b.String(),
			a.String(),
			actual.String(),
			expected.String(),
		)
	}
}

func TestShl(t *testing.T) {
	for i := uint(0); i < 64; i++ {
		require.Equal(t, Int128{lo: 1 << i}, Shl(Int64(1), i))
		require.Equal(t, Int128{hi: 1 << i}, Shl(Int64(1), i+64))
		require.Equal(t, Int128{hi: ^0, lo: uint64(int64(-1) << i)}, Shl(Int64(-1), i))
		require.Equal(t, Int128{hi: -1 << i}, Shl(Int64(-1), i+64))
	}
	require.Equal(t, Int128{}, Shl(Int64(1), 128))
	require.Equal(t, Int128{}, Shl(Int64(-1), 128))
}

func TestCompare(t *testing.T) {
	tc := [][2]Int128{
		{Int64(0), Int64(1)},
		{Int64(-1), Int64(0)},
		{MinInt128, Int64(0)},
		{MinInt128, Int64(-1)},
		{MinInt128, Int64(math.MinInt64)},
		{MinInt128, Uint64(math.MaxUint64)},
		{MinInt128, MaxInt128},
		{Int64(0), MaxInt128},
		{Int64(-1), MaxInt128},
		{Int64(math.MinInt64), MaxInt128},
		{Int64(math.MaxInt64), MaxInt128},
		{Uint64(math.MaxUint64), MaxInt128},
	}
	for _, vals := range tc {
		a, b := vals[0], vals[1]
		require.True(t, Less(a, b))
		require.False(t, Less(b, a))
		require.True(t, Greater(b, a))
		require.False(t, Greater(a, b))
		require.NotEqual(t, a, b)
		require.Equal(t, a, b)
	}
	require.Equal(t, Int64(0), Int64(0))
	require.NotEqual(t, Int64(1), Int64(0))
	require.Equal(t, Shl(Int64(1), 64), Add(Uint64(math.MaxUint64), Int64(1)))
}
