// Copyright 2026 Redpanda Data, Inc.
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

package parquetdecimal

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPow10Cached(t *testing.T) {
	cases := []struct {
		n    int
		want string
	}{
		{0, "1"},
		{1, "10"},
		{2, "100"},
		{9, "1000000000"},
		{18, "1000000000000000000"},
	}
	for _, tc := range cases {
		got := Pow10(tc.n)
		assert.Equal(t, tc.want, got.String(), "Pow10(%d)", tc.n)
	}
}

func TestPow10OutOfCache(t *testing.T) {
	got := Pow10(20)
	want := new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)
	assert.Equal(t, want.String(), got.String())
}

func TestPow10ReturnsFreshCopy(t *testing.T) {
	a := Pow10(2)
	a.SetInt64(0)
	b := Pow10(2)
	assert.Equal(t, "100", b.String(), "modifying one Pow10 result must not affect another")
}

func TestByteWidth(t *testing.T) {
	// Reference values from the Parquet decimal-storage spec table.
	cases := []struct {
		precision int
		want      int
	}{
		{1, 1},
		{2, 1},
		{3, 2},
		{4, 2},
		{9, 4},
		{10, 5},
		{18, 8},
		{19, 9},
		{38, 16},
	}
	for _, tc := range cases {
		got := ByteWidth(tc.precision)
		assert.Equal(t, tc.want, got, "ByteWidth(%d)", tc.precision)
	}
}

func TestEncodeBytesPositive(t *testing.T) {
	// 12345 at precision 9 → 4 bytes, big-endian.
	got := EncodeBytes(big.NewInt(12345), 9)
	require.Len(t, got, 4)
	assert.Equal(t, []byte{0x00, 0x00, 0x30, 0x39}, got)
}

func TestEncodeBytesZero(t *testing.T) {
	got := EncodeBytes(big.NewInt(0), 9)
	require.Len(t, got, 4)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00}, got)
}

func TestEncodeBytesNegative(t *testing.T) {
	// -1 at precision 9 → 4 bytes, all 0xFF (two's complement).
	got := EncodeBytes(big.NewInt(-1), 9)
	require.Len(t, got, 4)
	assert.Equal(t, []byte{0xFF, 0xFF, 0xFF, 0xFF}, got)
}

func TestEncodeBytesNegativeNontrivial(t *testing.T) {
	// -12345 at precision 9 → 4 bytes; two's complement of 0x00003039 is 0xFFFFCFC7.
	got := EncodeBytes(big.NewInt(-12345), 9)
	require.Len(t, got, 4)
	assert.Equal(t, []byte{0xFF, 0xFF, 0xCF, 0xC7}, got)
}

func TestEncodeBytesByteWidthBuckets(t *testing.T) {
	// Smallest positive value across each bucket — verifies the byte slice
	// length matches ByteWidth(precision) and the value is right-aligned.
	cases := []struct {
		precision int
		wantLen   int
	}{
		{9, 4},
		{18, 8},
		{38, 16},
	}
	for _, tc := range cases {
		got := EncodeBytes(big.NewInt(1), tc.precision)
		require.Len(t, got, tc.wantLen, "precision %d", tc.precision)
		// Value 1 is right-aligned: last byte is 0x01, all others are 0x00.
		for i := 0; i < tc.wantLen-1; i++ {
			assert.Equal(t, byte(0x00), got[i], "precision %d byte %d", tc.precision, i)
		}
		assert.Equal(t, byte(0x01), got[tc.wantLen-1], "precision %d trailing byte", tc.precision)
	}
}
