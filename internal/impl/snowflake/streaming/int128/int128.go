/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

// package int128 contains an implmentation of int128 that is more
// efficent (no allocations) compared to math/big.Int
//
// Several Snowflake data types are under the hood int128 (date/time),
// so we can use this type and not hurt performance.
package int128

import (
	"encoding/binary"
	"math/big"
	"math/bits"
)

// Int128 is a *signed* int128 type that is more efficent than big.Int
//
// Default value is 0
type Int128 struct {
	hi int64
	lo uint64
}

// Int64 casts an signed int64 to uint128
func Int64(v int64) Int128 {
	hi := int64(0)
	// sign extend
	if v < 0 {
		hi = ^hi
	}
	return Int128{
		hi: hi,
		lo: uint64(v),
	}
}

// Uint64 casts an unsigned int64 to uint128
func Uint64(v uint64) Int128 {
	return Int128{
		hi: 0,
		lo: v,
	}
}

// Add computes a + b
func Add(a, b Int128) Int128 {
	lo, carry := bits.Add64(a.lo, b.lo, 0)
	hi, _ := bits.Add64(uint64(a.hi), uint64(b.hi), carry)
	return Int128{int64(hi), lo}
}

// Sub computes a - b
func Sub(a, b Int128) Int128 {
	lo, carry := bits.Sub64(a.lo, b.lo, 0)
	hi, _ := bits.Sub64(uint64(a.hi), uint64(b.hi), carry)
	return Int128{int64(hi), lo}
}

// Mul computes a * b
func Mul(a, b Int128) Int128 {
	// algorithm is ported from absl::int128
	a32 := a.lo >> 32
	a00 := a.lo & 0xFFFFFFFF
	b32 := b.lo >> 32
	b00 := b.lo & 0xFFFFFFFF
	hi := uint64(a.hi)*b.lo + a.lo*uint64(b.hi) + a32*b32
	lo := a00 * b00
	i := Int128{int64(hi), lo}
	i = Add(i, Shl(Uint64(a32*b00), 32))
	i = Add(i, Shl(Uint64(a00*b32), 32))
	return i
}

// Neg computes -v
func Neg(v Int128) Int128 {
	return Sub(Int128{}, v)
}

// Shl returns a << i
func Shl(v Int128, amt uint) Int128 {
	n := amt - 64
	m := 64 - amt
	return Int128{
		hi: v.hi<<amt | int64(v.lo<<n) | int64(v.lo>>m),
		lo: v.lo << amt,
	}
}

// Less returns a < b
func Less(a, b Int128) bool {
	if a.hi == b.hi {
		return a.lo < b.lo
	} else {
		return a.hi < b.hi
	}
}

// Greater returns a > b
func Greater(a, b Int128) bool {
	if a.hi == b.hi {
		return a.lo > b.lo
	} else {
		return a.hi > b.hi
	}
}

// Bytes converts bi endian bytes to Int128
func Bytes(b []byte) Int128 {
	hi := int64(binary.BigEndian.Uint64(b[0:8]))
	lo := binary.BigEndian.Uint64(b[8:16])
	return Int128{
		hi: hi,
		lo: lo,
	}
}

// Bytes converts an Int128 into big endian bytes
func (i Int128) Bytes() [16]byte {
	b := [16]byte{}
	binary.BigEndian.PutUint64(b[0:8], uint64(i.hi))
	binary.BigEndian.PutUint64(b[8:16], i.lo)
	return b
}

// Int64 casts an Int128 to a int64 by truncating the bytes.
func (i Int128) Int64() int64 {
	return int64(i.lo)
}

// String returns the number as base 10 formatted string.
//
// This is not fast but it isn't on a hot path.
func (i Int128) String() string {
	v, _ := i.MarshalJSON()
	return string(v)
}

// MarshalJSON implements JSON serialization of
// an int128 like BigInteger in the Snowflake
// Java SDK with Jackson.
//
// This is not fast but it isn't on a hot path.
func (i Int128) MarshalJSON() ([]byte, error) {
	hi := big.NewInt(i.hi)
	hi = hi.Lsh(hi, 64)
	lo := &big.Int{}
	lo.SetUint64(i.lo)
	return hi.Or(hi, lo).Append(nil, 10), nil
}
