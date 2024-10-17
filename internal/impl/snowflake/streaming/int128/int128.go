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
	"fmt"
	"math"
	"math/big"
	"math/bits"
)

// Common constant values for int128
var (
	MaxInt128 = Bytes([]byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	MinInt128 = Bytes([]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	MaxInt64  = Int64(math.MaxInt64)
	MinInt64  = Int64(math.MinInt64)
	MaxInt32  = Int64(math.MaxInt32)
	MinInt32  = Int64(math.MinInt32)
	MaxInt16  = Int64(math.MaxInt16)
	MinInt16  = Int64(math.MinInt16)
	MaxInt8   = Int64(math.MaxInt8)
	MinInt8   = Int64(math.MinInt8)

	// For Snowflake, we need to do some quick multiplication to scale numbers
	// to make that fast we precompute some powers of 10 in a lookup table.
	Pow10Table = [10]Int128{}
)

func init() {
	n := Int64(1)
	Pow10Table[0] = n
	for i := range Pow10Table[1:] {
		n = Mul(n, Int64(10))
		Pow10Table[i+1] = n
	}
}

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

func fls128(n Int128) int {
	if n.hi != 0 {
		return 127 - bits.LeadingZeros64(uint64(n.hi))
	}
	return 64 - bits.LeadingZeros64(n.lo)
}

// Div computes a / b
//
// Division by zero panics
func Div(dividend, divisor Int128) Int128 {
	// algorithm is ported from absl::int128
	if divisor == (Int128{}) {
		panic("int128 division by zero")
	}
	negateQuotient := (dividend.hi < 0) != (divisor.hi < 0)
	if dividend.IsNegative() {
		dividend = Neg(dividend)
	}
	if divisor.IsNegative() {
		divisor = Neg(divisor)
	}
	if divisor == dividend {
		return Int64(1)
	}
	if uGt(divisor, dividend) {
		return Int128{}
	}
	denominator := divisor
	var quotient Int128
	shift := fls128(dividend) - fls128(denominator)
	denominator = Shl(denominator, uint(shift))
	// Uses shift-subtract algorithm to divide dividend by denominator. The
	// remainder will be left in dividend.
	for i := 0; i <= shift; i++ {
		quotient = Shl(quotient, 1)
		if uGt(dividend, denominator) {
			dividend = Sub(dividend, denominator)
			quotient = Or(quotient, Int64(1))
		}
		denominator = uShr(denominator, 1)
	}
	if negateQuotient {
		quotient = Neg(quotient)
	}
	return quotient
}

// uShr is unsigned shift right (no sign extending)
func uShr(v Int128, amt uint) Int128 {
	n := amt - 64
	m := 64 - amt
	return Int128{
		hi: int64(uint64(v.hi) >> amt),
		lo: v.lo>>amt | uint64(v.hi)>>n | uint64(v.hi)<<m,
	}
}

// uGt is unsigned greater than comparison
func uGt(a, b Int128) bool {
	if a.hi == b.hi {
		return a.lo >= b.lo
	} else {
		return uint64(a.hi) >= uint64(b.hi)
	}
}

// Neg computes -v
func Neg(v Int128) Int128 {
	return Sub(Int128{}, v)
}

// IsNegative is negative
func (i Int128) IsNegative() bool {
	return Less(i, Int128{})
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

// Or returns a | i
func Or(a Int128, b Int128) Int128 {
	return Int128{
		hi: a.hi | b.hi,
		lo: a.lo | b.lo,
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

// MustParse converted a base 10 formatted string into an Int128
// and panics otherwise
//
// Only use for testing.
func MustParse(str string) Int128 {
	n, ok := Parse(str)
	if !ok {
		panic(fmt.Sprintf("unable to parse %q into Int128", str))
	}
	return n
}

// Parse converted a base 10 formatted string into an Int128
//
// Not fast, but simple
func Parse(str string) (n Int128, ok bool) {
	var bi *big.Int
	bi, ok = big.NewInt(0).SetString(str, 10)
	if !ok {
		return
	}
	// Check for what would be overflow
	if bi.Cmp(MinInt128.bigInt()) < 0 {
		ok = false
		return
	} else if bi.Cmp(MaxInt128.bigInt()) > 0 {
		ok = false
		return
	}
	b := make([]byte, 16)
	b = bi.FillBytes(b)
	n = Bytes(b)
	if bi.Sign() < 0 {
		n = Neg(n)
	}
	return
}

// String returns the number as base 10 formatted string.
//
// This is not fast but it isn't on a hot path.
func (i Int128) String() string {
	return string(i.bigInt().Append(nil, 10))
}

// MarshalJSON implements JSON serialization of
// an int128 like BigInteger in the Snowflake
// Java SDK with Jackson.
//
// This is not fast but it isn't on a hot path.
func (i Int128) MarshalJSON() ([]byte, error) {
	return i.bigInt().Append(nil, 10), nil
}

func (i Int128) bigInt() *big.Int {
	hi := big.NewInt(i.hi)
	hi = hi.Lsh(hi, 64)
	lo := &big.Int{}
	lo.SetUint64(i.lo)
	return hi.Or(hi, lo)
}
