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
	MaxInt128 = FromBigEndian([]byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	MinInt128 = FromBigEndian([]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	MaxInt64  = FromInt64(math.MaxInt64)
	MinInt64  = FromInt64(math.MinInt64)
	MaxInt32  = FromInt64(math.MaxInt32)
	MinInt32  = FromInt64(math.MinInt32)
	MaxInt16  = FromInt64(math.MaxInt16)
	MinInt16  = FromInt64(math.MinInt16)
	MaxInt8   = FromInt64(math.MaxInt8)
	MinInt8   = FromInt64(math.MinInt8)
	one       = FromUint64(1)
	ten       = FromUint64(10)

	// For Snowflake, we need to do some quick multiplication to scale numbers
	// to make that fast we precompute some powers of 10 in a lookup table.
	Pow10Table = [...]Num{
		FromUint64(1e00),
		FromUint64(1e01),
		FromUint64(1e02),
		FromUint64(1e03),
		FromUint64(1e04),
		FromUint64(1e05),
		FromUint64(1e06),
		FromUint64(1e07),
		FromUint64(1e08),
		FromUint64(1e09),
		FromUint64(1e10),
		FromUint64(1e11),
		FromUint64(1e12),
		FromUint64(1e13),
		FromUint64(1e14),
		FromUint64(1e15),
		FromUint64(1e16),
		FromUint64(1e17),
		FromUint64(1e18),
		FromUint64(1e19),
		New(5, 7766279631452241920),
		New(54, 3875820019684212736),
		New(542, 1864712049423024128),
		New(5421, 200376420520689664),
		New(54210, 2003764205206896640),
		New(542101, 1590897978359414784),
		New(5421010, 15908979783594147840),
		New(54210108, 11515845246265065472),
		New(542101086, 4477988020393345024),
		New(5421010862, 7886392056514347008),
		New(54210108624, 5076944270305263616),
		New(542101086242, 13875954555633532928),
		New(5421010862427, 9632337040368467968),
		New(54210108624275, 4089650035136921600),
		New(542101086242752, 4003012203950112768),
		New(5421010862427522, 3136633892082024448),
		New(54210108624275221, 12919594847110692864),
		New(542101086242752217, 68739955140067328),
		New(5421010862427522170, 687399551400673280),
	}
)

// Num is a *signed* int128 type that is more efficent than big.Int
//
// Default value is 0
type Num struct {
	hi int64
	lo uint64
}

// New constructs an Int128 from two 64 bit integers.
func New(hi int64, lo uint64) Num {
	return Num{
		hi: hi,
		lo: lo,
	}
}

// FromInt64 casts an signed int64 to uint128
func FromInt64(v int64) Num {
	hi := int64(0)
	// sign extend
	if v < 0 {
		hi = ^hi
	}
	return Num{
		hi: hi,
		lo: uint64(v),
	}
}

// FromUint64 casts an unsigned int64 to uint128
func FromUint64(v uint64) Num {
	return Num{
		hi: 0,
		lo: v,
	}
}

// Add computes a + b
func Add(a, b Num) Num {
	lo, carry := bits.Add64(a.lo, b.lo, 0)
	hi, _ := bits.Add64(uint64(a.hi), uint64(b.hi), carry)
	return Num{int64(hi), lo}
}

// Sub computes a - b
func Sub(a, b Num) Num {
	lo, carry := bits.Sub64(a.lo, b.lo, 0)
	hi, _ := bits.Sub64(uint64(a.hi), uint64(b.hi), carry)
	return Num{int64(hi), lo}
}

// Mul computes a * b
func Mul(a, b Num) Num {
	hi, lo := bits.Mul64(a.lo, b.lo)
	hi += (uint64(a.hi) * b.lo) + (a.lo * uint64(b.hi))
	return Num{hi: int64(hi), lo: lo}
}

func fls128(n Num) int {
	if n.hi != 0 {
		return 127 - bits.LeadingZeros64(uint64(n.hi))
	}
	return 63 - bits.LeadingZeros64(n.lo)
}

// Neg computes -v
func Neg(n Num) Num {
	n.lo = ^n.lo + 1
	n.hi = ^n.hi
	if n.lo == 0 {
		n.hi += 1
	}
	return n
}

// Abs computes v < 0 ? -v : v
func (i Num) Abs() Num {
	if i.IsNegative() {
		return Neg(i)
	}
	return i
}

// IsNegative returns true if `i` is negative
func (i Num) IsNegative() bool {
	return i.hi < 0
}

// Shl returns a << i
func Shl(v Num, amt uint) Num {
	n := amt - 64
	m := 64 - amt
	return Num{
		hi: v.hi<<amt | int64(v.lo<<n) | int64(v.lo>>m),
		lo: v.lo << amt,
	}
}

// Or returns a | i
func Or(a, b Num) Num {
	return Num{
		hi: a.hi | b.hi,
		lo: a.lo | b.lo,
	}
}

// Less returns a < b
func Less(a, b Num) bool {
	if a.hi == b.hi {
		return a.lo < b.lo
	} else {
		return a.hi < b.hi
	}
}

// Greater returns a > b
func Greater(a, b Num) bool {
	if a.hi == b.hi {
		return a.lo > b.lo
	} else {
		return a.hi > b.hi
	}
}

// FromBigEndian converts bi endian bytes to Int128
func FromBigEndian(b []byte) Num {
	hi := int64(binary.BigEndian.Uint64(b[0:8]))
	lo := binary.BigEndian.Uint64(b[8:16])
	return Num{
		hi: hi,
		lo: lo,
	}
}

// ToBigEndian converts an Int128 into big endian bytes
func (i Num) ToBigEndian() []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], uint64(i.hi))
	binary.BigEndian.PutUint64(b[8:16], i.lo)
	return b
}

// AppendBigEndian converts an Int128 into big endian bytes
func (i Num) AppendBigEndian(b []byte) []byte {
	b = binary.BigEndian.AppendUint64(b, uint64(i.hi))
	return binary.BigEndian.AppendUint64(b, i.lo)
}

// ToInt64 casts an Int128 to a int64 by truncating the bytes.
func (i Num) ToInt64() int64 {
	return int64(i.lo)
}

// ToInt32 casts an Int128 to a int32 by truncating the bytes.
func (i Num) ToInt32() int32 {
	return int32(i.lo)
}

// ToInt16 casts an Int128 to a int16 by truncating the bytes.
func (i Num) ToInt16() int16 {
	return int16(i.lo)
}

// ToInt8 casts an Int128 to a int8 by truncating the bytes.
func (i Num) ToInt8() int8 {
	return int8(i.lo)
}

// Min computes min(a, b)
func Min(a, b Num) Num {
	if Less(a, b) {
		return a
	} else {
		return b
	}
}

// Max computes min(a, b)
func Max(a, b Num) Num {
	if Greater(a, b) {
		return a
	} else {
		return b
	}
}

// MustParse converted a base 10 formatted string into an Int128
// and panics otherwise
//
// Only use for testing.
func MustParse(str string) Num {
	n, ok := Parse(str)
	if !ok {
		panic(fmt.Sprintf("unable to parse %q into Int128", str))
	}
	return n
}

// Parse converted a base 10 formatted string into an Int128
//
// Not fast, but simple
func Parse(str string) (n Num, ok bool) {
	var bi *big.Int
	bi, ok = big.NewInt(0).SetString(str, 10)
	if !ok {
		return
	}
	return bigInt(bi)
}

// String returns the number as base 10 formatted string.
//
// This is not fast but it isn't on a hot path.
func (i Num) String() string {
	return string(i.bigInt().Append(nil, 10))
}

// MarshalJSON implements JSON serialization of
// an int128 like BigInteger in the Snowflake
// Java SDK with Jackson.
//
// This is not fast but it isn't on a hot path.
func (i Num) MarshalJSON() ([]byte, error) {
	return i.bigInt().Append(nil, 10), nil
}

func (i Num) bigInt() *big.Int {
	hi := big.NewInt(i.hi) // Preserves sign
	hi = hi.Lsh(hi, 64)
	lo := &big.Int{}
	lo.SetUint64(i.lo)
	return hi.Or(hi, lo)
}

var (
	maxBigInt128 = MaxInt128.bigInt()
	minBigInt128 = MinInt128.bigInt()
)

func bigInt(bi *big.Int) (n Num, ok bool) {
	// One cannot check BitLen here because that misses that MinInt128
	// requires 128 bits along with other out of range values. Instead
	// the better check is to explicitly compare our allowed bounds
	ok = bi.Cmp(minBigInt128) >= 0 && bi.Cmp(maxBigInt128) <= 0
	if !ok {
		return
	}
	b := bi.Bits()
	if len(b) == 0 {
		return
	}
	n.lo = uint64(b[0])
	if len(b) > 1 {
		n.hi = int64(b[1])
	}
	if bi.Sign() < 0 {
		n = Neg(n)
	}
	return
}

// ByteWidth returns the maximum number of bytes needed to store v
func ByteWidth(v Num) int {
	if v.IsNegative() {
		switch {
		case !Less(v, MinInt8):
			return 1
		case !Less(v, MinInt16):
			return 2
		case !Less(v, MinInt32):
			return 4
		case !Less(v, MinInt64):
			return 8
		}
		return 16
	}
	switch {
	case !Greater(v, MaxInt8):
		return 1
	case !Greater(v, MaxInt16):
		return 2
	case !Greater(v, MaxInt32):
		return 4
	case !Greater(v, MaxInt64):
		return 8
	}
	return 16
}
