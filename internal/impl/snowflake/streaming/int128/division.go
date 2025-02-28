// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The algorithm here is ported from absl so we attribute changes in this file
// under the same license, even though it's golang.

package int128

import "cmp"

// Div computes a / b
//
// Division by zero panics
func Div(dividend, divisor Num) Num {
	// algorithm is ported from absl::int128
	if divisor == (Num{}) {
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
		return FromInt64(1)
	}
	if CompareUnsigned(divisor, dividend) > 0 {
		return Num{}
	}
	denominator := divisor
	var quotient Num
	shift := fls128(dividend) - fls128(denominator)
	denominator = Shl(denominator, uint(shift))
	// Uses shift-subtract algorithm to divide dividend by denominator. The
	// remainder will be left in dividend.
	for i := 0; i <= shift; i++ {
		quotient = Shl(quotient, 1)
		if CompareUnsigned(dividend, denominator) >= 0 {
			dividend = Sub(dividend, denominator)
			quotient.lo |= 1
		}
		denominator = uShr(denominator, 1)
	}
	if negateQuotient {
		quotient = Neg(quotient)
	}
	return quotient
}

// Compare returns -1 if a < b, 0 if a == b, and 1 if a > b.
func Compare(a, b Num) int {
	r := cmp.Compare(a.hi, b.hi)
	if r == 0 {
		return cmp.Compare(a.lo, b.lo)
	}
	return r
}

// CompareUnsigned returns -1 if |a| < |b|, 0 if a == b, and 1 if |a| > |b|.
func CompareUnsigned(a, b Num) int {
	r := cmp.Compare(uint64(a.hi), uint64(b.hi))
	if r == 0 {
		return cmp.Compare(a.lo, b.lo)
	}
	return r
}

// uShr is unsigned shift right (no sign extending)
func uShr(v Num, amt uint) Num {
	n := amt - 64
	m := 64 - amt
	return Num{
		hi: int64(uint64(v.hi) >> amt),
		lo: v.lo>>amt | uint64(v.hi)>>n | uint64(v.hi)<<m,
	}
}
