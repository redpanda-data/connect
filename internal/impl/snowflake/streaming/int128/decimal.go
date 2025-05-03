// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Functionality in this file was derived (with modifications) from
// arrow-go and it's decimal128 package. We currently don't use that
// package directly due to bugs in the implementation, but hopefully
// we can upstream some fixes from that and then remove this package.

package int128

import (
	"errors"
	"fmt"
	"math"
	"math/big"
)

// FitsInPrecision returns true or false if the value currently held by
// n would fit within precision (0 < prec <= 38) without losing any data.
func (i Num) FitsInPrecision(prec int32) bool {
	if prec == 0 {
		// Precision 0 is valid in snowflake, even if it seems useless
		return i == Num{}
	}
	// The abs call does nothing for this value, so we need to handle it properly
	if i == MinInt128 {
		return false
	}
	return Less(i.Abs(), Pow10Table[prec])
}

func scalePositiveFloat64(v float64, prec, scale int32) (float64, error) {
	var pscale float64
	if scale >= -38 && scale <= 38 {
		pscale = float64PowersOfTen[scale+38]
	} else {
		pscale = math.Pow10(int(scale))
	}

	v *= pscale
	v = math.RoundToEven(v)
	maxabs := float64PowersOfTen[prec+38]
	if v <= -maxabs || v >= maxabs {
		return 0, fmt.Errorf("cannot convert %f to Int128(precision=%d, scale=%d): overflow", v, prec, scale)
	}
	return v, nil
}

func fromPositiveFloat64(v float64, prec, scale int32) (Num, error) {
	v, err := scalePositiveFloat64(v, prec, scale)
	if err != nil {
		return Num{}, err
	}

	hi := math.Floor(math.Ldexp(v, -64))
	low := v - math.Ldexp(hi, 64)
	return Num{hi: int64(hi), lo: uint64(low)}, nil
}

// this has to exist despite sharing some code with fromPositiveFloat64
// because if we don't do the casts back to float32 in between each
// step, we end up with a significantly different answer!
// Aren't floating point values so much fun?
//
// example value to use:
//
//	v := float32(1.8446746e+15)
//
// You'll end up with a different values if you do:
//
//	FromFloat64(float64(v), 20, 4)
//
// vs
//
//	FromFloat32(v, 20, 4)
//
// because float64(v) == 1844674629206016 rather than 1844674600000000
func fromPositiveFloat32(v float32, prec, scale int32) (Num, error) {
	val, err := scalePositiveFloat64(float64(v), prec, scale)
	if err != nil {
		return Num{}, err
	}

	hi := float32(math.Floor(math.Ldexp(float64(float32(val)), -64)))
	low := float32(val) - float32(math.Ldexp(float64(hi), 64))
	return Num{hi: int64(hi), lo: uint64(low)}, nil
}

// FromFloat32 returns a new Int128 constructed from the given float32
// value using the provided precision and scale. Will return an error if the
// value cannot be accurately represented with the desired precision and scale.
func FromFloat32(v float32, prec, scale int32) (Num, error) {
	if v < 0 {
		dec, err := fromPositiveFloat32(-v, prec, scale)
		if err != nil {
			return dec, err
		}
		return Neg(dec), nil
	}
	return fromPositiveFloat32(v, prec, scale)
}

// FromFloat64 returns a new Int128 constructed from the given float64
// value using the provided precision and scale. Will return an error if the
// value cannot be accurately represented with the desired precision and scale.
func FromFloat64(v float64, prec, scale int32) (Num, error) {
	if v < 0 {
		dec, err := fromPositiveFloat64(-v, prec, scale)
		if err != nil {
			return dec, err
		}
		return Neg(dec), nil
	}
	return fromPositiveFloat64(v, prec, scale)
}

var pt5 = big.NewFloat(0.5)

// FromString converts a string into an Int128 as long as it fits within the given precision and scale.
func FromString(v string, prec, scale int32) (n Num, err error) {
	n, err = fromStringFast(v, prec, scale)
	if err != nil {
		n, err = fromStringSlow(v, prec, scale)
	}
	return
}

var errFallbackNeeded = errors.New("fallback to slowpath needed")

// A parsing fast path
func fromStringFast(s string, prec, scale int32) (n Num, err error) {
	sLen := int32(len(s))
	// Even though there could be decimal points or negative/positive signs
	// we need to limit the length of the string to prevent overflow.
	//
	// Using numbers this large is probably rare anyways.
	if sLen == 0 || sLen > 38 {
		err = errFallbackNeeded
		return
	}
	s0 := s
	if s[0] == '-' || s[0] == '+' {
		s = s[1:]
		if len(s) == 0 {
			err = errFallbackNeeded
			return
		}
	}

	// The value between '.' - '0'
	// we can't write that expression because
	// go is strict about overflow in constants
	const dotMinusZero = 254
	for i, ch := range []byte(s) {
		ch -= '0'
		if ch > 9 {
			if ch == dotMinusZero {
				s = s[i+1:]
				goto fraction
			}
			return n, errFallbackNeeded
		}
		n = Add(Mul(n, ten), FromUint64(uint64(ch)))
	}
finish:
	if s0[0] == '-' {
		n = Neg(n)
	}
	// Rescale validates the the new number fits within the precision
	n, err = Rescale(n, prec, scale)
	return
fraction:
	for i, ch := range []byte(s) {
		ch -= '0'
		if ch > 9 {
			return n, errFallbackNeeded
		}
		if scale == 0 {
			// Round!
			if ch >= 5 {
				n = Add(n, one)
			}
			// We need to validate the rest of the number is valid
			// ie is not scientific notation
			for _, ch := range []byte(s[i+1:]) {
				ch -= '0'
				if ch > 9 {
					return n, errFallbackNeeded
				}
			}
			break
		}
		n = Add(Mul(n, ten), FromUint64(uint64(ch)))
		scale--
	}
	goto finish
}

func fromStringSlow(v string, prec, scale int32) (n Num, err error) {
	var out *big.Float
	out, _, err = big.ParseFloat(v, 10, 128, big.ToNearestAway)
	if err != nil {
		return
	}

	var ok bool
	if scale < 0 {
		var tmp big.Int
		val, _ := out.Int(&tmp)
		n, ok = bigInt(val)
		if !ok {
			err = fmt.Errorf("value out of range: %s", v)
			return
		}
		n = Div(n, Pow10Table[-scale])
	} else {
		p := (&big.Float{}).SetPrec(128).SetInt(Pow10Table[scale].bigInt())
		out = out.Mul(out, p)
		var tmp big.Int
		val, _ := out.Int(&tmp)
		// Round by subtracting the whole number so we only have the
		// fractional bit left, then compare it to 0.5, then adjust
		// the whole number according to IEEE RoundTiesToAway rounding
		// mode, which is to round away from zero if the fractional
		// part is |>=0.5|.
		p = p.SetInt(val)
		out = out.Sub(out, p)
		if out.Signbit() {
			if out.Cmp(pt5) <= 0 {
				val = val.Sub(val, big.NewInt(1))
			}
		} else {
			if out.Cmp(pt5) >= 0 {
				val = val.Add(val, big.NewInt(1))
			}
		}
		n, ok = bigInt(val)
		if !ok {
			err = fmt.Errorf("value out of range: %s", v)
			return
		}
	}

	if !n.FitsInPrecision(prec) {
		err = fmt.Errorf("val %s doesn't fit in precision %d", n.String(), prec)
	}
	return
}

// ToFloat32 returns a float32 value representative of this Int128,
// but with the given scale.
func (i Num) ToFloat32(scale int32) float32 {
	return float32(i.ToFloat64(scale))
}

func float64Positive(n Num, scale int32) float64 {
	const twoTo64 float64 = 1.8446744073709552e+19
	x := float64(n.hi) * twoTo64
	x += float64(n.lo)
	if scale >= -38 && scale <= 38 {
		return x * float64PowersOfTen[-scale+38]
	}

	return x * math.Pow10(-int(scale))
}

// ToFloat64 returns a float64 value representative of this Int128,
// but with the given scale.
func (i Num) ToFloat64(scale int32) float64 {
	if i.hi < 0 {
		return -float64Positive(Neg(i), scale)
	}
	return float64Positive(i, scale)
}

// Rescale returns a new number such that it is scaled to |scale| (the current
// scale is assumed to be zero). It also validates that the scaled value fits
// within the specified precision.
func Rescale(n Num, precision, scale int32) (out Num, err error) {
	if !n.FitsInPrecision(precision - scale) {
		err = fmt.Errorf("value (%s) out of range (precision=%d,scale=%d)", n.String(), precision, scale)
		return
	}
	if scale == 0 {
		out = n
		return
	}
	out = Mul(n, Pow10Table[scale])
	return
}

var float64PowersOfTen = [...]float64{
	1e-38, 1e-37, 1e-36, 1e-35, 1e-34, 1e-33, 1e-32, 1e-31, 1e-30, 1e-29,
	1e-28, 1e-27, 1e-26, 1e-25, 1e-24, 1e-23, 1e-22, 1e-21, 1e-20, 1e-19,
	1e-18, 1e-17, 1e-16, 1e-15, 1e-14, 1e-13, 1e-12, 1e-11, 1e-10, 1e-9,
	1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1e0, 1e1,
	1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11,
	1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21,
	1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29, 1e30, 1e31,
	1e32, 1e33, 1e34, 1e35, 1e36, 1e37, 1e38,
}
