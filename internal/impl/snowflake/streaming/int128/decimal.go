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
	"fmt"
	"math"
	"math/big"
)

// FitsInPrecision returns true or false if the value currently held by
// n would fit within precision (0 < prec <= 38) without losing any data.
func (n Int128) FitsInPrecision(prec int32) bool {
	return Less(n.Abs(), Pow10Table[prec])
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

func fromPositiveFloat64(v float64, prec, scale int32) (Int128, error) {
	v, err := scalePositiveFloat64(v, prec, scale)
	if err != nil {
		return Int128{}, err
	}

	hi := math.Floor(math.Ldexp(v, -64))
	low := v - math.Ldexp(hi, 64)
	return Int128{hi: int64(hi), lo: uint64(low)}, nil
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
func fromPositiveFloat32(v float32, prec, scale int32) (Int128, error) {
	val, err := scalePositiveFloat64(float64(v), prec, scale)
	if err != nil {
		return Int128{}, err
	}

	hi := float32(math.Floor(math.Ldexp(float64(float32(val)), -64)))
	low := float32(val) - float32(math.Ldexp(float64(hi), 64))
	return Int128{hi: int64(hi), lo: uint64(low)}, nil
}

// FromFloat32 returns a new Int128 constructed from the given float32
// value using the provided precision and scale. Will return an error if the
// value cannot be accurately represented with the desired precision and scale.
func FromFloat32(v float32, prec, scale int32) (Int128, error) {
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
func FromFloat64(v float64, prec, scale int32) (Int128, error) {
	if v < 0 {
		dec, err := fromPositiveFloat64(-v, prec, scale)
		if err != nil {
			return dec, err
		}
		return Neg(dec), nil
	}
	return fromPositiveFloat64(v, prec, scale)
}

var (
	pt5 = big.NewFloat(0.5)
)

func FromString(v string, prec, scale int32) (n Int128, err error) {
	// time for some math!
	// Our input precision means "number of digits of precision" but the
	// math/big library refers to precision in floating point terms
	// where it refers to the "number of bits of precision in the mantissa".
	// So we need to figure out how many bits we should use for precision,
	// based on the input precision. Too much precision and we aren't rounding
	// when we should. Too little precision and we round when we shouldn't.
	//
	// In general, the number of decimal digits you get from a given number
	// of bits will be:
	//
	//	digits = log[base 10](2^nbits)
	//
	// it thus follows that:
	//
	//	digits = nbits * log[base 10](2)
	//  nbits = digits / log[base 10](2)
	//
	// So we need to account for our scale since we're going to be multiplying
	// by 10^scale in order to get the integral value we're actually going to use
	// So to get our number of bits we do:
	//
	// 	(prec + scale + 1) / log[base10](2)
	//
	// Finally, we still have a sign bit, so we -1 to account for the sign bit.
	// Aren't floating point numbers fun?
	var precInBits = uint(math.Round(float64(prec+scale+1)/math.Log10(2))) + 1

	var out *big.Float
	out, _, err = big.ParseFloat(v, 10, 128, big.ToNearestEven)
	if err != nil {
		return
	}

	var ok bool
	if scale < 0 {
		var tmp big.Int
		val, _ := out.Int(&tmp)
		n, ok = bigInt(val)
		if !ok {
			err = fmt.Errorf("value out of range")
			return
		}
		n = Div(n, Pow10Table[-scale])
	} else {
		// Since we're going to truncate this to get an integer, we need to round
		// the value instead because of edge cases so that we match how other implementations
		// (e.g. C++) handles Decimal values. So if we're negative we'll subtract 0.5 and if
		// we're positive we'll add 0.5.
		p := (&big.Float{}).SetInt(Pow10Table[scale].bigInt())
		out.SetPrec(precInBits).Mul(out, p)
		if out.Signbit() {
			out.Sub(out, pt5)
		} else {
			out.Add(out, pt5)
		}

		var tmp big.Int
		val, _ := out.Int(&tmp)
		n, ok = bigInt(val)
		if !ok {
			err = fmt.Errorf("value out of range")
			return
		}
	}

	if !n.FitsInPrecision(prec) {
		err = fmt.Errorf("val %v doesn't fit in precision %d", n, prec)
	}
	return
}

// ToFloat32 returns a float32 value representative of this Int128,
// but with the given scale.
func (n Int128) Float32(scale int32) float32 {
	return float32(n.Float64(scale))
}

func float64Positive(n Int128, scale int32) float64 {
	const twoTo64 float64 = 1.8446744073709552e+19
	x := float64(n.hi) * twoTo64
	x += float64(n.lo)
	if scale >= -38 && scale <= 38 {
		return x * float64PowersOfTen[-scale+38]
	}

	return x * math.Pow10(-int(scale))
}

// Float64 returns a float64 value representative of this Int128,
// but with the given scale.
func (n Int128) Float64(scale int32) float64 {
	if n.hi < 0 {
		return -float64Positive(Neg(n), scale)
	}
	return float64Positive(n, scale)
}

var (
	float64PowersOfTen = [...]float64{
		1e-38, 1e-37, 1e-36, 1e-35, 1e-34, 1e-33, 1e-32, 1e-31, 1e-30, 1e-29,
		1e-28, 1e-27, 1e-26, 1e-25, 1e-24, 1e-23, 1e-22, 1e-21, 1e-20, 1e-19,
		1e-18, 1e-17, 1e-16, 1e-15, 1e-14, 1e-13, 1e-12, 1e-11, 1e-10, 1e-9,
		1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1e0, 1e1,
		1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11,
		1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21,
		1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29, 1e30, 1e31,
		1e32, 1e33, 1e34, 1e35, 1e36, 1e37, 1e38,
	}
)
