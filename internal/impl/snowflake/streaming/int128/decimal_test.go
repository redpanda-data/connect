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
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ulps64(actual, expected float64) int64 {
	ulp := math.Nextafter(actual, math.Inf(1)) - actual
	return int64(math.Abs((expected - actual) / ulp))
}

func ulps32(actual, expected float32) int64 {
	ulp := math.Nextafter32(actual, float32(math.Inf(1))) - actual
	return int64(math.Abs(float64((expected - actual) / ulp)))
}

func assertFloat32Approx(t *testing.T, x, y float32) bool {
	t.Helper()
	const maxulps int64 = 4
	ulps := ulps32(x, y)
	return assert.LessOrEqualf(t, ulps, maxulps, "%f not equal to %f (%d ulps)", x, y, ulps)
}

func assertFloat64Approx(t *testing.T, x, y float64) bool {
	t.Helper()
	const maxulps int64 = 4
	ulps := ulps64(x, y)
	return assert.LessOrEqualf(t, ulps, maxulps, "%f not equal to %f (%d ulps)", x, y, ulps)
}

func TestDecimalToReal(t *testing.T) {
	tests := []struct {
		decimalVal string
		scale      int32
		exp        float64
	}{
		{"0", 0, 0},
		{"0", 10, 0.0},
		{"0", -10, 0.0},
		{"1", 0, 1.0},
		{"12345", 0, 12345.0},
		{"12345", 1, 1234.5},
		// 2**62
		{"4611686018427387904", 0, math.Pow(2, 62)},
		// 2**63 + 2**62
		{"13835058055282163712", 0, math.Pow(2, 63) + math.Pow(2, 62)},
		// 2**64 + 2**62
		{"23058430092136939520", 0, math.Pow(2, 64) + math.Pow(2, 62)},
		// 10**38 - 2**103
		{"99999989858795198174164788026374356992", 0, math.Pow10(38) - math.Pow(2, 103)},
	}

	t.Run("float32", func(t *testing.T) {
		checkDecimalToFloat := func(t *testing.T, str string, v float32, scale int32) {
			bi, _ := (&big.Int{}).SetString(str, 10)
			dec, ok := bigInt(bi)
			assert.True(t, ok)
			assert.Equalf(t, v, dec.ToFloat32(scale), "Decimal Val: %s, Scale: %d, Val: %s", str, scale, dec.String())
		}
		for _, tt := range tests {
			t.Run(tt.decimalVal, func(t *testing.T) {
				checkDecimalToFloat(t, tt.decimalVal, float32(tt.exp), tt.scale)
				if tt.decimalVal != "0" {
					checkDecimalToFloat(t, "-"+tt.decimalVal, float32(-tt.exp), tt.scale)
				}
			})
		}

		t.Run("precision", func(t *testing.T) {
			// 2**63 + 2**40 (exactly representable in a float's 24 bits of precision)
			checkDecimalToFloat(t, "9223373136366403584", float32(9.223373e+18), 0)
			checkDecimalToFloat(t, "-9223373136366403584", float32(-9.223373e+18), 0)
			// 2**64 + 2**41 exactly representable in a float
			checkDecimalToFloat(t, "18446746272732807168", float32(1.8446746e+19), 0)
			checkDecimalToFloat(t, "-18446746272732807168", float32(-1.8446746e+19), 0)
		})

		t.Run("large values", func(t *testing.T) {
			checkApproxDecimalToFloat := func(str string, v float32, scale int32) {
				bi, _ := (&big.Int{}).SetString(str, 10)
				dec, ok := bigInt(bi)
				assert.True(t, ok)
				assertFloat32Approx(t, v, dec.ToFloat32(scale))
			}
			// exact comparisons would succeed on most platforms, but not all power-of-ten
			// factors are exactly representable in binary floating point, so we'll use
			// approx and ensure that the values are within 4 ULP (unit of least precision)
			for scale := int32(-38); scale <= 38; scale++ {
				checkApproxDecimalToFloat("1", float32(math.Pow10(-int(scale))), scale)
				checkApproxDecimalToFloat("123", float32(123)*float32(math.Pow10(-int(scale))), scale)
			}
		})
	})

	t.Run("float64", func(t *testing.T) {
		checkDecimalToFloat := func(t *testing.T, str string, v float64, scale int32) {
			bi, _ := (&big.Int{}).SetString(str, 10)
			dec, ok := bigInt(bi)
			assert.True(t, ok)
			assert.Equalf(t, v, dec.ToFloat64(scale), "Decimal Val: %s, Scale: %d", str, scale)
		}
		for _, tt := range tests {
			t.Run(tt.decimalVal, func(t *testing.T) {
				checkDecimalToFloat(t, tt.decimalVal, tt.exp, tt.scale)
				if tt.decimalVal != "0" {
					checkDecimalToFloat(t, "-"+tt.decimalVal, -tt.exp, tt.scale)
				}
			})
		}

		t.Run("precision", func(t *testing.T) {
			// 2**63 + 2**11 (exactly representable in float64's 53 bits of precision)
			checkDecimalToFloat(t, "9223373136366403584", float64(9.223373136366404e+18), 0)
			checkDecimalToFloat(t, "-9223373136366403584", float64(-9.223373136366404e+18), 0)

			// 2**64 - 2**11 (exactly representable in a float64)
			checkDecimalToFloat(t, "18446746272732807168", float64(1.8446746272732807e+19), 0)
			checkDecimalToFloat(t, "-18446746272732807168", float64(-1.8446746272732807e+19), 0)

			// 2**64 + 2**11 (exactly representable in a float64)
			checkDecimalToFloat(t, "18446744073709555712", float64(1.8446744073709556e+19), 0)
			checkDecimalToFloat(t, "-18446744073709555712", float64(-1.8446744073709556e+19), 0)

			// Almost 10**38 (minus 2**73)
			checkDecimalToFloat(t, "99999999999999978859343891977453174784", 9.999999999999998e+37, 0)
			checkDecimalToFloat(t, "-99999999999999978859343891977453174784", -9.999999999999998e+37, 0)
			checkDecimalToFloat(t, "99999999999999978859343891977453174784", 9.999999999999998e+27, 10)
			checkDecimalToFloat(t, "-99999999999999978859343891977453174784", -9.999999999999998e+27, 10)
			checkDecimalToFloat(t, "99999999999999978859343891977453174784", 9.999999999999998e+47, -10)
			checkDecimalToFloat(t, "-99999999999999978859343891977453174784", -9.999999999999998e+47, -10)
		})

		t.Run("large values", func(t *testing.T) {
			checkApproxDecimalToFloat := func(str string, v float64, scale int32) {
				bi, _ := (&big.Int{}).SetString(str, 10)
				dec, ok := bigInt(bi)
				assert.True(t, ok)
				assertFloat64Approx(t, v, dec.ToFloat64(scale))
			}
			// exact comparisons would succeed on most platforms, but not all power-of-ten
			// factors are exactly representable in binary floating point, so we'll use
			// approx and ensure that the values are within 4 ULP (unit of least precision)
			for scale := int32(-308); scale <= 306; scale++ {
				checkApproxDecimalToFloat("1", math.Pow10(-int(scale)), scale)
				checkApproxDecimalToFloat("123", float64(123)*math.Pow10(-int(scale)), scale)
			}
		})
	})
}

func TestDecimalFromFloat(t *testing.T) {
	tests := []struct {
		val              float64
		precision, scale int32
		expected         string
	}{
		{0, 1, 0, "0"},
		{-0, 1, 0, "0"},
		{0, 19, 4, "0.0000"},
		{math.Copysign(0.0, -1), 19, 4, "0.0000"},
		{123, 7, 4, "123.0000"},
		{-123, 7, 4, "-123.0000"},
		{456.78, 7, 4, "456.7800"},
		{-456.78, 7, 4, "-456.7800"},
		{456.784, 5, 2, "456.78"},
		{-456.784, 5, 2, "-456.78"},
		{456.786, 5, 2, "456.79"},
		{-456.786, 5, 2, "-456.79"},
		{999.99, 5, 2, "999.99"},
		{-999.99, 5, 2, "-999.99"},
		{123, 19, 0, "123"},
		{-123, 19, 0, "-123"},
		{123.4, 19, 0, "123"},
		{-123.4, 19, 0, "-123"},
		{123.6, 19, 0, "124"},
		{-123.6, 19, 0, "-124"},
		// 2**62
		{4.611686018427387904e+18, 19, 0, "4611686018427387904"},
		{-4.611686018427387904e+18, 19, 0, "-4611686018427387904"},
		// 2**63
		{9.223372036854775808e+18, 19, 0, "9223372036854775808"},
		{-9.223372036854775808e+18, 19, 0, "-9223372036854775808"},
		// 2**64
		{1.8446744073709551616e+19, 20, 0, "18446744073709551616"},
		{-1.8446744073709551616e+19, 20, 0, "-18446744073709551616"},
	}

	t.Run("float64", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.expected, func(t *testing.T) {
				n, err := FromFloat64(tt.val, tt.precision, tt.scale)
				assert.NoError(t, err)

				assert.Equal(t, tt.expected, big.NewFloat(n.ToFloat64(tt.scale)).Text('f', int(tt.scale)))
			})
		}

		t.Run("large values", func(t *testing.T) {
			// test entire float64 range
			for scale := int32(-308); scale <= 308; scale++ {
				val := math.Pow10(int(scale))
				n, err := FromFloat64(val, 1, -scale)
				assert.NoError(t, err)
				assert.Equal(t, "1", n.bigInt().String())
			}

			for scale := int32(-307); scale <= 306; scale++ {
				val := 123 * math.Pow10(int(scale))
				n, err := FromFloat64(val, 2, -scale-1)
				assert.NoError(t, err)
				assert.Equal(t, "12", n.bigInt().String())
				n, err = FromFloat64(val, 3, -scale)
				assert.NoError(t, err)
				assert.Equal(t, "123", n.bigInt().String())
				n, err = FromFloat64(val, 4, -scale+1)
				assert.NoError(t, err)
				assert.Equal(t, "1230", n.bigInt().String())
			}
		})
	})

	t.Run("float32", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.expected, func(t *testing.T) {
				n, err := FromFloat32(float32(tt.val), tt.precision, tt.scale)
				assert.NoError(t, err)

				assert.Equal(t, tt.expected, big.NewFloat(float64(n.ToFloat32(tt.scale))).Text('f', int(tt.scale)))
			})
		}

		t.Run("large values", func(t *testing.T) {
			// test entire float32 range
			for scale := int32(-38); scale <= 38; scale++ {
				val := float32(math.Pow10(int(scale)))
				n, err := FromFloat32(val, 1, -scale)
				assert.NoError(t, err)
				assert.Equal(t, "1", n.bigInt().String())
			}

			for scale := int32(-37); scale <= 36; scale++ {
				val := 123 * float32(math.Pow10(int(scale)))
				n, err := FromFloat32(val, 2, -scale-1)
				assert.NoError(t, err)
				assert.Equal(t, "12", n.bigInt().String())
				n, err = FromFloat32(val, 3, -scale)
				assert.NoError(t, err)
				assert.Equal(t, "123", n.bigInt().String())
				n, err = FromFloat32(val, 4, -scale+1)
				assert.NoError(t, err)
				assert.Equal(t, "1230", n.bigInt().String())
			}
		})
	})
}

func TestFromString(t *testing.T) {
	tests := []struct {
		s             string
		expected      int64
		expectedScale int32
	}{
		{"12.3", 123, 1},
		{"0.00123", 123, 5},
		{"1.23e-8", 123, 10},
		{"-1.23E-8", -123, 10},
		{"1.23e+3", 1230, 0},
		{"-1.23E+3", -1230, 0},
		{"1.23e+5", 123000, 0},
		{"1.2345E+7", 12345000, 0},
		{"1.23e-8", 123, 10},
		{"-1.23E-8", -123, 10},
		{"1.23E+3", 1230, 0},
		{"-1.23e+3", -1230, 0},
		{"1.23e+5", 123000, 0},
		{"1.2345e+7", 12345000, 0},
		{"0000000", 0, 0},
		{"000.0000", 0, 4},
		{".00000", 0, 5},
		{"1e1", 10, 0},
		{"+234.567", 234567, 3},
		{"1e-37", 1, 37},
		{"2112.33", 211233, 2},
		{"-2112.33", -211233, 2},
		{"12E2", 12, -2},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_%d", tt.s, tt.expectedScale), func(t *testing.T) {
			n, err := FromString(tt.s, 37, tt.expectedScale)
			assert.NoError(t, err)

			ex := FromInt64(tt.expected)
			assert.Equal(t, ex, n, "got: %s, want: %d", n.String(), tt.expected)
		})
	}
}

func TestFromStringFast(t *testing.T) {
	tests := []string{
		"0",
		"0924535.11610",
		"480754368.9554427",
		"1",
		"11",
		"11.1",
		"12345.12345",
		"99999999999999999999999999999999999999",
		"-99999999999999999999999999999999999999",
	}

	for _, str := range tests {
		str := str
		digitCount, leadingDigits := computeDecimalParameters(str)
		t.Run(str, func(t *testing.T) {
			cases := 0
			for prec := int32(38); prec >= digitCount; prec-- {
				maxScale := prec - leadingDigits
				for scale := maxScale; scale >= 0; scale-- {
					actual, actualErr := fromStringFast(str, prec, scale)
					assert.NoError(t, actualErr)
					expected, expectedErr := fromStringSlow(str, prec, scale)
					assert.NoError(t, expectedErr)
					assert.Equal(
						t,
						expected,
						actual,
						"NUMBER(%d, %d): want: %s, got: %s",
						prec, scale,
						expected.String(),
						actual.String(),
					)
					cases++
				}
			}
		})
	}
	// Try to stress some edge cases where we could overflow but result in something
	// valid after
	t.Run("OverflowEdgeCase", func(t *testing.T) {
		v, err := fromStringFast(strings.Repeat("9", 40), 38, 0)
		assert.Error(t, err, "got: %v", v)
		v, err = fromStringFast(strings.Repeat("9", 40), 38, 37)
		assert.Error(t, err, "got: %v", v)
		v, err = fromStringFast(strings.Repeat("9", 40), 38, 38)
		assert.Error(t, err, "got: %v", v)
		v, err = fromStringFast("9"+strings.Repeat("0", 39), 38, 0)
		assert.Error(t, err, "got: %v", v)
		v, err = fromStringFast("9"+strings.Repeat("0", 39), 38, 37)
		assert.Error(t, err, "got: %v", v)
		v, err = fromStringFast("9"+strings.Repeat("0", 39), 38, 38)
		assert.Error(t, err, "got: %v", v)
	})
}

func TestFromStringFastVsSlowRandomized(t *testing.T) {
	for i := 0; i < 100; i++ {
		precision := rand.N(37) + 2
		scale := rand.N(precision - 1)
		str := ""
		for j := 0; j < precision; j++ {
			str += strconv.Itoa(rand.N(10))
		}
		str += "."
		for j := 0; j < scale; j++ {
			str += strconv.Itoa(rand.N(10))
		}
		fastN, fastErr := fromStringFast(str, int32(precision), int32(scale))
		slowN, slowErr := fromStringSlow(str, int32(precision), int32(scale))
		require.Equal(t, slowErr == nil, fastErr == nil)
		if slowErr == nil && fastErr == nil {
			require.Equal(t, fastN, slowN, "%s: %s vs %s", str, fastN, slowN)
		}
	}
}

func BenchmarkParsing(b *testing.B) {
	tests := []string{
		"1",
		"11",
		"11.1",
		"12345.12345",
		"99999999999999999999999999999999999999",
		"-99999999999999999999999999999999999999",
		"1234567890.1234567890",
	}
	for _, test := range tests {
		test := test
		digitCount, leadingDigits := computeDecimalParameters(test)
		scale := digitCount - leadingDigits
		b.Run("fast_"+test, func(b *testing.B) {
			b.SetBytes(int64(len(test)))
			for i := 0; i < b.N; i++ {
				_, err := fromStringFast(test, digitCount, scale)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("slow_"+test, func(b *testing.B) {
			b.SetBytes(int64(len(test)))
			for i := 0; i < b.N; i++ {
				_, err := fromStringSlow(test, digitCount, scale)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func computeDecimalParameters(str string) (digitCount int32, leadingDigits int32) {
	foundFraction := false
	for _, r := range str {
		if r == '.' {
			foundFraction = true
			continue
		}
		if r != '-' {
			digitCount++
			if !foundFraction {
				leadingDigits++
			}
		}
	}
	return
}
