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

// Package parquetdecimal provides helpers for encoding decimal values into
// the Parquet two's-complement big-endian byte form. Shared by the parquet
// encoder and the iceberg shredder.
package parquetdecimal

import (
	"math"
	"math/big"
)

// ByteWidth returns the minimum number of bytes needed to store a
// two's-complement integer with the given decimal precision.
//
//	ceil((log10(2) + precision) / log10(256))
func ByteWidth(precision int) int {
	return int(math.Ceil((math.Log10(2) + float64(precision)) / math.Log10(256)))
}

// pow10Cache holds precomputed powers of 10 for scales 0–18.
var pow10Cache [19]*big.Int

func init() {
	pow10Cache[0] = big.NewInt(1)
	for i := 1; i < len(pow10Cache); i++ {
		pow10Cache[i] = new(big.Int).Mul(pow10Cache[i-1], big.NewInt(10))
	}
}

// Pow10 returns 10^n as a fresh *big.Int.
func Pow10(n int) *big.Int {
	if n >= 0 && n < len(pow10Cache) {
		return new(big.Int).Set(pow10Cache[n])
	}
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

// EncodeBytes encodes an unscaled big.Int as a two's-complement big-endian
// byte slice of length ByteWidth(precision). Used for Parquet
// FIXED_LEN_BYTE_ARRAY decimal storage and Avro decimal logical-type values.
func EncodeBytes(unscaled *big.Int, precision int) []byte {
	numBytes := ByteWidth(precision)

	if unscaled.Sign() >= 0 {
		b := unscaled.Bytes()
		result := make([]byte, numBytes)
		copy(result[numBytes-len(b):], b)
		return result
	}

	modulus := new(big.Int).Lsh(big.NewInt(1), uint(numBytes*8))
	tc := new(big.Int).Add(modulus, unscaled)
	b := tc.Bytes()
	result := make([]byte, numBytes)
	for i := range result {
		result[i] = 0xFF
	}
	copy(result[numBytes-len(b):], b)
	return result
}
