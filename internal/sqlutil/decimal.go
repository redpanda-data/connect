// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

// Package sqlutil contains helpers shared between the SQL-source CDC inputs
// (Postgres, MySQL, MSSQL, Oracle) for adopting benthos's common-schema
// types — currently the canonical-string form of decimal values.
package sqlutil

import (
	"fmt"
	"math/big"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// CanonicaliseDecimal converts a driver-emitted decimal text representation
// into the canonical decimal string for schema.Decimal(precision, scale).
// The output:
//
//   - has a leading minus only for negative values (no leading +);
//   - has no leading zeros except a single 0 before the decimal point;
//   - has exactly scale fractional digits (right-padded with zeros if the
//     input had fewer);
//   - never uses scientific notation, thousands separators, or whitespace.
//
// Inputs in canonical form already (the common case for MySQL DECIMAL) take
// a fast path via schema.ParseDecimal. Inputs in extended forms — leading
// +, scientific notation, missing trailing zeros — fall back to a big.Float
// parse, which is exact within the configured precision when the magnitude
// fits in 256 bits (covers all decimals up to precision 38).
//
// The result is validated against the column's precision; values exceeding
// it produce an error.
func CanonicaliseDecimal(text string, precision, scale int32) (string, error) {
	params := schema.DecimalParams{Precision: precision, Scale: scale}

	// Fast path: input already matches the canonical-string contract at the
	// declared scale. Short-circuits before any allocation when possible.
	if unscaled, err := params.Parse(text); err == nil {
		return params.Format(unscaled)
	}

	// Slower path: permit extended forms (scientific notation, leading +,
	// fewer fractional digits than scale, etc.).
	bf, _, err := new(big.Float).SetPrec(256).Parse(text, 10)
	if err != nil {
		return "", fmt.Errorf("parsing decimal %q: %w", text, err)
	}
	scaleFactor := new(big.Float).SetPrec(256).SetInt(pow10(scale))
	bf.Mul(bf, scaleFactor)
	if bf.Sign() < 0 {
		bf.Sub(bf, new(big.Float).SetFloat64(0.5))
	} else {
		bf.Add(bf, new(big.Float).SetFloat64(0.5))
	}
	unscaled, _ := bf.Int(nil)

	return params.Format(unscaled)
}

// CanonicaliseDecimalBytes is the []byte-input convenience over
// CanonicaliseDecimal — used by drivers (notably the MSSQL TDS driver and
// pgx) that surface decimals as raw byte slices rather than strings.
func CanonicaliseDecimalBytes(b []byte, precision, scale int32) (string, error) {
	return CanonicaliseDecimal(string(b), precision, scale)
}

// CanonicaliseBigDecimal converts a driver-emitted decimal text representation
// into the canonical BigDecimal string. Unlike CanonicaliseDecimal, no
// precision or scale is enforced: the scale is recovered from the input's
// fractional digits and the value is re-emitted at that natural scale.
//
// Used by sources whose decimal columns are arbitrary-precision: Postgres
// unparameterised NUMERIC, Oracle NUMBER without DATA_PRECISION, MongoDB
// Decimal128.
func CanonicaliseBigDecimal(text string) (string, error) {
	unscaled, scale, err := schema.ParseBigDecimal(text)
	if err != nil {
		return "", err
	}
	return schema.FormatBigDecimal(unscaled, scale)
}

// CanonicaliseBigDecimalBytes is the []byte-input convenience over
// CanonicaliseBigDecimal.
func CanonicaliseBigDecimalBytes(b []byte) (string, error) {
	return CanonicaliseBigDecimal(string(b))
}

func pow10(n int32) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}
