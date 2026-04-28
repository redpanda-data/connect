// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

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
// +, scientific notation, missing trailing zeros — fall back to a big.Rat
// parse, which is exact for any decimal value the input can represent.
// Values whose magnitude exceeds the column's precision, or whose
// fractional component cannot be represented exactly at the declared scale,
// are rejected rather than silently rounded.
func CanonicaliseDecimal(text string, precision, scale int32) (string, error) {
	params := schema.DecimalParams{Precision: precision, Scale: scale}

	// Fast path: input already matches the canonical-string contract at the
	// declared scale. Short-circuits before any allocation when possible.
	if unscaled, err := params.Parse(text); err == nil {
		return params.Format(unscaled)
	}

	// Slower path: accept extended forms (leading +, scientific notation,
	// values shorter or longer than declared scale) via big.Rat. Rationals
	// represent any decimal exactly, so the scale-fit check below is a
	// genuine "did this value lose precision" test rather than a
	// floating-point rounding question.
	rat, ok := new(big.Rat).SetString(text)
	if !ok {
		return "", fmt.Errorf("parsing decimal %q: invalid format", text)
	}
	scaled := new(big.Rat).Mul(rat, new(big.Rat).SetInt(pow10(scale)))
	if !scaled.IsInt() {
		return "", fmt.Errorf("decimal %q has more fractional digits than the column's scale %d", text, scale)
	}
	unscaled := scaled.Num()

	out, err := params.Format(unscaled)
	if err != nil {
		return "", fmt.Errorf("canonicalising decimal text %q at (%d, %d): %w", text, precision, scale, err)
	}
	return out, nil
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
