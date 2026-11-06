// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package saphana

import (
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"unicode/utf8"

	gohdb "github.com/SAP/go-hdb/driver"

	"github.com/redpanda-data/benthos/v4/public/schema"

	"github.com/redpanda-data/connect/v4/internal/sqlutil"
)

// lobScanner is the shape go-hdb gives a LOB column (CLOB, NCLOB, BLOB, TEXT)
// scanned into *any: the content is only reachable by draining it through
// Scan(io.Writer). Passing such a value to encoding/json yields "{}".
type lobScanner interface {
	Scan(wr io.Writer) error
}

// normalizeHANAValue converts go-hdb-specific types to JSON-friendly Go types.
// NVARCHAR/VARCHAR arrive as []byte off the wire; DECIMAL as gohdb.Decimal
// (big.Rat alias); LOBs as a lob scanner that must be drained while the
// cursor is still open. colType carries schema metadata for the column (nil
// when schema is unavailable). numericMapping controls how DECIMAL/NUMERIC
// values are emitted.
func normalizeHANAValue(v any, colType *schema.Common, numericMapping string) (any, error) {
	switch val := v.(type) {
	case []byte:
		return normalizeBytes(val, colType), nil
	case string:
		// Every binary type reaches us as []byte except the spatial ones
		// (ST_POINT, ST_GEOMETRY): go-hdb decodes those through its hex field
		// reader into a hex string. A string under a bytes-typed column can
		// only be that, so decode it to the WKB the schema advertises. A
		// string that is not valid hex is passed through untouched.
		if colType != nil && colType.Type == schema.ByteArray {
			if b, err := hex.DecodeString(val); err == nil {
				return b, nil
			}
		}
		return val, nil
	case lobScanner:
		var b []byte
		if err := gohdb.ScanLobBytes(val, &b); err != nil {
			return nil, fmt.Errorf("reading LOB column: %w", err)
		}
		return normalizeBytes(b, colType), nil
	case gohdb.Decimal:
		return normalizeDecimal((*big.Rat)(&val), colType, numericMapping), nil
	case *gohdb.Decimal:
		if val == nil {
			return nil, nil
		}
		return normalizeDecimal((*big.Rat)(val), colType, numericMapping), nil
	case *big.Rat:
		if val == nil {
			return nil, nil
		}
		return normalizeDecimal(val, colType, numericMapping), nil
	}
	return v, nil
}

// normalizeBytes decides whether raw column bytes are text or binary. go-hdb
// returns text columns as []byte, so those convert to string; binary columns
// must stay []byte so JSON base64-encodes them losslessly — an invalid-UTF-8
// string would be mangled into U+FFFD replacement characters by encoding/json.
func normalizeBytes(val []byte, colType *schema.Common) any {
	if colType != nil && colType.Type == schema.ByteArray {
		return val
	}
	// Even a column the catalog calls text is only converted when the bytes
	// really are UTF-8: a lossy conversion is worse than a schema mismatch.
	if utf8.Valid(val) {
		return string(val)
	}
	return val
}

// normalizeDecimal converts a big.Rat decimal to the representation selected
// by numericMapping, guided by the column's schema type when available.
func normalizeDecimal(r *big.Rat, colType *schema.Common, numericMapping string) any {
	// Determine the canonical form from schema type when available.
	if colType != nil {
		switch colType.Type {
		case schema.Int64:
			// DECIMAL(p,0) that fits in int64 — return the integer directly.
			if r.IsInt() && r.Num().IsInt64() {
				return r.Num().Int64()
			}
		case schema.Float64:
			// best_fit mapped this column to a double at the schema layer.
			f, _ := r.Float64()
			return f
		case schema.Decimal:
			if colType.Logical != nil && colType.Logical.Decimal != nil {
				p := colType.Logical.Decimal.Precision
				s := colType.Logical.Decimal.Scale
				// Pass the exact value, not one pre-rounded to the cached
				// scale: the schema cache is addition-only, so after an online
				// ALTER widens the scale the helper's over-scale rejection is
				// what keeps values from being silently truncated (falling
				// through to the exact BigDecimal path below).
				if out, err := sqlutil.CanonicaliseDecimal(ratToNaturalDecimalString(r), p, s); err == nil {
					return out
				}
			}
		}
	}

	// Without schema guidance best_fit still maps values that fit: integers
	// to int64, values within float64's safe digit range to float64.
	if numericMapping == shNumericMappingBestFit {
		if r.IsInt() && r.Num().IsInt64() {
			return r.Num().Int64()
		}
		if text := ratToNaturalDecimalString(r); decimalFitsFloat64(text) {
			if f, err := strconv.ParseFloat(text, 64); err == nil {
				return f
			}
		}
	}

	// BigDecimal fallback: recover the natural scale from the denominator
	// (HANA DECIMAL denominators are always powers of 10) then canonicalise.
	if out, err := sqlutil.CanonicaliseBigDecimal(ratToNaturalDecimalString(r)); err == nil {
		return out
	}
	f, _ := r.Float64()
	return f
}

// decimalFitsFloat64 reports whether a decimal string's significant digits fit
// within float64's lossless range.
func decimalFitsFloat64(text string) bool {
	digits := 0
	seenNonZero := false
	for _, c := range text {
		if c < '0' || c > '9' {
			continue
		}
		if c == '0' && !seenNonZero {
			continue
		}
		seenNonZero = true
		digits++
	}
	return digits <= float64MaxSafeDigits
}

// ratToNaturalDecimalString converts a *big.Rat to a decimal string using the
// minimal number of fractional digits that exactly represent the value.
// big.Rat keeps fractions reduced, so a terminating decimal's denominator is
// 2^a·5^b rather than a power of 10 (0.5 is 1/2, 12.75 is 51/4); the exact
// scale is max(a, b). Any other prime factor means a non-terminating
// expansion, which HANA DECIMAL cannot produce, so that case falls back to a
// fixed 38 digits (HANA's maximum precision).
func ratToNaturalDecimalString(r *big.Rat) string {
	denom := new(big.Int).Set(r.Denom())
	twos := stripFactor(denom, 2)
	fives := stripFactor(denom, 5)
	if denom.Cmp(big.NewInt(1)) != 0 {
		return r.FloatString(38)
	}
	return r.FloatString(max(twos, fives))
}

// stripFactor divides n by p while divisible, returning the multiplicity.
func stripFactor(n *big.Int, p int64) int {
	pBig := big.NewInt(p)
	count := 0
	for {
		q, rem := new(big.Int).DivMod(n, pBig, new(big.Int))
		if rem.Sign() != 0 || n.Sign() == 0 {
			return count
		}
		n.Set(q)
		count++
	}
}
