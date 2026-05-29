// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlutil

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// CoerceToCommon converts a raw value — as produced by a SQL driver scan or a
// CDC redo-log parse — into the canonical Go representation for the given
// common-schema column. It is the single coercion point shared by the SQL-CDC
// snapshot (sql.Scan) and streaming (redo parse) paths so that both yield
// identical value types for the same column.
//
// Numeric inputs are accepted as any Go numeric type (int64, float64,
// json.Number, or a numeric string), which matters because the two paths
// surface numbers differently: snapshot scans produce typed Go values while
// redo parsing produces int64 / json.Number / quoted strings. The output is
// keyed solely on the column's common type:
//
//   - Int64               → int64
//   - Float32 / Float64   → float64
//   - Decimal             → canonical decimal string (never a bare number)
//   - BigDecimal          → canonical big-decimal string (never a bare number)
//   - anything else       → returned unchanged
//
// A nil value is returned as-is. On a recoverable conversion failure (e.g. a
// non-numeric string for a numeric column) the original value is returned
// alongside a non-nil error so callers can log and preserve the input rather
// than emitting a wrong type.
func CoerceToCommon(c schema.Common, v any) (any, error) {
	if v == nil {
		return nil, nil
	}

	switch c.Type {
	case schema.Int64:
		n, err := toInt64(v)
		if err != nil {
			return v, err
		}
		return n, nil
	case schema.Float32, schema.Float64:
		f, err := toFloat64(v)
		if err != nil {
			return v, err
		}
		return f, nil
	case schema.Decimal:
		// Without precision/scale we cannot canonicalise; leave the value be.
		if c.Logical == nil || c.Logical.Decimal == nil {
			return v, nil
		}
		s, ok := numericText(v)
		if !ok {
			return v, nil
		}
		out, err := CanonicaliseDecimal(s, c.Logical.Decimal.Precision, c.Logical.Decimal.Scale)
		if err != nil {
			return v, err
		}
		return out, nil
	case schema.BigDecimal:
		s, ok := numericText(v)
		if !ok {
			return v, nil
		}
		out, err := CanonicaliseBigDecimal(s)
		if err != nil {
			return v, err
		}
		return out, nil
	default:
		return v, nil
	}
}

// numericText renders a numeric value as the decimal text the canonicaliser
// expects. json.Number is already its own textual form; integers and floats
// are formatted without scientific notation so the big.Rat/decimal parsers
// accept them. Returns false for non-numeric inputs (e.g. time.Time, []byte),
// which the caller leaves untouched.
func numericText(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case json.Number:
		return t.String(), true
	case int:
		return strconv.FormatInt(int64(t), 10), true
	case int32:
		return strconv.FormatInt(int64(t), 10), true
	case int64:
		return strconv.FormatInt(t, 10), true
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32), true
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), true
	default:
		return "", false
	}
}

func toInt64(v any) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case int:
		return int64(t), nil
	case int32:
		return int64(t), nil
	case json.Number:
		return t.Int64()
	case string:
		return strconv.ParseInt(t, 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toFloat64(v any) (float64, error) {
	var f float64
	switch t := v.(type) {
	case float64:
		f = t
	case float32:
		f = float64(t)
	case int64:
		f = float64(t)
	case json.Number:
		parsed, err := t.Float64()
		if err != nil {
			return 0, err
		}
		f = parsed
	case string:
		parsed, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return 0, err
		}
		f = parsed
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, fmt.Errorf("non-finite float value %v", v)
	}
	return f, nil
}
