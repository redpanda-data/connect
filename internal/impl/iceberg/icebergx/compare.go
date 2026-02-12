/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package icebergx

import (
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
)

// compareOptionalLiteral compares two optional literals.
// Null values sort before non-null values.
func compareOptionalLiteral(a, b iceberg.Optional[iceberg.Literal]) int {
	if !a.Valid && !b.Valid {
		return 0
	}
	if !a.Valid {
		return -1 // null < non-null
	}
	if !b.Valid {
		return 1 // non-null > null
	}
	return compareLiteral(a.Val, b.Val)
}

// compareLiteral compares two iceberg literals.
// Returns negative if a < b, 0 if equal, positive if a > b.
func compareLiteral(a, b iceberg.Literal) int {
	switch av := a.(type) {
	case iceberg.BoolLiteral:
		bv := b.(iceberg.BoolLiteral)
		return av.Comparator()(av.Value(), bv.Value())
	case iceberg.Int32Literal:
		bv := b.(iceberg.Int32Literal)
		return av.Comparator()(av.Value(), bv.Value())
	case iceberg.Int64Literal:
		bv := b.(iceberg.Int64Literal)
		return av.Comparator()(av.Value(), bv.Value())
	case iceberg.Float32Literal:
		bv := b.(iceberg.Float32Literal)
		return av.Comparator()(av.Value(), bv.Value())
	case iceberg.Float64Literal:
		bv := b.(iceberg.Float64Literal)
		return av.Comparator()(av.Value(), bv.Value())
	case iceberg.DateLiteral:
		bv := b.(iceberg.DateLiteral)
		return av.Comparator()(av.Value(), bv.Value())
	case iceberg.TimeLiteral:
		bv := b.(iceberg.TimeLiteral)
		return av.Comparator()(av.Value(), bv.Value())
	case iceberg.TimestampLiteral:
		bv := b.(iceberg.TimestampLiteral)
		return av.Comparator()(av.Value(), bv.Value())
	case iceberg.StringLiteral:
		bv := b.(iceberg.StringLiteral)
		return av.Comparator()(av.Value(), bv.Value())
	case iceberg.UUIDLiteral:
		bv := b.(iceberg.UUIDLiteral)
		return av.Comparator()(av.Value(), bv.Value())
	case iceberg.BinaryLiteral:
		bv := b.(iceberg.BinaryLiteral)
		return av.Comparator()(av.Value(), bv.Value())
	case iceberg.FixedLiteral:
		bv := b.(iceberg.FixedLiteral)
		return av.Comparator()(av.Value(), bv.Value())
	default:
		// Fall back to string comparison for unknown types
		return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
	}
}
