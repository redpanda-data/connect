// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import "github.com/redpanda-data/benthos/v4/public/schema"

// NumberToCommon builds the schema.Common entry for an Oracle NUMBER-family
// column from its precision and scale. It is the single source of truth for
// NUMBER → common-type derivation, shared by the snapshot path (which reads
// precision/scale from go-ora's sql.ColumnType) and the streaming schema cache
// (which reads them from ALL_TAB_COLUMNS), so both modes agree on whether a
// column is an Int64, a bounded Decimal, or an arbitrary-precision BigDecimal.
//
// The mapping picks the most specific lossless representation:
//   - !hasDecimalInfo: BigDecimal — Oracle's "floating decimal" with no
//     declared precision/scale.
//   - scale > precision (driver sentinel for undeclared scale, e.g. go-ora's
//     (38, 255) for bare NUMBER): BigDecimal.
//   - scale == 0 && 0 < precision <= 18: Int64 (scale-0 NUMBER fits losslessly
//     in int64).
//   - scale < 0: rounded to Decimal(precision, 0) — Avro/Parquet/Iceberg can't
//     represent negative scale.
//   - otherwise: Decimal(precision, scale), falling back to BigDecimal when the
//     precision exceeds the bounded Decimal cap.
func NumberToCommon(name string, precision, scale int64, hasDecimalInfo bool) schema.Common {
	if !hasDecimalInfo {
		return schema.NewBigDecimal(name, true)
	}
	if scale < 0 {
		scale = 0
	}
	// Treat scale-greater-than-precision as undeclared (driver sentinel).
	if scale > precision {
		return schema.NewBigDecimal(name, true)
	}
	if scale == 0 && precision > 0 && precision <= MaxInt64DecimalPrecision {
		return schema.Common{Name: name, Type: schema.Int64, Optional: true}
	}
	if c, err := schema.NewDecimal(name, int32(precision), int32(scale), true); err == nil {
		return c
	}
	// Precision out of bounds for the bounded Decimal type — fall back to
	// BigDecimal so the source remains lossless.
	return schema.NewBigDecimal(name, true)
}
