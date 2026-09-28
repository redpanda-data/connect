/*
 * Copyright 2026 Redpanda Data, Inc.
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

	"github.com/apache/iceberg-go"
)

// TimestampEncodingProperty is the Iceberg table property that pins how this
// connector annotates no-timezone `timestamp` columns in the parquet files it
// writes. It exists because a released version of the connector wrote them
// with the parquet logical-type annotation isAdjustedToUTC=true (the
// "legacy" encoding, spec-incorrect but harmless for append-only reads),
// and silently switching an existing table to the spec-correct
// isAdjustedToUTC=false would leave it with mixed annotations — and break
// copy-on-write rewrites of the old files. The property makes the choice
// per-table, permanent and visible: new tables are created with "spec",
// existing tables are pinned to whatever their data files already contain.
const TimestampEncodingProperty = "redpanda-connect.timestamp-encoding"

// TimestampEncoding selects the parquet isAdjustedToUTC annotation written
// for no-timezone iceberg `timestamp` columns. `timestamptz` columns are
// always written UTC-adjusted regardless of the encoding.
type TimestampEncoding int

const (
	// TimestampEncodingSpec writes no-tz `timestamp` columns with
	// isAdjustedToUTC=false, as the Iceberg spec requires. The zero value:
	// every new table gets this.
	TimestampEncodingSpec TimestampEncoding = iota
	// TimestampEncodingLegacy writes no-tz `timestamp` columns with
	// isAdjustedToUTC=true, byte-identical to what pre-fix connector
	// versions produced, so existing tables never become mixed.
	TimestampEncodingLegacy
)

// String returns the property value form of the encoding ("spec" / "legacy").
func (e TimestampEncoding) String() string {
	switch e {
	case TimestampEncodingLegacy:
		return "legacy"
	default:
		return "spec"
	}
}

// ParseTimestampEncoding parses a TimestampEncodingProperty value. Unknown
// values are a hard error: guessing here could silently mix parquet
// annotations within one table.
func ParseTimestampEncoding(s string) (TimestampEncoding, error) {
	switch s {
	case "spec":
		return TimestampEncodingSpec, nil
	case "legacy":
		return TimestampEncodingLegacy, nil
	default:
		return 0, fmt.Errorf("invalid table property %s value %q: must be %q or %q", TimestampEncodingProperty, s, TimestampEncodingSpec, TimestampEncodingLegacy)
	}
}

// SchemaHasNoTZTimestamp reports whether any leaf column of the schema
// (including nested struct/list/map leaves) is a no-timezone `timestamp`.
// Only those columns are affected by the timestamp encoding; a schema
// without them is encoding-agnostic.
func SchemaHasNoTZTimestamp(schema *iceberg.Schema) bool {
	st := schema.AsStruct()
	for leaf := range schemaLeaves(&st, -1, nil) {
		if _, ok := leaf.Type.(iceberg.TimestampType); ok {
			return true
		}
	}
	return false
}
