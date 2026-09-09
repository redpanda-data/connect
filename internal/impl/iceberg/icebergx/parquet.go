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
	"iter"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/parquet-go/parquet-go"

	"github.com/redpanda-data/connect/v4/internal/impl/parquet/parquetdecimal"
)

// BuildParquetSchema builds a parquet schema from an iceberg schema and returns
// a mapping from field ID to column index. tsEncoding selects the
// isAdjustedToUTC annotation written for no-timezone `timestamp` columns —
// it must match the encoding of the table's existing data files so a table
// never carries mixed annotations (see TimestampEncodingProperty).
func BuildParquetSchema(schema *iceberg.Schema, tsEncoding TimestampEncoding) (_ *parquet.Schema, fieldIDToColIdx map[int]int, err error) {
	group := make(parquet.Group)

	for _, field := range schema.Fields() {
		node, err := icebergFieldToParquet(field, tsEncoding)
		if err != nil {
			return nil, nil, fmt.Errorf("field %s: %w", field.Name, err)
		}
		group[field.Name] = node
	}
	pqSchema := parquet.NewSchema("root", group)

	// Walk the iceberg schema and build up a mapping of field ID -> column index
	fieldToCol := make(map[int]int)
	st := schema.AsStruct()
	for leaf := range schemaLeaves(&st, -1, nil) {
		col, ok := pqSchema.Lookup(leaf.Path...)
		if !ok {
			return nil, nil, fmt.Errorf("invalid schema mapping for %s", strings.Join(leaf.Path, "."))
		}
		fieldToCol[leaf.FieldID] = col.ColumnIndex
	}

	return pqSchema, fieldToCol, nil
}

type schemaLeaf struct {
	FieldID int
	Type    iceberg.Type
	Path    []string
}

// schemaLeaves walks an iceberg struct yielding each leaf in the parquet schema.
func schemaLeaves(root iceberg.Type, fieldID int, path []string) iter.Seq[schemaLeaf] {
	walkStruct := func(st *iceberg.StructType, yield func(schemaLeaf) bool) bool {
		for _, field := range st.Fields() {
			for leaf := range schemaLeaves(field.Type, field.ID, append(path, field.Name)) {
				if !yield(leaf) {
					return false
				}
			}
		}
		return true
	}
	walkList := func(lt *iceberg.ListType, yield func(schemaLeaf) bool) bool {
		for leaf := range schemaLeaves(lt.Element, lt.ElementID, append(path, "list", "element")) {
			if !yield(leaf) {
				return false
			}
		}
		return true
	}
	walkMap := func(mt *iceberg.MapType, yield func(schemaLeaf) bool) bool {
		for leaf := range schemaLeaves(mt.KeyType, mt.KeyID, append(path, "key_value", "key")) {
			if !yield(leaf) {
				return false
			}
		}
		for leaf := range schemaLeaves(mt.ValueType, mt.ValueID, append(path, "key_value", "value")) {
			if !yield(leaf) {
				return false
			}
		}
		return true
	}
	return func(yield func(schemaLeaf) bool) {
		switch t := root.(type) {
		case *iceberg.StructType:
			walkStruct(t, yield)
		case *iceberg.ListType:
			walkList(t, yield)
		case *iceberg.MapType:
			walkMap(t, yield)
		default:
			yield(schemaLeaf{
				FieldID: fieldID,
				Type:    t,
				Path:    path,
			})
		}
	}
}

// icebergFieldToParquet converts an iceberg field to a parquet node.
func icebergFieldToParquet(field iceberg.NestedField, tsEncoding TimestampEncoding) (parquet.Node, error) {
	node, err := icebergTypeToParquet(field.Type, tsEncoding)
	if err != nil {
		return nil, err
	}

	// Add optional wrapper if not required
	if !field.Required {
		node = parquet.Optional(node)
	}

	node = parquet.FieldID(node, field.ID)

	return node, nil
}

// icebergTypeToParquet converts an iceberg type to a parquet node.
func icebergTypeToParquet(t iceberg.Type, tsEncoding TimestampEncoding) (parquet.Node, error) {
	switch t := t.(type) {
	case iceberg.BooleanType:
		return parquet.Leaf(parquet.BooleanType), nil
	case iceberg.Int32Type:
		return parquet.Int(32), nil
	case iceberg.Int64Type:
		return parquet.Int(64), nil
	case iceberg.Float32Type:
		return parquet.Leaf(parquet.FloatType), nil
	case iceberg.Float64Type:
		return parquet.Leaf(parquet.DoubleType), nil
	case iceberg.StringType:
		return parquet.String(), nil
	case iceberg.BinaryType:
		return parquet.Leaf(parquet.ByteArrayType), nil
	case iceberg.DateType:
		return parquet.Date(), nil
	case iceberg.TimeType:
		return parquet.Time(parquet.Microsecond), nil
	case iceberg.TimestampType:
		// A no-timezone Iceberg `timestamp` must be written with the parquet
		// logical-type annotation isAdjustedToUTC=false (per the Iceberg spec).
		// parquet.Timestamp defaults this to true, which would round-trip back
		// through iceberg-go as `timestamptz` and break copy-on-write file
		// rewrites (the strict rewrite visitor refuses timestamptz -> timestamp).
		// This mirrors iceberg-go's own Arrow writer, which encodes a no-tz
		// timestamp with an empty Arrow time zone (isAdjustedToUTC=false).
		//
		// EXCEPT for tables pinned to the legacy encoding
		// (TimestampEncodingLegacy): released connector versions wrote
		// isAdjustedToUTC=true, so tables holding such files must keep
		// receiving it — a table must never carry mixed annotations for one
		// column. The per-table choice is resolved from
		// TimestampEncodingProperty (see that constant's doc).
		return parquet.TimestampAdjusted(parquet.Microsecond, tsEncoding == TimestampEncodingLegacy), nil
	case iceberg.TimestampTzType:
		// A `timestamptz` is UTC-adjusted: isAdjustedToUTC=true (parquet.Timestamp's
		// default). iceberg-go reads this back as arrow timestamp[tz=UTC] -> timestamptz.
		return parquet.TimestampAdjusted(parquet.Microsecond, true), nil
	case iceberg.UUIDType:
		return parquet.UUID(), nil
	case iceberg.DecimalType:
		return parquet.Decimal(t.Scale(), t.Precision(), parquet.FixedLenByteArrayType(DecimalByteWidth(t.Precision()))), nil
	case *iceberg.StructType:
		group := make(parquet.Group, len(t.Fields()))
		for _, f := range t.Fields() {
			node, err := icebergFieldToParquet(f, tsEncoding)
			if err != nil {
				return nil, err
			}
			group[f.Name] = node
		}
		return group, nil
	case *iceberg.ListType:
		elem, err := icebergTypeToParquet(t.Element, tsEncoding)
		if err != nil {
			return nil, err
		}
		if !t.ElementRequired {
			elem = parquet.Optional(elem)
		}
		elem = parquet.FieldID(elem, t.ElementID)
		return parquet.List(elem), nil
	case *iceberg.MapType:
		key, err := icebergTypeToParquet(t.KeyType, tsEncoding)
		if err != nil {
			return nil, err
		}
		key = parquet.FieldID(key, t.KeyID)
		val, err := icebergTypeToParquet(t.ValueType, tsEncoding)
		if err != nil {
			return nil, err
		}
		val = parquet.FieldID(val, t.ValueID)
		if !t.ValueRequired {
			val = parquet.Optional(val)
		}
		return parquet.Map(key, val), nil
	default:
		return nil, fmt.Errorf("unsupported iceberg type: %T", t)
	}
}

// DecimalByteWidth returns the minimum number of bytes needed to store a
// two's complement integer with the given decimal precision. Thin alias
// over [parquetdecimal.ByteWidth] for backwards compatibility within the
// iceberg packages; new callers should use parquetdecimal directly.
func DecimalByteWidth(precision int) int {
	return parquetdecimal.ByteWidth(precision)
}
