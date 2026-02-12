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
	"iter"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/parquet-go/parquet-go"
)

// BuildParquetSchema builds a parquet schema from an iceberg schema and returns
// a mapping from field ID to column index.
func BuildParquetSchema(schema *iceberg.Schema) (_ *parquet.Schema, fieldIDToColIdx map[int]int, err error) {
	group := make(parquet.Group)

	for _, field := range schema.Fields() {
		node, err := icebergFieldToParquet(field)
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

// schemaLeaves walks an iceberg struct yielding each leaf in the parquet schema
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
func icebergFieldToParquet(field iceberg.NestedField) (parquet.Node, error) {
	node, err := icebergTypeToParquet(field.Type)
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
func icebergTypeToParquet(t iceberg.Type) (parquet.Node, error) {
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
		return parquet.Timestamp(parquet.Microsecond), nil
	case iceberg.TimestampTzType:
		return parquet.Timestamp(parquet.Microsecond), nil
	case iceberg.UUIDType:
		return parquet.UUID(), nil
	case *iceberg.StructType:
		group := make(parquet.Group, len(t.Fields()))
		for _, f := range t.Fields() {
			node, err := icebergFieldToParquet(f)
			if err != nil {
				return nil, err
			}
			group[f.Name] = node
		}
		return group, nil
	case *iceberg.ListType:
		elem, err := icebergTypeToParquet(t.Element)
		if err != nil {
			return nil, err
		}
		if !t.ElementRequired {
			elem = parquet.Optional(elem)
		}
		elem = parquet.FieldID(elem, t.ElementID)
		return parquet.List(elem), nil
	case *iceberg.MapType:
		key, err := icebergTypeToParquet(t.KeyType)
		if err != nil {
			return nil, err
		}
		key = parquet.FieldID(key, t.KeyID)
		val, err := icebergTypeToParquet(t.ValueType)
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
