// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/iceberg-go"
)

// typeInferrer holds state for inferring Iceberg types from Go values.
// It tracks field IDs for nested structures to ensure unique IDs across the
// schema. When caseSensitive is false, the inferrer rejects struct values
// that contain two keys differing only in case so they don't materialise as
// duplicate iceberg fields.
type typeInferrer struct {
	nextFieldID   int
	caseSensitive bool
}

// newTypeInferrer creates a new type inferrer starting with field ID 1.
func newTypeInferrer(caseSensitive bool) *typeInferrer {
	return &typeInferrer{nextFieldID: 1, caseSensitive: caseSensitive}
}

// allocateFieldID returns the next available field ID and increments the counter.
func (ti *typeInferrer) allocateFieldID() int {
	id := ti.nextFieldID
	ti.nextFieldID++
	return id
}

// InferIcebergType infers an Iceberg type from a Go value.
// This uses the following strategy:
//   - nil → nil (caller should skip)
//   - string → StringType
//   - bool → BooleanType
//   - int8, int16, int32, uint8, uint16 → Int32Type
//   - int, int64, uint, uint32, uint64 → Int64Type
//   - float32 → Float32Type
//   - float64 → Float64Type
//   - json.Number → Float64Type
//   - time.Time → TimestampTzType
//   - []byte → BinaryType
//   - []any → ListType (recursive)
//   - map[string]any → StructType (recursive)
//
// Returns nil if the value is nil (the caller should skip this field).
// Returns an error for unsupported types.
//
// This entry point uses case-sensitive struct construction. Callers that need
// iceberg's case-insensitive convention should construct a typeInferrer
// directly with caseSensitive=false.
func InferIcebergType(value any) (iceberg.Type, error) {
	ti := newTypeInferrer(true)
	return ti.inferType(value)
}

// inferType is the internal recursive implementation.
func (ti *typeInferrer) inferType(value any) (iceberg.Type, error) {
	if value == nil {
		return nil, nil
	}

	switch v := value.(type) {
	case string:
		return iceberg.StringType{}, nil

	case bool:
		return iceberg.BooleanType{}, nil

	// Small integer types → Int32Type (Iceberg "int")
	case int8:
		return iceberg.Int32Type{}, nil
	case int16:
		return iceberg.Int32Type{}, nil
	case int32:
		return iceberg.Int32Type{}, nil
	case uint8:
		return iceberg.Int32Type{}, nil
	case uint16:
		return iceberg.Int32Type{}, nil

	// Large integer types → Int64Type (Iceberg "long")
	case int:
		return iceberg.Int64Type{}, nil
	case int64:
		return iceberg.Int64Type{}, nil
	case uint:
		return iceberg.Int64Type{}, nil
	case uint32:
		return iceberg.Int64Type{}, nil
	case uint64:
		// Iceberg has no unsigned 64-bit type. Values above math.MaxInt64
		// will be rejected at shred time with an overflow error.
		return iceberg.Int64Type{}, nil

	// Float types → preserve precision
	case float32:
		return iceberg.Float32Type{}, nil
	case float64:
		return iceberg.Float64Type{}, nil

	case json.Number:
		// Default to Float64Type (double) for json.Number to avoid silent truncation
		// during schema evolution. If the first value seen is integer-parseable and we
		// infer Int64, subsequent fractional values like "100.5" are silently coerced
		// to int64 (e.g. 100) with no error. Users who want integer columns for
		// json.Number fields should use schema_metadata or new_column_type_mapping.
		return iceberg.Float64Type{}, nil

	case time.Time:
		return iceberg.TimestampTzType{}, nil

	case []byte:
		return iceberg.BinaryType{}, nil

	case []any:
		return ti.inferListType(v)

	case map[string]any:
		return ti.inferStructType(v)

	default:
		return nil, fmt.Errorf("unsupported type for schema inference: %T", value)
	}
}

// inferListType infers an Iceberg ListType from a Go slice.
func (ti *typeInferrer) inferListType(slice []any) (iceberg.Type, error) {
	// Find first non-nil element to infer element type
	var elementType iceberg.Type = iceberg.StringType{} // default to string if all nil
	for _, elem := range slice {
		if elem != nil {
			var err error
			elementType, err = ti.inferType(elem)
			if err != nil {
				return nil, fmt.Errorf("inferring list element type: %w", err)
			}
			if elementType != nil {
				break
			}
		}
	}

	return &iceberg.ListType{
		ElementID:       ti.allocateFieldID(),
		Element:         elementType,
		ElementRequired: false, // Elements are optional for flexibility
	}, nil
}

// inferStructType infers an Iceberg StructType from a Go map.
func (ti *typeInferrer) inferStructType(m map[string]any) (*iceberg.StructType, error) {
	if len(m) == 0 {
		// Empty struct - can't infer field types
		return &iceberg.StructType{FieldList: []iceberg.NestedField{}}, nil
	}

	// In case-insensitive mode two keys differing only in case would each
	// materialise as a struct field; subsequent reads or schema updates would
	// then collide. Refuse here so the failure surfaces at the source rather
	// than as a confusing iceberg-side error later.
	if !ti.caseSensitive {
		if a, b, ok := findCaseOnlyDuplicate(m); ok {
			return nil, fmt.Errorf("ambiguous struct fields: %q and %q differ only in case", a, b)
		}
	}

	fields := make([]iceberg.NestedField, 0, len(m))
	for name, value := range m {
		fieldType, err := ti.inferType(value)
		if err != nil {
			return nil, fmt.Errorf("inferring type for field %q: %w", name, err)
		}
		if fieldType == nil {
			// Skip nil values - we can't infer their type
			continue
		}
		fields = append(fields, iceberg.NestedField{
			ID:       ti.allocateFieldID(),
			Name:     name,
			Type:     fieldType,
			Required: false, // All fields are optional for flexibility
		})
	}

	return &iceberg.StructType{FieldList: fields}, nil
}

// InferIcebergTypeForAddColumn infers the type for a new column to be added via schema evolution.
// This is similar to InferIcebergType but handles the special case where we need
// to add a column at a specific path in the schema. Uses case-sensitive struct
// construction; internal callers that need the case-insensitive variant should
// use inferIcebergTypeForAddColumn.
func InferIcebergTypeForAddColumn(value any) (iceberg.Type, error) {
	return inferIcebergTypeForAddColumn(value, true)
}

// inferIcebergTypeForAddColumn is the case-sensitivity-aware internal variant
// used by the type resolver so that nested struct values with case-only
// duplicate keys are rejected when iceberg's case-insensitive convention is
// in effect.
func inferIcebergTypeForAddColumn(value any, caseSensitive bool) (iceberg.Type, error) {
	if value == nil {
		return iceberg.StringType{}, nil // Default to string for nil
	}
	ti := newTypeInferrer(caseSensitive)
	return ti.inferType(value)
}
