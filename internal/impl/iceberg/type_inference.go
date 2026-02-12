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
// It tracks field IDs for nested structures to ensure unique IDs across the schema.
type typeInferrer struct {
	nextFieldID int
}

// newTypeInferrer creates a new type inferrer starting with field ID 1.
func newTypeInferrer() *typeInferrer {
	return &typeInferrer{nextFieldID: 1}
}

// allocateFieldID returns the next available field ID and increments the counter.
func (ti *typeInferrer) allocateFieldID() int {
	id := ti.nextFieldID
	ti.nextFieldID++
	return id
}

// InferIcebergType infers an Iceberg type from a Go value.
// This uses a simple strategy where:
//   - nil → nil (caller should skip)
//   - string → StringType
//   - bool → BooleanType
//   - all numeric types → Float64Type (double)
//   - time.Time → TimestampTzType
//   - []byte → BinaryType
//   - []any → ListType (recursive)
//   - map[string]any → StructType (recursive)
//
// Returns nil if the value is nil (the caller should skip this field).
// Returns an error for unsupported types.
func InferIcebergType(value any) (iceberg.Type, error) {
	ti := newTypeInferrer()
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

	// All numeric types map to double (Float64Type) for simplicity
	case int:
		return iceberg.Float64Type{}, nil
	case int8:
		return iceberg.Float64Type{}, nil
	case int16:
		return iceberg.Float64Type{}, nil
	case int32:
		return iceberg.Float64Type{}, nil
	case int64:
		return iceberg.Float64Type{}, nil
	case uint:
		return iceberg.Float64Type{}, nil
	case uint8:
		return iceberg.Float64Type{}, nil
	case uint16:
		return iceberg.Float64Type{}, nil
	case uint32:
		return iceberg.Float64Type{}, nil
	case uint64:
		return iceberg.Float64Type{}, nil
	case float32:
		return iceberg.Float64Type{}, nil
	case float64:
		return iceberg.Float64Type{}, nil

	case json.Number:
		// JSON numbers are treated as double
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

// BuildSchemaFromRecord builds an Iceberg schema from a record (map[string]any).
// This is used when creating a new table to infer the initial schema from the first message.
// All fields are created as optional (Required: false) to allow for missing values.
// The schema ID is set to 0 as it will be assigned by the catalog.
func BuildSchemaFromRecord(record map[string]any) (*iceberg.Schema, error) {
	ti := newTypeInferrer()
	fields := make([]iceberg.NestedField, 0, len(record))

	for name, value := range record {
		fieldType, err := ti.inferType(value)
		if err != nil {
			return nil, fmt.Errorf("inferring type for field %q: %w", name, err)
		}
		if fieldType == nil {
			// Skip nil values - we can't infer their type
			// They'll be added later via schema evolution if seen with a value
			continue
		}
		fields = append(fields, iceberg.NestedField{
			ID:       ti.allocateFieldID(),
			Name:     name,
			Type:     fieldType,
			Required: false, // All fields are optional
		})
	}

	// Schema ID 0 - will be assigned by the catalog
	return iceberg.NewSchema(0, fields...), nil
}

// InferIcebergTypeForAddColumn infers the type for a new column to be added via schema evolution.
// This is similar to InferIcebergType but handles the special case where we need
// to add a column at a specific path in the schema.
func InferIcebergTypeForAddColumn(value any) (iceberg.Type, error) {
	if value == nil {
		return iceberg.StringType{}, nil // Default to string for nil
	}
	return InferIcebergType(value)
}
