// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"errors"
	"fmt"

	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
)

var (
	_ error            = &NewFieldError{}
	_ SchemaFieldError = &NewFieldError{}
	_ error            = &BatchSchemaEvolutionError{}
)

// SchemaFieldError represents an error related to a schema field that needs evolution.
type SchemaFieldError interface {
	error
	// ParentPath returns the path to the parent element containing the new field.
	// Empty path means the field is at the root level.
	ParentPath() icebergx.Path
	// FieldName returns the name of the field that caused the error.
	FieldName() string
	// Value returns a sample value from the field for type inference.
	Value() any
}

// NewFieldError represents a single unknown field discovered during record shredding.
// This error is returned when the shredder encounters a field that doesn't exist
// in the current table schema.
type NewFieldError struct {
	parentPath icebergx.Path
	fieldName  string
	value      any
}

// NewNewFieldError creates a NewFieldError for a field that was discovered during shredding.
func NewNewFieldError(parentPath icebergx.Path, fieldName string, value any) *NewFieldError {
	return &NewFieldError{
		parentPath: parentPath,
		fieldName:  fieldName,
		value:      value,
	}
}

// ParentPath returns the path to the parent element containing the new field.
func (e *NewFieldError) ParentPath() icebergx.Path {
	return e.parentPath
}

// FieldName returns the name of the new field.
func (e *NewFieldError) FieldName() string {
	return e.fieldName
}

// Value returns a sample value from the field for type inference.
func (e *NewFieldError) Value() any {
	return e.value
}

// Error implements the error interface.
func (e *NewFieldError) Error() string {
	if len(e.parentPath) == 0 {
		return fmt.Sprintf("unknown field %q at root level", e.fieldName)
	}
	return fmt.Sprintf("unknown field %q at path %s", e.fieldName, e.parentPath.String())
}

// FullPath returns the complete path to the field including the field name.
func (e *NewFieldError) FullPath() icebergx.Path {
	return append(e.parentPath, icebergx.PathSegment{
		Kind: icebergx.PathField,
		Name: e.fieldName,
	})
}

// BatchSchemaEvolutionError collects multiple NewFieldErrors from a batch.
// This error is returned when schema evolution is needed and the router
// should handle adding the new columns to the table.
type BatchSchemaEvolutionError struct {
	Errors []*NewFieldError
}

// NewBatchSchemaEvolutionError creates a BatchSchemaEvolutionError from a slice of field errors.
func NewBatchSchemaEvolutionError(errors []*NewFieldError) *BatchSchemaEvolutionError {
	return &BatchSchemaEvolutionError{Errors: errors}
}

// Error implements the error interface.
func (e *BatchSchemaEvolutionError) Error() string {
	errs := make([]error, len(e.Errors))
	for i, err := range e.Errors {
		errs[i] = err
	}
	return errors.Join(errs...).Error()
}

// Unwrap returns the underlying errors for errors.Is/As support.
func (e *BatchSchemaEvolutionError) Unwrap() []error {
	errs := make([]error, len(e.Errors))
	for i, err := range e.Errors {
		errs[i] = err
	}
	return errs
}

// GroupByParentPath groups the new field errors by their parent path.
// This is useful when adding columns to nested structs, as all columns
// for the same struct can be added in a single schema update.
func (e *BatchSchemaEvolutionError) GroupByParentPath() map[string][]*NewFieldError {
	groups := make(map[string][]*NewFieldError)
	for _, err := range e.Errors {
		key := err.parentPath.String()
		groups[key] = append(groups[key], err)
	}
	return groups
}
