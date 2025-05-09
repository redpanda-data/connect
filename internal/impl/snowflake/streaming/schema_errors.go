/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package streaming

import (
	"errors"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// SchemaMismatchError occurs when the user provided data has data that
// doesn't match the schema *and* the table can be evolved to accommodate
//
// This can be used as a mechanism to evolve the schema dynamically.
type SchemaMismatchError interface {
	error
	ColumnName() string
	Value() any
}

var _ error = &BatchSchemaMismatchError[SchemaMismatchError]{}

// BatchSchemaMismatchError is when multiple schema mismatch errors happen at once
type BatchSchemaMismatchError[T SchemaMismatchError] struct {
	Errors []T
}

// Error implements the error interface
func (e *BatchSchemaMismatchError[T]) Error() string {
	errs := []error{}
	for _, err := range e.Errors {
		errs = append(errs, err)
	}
	return errors.Join(errs...).Error()
}

var (
	_ error               = &NonNullColumnError{}
	_ SchemaMismatchError = &NonNullColumnError{}
)

// NonNullColumnError occurs when a column with a NOT NULL constraint
// gets a value with a `NULL` value.
type NonNullColumnError struct {
	message    *service.Message
	columnName string
}

// ColumnName returns the column name with the NOT NULL constraint
func (e *NonNullColumnError) ColumnName() string {
	// This name comes directly from the Snowflake API so I hope this is properly quoted...
	return e.columnName
}

// Value returns nil
func (e *NonNullColumnError) Value() any {
	return nil
}

// Message returns the message that caused this error
func (e *NonNullColumnError) Message() *service.Message {
	return e.message
}

// Error implements the error interface
func (e *NonNullColumnError) Error() string {
	return fmt.Sprintf("column %q has a NOT NULL constraint and recieved a nil value", e.columnName)
}

var (
	_ error               = &MissingColumnError{}
	_ SchemaMismatchError = &MissingColumnError{}
)

// MissingColumnError occurs when a column that is not in the table is
// found on a record
type MissingColumnError struct {
	message    *service.Message
	columnName string
	val        any
}

// NewMissingColumnError creates a new MissingColumnError object
func NewMissingColumnError(message *service.Message, rawName string, val any) *MissingColumnError {
	return &MissingColumnError{message, rawName, val}
}

// Message returns the message that caused this error
func (e *MissingColumnError) Message() *service.Message {
	return e.message
}

// ColumnName returns the column name of the data that was not in the table
//
// NOTE this is escaped, so it's valid to use this directly in a SQL statement
// but I wish that Snowflake would just allow `identifier` for ALTER column.
func (e *MissingColumnError) ColumnName() string {
	return quoteColumnName(e.columnName)
}

// RawName is the unquoted name of the new column - DO NOT USE IN SQL!
// This is the more intutitve name for users in the mapping function
func (e *MissingColumnError) RawName() string {
	return e.columnName
}

// Value returns the value that was associated with the missing column
func (e *MissingColumnError) Value() any {
	return e.val
}

// Error implements the error interface
func (e *MissingColumnError) Error() string {
	return fmt.Sprintf("new data %+v with the name %q does not have an associated column", e.val, e.columnName)
}

// InvalidTimestampFormatError is when a timestamp column has a string value not in RFC3339 format.
type InvalidTimestampFormatError struct {
	columnType string
	val        string
}

// Error implements the error interface
func (e *InvalidTimestampFormatError) Error() string {
	return fmt.Sprintf("unable to parse %s value from %q - string time values must be in RFC 3339 format", e.columnType, e.val)
}
