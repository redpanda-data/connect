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

import "fmt"

// SchemaMismatchError occurs when the user provided data has data that
// doesn't match the schema *and* the table can be evolved to accomidate
//
// This can be used as a mechanism to evolve the schema dynamically.
type SchemaMismatchError interface {
	ColumnName() string
	Value() any
}

var _ error = NonNullColumnError{}
var _ SchemaMismatchError = NonNullColumnError{}

// NonNullColumnError occurs when a column with a NOT NULL constraint
// gets a value with a `NULL` value.
type NonNullColumnError struct {
	columnName string
}

// ColumnName returns the column name with the NOT NULL constraint
func (e NonNullColumnError) ColumnName() string {
	// This name comes directly from the Snowflake API so I hope this is properly quoted...
	return e.columnName
}

// ColumnName returns nil
func (e NonNullColumnError) Value() any {
	return nil
}

// Error implements the error interface
func (e NonNullColumnError) Error() string {
	return fmt.Sprintf("column %q has a NOT NULL constraint and recieved a nil value", e.columnName)
}

var _ error = MissingColumnError{}
var _ SchemaMismatchError = MissingColumnError{}

// MissingColumnError occurs when a column that is not in the table is
// found on a record
type MissingColumnError struct {
	columnName string
	val        any
}

// ColumnName returns the column name of the data that was not in the table
//
// NOTE this is escaped, so it's valid to use this directly in a SQL statement
// but I wish that Snowflake would just allow `identifier` for ALTER column.
func (e MissingColumnError) ColumnName() string {
	return quoteColumnName(e.columnName)
}

// The raw name of the new column - DO NOT USE IN SQL!
// This is the more intutitve name for users in the mapping function
func (e MissingColumnError) RawName() string {
	return e.columnName
}

// ColumnName returns the value that was associated with the missing column
func (e MissingColumnError) Value() any {
	return e.val
}

// Error implements the error interface
func (e MissingColumnError) Error() string {
	return fmt.Sprintf("new data %+v with the name %q does not have an associated column", e.val, e.columnName)
}
