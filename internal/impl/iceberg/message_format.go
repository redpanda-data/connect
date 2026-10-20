// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"fmt"
	"slices"

	"github.com/apache/iceberg-go"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// MessageFormat is the shape of incoming messages, set by the
// `message_format` field.
type MessageFormat string

const (
	// MessageFormatObject messages are objects keyed by column name.
	MessageFormatObject MessageFormat = "object"
	// MessageFormatArray messages are arrays of values, matched by position
	// to the table's top-level columns in schema order.
	MessageFormatArray MessageFormat = "array"
)

// arraySchemaEvolutionConflict is shared by the config lint rule and the
// constructor check, so both reject the combination with the same text.
const arraySchemaEvolutionConflict = "message_format: array requires schema_evolution.enabled: false, because arrays carry no column names to create or evolve a table from"

// arrayToRow converts a positional array into the column-keyed row every write
// path consumes, so array mode reuses the object-mode code unchanged. A short
// array leaves its trailing columns absent rather than null: the shredder then
// writes null for an optional column and raises its required-field error for a
// required one, exactly as for an object missing those keys.
//
// Top-level []byte elements, which `content().split(...)` produces, become
// strings. Array elements are text by nature, and left as bytes a decimal,
// binary or uuid column would read them as raw binary instead: "12345.67" into
// a decimal(18,2), whose width is also 8 bytes, would be stored as its ASCII
// codes. Nested values keep their object-mode meaning.
func arrayToRow(arr []any, columns []string) (map[string]any, error) {
	if len(arr) > len(columns) {
		return nil, fmt.Errorf("array has %d elements but the table has %d columns", len(arr), len(columns))
	}
	row := make(map[string]any, len(arr))
	for i, v := range arr {
		if b, isBytes := v.([]byte); isBytes {
			v = string(b)
		}
		row[columns[i]] = v
	}
	return row, nil
}

// arrayColumnsRemapped reports whether a rebuilt writer's column order moves
// any position that the previous order assigned, so an array that used to land
// in one column would now land in another. Appending columns at the end keeps
// every existing position and is not a remap. A nil prev is the first writer
// for the table.
func arrayColumnsRemapped(prev, cur []string) bool {
	if prev == nil {
		return false
	}
	return len(cur) < len(prev) || !slices.Equal(prev, cur[:len(prev)])
}

func topLevelColumnNames(s *iceberg.Schema) []string {
	fields := s.Fields()
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	return names
}

// notObjectError reports an object-mode message that is not an object. It is
// a distinct type so each call site can keep its own wording for this case.
type notObjectError struct {
	got any
}

func (e notObjectError) Error() string {
	return fmt.Sprintf("message is not an object, got %T", e.got)
}

// messageRow reads a message as the column-keyed row the write paths consume,
// according to the configured message_format. In object mode a non-object
// message yields a notObjectError.
func (w *writer) messageRow(msg *service.Message) (map[string]any, error) {
	structured, err := msg.AsStructured()
	if err != nil {
		if w.messageFormat == MessageFormatArray {
			return nil, fmt.Errorf("message_format is array: %w", err)
		}
		return nil, err
	}
	if w.messageFormat != MessageFormatArray {
		row, ok := structured.(map[string]any)
		if !ok {
			return nil, notObjectError{got: structured}
		}
		return row, nil
	}
	arr, ok := structured.([]any)
	if !ok {
		return nil, fmt.Errorf("message_format is array but message is %T", structured)
	}
	return arrayToRow(arr, w.arrayColumns)
}
