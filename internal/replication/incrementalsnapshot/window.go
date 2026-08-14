// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"fmt"
	"strings"
)

// windowKey identifies a buffered row by table and primary key, formed by
// joining their string forms with a separator unlikely to collide in
// practice for the scalar PK values this package targets.
type windowKey string

const windowKeySeparator = "\x1f"

func newWindowKey(table TableID, pk PrimaryKey) windowKey {
	var b strings.Builder
	b.WriteString(table.String())
	for _, v := range pk {
		b.WriteString(windowKeySeparator)
		fmt.Fprintf(&b, "%v", v)
	}
	return windowKey(b.String())
}

// WindowBuffer is an ordered, deduplicated buffer of Rows keyed by (table,
// primary key). Rows are held in the order they were added; Remove excises a
// row from the middle without disturbing the order of the rest.
type WindowBuffer struct {
	rows    []Row
	indexOf map[windowKey]int
}

// NewWindowBuffer constructs an empty WindowBuffer.
func NewWindowBuffer() *WindowBuffer {
	return &WindowBuffer{
		indexOf: make(map[windowKey]int),
	}
}

// Add appends a row to the buffer, preserving insertion order.
func (w *WindowBuffer) Add(row Row) {
	key := newWindowKey(row.Table, row.PK)
	w.indexOf[key] = len(w.rows)
	w.rows = append(w.rows, row)
}

// Remove excises the row matching table and pk from the buffer, if present.
// It returns true if a row was removed.
func (w *WindowBuffer) Remove(table TableID, pk PrimaryKey) bool {
	key := newWindowKey(table, pk)
	idx, exists := w.indexOf[key]
	if !exists {
		return false
	}

	delete(w.indexOf, key)
	w.rows = append(w.rows[:idx], w.rows[idx+1:]...)

	// Shift every later row's index down by one to match.
	for k, i := range w.indexOf {
		if i > idx {
			w.indexOf[k] = i - 1
		}
	}
	return true
}

// Flush returns the buffered rows in original insertion order and clears the
// buffer.
func (w *WindowBuffer) Flush() []Row {
	rows := w.rows
	w.rows = nil
	w.indexOf = make(map[windowKey]int)
	return rows
}

// Len returns the number of rows currently buffered.
func (w *WindowBuffer) Len() int {
	return len(w.rows)
}
