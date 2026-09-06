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

// windowKey identifies a buffered row by table and primary key. It joins
// the text form of each part with a separator. The primary key values in
// this package are simple values, so the separator does not occur in them.
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

// WindowBuffer holds Row values in a buffer. Each row has a key of table and
// primary key, and the buffer holds one row for each key. The buffer keeps
// the rows in the order that the caller added them. Remove takes one row
// out of the buffer and does not change the order of the other rows.
type WindowBuffer struct {
	rows    []Row
	indexOf map[windowKey]int
}

// NewWindowBuffer makes an empty WindowBuffer.
func NewWindowBuffer() *WindowBuffer {
	return &WindowBuffer{
		indexOf: make(map[windowKey]int),
	}
}

// Add puts a row at the end of the buffer.
func (w *WindowBuffer) Add(row Row) {
	key := newWindowKey(row.Table, row.PK)
	w.indexOf[key] = len(w.rows)
	w.rows = append(w.rows, row)
}

// Remove takes the row for table and pk out of the buffer. It returns true
// if the buffer held that row.
func (w *WindowBuffer) Remove(table TableID, pk PrimaryKey) bool {
	key := newWindowKey(table, pk)
	idx, exists := w.indexOf[key]
	if !exists {
		return false
	}

	delete(w.indexOf, key)
	w.rows = append(w.rows[:idx], w.rows[idx+1:]...)

	// Decrease the index of each later row by one.
	for k, i := range w.indexOf {
		if i > idx {
			w.indexOf[k] = i - 1
		}
	}
	return true
}

// Flush returns the rows in the order that the caller added them. It then
// empties the buffer.
func (w *WindowBuffer) Flush() []Row {
	rows := w.rows
	w.rows = nil
	w.indexOf = make(map[windowKey]int)
	return rows
}

// Len returns the number of rows in the buffer.
func (w *WindowBuffer) Len() int {
	return len(w.rows)
}
