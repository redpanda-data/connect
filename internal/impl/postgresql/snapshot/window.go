// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package snapshot

import (
	"fmt"
	"strings"
)

// windowKey uniquely identifies a buffered row by table and primary key. It
// is built via fmt.Sprintf("%v", ...) of the table and each PK element,
// joined with a separator that's unlikely to appear in practice. This is
// simple and correct for the scalar PK values we expect (ints, strings,
// etc); the only caveat is a theoretical collision if a PK element's string
// representation happens to contain the separator itself in a way that
// aliases with a different tuple - not a concern for the numeric/string
// primary keys this package targets.
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

// windowBuffer is an ordered, deduplicated buffer of Rows keyed by (table,
// primary key). Rows are held in the order they were added; Remove excises a
// row from the middle without disturbing the order of the rest.
type windowBuffer struct {
	rows    []Row
	indexOf map[windowKey]int
}

func newWindowBuffer() *windowBuffer {
	return &windowBuffer{
		indexOf: make(map[windowKey]int),
	}
}

// Add appends a row to the buffer, preserving insertion order.
func (w *windowBuffer) Add(row Row) {
	key := newWindowKey(row.Table, row.PK)
	w.indexOf[key] = len(w.rows)
	w.rows = append(w.rows, row)
}

// Remove excises the row matching table and pk from the buffer, if present.
// It returns true if a row was removed.
func (w *windowBuffer) Remove(table TableID, pk PrimaryKey) bool {
	key := newWindowKey(table, pk)
	idx, exists := w.indexOf[key]
	if !exists {
		return false
	}

	delete(w.indexOf, key)
	w.rows = append(w.rows[:idx], w.rows[idx+1:]...)

	// Removing a row shifts every subsequent row's index down by one; fix
	// up the index map so future lookups remain correct.
	for k, i := range w.indexOf {
		if i > idx {
			w.indexOf[k] = i - 1
		}
	}
	return true
}

// Flush returns the buffered rows in original insertion order and clears the
// buffer.
func (w *windowBuffer) Flush() []Row {
	rows := w.rows
	w.rows = nil
	w.indexOf = make(map[windowKey]int)
	return rows
}

// Len returns the number of rows currently buffered.
func (w *windowBuffer) Len() int {
	return len(w.rows)
}
