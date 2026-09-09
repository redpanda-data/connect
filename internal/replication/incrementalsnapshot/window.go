// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"fmt"
	"strings"
)

// windowKey identifies a buffered row by table and key, joining their text
// forms with a separator that will not occur in the scalar key values this
// package targets.
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
// key). Remove is O(1) and preserves the order of the remaining rows; Flush
// returns them in insertion order.
type WindowBuffer struct {
	rows    []Row
	indexOf map[windowKey]int
	// deleted marks the slots Remove has vacated, and live counts the rest.
	//
	// Remove runs on the caller's replication loop for every streamed row on
	// a snapshotted table, so it must not re-index indexOf or shift rows: a
	// chunk drained row by row would then cost O(len(rows)^2), enough at a
	// large chunk size to stall that loop past the server's timeout. Marking
	// the slot instead leaves the compaction to Flush, which runs once.
	deleted []bool
	live    int
}

// NewWindowBuffer returns an empty WindowBuffer.
func NewWindowBuffer() *WindowBuffer {
	return &WindowBuffer{
		indexOf: make(map[windowKey]int),
	}
}

// Add puts a row at the end of the buffer. If the buffer already holds that
// key, the new row replaces it and keeps the later position.
func (w *WindowBuffer) Add(row Row) {
	key := newWindowKey(row.Table, row.PK)
	if idx, exists := w.indexOf[key]; exists {
		w.deleted[idx] = true
		w.live--
	}
	w.indexOf[key] = len(w.rows)
	w.rows = append(w.rows, row)
	w.deleted = append(w.deleted, false)
	w.live++
}

// Remove excises the row for table and pk, reporting whether it was there.
func (w *WindowBuffer) Remove(table TableID, pk PrimaryKey) bool {
	key := newWindowKey(table, pk)
	idx, exists := w.indexOf[key]
	if !exists {
		return false
	}

	delete(w.indexOf, key)
	w.deleted[idx] = true
	w.live--
	return true
}

// Flush returns the buffered rows in insertion order and empties the buffer.
func (w *WindowBuffer) Flush() []Row {
	rows := w.rows
	if w.live < len(rows) {
		compacted := make([]Row, 0, w.live)
		for i, row := range rows {
			if !w.deleted[i] {
				compacted = append(compacted, row)
			}
		}
		rows = compacted
	}

	w.rows = nil
	w.deleted = nil
	w.live = 0
	w.indexOf = make(map[windowKey]int)
	return rows
}

// Len returns the number of rows in the buffer, excluding removed ones.
func (w *WindowBuffer) Len() int {
	return w.live
}
