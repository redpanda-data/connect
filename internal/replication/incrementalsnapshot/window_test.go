// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWindowBufferInsertionOrderPreserved(t *testing.T) {
	table := TableID{Schema: "public", Table: "orders"}
	w := NewWindowBuffer()

	rows := []Row{
		{Table: table, PK: PrimaryKey{1}, Data: map[string]any{"v": "a"}},
		{Table: table, PK: PrimaryKey{2}, Data: map[string]any{"v": "b"}},
		{Table: table, PK: PrimaryKey{3}, Data: map[string]any{"v": "c"}},
	}
	for _, r := range rows {
		w.Add(r)
	}

	require.Equal(t, len(rows), w.Len())
	assert.Equal(t, rows, w.Flush())
}

func TestWindowBufferRemoveAbsentIsNoop(t *testing.T) {
	table := TableID{Schema: "public", Table: "orders"}
	w := NewWindowBuffer()
	w.Add(Row{Table: table, PK: PrimaryKey{1}})

	removed := w.Remove(table, PrimaryKey{999})
	assert.False(t, removed)
	assert.Equal(t, 1, w.Len())
}

func TestWindowBufferFlushClearsBuffer(t *testing.T) {
	table := TableID{Schema: "public", Table: "orders"}
	w := NewWindowBuffer()
	w.Add(Row{Table: table, PK: PrimaryKey{1}})

	first := w.Flush()
	assert.Len(t, first, 1)

	second := w.Flush()
	assert.Empty(t, second)
	assert.Equal(t, 0, w.Len())
}

func TestWindowBufferCompositePrimaryKeys(t *testing.T) {
	table := TableID{Schema: "public", Table: "orders"}
	w := NewWindowBuffer()

	rowA := Row{Table: table, PK: PrimaryKey{1, "a"}, Data: map[string]any{"v": "row-a"}}
	rowB := Row{Table: table, PK: PrimaryKey{1, "b"}, Data: map[string]any{"v": "row-b"}}
	w.Add(rowA)
	w.Add(rowB)

	removed := w.Remove(table, PrimaryKey{1, "a"})
	assert.True(t, removed)
	assert.Equal(t, 1, w.Len())

	remaining := w.Flush()
	require.Len(t, remaining, 1)
	assert.Equal(t, rowB, remaining[0])
}

func TestWindowBufferRemoveMiddlePreservesOrderOfRest(t *testing.T) {
	table := TableID{Schema: "public", Table: "orders"}
	w := NewWindowBuffer()

	rowA := Row{Table: table, PK: PrimaryKey{1}}
	rowB := Row{Table: table, PK: PrimaryKey{2}}
	rowC := Row{Table: table, PK: PrimaryKey{3}}
	w.Add(rowA)
	w.Add(rowB)
	w.Add(rowC)

	require.True(t, w.Remove(table, PrimaryKey{2}))

	assert.Equal(t, []Row{rowA, rowC}, w.Flush())
}

func TestWindowBufferDifferentTablesSamePKAreDistinct(t *testing.T) {
	tableA := TableID{Schema: "public", Table: "orders"}
	tableB := TableID{Schema: "public", Table: "customers"}
	w := NewWindowBuffer()

	rowA := Row{Table: tableA, PK: PrimaryKey{1}}
	rowB := Row{Table: tableB, PK: PrimaryKey{1}}
	w.Add(rowA)
	w.Add(rowB)

	require.True(t, w.Remove(tableA, PrimaryKey{1}))
	remaining := w.Flush()
	require.Len(t, remaining, 1)
	assert.Equal(t, rowB, remaining[0])
}
