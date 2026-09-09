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

func TestWindowBufferRemoveKeepsOrder(t *testing.T) {
	// Removal marks a slot rather than shifting the slice, so Flush has to
	// compact while preserving insertion order.
	table := TableID{Schema: "public", Table: "a"}
	w := NewWindowBuffer()
	for pk := 1; pk <= 6; pk++ {
		w.Add(Row{Table: table, PK: PrimaryKey{pk}})
	}

	// Take one from each end and two from the middle.
	for _, pk := range []int{1, 3, 4, 6} {
		require.True(t, w.Remove(table, PrimaryKey{pk}))
	}
	assert.Equal(t, 2, w.Len())

	flushed := w.Flush()
	require.Len(t, flushed, 2)
	assert.Equal(t, PrimaryKey{2}, flushed[0].PK)
	assert.Equal(t, PrimaryKey{5}, flushed[1].PK)
	assert.Zero(t, w.Len(), "Flush must empty the buffer")
}

func TestWindowBufferRemoveEveryRow(t *testing.T) {
	table := TableID{Schema: "public", Table: "a"}
	w := NewWindowBuffer()
	for pk := 1; pk <= 3; pk++ {
		w.Add(Row{Table: table, PK: PrimaryKey{pk}})
	}
	for pk := 1; pk <= 3; pk++ {
		require.True(t, w.Remove(table, PrimaryKey{pk}))
	}

	assert.Zero(t, w.Len())
	assert.Empty(t, w.Flush())
}

func TestWindowBufferAddReplacesExistingKey(t *testing.T) {
	// A repeated key must not leave the earlier row behind to be emitted as
	// well, since the buffer holds one row per key.
	table := TableID{Schema: "public", Table: "a"}
	w := NewWindowBuffer()
	w.Add(Row{Table: table, PK: PrimaryKey{1}, Data: map[string]any{"v": "first"}})
	w.Add(Row{Table: table, PK: PrimaryKey{2}})
	w.Add(Row{Table: table, PK: PrimaryKey{1}, Data: map[string]any{"v": "second"}})

	assert.Equal(t, 2, w.Len())
	flushed := w.Flush()
	require.Len(t, flushed, 2)
	assert.Equal(t, PrimaryKey{2}, flushed[0].PK)
	assert.Equal(t, PrimaryKey{1}, flushed[1].PK)
	assert.Equal(t, "second", flushed[1].Data["v"])
}

func TestWindowBufferAddAfterRemoveSameKey(t *testing.T) {
	// The stream can evict a key that a later chunk then re-reads.
	table := TableID{Schema: "public", Table: "a"}
	w := NewWindowBuffer()
	w.Add(Row{Table: table, PK: PrimaryKey{1}})
	require.True(t, w.Remove(table, PrimaryKey{1}))
	w.Add(Row{Table: table, PK: PrimaryKey{1}})

	assert.Equal(t, 1, w.Len())
	require.True(t, w.Remove(table, PrimaryKey{1}), "the re-added row must be removable")
	assert.Zero(t, w.Len())
}

// BenchmarkWindowBufferDrainChunk drains a full chunk one row at a time,
// which is what a table under sustained write load does during its backfill.
// Removal must not depend on the chunk size.
func BenchmarkWindowBufferDrainChunk(b *testing.B) {
	table := TableID{Schema: "public", Table: "a"}
	for _, size := range []int{1024, 16384, 100000} {
		b.Run(fmt.Sprintf("chunk=%d", size), func(b *testing.B) {
			for b.Loop() {
				w := NewWindowBuffer()
				for pk := range size {
					w.Add(Row{Table: table, PK: PrimaryKey{pk}})
				}
				for pk := range size {
					w.Remove(table, PrimaryKey{pk})
				}
				w.Flush()
			}
		})
	}
}
