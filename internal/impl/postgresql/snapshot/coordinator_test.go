// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package snapshot

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

func TestCoordinatorFullScenario(t *testing.T) {
	tableA := incrementalsnapshot.TableID{Schema: "public", Table: "a"}
	tableB := incrementalsnapshot.TableID{Schema: "public", Table: "b"}

	const chunkSize = 3

	// A: one full chunk, then an empty follow-up (zero-row advance path).
	rowsA := []incrementalsnapshot.Row{rowFor(tableA, 1), rowFor(tableA, 2), rowFor(tableA, 3)}
	// B: a short chunk (short-chunk advance path).
	rowsB := []incrementalsnapshot.Row{rowFor(tableB, 10), rowFor(tableB, 11)}

	mock := newScriptedMockDeps(map[string]*mockTable{
		tableA.String(): {pkCols: []string{"id"}, rows: rowsA, maxPK: incrementalsnapshot.PrimaryKey{3}},
		tableB.String(): {pkCols: []string{"id"}, rows: rowsB, maxPK: incrementalsnapshot.PrimaryKey{11}},
	})

	// low/high pairs: A chunk1, A chunk2 (empty, triggers advance), B chunk1.
	mock.pushWatermark(Watermark{Xmin: 100, Xmax: 100}) // low, A chunk 1
	mock.pushWatermark(Watermark{Xmin: 105, Xmax: 105}) // high, A chunk 1
	mock.pushWatermark(Watermark{Xmin: 110, Xmax: 110}) // low, A chunk 2 (empty)
	mock.pushWatermark(Watermark{Xmin: 112, Xmax: 112}) // high, A chunk 2 (empty)
	mock.pushWatermark(Watermark{Xmin: 120, Xmax: 120}) // low, B chunk 1
	mock.pushWatermark(Watermark{Xmin: 125, Xmax: 125}) // high, B chunk 1

	cfg := incrementalsnapshot.Config{
		Tables:    []incrementalsnapshot.TableID{tableA, tableB},
		ChunkSize: chunkSize,
		Deps:      mock.deps(chunkSize),
	}

	coord, err := NewCoordinator(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(context.Background()))

	require.False(t, coord.Done())
	require.NotNil(t, coord.current)
	assert.Equal(t, tableA, *coord.current)
	assert.Equal(t, incrementalsnapshot.PrimaryKey{3}, coord.lastSentPK)
	assert.Equal(t, 3, coord.window.Len())

	// Streamed row PK=2 arrives while still buffered.
	removed := coord.OnStreamedRow(tableA, incrementalsnapshot.PrimaryKey{2})
	assert.True(t, removed)

	// Different table: must not touch the window.
	removedOther := coord.OnStreamedRow(tableB, incrementalsnapshot.PrimaryKey{999})
	assert.False(t, removedOther)

	// txid below low.Xmin: window must not open yet.
	emitted, changed, err := coord.OnCommit(context.Background(), 50)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Nil(t, emitted)

	// txid==0 must always be a no-op, even once otherwise eligible.
	emitted, changed, err = coord.OnCommit(context.Background(), 0)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Nil(t, emitted)

	// txid==low.Xmin: opens, but doesn't close (threshold=105).
	emitted, changed, err = coord.OnCommit(context.Background(), 100)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Nil(t, emitted)

	// txid at the threshold exactly: still not closed (<=).
	emitted, changed, err = coord.OnCommit(context.Background(), 105)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Nil(t, emitted)

	// txid > threshold: closes, flushes, advances to B via A's empty follow-up.
	emitted, changed, err = coord.OnCommit(context.Background(), 106)
	require.NoError(t, err)
	assert.True(t, changed)
	require.Len(t, emitted, 2)
	assert.Equal(t, incrementalsnapshot.PrimaryKey{1}, emitted[0].PK)
	assert.Equal(t, incrementalsnapshot.PrimaryKey{3}, emitted[1].PK)

	// Short B chunk resets current to nil immediately.
	assert.Nil(t, coord.current)
	assert.Equal(t, incrementalsnapshot.PrimaryKey{11}, coord.lastSentPK)
	assert.Equal(t, 2, coord.window.Len())

	// B's open/close cycle (low=120, high=125).
	_, changed, err = coord.OnCommit(context.Background(), 119)
	require.NoError(t, err)
	assert.False(t, changed)

	_, changed, err = coord.OnCommit(context.Background(), 120)
	require.NoError(t, err)
	assert.False(t, changed)

	emitted, changed, err = coord.OnCommit(context.Background(), 126)
	require.NoError(t, err)
	assert.True(t, changed)
	require.Len(t, emitted, 2)
	assert.Equal(t, incrementalsnapshot.PrimaryKey{10}, emitted[0].PK)
	assert.Equal(t, incrementalsnapshot.PrimaryKey{11}, emitted[1].PK)

	// B's short chunk skipped the zero-row round-trip straight to done.
	assert.True(t, coord.Done())
}

func TestCoordinatorZeroRowAdvanceBetweenTables(t *testing.T) {
	tableA := incrementalsnapshot.TableID{Schema: "public", Table: "a"}
	tableB := incrementalsnapshot.TableID{Schema: "public", Table: "b"}

	const chunkSize = 2

	rowsA := []incrementalsnapshot.Row{rowFor(tableA, 1), rowFor(tableA, 2)}
	rowsB := []incrementalsnapshot.Row{rowFor(tableB, 5), rowFor(tableB, 6)}

	mock := newScriptedMockDeps(map[string]*mockTable{
		tableA.String(): {pkCols: []string{"id"}, rows: rowsA, maxPK: incrementalsnapshot.PrimaryKey{2}},
		tableB.String(): {pkCols: []string{"id"}, rows: rowsB, maxPK: incrementalsnapshot.PrimaryKey{6}},
	})

	// A's chunk isn't short, so a real zero-row fetch precedes advancing to
	// B: 4 pairs (A full, A empty, B full, B empty->done).
	mock.pushWatermark(Watermark{Xmin: 10, Xmax: 10})
	mock.pushWatermark(Watermark{Xmin: 11, Xmax: 11})
	mock.pushWatermark(Watermark{Xmin: 12, Xmax: 12})
	mock.pushWatermark(Watermark{Xmin: 13, Xmax: 13})
	mock.pushWatermark(Watermark{Xmin: 14, Xmax: 14})
	mock.pushWatermark(Watermark{Xmin: 15, Xmax: 15})
	mock.pushWatermark(Watermark{Xmin: 16, Xmax: 16})
	mock.pushWatermark(Watermark{Xmin: 17, Xmax: 17})

	cfg := incrementalsnapshot.Config{
		Tables:    []incrementalsnapshot.TableID{tableA, tableB},
		ChunkSize: chunkSize,
		Deps:      mock.deps(chunkSize),
	}

	coord, err := NewCoordinator(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(context.Background()))

	require.NotNil(t, coord.current)
	assert.Equal(t, tableA, *coord.current)
	assert.Equal(t, 2, coord.window.Len())

	// Closes A's window; triggers A's zero-row fetch, advancing to B.
	_, changed, err := coord.OnCommit(context.Background(), 10)
	require.NoError(t, err)
	assert.False(t, changed) // window opened, not yet closed

	emitted, changed, err := coord.OnCommit(context.Background(), 12)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Len(t, emitted, 2)

	require.NotNil(t, coord.current)
	assert.Equal(t, tableB, *coord.current)
	assert.Equal(t, 2, coord.window.Len())

	// Closes B's window; B's zero-row follow-up exhausts the queue.
	_, changed, err = coord.OnCommit(context.Background(), 14)
	require.NoError(t, err)
	assert.False(t, changed)

	emitted, changed, err = coord.OnCommit(context.Background(), 16)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Len(t, emitted, 2)

	assert.True(t, coord.Done())
}

func TestCoordinatorOnCommitNoopsWhenDone(t *testing.T) {
	cfg := incrementalsnapshot.Config{
		Tables:    nil,
		ChunkSize: 10,
		Deps:      newScriptedMockDeps(map[string]*mockTable{}).deps(10),
	}

	coord, err := NewCoordinator(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(context.Background()))
	require.True(t, coord.Done())

	emitted, changed, err := coord.OnCommit(context.Background(), 100)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Nil(t, emitted)

	assert.False(t, coord.OnStreamedRow(incrementalsnapshot.TableID{Schema: "public", Table: "a"}, incrementalsnapshot.PrimaryKey{1}))
}

func TestCoordinatorResumeAlwaysDerivesFreshWatermark(t *testing.T) {
	tableA := incrementalsnapshot.TableID{Schema: "public", Table: "a"}
	tableB := incrementalsnapshot.TableID{Schema: "public", Table: "b"}

	const chunkSize = 2

	rowsA := []incrementalsnapshot.Row{rowFor(tableA, 1), rowFor(tableA, 2)}
	rowsB := []incrementalsnapshot.Row{rowFor(tableB, 5)}

	mock := newScriptedMockDeps(map[string]*mockTable{
		tableA.String(): {pkCols: []string{"id"}, rows: rowsA, maxPK: incrementalsnapshot.PrimaryKey{2}},
		tableB.String(): {pkCols: []string{"id"}, rows: rowsB, maxPK: incrementalsnapshot.PrimaryKey{5}},
	})
	mock.pushWatermark(Watermark{Xmin: 1, Xmax: 1})
	mock.pushWatermark(Watermark{Xmin: 2, Xmax: 2})

	cfg := incrementalsnapshot.Config{
		Tables:    []incrementalsnapshot.TableID{tableA, tableB},
		ChunkSize: chunkSize,
		Deps:      mock.deps(chunkSize),
	}

	coord, err := NewCoordinator(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(context.Background()))

	callsBeforeResume := mock.watermarkCalls
	require.Positive(t, callsBeforeResume)

	// Pre-flush checkpoint must report the baseline, not tableA's unflushed
	// first chunk.
	state := coord.State()
	require.False(t, state.Done)
	assert.Nil(t, state.CurrentTable)
	assert.Equal(t, []incrementalsnapshot.TableID{tableA, tableB}, state.RemainingTables)

	// New watermarks for the "restart".
	mock.pushWatermark(Watermark{Xmin: 3, Xmax: 3})
	mock.pushWatermark(Watermark{Xmin: 4, Xmax: 4})

	resumed, err := NewCoordinator(cfg, state)
	require.NoError(t, err)
	require.NoError(t, resumed.Start(context.Background()))

	// Call count must increase: watermarks are never persisted, only re-derived.
	assert.Greater(t, mock.watermarkCalls, callsBeforeResume)
	assert.Positive(t, mock.forceFreshCalls)
}

// TestCoordinatorResumeRefetchesUnflushedChunk: State() must only advance
// once flushed, or a resumed coordinator skips unflushed rows.
func TestCoordinatorResumeRefetchesUnflushedChunk(t *testing.T) {
	// sortedRowsAfter is a pure "WHERE pk > lower LIMIT limit" query, unlike
	// scriptedMockDeps' stateful cursor -- needed so re-querying a bound is
	// idempotent. Assumes single-column int keys.
	sortedRowsAfter := func(rows []incrementalsnapshot.Row, lower incrementalsnapshot.PrimaryKey, limit int) []incrementalsnapshot.Row {
		start := 0
		if lower != nil {
			start = len(rows)
			for i, r := range rows {
				if r.PK[0].(int) > lower[0].(int) {
					start = i
					break
				}
			}
		}
		if start >= len(rows) {
			return nil
		}
		return rows[start:min(start+limit, len(rows))]
	}

	tableA := incrementalsnapshot.TableID{Schema: "public", Table: "a"}
	const chunkSize = 2
	rowsA := []incrementalsnapshot.Row{rowFor(tableA, 1), rowFor(tableA, 2), rowFor(tableA, 3), rowFor(tableA, 4)}
	maxPK := incrementalsnapshot.PrimaryKey{4}

	watermarks := []Watermark{
		{Xmin: 1, Xmax: 1}, {Xmin: 2, Xmax: 2}, // chunk 1 ([1,2]): low, high
		{Xmin: 3, Xmax: 3}, {Xmin: 4, Xmax: 4}, // chunk 2 ([3,4]): low, high
	}
	var watermarkCalls int
	var fetchLog []string

	deps := incrementalsnapshot.Deps{
		ResolvePrimaryKey: func(context.Context, incrementalsnapshot.TableID) ([]string, error) { return []string{"id"}, nil },
		ResolveMaxKey: func(context.Context, incrementalsnapshot.TableID, []string, string) (incrementalsnapshot.PrimaryKey, error) {
			return maxPK, nil
		},
		ForceFreshTransaction: func(context.Context) error { return nil },
		ResolveWatermark: func(context.Context) (any, error) {
			idx := min(watermarkCalls, len(watermarks)-1)
			watermarkCalls++
			return watermarks[idx], nil
		},
		FetchChunk: func(_ context.Context, _ incrementalsnapshot.TableID, _ []string, _ string, args []any) ([]incrementalsnapshot.Row, error) {
			var lower incrementalsnapshot.PrimaryKey
			if len(args) > 1 {
				lower = incrementalsnapshot.PrimaryKey{args[0]}
			}
			fetchLog = append(fetchLog, fmt.Sprint(lower))
			return sortedRowsAfter(rowsA, lower, chunkSize), nil
		},
	}

	cfg := incrementalsnapshot.Config{Tables: []incrementalsnapshot.TableID{tableA}, ChunkSize: chunkSize, Deps: deps}

	coord, err := NewCoordinator(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(context.Background())) // fetches chunk 1 ([1,2])

	_, changed, err := coord.OnCommit(context.Background(), 1) // opens, doesn't close (1 <= closeThreshold 2)
	require.NoError(t, err)
	assert.False(t, changed)

	emitted, changed, err := coord.OnCommit(context.Background(), 3) // closes: flushes chunk 1, fetches chunk 2
	require.NoError(t, err)
	require.True(t, changed)
	require.Len(t, emitted, 2)
	assert.Equal(t, incrementalsnapshot.PrimaryKey{1}, emitted[0].PK)
	assert.Equal(t, incrementalsnapshot.PrimaryKey{2}, emitted[1].PK)

	// Chunk 2 fetched but not flushed; checkpoint must reflect only chunk 1.
	state := coord.State()
	require.False(t, state.Done)
	require.NotNil(t, state.CurrentTable)
	assert.Equal(t, tableA, *state.CurrentTable)
	assert.Equal(t, incrementalsnapshot.PrimaryKey{2}, state.LastSentPK)
	require.Len(t, fetchLog, 2, "sanity: exactly one FetchChunk call for chunk 1's fetch so far")

	resumed, err := NewCoordinator(cfg, state)
	require.NoError(t, err)
	require.NoError(t, resumed.Start(context.Background())) // must refetch chunk 2, not skip it

	require.Len(t, fetchLog, 3)
	assert.Equal(t, fetchLog[1], fetchLog[2], "resumed coordinator must request the same lower bound as the original chunk 2 fetch")

	emitted, changed, err = resumed.OnCommit(context.Background(), 5) // opens and closes in one call (5 > closeThreshold 4)
	require.NoError(t, err)
	require.True(t, changed)
	require.Len(t, emitted, 2, "chunk 2's rows must still be emitted exactly once, on the resumed coordinator")
	assert.Equal(t, incrementalsnapshot.PrimaryKey{3}, emitted[0].PK)
	assert.Equal(t, incrementalsnapshot.PrimaryKey{4}, emitted[1].PK)
}

func TestCoordinatorConfigValidation(t *testing.T) {
	validDeps := newScriptedMockDeps(map[string]*mockTable{}).deps(1)

	t.Run("zero chunk size", func(t *testing.T) {
		_, err := NewCoordinator(incrementalsnapshot.Config{ChunkSize: 0, Deps: validDeps}, nil)
		require.Error(t, err)
	})

	t.Run("negative chunk size", func(t *testing.T) {
		_, err := NewCoordinator(incrementalsnapshot.Config{ChunkSize: -1, Deps: validDeps}, nil)
		require.Error(t, err)
	})

	t.Run("missing dep", func(t *testing.T) {
		deps := validDeps
		deps.FetchChunk = nil
		_, err := NewCoordinator(incrementalsnapshot.Config{ChunkSize: 1, Deps: deps}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "FetchChunk")
	})
}

// mockTable is a fixture of fake rows used to script FetchChunk/ResolveMaxKey
// without a real database.
type mockTable struct {
	pkCols []string
	rows   []incrementalsnapshot.Row // full ordered set of rows in the table, PK-ascending
	maxPK  incrementalsnapshot.PrimaryKey
}

// scriptedMockDeps serves chunk data from an in-memory fixture and scripted
// watermarks, tracking a per-table cursor instead of parsing SQL/args.
type scriptedMockDeps struct {
	tables map[string]*mockTable
	cursor map[string]int

	watermarks      []Watermark
	watermarkCalls  int
	forceFreshCalls int
}

func newScriptedMockDeps(tables map[string]*mockTable) *scriptedMockDeps {
	return &scriptedMockDeps{
		tables: tables,
		cursor: make(map[string]int),
	}
}

func (m *scriptedMockDeps) pushWatermark(wm Watermark) {
	m.watermarks = append(m.watermarks, wm)
}

func (m *scriptedMockDeps) deps(chunkSize int) incrementalsnapshot.Deps {
	return incrementalsnapshot.Deps{
		ResolvePrimaryKey: func(_ context.Context, table incrementalsnapshot.TableID) ([]string, error) {
			return m.tables[table.String()].pkCols, nil
		},
		ResolveMaxKey: func(_ context.Context, table incrementalsnapshot.TableID, _ []string, _ string) (incrementalsnapshot.PrimaryKey, error) {
			return m.tables[table.String()].maxPK, nil
		},
		ResolveWatermark: func(context.Context) (any, error) {
			idx := m.watermarkCalls
			if idx >= len(m.watermarks) {
				idx = len(m.watermarks) - 1
			}
			m.watermarkCalls++
			return m.watermarks[idx], nil
		},
		ForceFreshTransaction: func(context.Context) error {
			m.forceFreshCalls++
			return nil
		},
		FetchChunk: func(_ context.Context, table incrementalsnapshot.TableID, _ []string, _ string, _ []any) ([]incrementalsnapshot.Row, error) {
			key := table.String()
			mt := m.tables[key]
			start := m.cursor[key]
			if start >= len(mt.rows) {
				return nil, nil
			}
			end := min(start+chunkSize, len(mt.rows))
			chunk := mt.rows[start:end]
			m.cursor[key] = end
			return chunk, nil
		},
	}
}

func rowFor(table incrementalsnapshot.TableID, pk int) incrementalsnapshot.Row {
	return incrementalsnapshot.Row{
		Table: table,
		PK:    incrementalsnapshot.PrimaryKey{pk},
		Data:  map[string]any{"id": pk},
	}
}
