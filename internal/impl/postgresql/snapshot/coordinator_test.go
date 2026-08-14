// Copyright 2025 Redpanda Data, Inc.
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

	"github.com/redpanda-data/connect/v4/internal/replication"
)

// mockTable is a hand-rolled fixture describing the fake rows a table
// "contains", used to script FetchChunk/ResolveMaxKey responses without a
// real database.
type mockTable struct {
	pkCols []string
	rows   []replication.Row // full ordered set of rows in the table, PK-ascending
	maxPK  replication.PrimaryKey
}

// scriptedMockDeps builds a Deps that serves chunk data purely from an
// in-memory fixture, and lets the test script the watermark sequence
// returned by ResolveWatermark. It tracks, per table, how many rows have
// already been served, and serves the next chunk (bounded by chunkSize)
// directly from the fixture -- avoiding any need to parse the generated
// SQL/args from buildChunkQuery.
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

func (m *scriptedMockDeps) deps(chunkSize int) replication.Deps {
	return replication.Deps{
		ResolvePrimaryKey: func(_ context.Context, table replication.TableID) ([]string, error) {
			return m.tables[table.String()].pkCols, nil
		},
		ResolveMaxKey: func(_ context.Context, table replication.TableID, _ []string, _ string) (replication.PrimaryKey, error) {
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
		FetchChunk: func(_ context.Context, table replication.TableID, _ []string, _ string, _ []any) ([]replication.Row, error) {
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

func rowFor(table replication.TableID, pk int) replication.Row {
	return replication.Row{
		Table: table,
		PK:    replication.PrimaryKey{pk},
		Data:  map[string]any{"id": pk},
	}
}

func TestCoordinator_FullScenario(t *testing.T) {
	tableA := replication.TableID{Schema: "public", Table: "a"}
	tableB := replication.TableID{Schema: "public", Table: "b"}

	const chunkSize = 3

	// Table A has exactly one full chunk (3 rows) then a short/empty
	// follow-up (0 rows) - exercises the "advance on zero rows" path.
	rowsA := []replication.Row{rowFor(tableA, 1), rowFor(tableA, 2), rowFor(tableA, 3)}
	// Table B has fewer rows than a full chunk (2 rows) - exercises the
	// "advance on short chunk" path in the same pass rows are buffered.
	rowsB := []replication.Row{rowFor(tableB, 10), rowFor(tableB, 11)}

	mock := newScriptedMockDeps(map[string]*mockTable{
		tableA.String(): {pkCols: []string{"id"}, rows: rowsA, maxPK: replication.PrimaryKey{3}},
		tableB.String(): {pkCols: []string{"id"}, rows: rowsB, maxPK: replication.PrimaryKey{11}},
	})

	// Watermark sequence: low/high pair for table A's full chunk, then
	// low/high for table A's trailing empty chunk (which triggers the
	// advance to B), then low/high for table B's short chunk.
	mock.pushWatermark(Watermark{Xmin: 100, Xmax: 100}) // low, A chunk 1
	mock.pushWatermark(Watermark{Xmin: 105, Xmax: 105}) // high, A chunk 1
	mock.pushWatermark(Watermark{Xmin: 110, Xmax: 110}) // low, A chunk 2 (empty)
	mock.pushWatermark(Watermark{Xmin: 112, Xmax: 112}) // high, A chunk 2 (empty)
	mock.pushWatermark(Watermark{Xmin: 120, Xmax: 120}) // low, B chunk 1
	mock.pushWatermark(Watermark{Xmin: 125, Xmax: 125}) // high, B chunk 1

	cfg := replication.Config{
		Tables:    []replication.TableID{tableA, tableB},
		ChunkSize: chunkSize,
		Deps:      mock.deps(chunkSize),
	}

	coord, err := NewCoordinator(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(context.Background()))

	require.False(t, coord.Done())
	require.NotNil(t, coord.current)
	assert.Equal(t, tableA, *coord.current)
	assert.Equal(t, replication.PrimaryKey{3}, coord.lastSentPK)
	assert.Equal(t, 3, coord.window.Len())

	// Simulate the replication stream delivering a fresher version of row
	// PK=2 while it's still sitting in the buffered window.
	removed := coord.OnStreamedRow(tableA, replication.PrimaryKey{2})
	assert.True(t, removed)

	// A streamed row for a different table must never touch the window.
	removedOther := coord.OnStreamedRow(tableB, replication.PrimaryKey{999})
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

	// txid reaches low.Xmin (100): opens the window, but doesn't close it
	// yet since it hasn't passed max(high.Xmax, low.Xmax) = 105.
	emitted, changed, err = coord.OnCommit(context.Background(), 100)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Nil(t, emitted)

	// txid at the threshold exactly: still not closed (<=).
	emitted, changed, err = coord.OnCommit(context.Background(), 105)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Nil(t, emitted)

	// txid exceeds the threshold: window closes, flush happens, and the
	// coordinator plans the next chunk (table A's trailing empty chunk,
	// which advances straight to table B).
	emitted, changed, err = coord.OnCommit(context.Background(), 106)
	require.NoError(t, err)
	assert.True(t, changed)
	require.Len(t, emitted, 2)
	assert.Equal(t, replication.PrimaryKey{1}, emitted[0].PK)
	assert.Equal(t, replication.PrimaryKey{3}, emitted[1].PK)

	// Table A's zero-row follow-up chunk should have advanced us straight to
	// table B, whose chunk (2 rows) was short (< chunkSize=3). Since a short
	// chunk means the table is already known to be exhausted, current is
	// reset to nil immediately (skipping a wasted zero-row round-trip on the
	// next advance) rather than continuing to point at table B.
	assert.Nil(t, coord.current)
	assert.Equal(t, replication.PrimaryKey{11}, coord.lastSentPK)
	assert.Equal(t, 2, coord.window.Len())

	// Drive table B's window open/close cycle using the watermarks pushed
	// for its chunk (low=120, high=125).
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
	assert.Equal(t, replication.PrimaryKey{10}, emitted[0].PK)
	assert.Equal(t, replication.PrimaryKey{11}, emitted[1].PK)

	// Table B's chunk was short, so planNextChunk should have advanced
	// straight to "no more tables" and marked the coordinator done, without
	// needing another zero-row round-trip.
	assert.True(t, coord.Done())
}

func TestCoordinator_ZeroRowAdvanceBetweenTables(t *testing.T) {
	tableA := replication.TableID{Schema: "public", Table: "a"}
	tableB := replication.TableID{Schema: "public", Table: "b"}

	const chunkSize = 2

	rowsA := []replication.Row{rowFor(tableA, 1), rowFor(tableA, 2)}
	rowsB := []replication.Row{rowFor(tableB, 5), rowFor(tableB, 6)}

	mock := newScriptedMockDeps(map[string]*mockTable{
		tableA.String(): {pkCols: []string{"id"}, rows: rowsA, maxPK: replication.PrimaryKey{2}},
		tableB.String(): {pkCols: []string{"id"}, rows: rowsB, maxPK: replication.PrimaryKey{6}},
	})

	// A's only chunk is exactly chunkSize (2 rows) so it is NOT short - the
	// coordinator must issue a genuine zero-row follow-up fetch for A before
	// advancing to B. That's 4 watermark pairs total: A full chunk, A empty
	// chunk, B full chunk, B empty chunk (which finally sets done=true).
	mock.pushWatermark(Watermark{Xmin: 10, Xmax: 10})
	mock.pushWatermark(Watermark{Xmin: 11, Xmax: 11})
	mock.pushWatermark(Watermark{Xmin: 12, Xmax: 12})
	mock.pushWatermark(Watermark{Xmin: 13, Xmax: 13})
	mock.pushWatermark(Watermark{Xmin: 14, Xmax: 14})
	mock.pushWatermark(Watermark{Xmin: 15, Xmax: 15})
	mock.pushWatermark(Watermark{Xmin: 16, Xmax: 16})
	mock.pushWatermark(Watermark{Xmin: 17, Xmax: 17})

	cfg := replication.Config{
		Tables:    []replication.TableID{tableA, tableB},
		ChunkSize: chunkSize,
		Deps:      mock.deps(chunkSize),
	}

	coord, err := NewCoordinator(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(context.Background()))

	require.NotNil(t, coord.current)
	assert.Equal(t, tableA, *coord.current)
	assert.Equal(t, 2, coord.window.Len())

	// Close the window for A's full chunk; this should trigger the
	// zero-row follow-up fetch for A, advancing straight to B.
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

	// Close the window for B's full chunk; triggers B's zero-row follow-up,
	// which exhausts the table queue and marks done.
	_, changed, err = coord.OnCommit(context.Background(), 14)
	require.NoError(t, err)
	assert.False(t, changed)

	emitted, changed, err = coord.OnCommit(context.Background(), 16)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Len(t, emitted, 2)

	assert.True(t, coord.Done())
}

func TestCoordinator_OnCommitNoopsWhenDone(t *testing.T) {
	cfg := replication.Config{
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

	assert.False(t, coord.OnStreamedRow(replication.TableID{Schema: "public", Table: "a"}, replication.PrimaryKey{1}))
}

func TestCoordinator_ResumeAlwaysDerivesFreshWatermark(t *testing.T) {
	tableA := replication.TableID{Schema: "public", Table: "a"}
	tableB := replication.TableID{Schema: "public", Table: "b"}

	const chunkSize = 2

	rowsA := []replication.Row{rowFor(tableA, 1), rowFor(tableA, 2)}
	rowsB := []replication.Row{rowFor(tableB, 5)}

	mock := newScriptedMockDeps(map[string]*mockTable{
		tableA.String(): {pkCols: []string{"id"}, rows: rowsA, maxPK: replication.PrimaryKey{2}},
		tableB.String(): {pkCols: []string{"id"}, rows: rowsB, maxPK: replication.PrimaryKey{5}},
	})
	mock.pushWatermark(Watermark{Xmin: 1, Xmax: 1})
	mock.pushWatermark(Watermark{Xmin: 2, Xmax: 2})

	cfg := replication.Config{
		Tables:    []replication.TableID{tableA, tableB},
		ChunkSize: chunkSize,
		Deps:      mock.deps(chunkSize),
	}

	coord, err := NewCoordinator(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(context.Background()))

	callsBeforeResume := mock.watermarkCalls
	require.Positive(t, callsBeforeResume)

	// Take a checkpoint immediately after Start, before anything has been
	// flushed: State() must report the pre-fetch baseline (both tables still
	// fully pending), not tableA's just-fetched-but-unflushed first chunk --
	// otherwise a crash right here would resume past that chunk and skip it.
	state := coord.State()
	require.False(t, state.Done)
	assert.Nil(t, state.CurrentTable)
	assert.Equal(t, []replication.TableID{tableA, tableB}, state.RemainingTables)

	// New watermarks for the "restart".
	mock.pushWatermark(Watermark{Xmin: 3, Xmax: 3})
	mock.pushWatermark(Watermark{Xmin: 4, Xmax: 4})

	resumed, err := NewCoordinator(cfg, state)
	require.NoError(t, err)
	require.NoError(t, resumed.Start(context.Background()))

	// A fresh watermark call must have happened on resume: the call count
	// must have increased, proving the coordinator never reused a
	// persisted watermark (State carries none) and always re-derives one.
	assert.Greater(t, mock.watermarkCalls, callsBeforeResume)
	assert.Positive(t, mock.forceFreshCalls)
}

// sortedRowsAfter models a "WHERE pk > lower ORDER BY pk LIMIT limit" query
// against a static, PK-sorted row set, purely as a function of (lower,
// limit) -- unlike scriptedMockDeps' cursor-based FetchChunk, this makes
// re-querying the same lower bound idempotent, which is essential for
// testing that a resumed Coordinator correctly refetches (rather than
// permanently skips) a chunk that was fetched but never flushed before a
// simulated crash. Assumes single-column int primary keys, which is all this
// test needs.
func sortedRowsAfter(rows []replication.Row, lower replication.PrimaryKey, limit int) []replication.Row {
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

// TestCoordinator_ResumeRefetchesUnflushedChunk is a regression test for a
// bug where State() reported the boundary of a chunk that had been fetched
// (and so was about to be skipped on resume) but never flushed to any
// caller, permanently losing that chunk's rows after a crash/restart. It
// pins down the fix: State() must only ever advance once a chunk has
// actually been flushed.
func TestCoordinator_ResumeRefetchesUnflushedChunk(t *testing.T) {
	tableA := replication.TableID{Schema: "public", Table: "a"}
	const chunkSize = 2
	rowsA := []replication.Row{rowFor(tableA, 1), rowFor(tableA, 2), rowFor(tableA, 3), rowFor(tableA, 4)}
	maxPK := replication.PrimaryKey{4}

	watermarks := []Watermark{
		{Xmin: 1, Xmax: 1}, {Xmin: 2, Xmax: 2}, // chunk 1 ([1,2]): low, high
		{Xmin: 3, Xmax: 3}, {Xmin: 4, Xmax: 4}, // chunk 2 ([3,4]): low, high
	}
	var watermarkCalls int
	var fetchLog []string

	deps := replication.Deps{
		ResolvePrimaryKey: func(context.Context, replication.TableID) ([]string, error) { return []string{"id"}, nil },
		ResolveMaxKey: func(context.Context, replication.TableID, []string, string) (replication.PrimaryKey, error) {
			return maxPK, nil
		},
		ForceFreshTransaction: func(context.Context) error { return nil },
		ResolveWatermark: func(context.Context) (any, error) {
			idx := min(watermarkCalls, len(watermarks)-1)
			watermarkCalls++
			return watermarks[idx], nil
		},
		FetchChunk: func(_ context.Context, _ replication.TableID, _ []string, _ string, args []any) ([]replication.Row, error) {
			var lower replication.PrimaryKey
			if len(args) > 1 {
				lower = replication.PrimaryKey{args[0]}
			}
			fetchLog = append(fetchLog, fmt.Sprint(lower))
			return sortedRowsAfter(rowsA, lower, chunkSize), nil
		},
	}

	cfg := replication.Config{Tables: []replication.TableID{tableA}, ChunkSize: chunkSize, Deps: deps}

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
	assert.Equal(t, replication.PrimaryKey{1}, emitted[0].PK)
	assert.Equal(t, replication.PrimaryKey{2}, emitted[1].PK)

	// "Crash" here: chunk 2 ([3,4]) has been fetched into the window but
	// never flushed. The checkpoint must reflect only chunk 1.
	state := coord.State()
	require.False(t, state.Done)
	require.NotNil(t, state.CurrentTable)
	assert.Equal(t, tableA, *state.CurrentTable)
	assert.Equal(t, replication.PrimaryKey{2}, state.LastSentPK)
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
	assert.Equal(t, replication.PrimaryKey{3}, emitted[0].PK)
	assert.Equal(t, replication.PrimaryKey{4}, emitted[1].PK)
}

func TestCoordinator_ConfigValidation(t *testing.T) {
	validDeps := newScriptedMockDeps(map[string]*mockTable{}).deps(1)

	t.Run("zero chunk size", func(t *testing.T) {
		_, err := NewCoordinator(replication.Config{ChunkSize: 0, Deps: validDeps}, nil)
		require.Error(t, err)
	})

	t.Run("negative chunk size", func(t *testing.T) {
		_, err := NewCoordinator(replication.Config{ChunkSize: -1, Deps: validDeps}, nil)
		require.Error(t, err)
	})

	t.Run("missing dep", func(t *testing.T) {
		deps := validDeps
		deps.FetchChunk = nil
		_, err := NewCoordinator(replication.Config{ChunkSize: 1, Deps: deps}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "FetchChunk")
	})
}
