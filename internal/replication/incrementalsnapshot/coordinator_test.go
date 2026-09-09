// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testWatermark is a minimal Watermark[uint64] for exercising the
// coordinator: an xmin/xmax pair over integer transaction ids, mirroring the
// shape real databases report without depending on any of them.
type testWatermark struct {
	Xmin uint64
	Xmax uint64
}

func (w testWatermark) OpensAt(pos uint64) bool  { return pos >= w.Xmin }
func (w testWatermark) ClosesAt(pos uint64) bool { return pos > w.Xmax }
func (w testWatermark) Quiesced() bool           { return w.Xmin == w.Xmax }

// testConfig saves repeating the coordinator's type arguments at every
// construction site.
type testConfig = CoordinatorConfig[uint64, testWatermark]

// collect keeps the rows that one call releases. It lets the older tests
// use the EmitFunc parameter.
func collect(rows *[][]Row) EmitFunc {
	return func(r []Row) error {
		*rows = append(*rows, r)
		return nil
	}
}

// onCommit calls OnCommit and joins the released chunks. The tests that are
// older than the drain then still read one chunk for each commit.
func onCommit[W Watermark[uint64]](t *testing.T, c *Coordinator[uint64, W], pos uint64) (emitted []Row, changed bool, err error) {
	t.Helper()
	var chunks [][]Row
	changed, err = c.OnCommit(t.Context(), pos, collect(&chunks))
	for _, chunk := range chunks {
		emitted = append(emitted, chunk...)
	}
	return emitted, changed, err
}

func TestCoordinatorFullScenario(t *testing.T) {
	tableA := TableID{Schema: "public", Table: "a"}
	tableB := TableID{Schema: "public", Table: "b"}

	const chunkSize = 3

	// A: one full chunk, then an empty follow-up (zero-row advance path).
	rowsA := []Row{rowFor(tableA, 1), rowFor(tableA, 2), rowFor(tableA, 3)}
	// B: a short chunk (short-chunk advance path).
	rowsB := []Row{rowFor(tableB, 10), rowFor(tableB, 11)}

	mock := newScriptedMockDeps(map[string]*mockTable{
		tableA.String(): {pkCols: []string{"id"}, rows: rowsA, maxPK: PrimaryKey{3}},
		tableB.String(): {pkCols: []string{"id"}, rows: rowsB, maxPK: PrimaryKey{11}},
	}, chunkSize)

	// low/high pairs: A chunk1, A chunk2 (empty, triggers advance), B chunk1.
	mock.pushWatermark(testWatermark{Xmin: 100, Xmax: 100}) // low, A chunk 1
	mock.pushWatermark(testWatermark{Xmin: 105, Xmax: 105}) // high, A chunk 1
	mock.pushWatermark(testWatermark{Xmin: 110, Xmax: 110}) // low, A chunk 2 (empty)
	mock.pushWatermark(testWatermark{Xmin: 112, Xmax: 112}) // high, A chunk 2 (empty)
	mock.pushWatermark(testWatermark{Xmin: 120, Xmax: 120}) // low, B chunk 1
	mock.pushWatermark(testWatermark{Xmin: 125, Xmax: 125}) // high, B chunk 1

	cfg := testConfig{
		Tables:    []TableID{tableA, tableB},
		ChunkSize: chunkSize,
		Deps:      mock,
	}

	coord, err := NewCoordinator(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(t.Context()))

	require.False(t, coord.Done())
	require.NotNil(t, coord.current)
	assert.Equal(t, tableA, *coord.current)
	assert.Equal(t, PrimaryKey{3}, coord.lastSentPK)
	assert.Equal(t, 3, coord.window.Len())

	// Streamed row PK=2 arrives while still buffered.
	removed := coord.OnStreamedRow(tableA, PrimaryKey{2})
	assert.True(t, removed)

	// Different table: must not touch the window.
	removedOther := coord.OnStreamedRow(tableB, PrimaryKey{999})
	assert.False(t, removedOther)

	// txid below low.Xmin: window must not open yet.
	emitted, changed, err := onCommit(t, coord, 50)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Nil(t, emitted)

	// A position that is smaller than low.Xmin does nothing, at any value.
	// The coordinator has no special test for a zero position and asks the
	// watermark. The caller removes unknown positions. Refer to OnCommit.
	emitted, changed, err = onCommit(t, coord, 0)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Nil(t, emitted)

	// txid==low.Xmin: opens, but doesn't close (threshold=105).
	emitted, changed, err = onCommit(t, coord, 100)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Nil(t, emitted)

	// txid at the threshold exactly: still not closed (<=).
	emitted, changed, err = onCommit(t, coord, 105)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Nil(t, emitted)

	// txid > threshold: closes, flushes, advances to B via A's empty follow-up.
	emitted, changed, err = onCommit(t, coord, 106)
	require.NoError(t, err)
	assert.True(t, changed)
	require.Len(t, emitted, 2)
	assert.Equal(t, PrimaryKey{1}, emitted[0].PK)
	assert.Equal(t, PrimaryKey{3}, emitted[1].PK)

	// The chunk of table B was not full, so table B is complete. current
	// stays at table B until the next plan, and OnStreamedRow can therefore
	// still remove the buffered rows.
	assert.True(t, coord.currentExhausted)
	require.NotNil(t, coord.current)
	assert.Equal(t, tableB, *coord.current)
	assert.Equal(t, PrimaryKey{11}, coord.lastSentPK)
	assert.Equal(t, 2, coord.window.Len())

	// B's open/close cycle (low=120, high=125).
	_, changed, err = onCommit(t, coord, 119)
	require.NoError(t, err)
	assert.False(t, changed)

	_, changed, err = onCommit(t, coord, 120)
	require.NoError(t, err)
	assert.False(t, changed)

	emitted, changed, err = onCommit(t, coord, 126)
	require.NoError(t, err)
	assert.True(t, changed)
	require.Len(t, emitted, 2)
	assert.Equal(t, PrimaryKey{10}, emitted[0].PK)
	assert.Equal(t, PrimaryKey{11}, emitted[1].PK)

	// B's short chunk skipped the zero-row round-trip straight to done.
	assert.True(t, coord.Done())
}

func TestCoordinatorZeroRowAdvanceBetweenTables(t *testing.T) {
	tableA := TableID{Schema: "public", Table: "a"}
	tableB := TableID{Schema: "public", Table: "b"}

	const chunkSize = 2

	rowsA := []Row{rowFor(tableA, 1), rowFor(tableA, 2)}
	rowsB := []Row{rowFor(tableB, 5), rowFor(tableB, 6)}

	mock := newScriptedMockDeps(map[string]*mockTable{
		tableA.String(): {pkCols: []string{"id"}, rows: rowsA, maxPK: PrimaryKey{2}},
		tableB.String(): {pkCols: []string{"id"}, rows: rowsB, maxPK: PrimaryKey{6}},
	}, chunkSize)

	// A's chunk isn't short, so a real zero-row fetch precedes advancing to
	// B: 4 pairs (A full, A empty, B full, B empty->done).
	mock.pushWatermark(testWatermark{Xmin: 10, Xmax: 10})
	mock.pushWatermark(testWatermark{Xmin: 11, Xmax: 11})
	mock.pushWatermark(testWatermark{Xmin: 12, Xmax: 12})
	mock.pushWatermark(testWatermark{Xmin: 13, Xmax: 13})
	mock.pushWatermark(testWatermark{Xmin: 14, Xmax: 14})
	mock.pushWatermark(testWatermark{Xmin: 15, Xmax: 15})
	mock.pushWatermark(testWatermark{Xmin: 16, Xmax: 16})
	mock.pushWatermark(testWatermark{Xmin: 17, Xmax: 17})

	cfg := testConfig{
		Tables:    []TableID{tableA, tableB},
		ChunkSize: chunkSize,
		Deps:      mock,
	}

	coord, err := NewCoordinator(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(t.Context()))

	require.NotNil(t, coord.current)
	assert.Equal(t, tableA, *coord.current)
	assert.Equal(t, 2, coord.window.Len())

	// Closes A's window; triggers A's zero-row fetch, advancing to B.
	_, changed, err := onCommit(t, coord, 10)
	require.NoError(t, err)
	assert.False(t, changed) // window opened, not yet closed

	emitted, changed, err := onCommit(t, coord, 12)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Len(t, emitted, 2)

	require.NotNil(t, coord.current)
	assert.Equal(t, tableB, *coord.current)
	assert.Equal(t, 2, coord.window.Len())

	// Closes B's window; B's zero-row follow-up exhausts the queue.
	_, changed, err = onCommit(t, coord, 14)
	require.NoError(t, err)
	assert.False(t, changed)

	emitted, changed, err = onCommit(t, coord, 16)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Len(t, emitted, 2)

	assert.True(t, coord.Done())
}

func TestCoordinatorOnCommitNoopsWhenDone(t *testing.T) {
	cfg := testConfig{
		Tables:    nil,
		ChunkSize: 10,
		Deps:      newScriptedMockDeps(map[string]*mockTable{}, 10),
	}

	coord, err := NewCoordinator(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(t.Context()))
	require.True(t, coord.Done())

	emitted, changed, err := onCommit(t, coord, 100)
	require.NoError(t, err)
	assert.True(t, changed, "the Done checkpoint must reach the caller")
	assert.Empty(t, emitted, "it carries state only, no rows")

	emitted, changed, err = onCommit(t, coord, 101)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Nil(t, emitted)

	assert.False(t, coord.OnStreamedRow(TableID{Schema: "public", Table: "a"}, PrimaryKey{1}))
}

// TestCoordinatorSkipsEmptyTable: a table with no rows (ResolveMaxKey
// returns a nil PrimaryKey with a nil error) must be skipped as
// already-backfilled rather than failing the coordinator outright.
func TestCoordinatorSkipsEmptyTable(t *testing.T) {
	tableEmpty := TableID{Schema: "public", Table: "empty"}
	tableB := TableID{Schema: "public", Table: "b"}

	const chunkSize = 2
	rowsB := []Row{rowFor(tableB, 1), rowFor(tableB, 2)}

	mock := newScriptedMockDeps(map[string]*mockTable{
		tableEmpty.String(): {pkCols: []string{"id"}, rows: nil, maxPK: nil},
		tableB.String():     {pkCols: []string{"id"}, rows: rowsB, maxPK: PrimaryKey{2}},
	}, chunkSize)
	mock.pushWatermark(testWatermark{Xmin: 1, Xmax: 1})
	mock.pushWatermark(testWatermark{Xmin: 2, Xmax: 2})

	cfg := testConfig{
		Tables:    []TableID{tableEmpty, tableB},
		ChunkSize: chunkSize,
		Deps:      mock,
	}

	coord, err := NewCoordinator(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(t.Context()))

	// The empty table must be skipped entirely, straight on to table B,
	// without erroring or wasting a watermark/fetch round trip on it.
	require.False(t, coord.Done())
	require.NotNil(t, coord.current)
	assert.Equal(t, tableB, *coord.current)
	assert.Equal(t, 2, coord.window.Len())

	emitted, changed, err := onCommit(t, coord, 3)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Len(t, emitted, 2)
	assert.True(t, coord.Done())
}

// TestCoordinatorAllTablesEmpty: every configured table having no rows must
// mark the coordinator done immediately, not fail the whole incremental
// snapshot (and therefore the replication stream it runs alongside).
func TestCoordinatorAllTablesEmpty(t *testing.T) {
	tableA := TableID{Schema: "public", Table: "a"}
	tableB := TableID{Schema: "public", Table: "b"}

	mock := newScriptedMockDeps(map[string]*mockTable{
		tableA.String(): {pkCols: []string{"id"}, rows: nil, maxPK: nil},
		tableB.String(): {pkCols: []string{"id"}, rows: nil, maxPK: nil},
	}, 10)

	cfg := testConfig{
		Tables:    []TableID{tableA, tableB},
		ChunkSize: 10,
		Deps:      mock,
	}

	coord, err := NewCoordinator(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(t.Context()))
	assert.True(t, coord.Done())
}

func TestCoordinatorResumeAlwaysDerivesFreshWatermark(t *testing.T) {
	tableA := TableID{Schema: "public", Table: "a"}
	tableB := TableID{Schema: "public", Table: "b"}

	const chunkSize = 2

	rowsA := []Row{rowFor(tableA, 1), rowFor(tableA, 2)}
	rowsB := []Row{rowFor(tableB, 5)}

	mock := newScriptedMockDeps(map[string]*mockTable{
		tableA.String(): {pkCols: []string{"id"}, rows: rowsA, maxPK: PrimaryKey{2}},
		tableB.String(): {pkCols: []string{"id"}, rows: rowsB, maxPK: PrimaryKey{5}},
	}, chunkSize)
	mock.pushWatermark(testWatermark{Xmin: 1, Xmax: 1})
	mock.pushWatermark(testWatermark{Xmin: 2, Xmax: 2})

	cfg := testConfig{
		Tables:    []TableID{tableA, tableB},
		ChunkSize: chunkSize,
		Deps:      mock,
	}

	coord, err := NewCoordinator(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(t.Context()))

	callsBeforeResume := mock.watermarkCalls
	require.Positive(t, callsBeforeResume)

	// Pre-flush checkpoint must report the baseline, not tableA's unflushed
	// first chunk.
	state := coord.State()
	require.False(t, state.Done)
	assert.Nil(t, state.CurrentTable)
	assert.Equal(t, []TableID{tableA, tableB}, state.RemainingTables)

	// New watermarks for the "restart".
	mock.pushWatermark(testWatermark{Xmin: 3, Xmax: 3})
	mock.pushWatermark(testWatermark{Xmin: 4, Xmax: 4})

	resumed, err := NewCoordinator(cfg, state)
	require.NoError(t, err)
	require.NoError(t, resumed.Start(t.Context()))

	// Call count must increase: watermarks are never persisted, only re-derived.
	assert.Greater(t, mock.watermarkCalls, callsBeforeResume)
	assert.Positive(t, mock.forceFreshCalls)
}

// TestCoordinatorResumeRefetchesUnflushedChunk: State() must only advance
// once flushed, or a resumed coordinator skips unflushed rows.
func TestCoordinatorResumeRefetchesUnflushedChunk(t *testing.T) {
	tableA := TableID{Schema: "public", Table: "a"}
	const chunkSize = 2
	rowsA := []Row{rowFor(tableA, 1), rowFor(tableA, 2), rowFor(tableA, 3), rowFor(tableA, 4)}

	deps := &refetchMockDeps{
		rows:      rowsA,
		maxPK:     PrimaryKey{4},
		chunkSize: chunkSize,
		watermarks: []testWatermark{
			{Xmin: 1, Xmax: 1}, {Xmin: 2, Xmax: 2}, // chunk 1 ([1,2]): low, high
			{Xmin: 3, Xmax: 3}, {Xmin: 4, Xmax: 4}, // chunk 2 ([3,4]): low, high
		},
	}

	cfg := testConfig{Tables: []TableID{tableA}, ChunkSize: chunkSize, Deps: deps}

	coord, err := NewCoordinator(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(t.Context())) // fetches chunk 1 ([1,2])

	_, changed, err := onCommit(t, coord, 1) // opens, doesn't close (1 <= closeThreshold 2)
	require.NoError(t, err)
	assert.False(t, changed)

	emitted, changed, err := onCommit(t, coord, 3) // closes: flushes chunk 1, fetches chunk 2
	require.NoError(t, err)
	require.True(t, changed)
	require.Len(t, emitted, 2)
	assert.Equal(t, PrimaryKey{1}, emitted[0].PK)
	assert.Equal(t, PrimaryKey{2}, emitted[1].PK)

	// Chunk 2 fetched but not flushed; checkpoint must reflect only chunk 1.
	state := coord.State()
	require.False(t, state.Done)
	require.NotNil(t, state.CurrentTable)
	assert.Equal(t, tableA, *state.CurrentTable)
	assert.Equal(t, PrimaryKey{2}, state.LastSentPK)
	require.Len(t, deps.fetchLog, 2, "sanity: exactly one FetchChunk call for chunk 1's fetch so far")

	resumed, err := NewCoordinator(cfg, state)
	require.NoError(t, err)
	require.NoError(t, resumed.Start(t.Context())) // must refetch chunk 2, not skip it

	require.Len(t, deps.fetchLog, 3)
	assert.Equal(t, deps.fetchLog[1], deps.fetchLog[2], "resumed coordinator must request the same lower bound as the original chunk 2 fetch")

	emitted, changed, err = onCommit(t, resumed, 5) // opens and closes in one call (5 > closeThreshold 4)
	require.NoError(t, err)
	require.True(t, changed)
	require.Len(t, emitted, 2, "chunk 2's rows must still be emitted exactly once, on the resumed coordinator")
	assert.Equal(t, PrimaryKey{3}, emitted[0].PK)
	assert.Equal(t, PrimaryKey{4}, emitted[1].PK)
}

func TestCoordinatorConfigValidation(t *testing.T) {
	validDeps := newScriptedMockDeps(map[string]*mockTable{}, 1)

	t.Run("zero chunk size", func(t *testing.T) {
		_, err := NewCoordinator(testConfig{ChunkSize: 0, Deps: validDeps}, nil)
		require.Error(t, err)
	})

	t.Run("negative chunk size", func(t *testing.T) {
		_, err := NewCoordinator(testConfig{ChunkSize: -1, Deps: validDeps}, nil)
		require.Error(t, err)
	})

	t.Run("nil deps", func(t *testing.T) {
		_, err := NewCoordinator(testConfig{ChunkSize: 1, Deps: nil}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Deps")
	})
}

// mockTable is a fixture of fake rows used to script FetchChunk/ResolveMaxKey
// without a real database.
type mockTable struct {
	pkCols []string
	rows   []Row // full ordered set of rows in the table, PK-ascending
	maxPK  PrimaryKey
}

// scriptedMockDeps implements Deps, serving chunk data
// from an in-memory fixture and scripted watermarks, tracking a per-table
// cursor instead of parsing SQL/args.
type scriptedMockDeps struct {
	tables    map[string]*mockTable
	cursor    map[string]int
	chunkSize int

	watermarks      []testWatermark
	watermarkCalls  int
	forceFreshCalls int
}

func newScriptedMockDeps(tables map[string]*mockTable, chunkSize int) *scriptedMockDeps {
	return &scriptedMockDeps{
		tables:    tables,
		cursor:    make(map[string]int),
		chunkSize: chunkSize,
	}
}

func (m *scriptedMockDeps) pushWatermark(wm testWatermark) {
	m.watermarks = append(m.watermarks, wm)
}

func (m *scriptedMockDeps) ResolvePrimaryKey(_ context.Context, table TableID) ([]string, error) {
	return m.tables[table.String()].pkCols, nil
}

func (m *scriptedMockDeps) ResolveMaxKey(_ context.Context, table TableID, _ []string) (PrimaryKey, error) {
	return m.tables[table.String()].maxPK, nil
}

func (m *scriptedMockDeps) ResolveWatermark(context.Context) (testWatermark, error) {
	idx := m.watermarkCalls
	if idx >= len(m.watermarks) {
		idx = len(m.watermarks) - 1
	}
	m.watermarkCalls++
	return m.watermarks[idx], nil
}

func (m *scriptedMockDeps) ForceFreshTransaction(context.Context) error {
	m.forceFreshCalls++
	return nil
}

func (m *scriptedMockDeps) FetchChunk(_ context.Context, table TableID, _ []string, _, _ PrimaryKey, _ int) ([]Row, error) {
	key := table.String()
	mt := m.tables[key]
	start := m.cursor[key]
	if start >= len(mt.rows) {
		return nil, nil
	}
	end := min(start+m.chunkSize, len(mt.rows))
	chunk := mt.rows[start:end]
	m.cursor[key] = end
	return chunk, nil
}

func rowFor(table TableID, pk int) Row {
	return Row{
		Table: table,
		PK:    PrimaryKey{pk},
		Data:  map[string]any{"id": pk},
	}
}

// refetchMockDeps is a purpose-built Deps used only by
// TestCoordinatorResumeRefetchesUnflushedChunk: it logs every FetchChunk
// call's lower bound and serves a static, PK-sorted row set, so re-querying
// the same lower bound is idempotent -- proving a resumed Coordinator
// refetches rather than skips a chunk. Assumes single-column int keys.
type refetchMockDeps struct {
	rows      []Row
	maxPK     PrimaryKey
	chunkSize int

	watermarks     []testWatermark
	watermarkCalls int
	fetchLog       []string
}

func (*refetchMockDeps) ResolvePrimaryKey(context.Context, TableID) ([]string, error) {
	return []string{"id"}, nil
}

func (d *refetchMockDeps) ResolveMaxKey(context.Context, TableID, []string) (PrimaryKey, error) {
	return d.maxPK, nil
}

func (*refetchMockDeps) ForceFreshTransaction(context.Context) error {
	return nil
}

func (d *refetchMockDeps) ResolveWatermark(context.Context) (testWatermark, error) {
	idx := min(d.watermarkCalls, len(d.watermarks)-1)
	d.watermarkCalls++
	return d.watermarks[idx], nil
}

func (d *refetchMockDeps) FetchChunk(_ context.Context, _ TableID, _ []string, lower, _ PrimaryKey, _ int) ([]Row, error) {
	d.fetchLog = append(d.fetchLog, fmt.Sprint(lower))
	return sortedRowsAfter(d.rows, lower, d.chunkSize), nil
}

// sortedRowsAfter is a pure "WHERE pk > lower LIMIT limit" query over a
// static, PK-sorted row set. Assumes single-column int primary keys.
func sortedRowsAfter(rows []Row, lower PrimaryKey, limit int) []Row {
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

// TestCoordinatorDedupsBufferedFinalChunk tests the last chunk of a table.
// That chunk is not full, but its rows are in the buffer like all other
// rows. Therefore a change that streams in before the window closes must
// remove its row from the buffer.
//
// This test is a regression test. The coordinator set current to nil as soon
// as it buffered a chunk that was not full. OnStreamedRow then did nothing,
// and the stream emitted the old snapshot row after the new change.
func TestCoordinatorDedupsBufferedFinalChunk(t *testing.T) {
	table := TableID{Schema: "public", Table: "a"}

	// chunkSize 4 against 2 rows: the table's only chunk is a short one.
	const chunkSize = 4
	mock := newScriptedMockDeps(map[string]*mockTable{
		table.String(): {
			pkCols: []string{"id"},
			rows:   []Row{rowFor(table, 1), rowFor(table, 2)},
			maxPK:  PrimaryKey{2},
		},
	}, chunkSize)
	mock.pushWatermark(testWatermark{Xmin: 100, Xmax: 100}) // low
	mock.pushWatermark(testWatermark{Xmin: 105, Xmax: 105}) // high

	coord, err := NewCoordinator(testConfig{
		Tables:    []TableID{table},
		ChunkSize: chunkSize,
		Deps:      mock,
	}, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(t.Context()))

	require.Equal(t, 2, coord.window.Len())

	// An update of row 1 commits while the chunk is still in the buffer.
	// The pipeline has already sent that update. The update must remove the
	// old row from the buffer. The old row must not replace the update at
	// the flush.
	require.True(t, coord.OnStreamedRow(table, PrimaryKey{1}))
	assert.Equal(t, 1, coord.window.Len())

	// A row of another table must not change the buffer.
	other := TableID{Schema: "public", Table: "other"}
	assert.False(t, coord.OnStreamedRow(other, PrimaryKey{1}))

	// The window closes. The coordinator emits the other row only. No table
	// remains, so the snapshot is complete.
	emitted, changed, err := onCommit(t, coord, 106)
	require.NoError(t, err)
	require.True(t, changed)
	require.Len(t, emitted, 1)
	assert.Equal(t, PrimaryKey{2}, emitted[0].PK)
	assert.True(t, coord.Done())
}

// newDrainCoordinator makes a coordinator for one table of six rows in three
// equal chunks. Each watermark is the same and is quiesced. The test
// database is therefore completely quiet.
func newDrainCoordinator(t *testing.T, maxDrain int) (*Coordinator[uint64, testWatermark], TableID) {
	t.Helper()
	table := TableID{Schema: "public", Table: "a"}

	const chunkSize = 2
	rows := []Row{
		rowFor(table, 1), rowFor(table, 2),
		rowFor(table, 3), rowFor(table, 4),
		rowFor(table, 5), rowFor(table, 6),
	}
	mock := newScriptedMockDeps(map[string]*mockTable{
		table.String(): {pkCols: []string{"id"}, rows: rows, maxPK: PrimaryKey{6}},
	}, chunkSize)
	// One watermark is enough. The test double repeats its last watermark,
	// so each chunk gets the same quiesced pair.
	mock.pushWatermark(testWatermark{Xmin: 100, Xmax: 100})

	coord, err := NewCoordinator(testConfig{
		Tables:         []TableID{table},
		ChunkSize:      chunkSize,
		Deps:           mock,
		MaxDrainChunks: maxDrain,
	}, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(t.Context()))
	return coord, table
}

func TestCoordinatorDrainsQuietDatabaseInOneCommit(t *testing.T) {
	// A chunk that the coordinator reads while no transaction is in flight
	// needs no row removal. If the coordinator keeps that chunk, it must
	// wait for the next commit. On a quiet table that commit comes only with
	// the next heartbeat. Therefore all three chunks must come out on this
	// one commit.
	coord, _ := newDrainCoordinator(t, DefaultMaxDrainChunks)

	var chunks [][]Row
	changed, err := coord.OnCommit(t.Context(), 101, collect(&chunks))
	require.NoError(t, err)
	require.True(t, changed)

	require.Len(t, chunks, 4, "every chunk should drain on one commit")
	assert.Equal(t, PrimaryKey{1}, chunks[0][0].PK)
	assert.Equal(t, PrimaryKey{3}, chunks[1][0].PK)
	assert.Equal(t, PrimaryKey{5}, chunks[2][0].PK)
	assert.Empty(t, chunks[3])
	assert.True(t, coord.Done())
}

func TestCoordinatorDrainRespectsMaxDrainChunks(t *testing.T) {
	// The coordinator emits on the replication loop of the caller.
	// Therefore the drain must return control and must not read the full
	// table.
	coord, _ := newDrainCoordinator(t, 1)

	var chunks [][]Row
	changed, err := coord.OnCommit(t.Context(), 101, collect(&chunks))
	require.NoError(t, err)
	require.True(t, changed)

	// The chunk of the window, and one more chunk from the drain.
	require.Len(t, chunks, 2)
	assert.False(t, coord.Done(), "a chunk stays buffered for the next commit")
}

func TestCoordinatorDrainStopsOnConcurrentActivity(t *testing.T) {
	table := TableID{Schema: "public", Table: "a"}

	const chunkSize = 2
	mock := newScriptedMockDeps(map[string]*mockTable{
		table.String(): {
			pkCols: []string{"id"},
			rows:   []Row{rowFor(table, 1), rowFor(table, 2), rowFor(table, 3), rowFor(table, 4)},
			maxPK:  PrimaryKey{4},
		},
	}, chunkSize)
	// Chunk 1 gets two equal watermarks. Chunk 2 gets two different
	// watermarks, because a transaction started during that read. A later
	// row can replace a row of chunk 2, so the coordinator must keep it.
	mock.pushWatermark(testWatermark{Xmin: 100, Xmax: 100}) // low, chunk 1
	mock.pushWatermark(testWatermark{Xmin: 100, Xmax: 100}) // high, chunk 1
	mock.pushWatermark(testWatermark{Xmin: 100, Xmax: 100}) // low, chunk 2
	mock.pushWatermark(testWatermark{Xmin: 102, Xmax: 102}) // high, chunk 2

	coord, err := NewCoordinator(testConfig{
		Tables:    []TableID{table},
		ChunkSize: chunkSize,
		Deps:      mock,
	}, nil)
	require.NoError(t, err)
	require.NoError(t, coord.Start(t.Context()))

	var chunks [][]Row
	changed, err := coord.OnCommit(t.Context(), 101, collect(&chunks))
	require.NoError(t, err)
	require.True(t, changed)

	require.Len(t, chunks, 1, "the disturbed chunk must stay buffered for dedup")
	assert.Equal(t, PrimaryKey{1}, chunks[0][0].PK)
	assert.Equal(t, 2, coord.window.Len())
}

func TestCoordinatorDrainPropagatesEmitError(t *testing.T) {
	// At the call site, emit sends to a channel. Its error is therefore how
	// a stopped stream ends a drain.
	coord, _ := newDrainCoordinator(t, DefaultMaxDrainChunks)

	wantErr := errors.New("downstream gone")
	calls := 0
	_, err := coord.OnCommit(t.Context(), 101, func([]Row) error {
		calls++
		if calls == 2 {
			return wantErr
		}
		return nil
	})
	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, 2, calls, "the drain must stop at the failing emit")
}

func TestCoordinatorForcesFreshTransactionOnceOnly(t *testing.T) {
	// A call for each watermark uses two transaction ids for each chunk. It
	// also makes the two watermarks of each read different, and this stops
	// the drain.
	coord, _ := newDrainCoordinator(t, DefaultMaxDrainChunks)

	_, err := coord.OnCommit(t.Context(), 101, func([]Row) error { return nil })
	require.NoError(t, err)

	deps := coord.cfg.Deps.(*scriptedMockDeps)
	assert.Equal(t, 1, deps.forceFreshCalls, "only Start should force a transaction id")
}

func TestCoordinatorEmitsTerminalStateAfterResume(t *testing.T) {
	table := TableID{Schema: "public", Table: "a"}

	const chunkSize = 2
	mock := newScriptedMockDeps(map[string]*mockTable{
		table.String(): {pkCols: []string{"id"}, rows: nil, maxPK: PrimaryKey{2}},
	}, chunkSize)
	mock.pushWatermark(testWatermark{Xmin: 100, Xmax: 100})

	// A checkpoint written just before the backfill finished: the table is
	// current, its keys are exhausted, but done is still false.
	resume := &State{
		Version:      CurrentStateVersion,
		CurrentTable: &table,
		LastSentPK:   PrimaryKey{2},
		MaxPK:        PrimaryKey{2},
	}

	coord, err := NewCoordinator(testConfig{
		Tables:    []TableID{table},
		ChunkSize: chunkSize,
		Deps:      mock,
	}, resume)
	require.NoError(t, err)
	require.NoError(t, coord.Start(t.Context()))
	require.True(t, coord.Done(), "the empty chunk read should complete the snapshot")

	var chunks [][]Row
	changed, err := coord.OnCommit(t.Context(), 101, collect(&chunks))
	require.NoError(t, err)
	require.True(t, changed, "the caller must see the completion so it can checkpoint and report it")
	require.Len(t, chunks, 1)
	assert.Empty(t, chunks[0], "the terminal checkpoint carries no rows")
	assert.True(t, coord.State().Done, "the persisted state must now be done")

	// Nothing further: the checkpoint is written once.
	chunks = nil
	changed, err = coord.OnCommit(t.Context(), 102, collect(&chunks))
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Empty(t, chunks)
}
