// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// incTestCSNBytes converts a uint64 into the 8-byte big-endian representation
// that NewCSNFromDBValue will parse back to the same uint64 value.
func incTestCSNBytes(val uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, val)
	return b
}

// ---------------------------------------------------------------------------
// Task 1: IncrementalSnapshotContext
// ---------------------------------------------------------------------------

// jsonRoundTrip mirrors the actual save/load path used by saveIncrementalContext
// and loadIncrementalContext: json.Marshal to save, json.Decoder.UseNumber to
// load (preserving BIGINT PKs as json.Number instead of float64).
func jsonRoundTrip(t *testing.T, src *IncrementalSnapshotContext) *IncrementalSnapshotContext {
	t.Helper()
	b, err := json.Marshal(src)
	require.NoError(t, err)
	var dst IncrementalSnapshotContext
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&dst))
	if dst.LastEmittedPK == nil {
		dst.LastEmittedPK = map[string][]any{}
	}
	if dst.MaxPK == nil {
		dst.MaxPK = map[string][]any{}
	}
	return &dst
}

func TestIncrementalSnapshotContextRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := &IncrementalSnapshotContext{
		Tables:        []IncrementalTable{{Schema: "S", Name: "T", SignalID: "sig1"}},
		LastEmittedPK: map[string][]any{"S.T": {json.Number("42")}},
		MaxPK:         map[string][]any{"S.T": {json.Number("1000")}},
	}
	got := jsonRoundTrip(t, ctx)
	assert.Equal(t, ctx.Tables, got.Tables)
	assert.Equal(t, ctx.LastEmittedPK, got.LastEmittedPK)
	assert.Equal(t, ctx.MaxPK, got.MaxPK)
}

func TestIncrementalSnapshotContextEmpty(t *testing.T) {
	t.Parallel()

	got := jsonRoundTrip(t, &IncrementalSnapshotContext{})
	assert.False(t, got.Running())
	assert.Nil(t, got.CurrentTable())
}

func TestIncrementalSnapshotContextRunning(t *testing.T) {
	t.Parallel()

	ctx := &IncrementalSnapshotContext{
		Tables: []IncrementalTable{
			{Schema: "S", Name: "T1", SignalID: "s1"},
			{Schema: "S", Name: "T2", SignalID: "s2"},
		},
	}
	assert.True(t, ctx.Running())
	assert.Equal(t, &ctx.Tables[0], ctx.CurrentTable())

	ctx.AdvanceTable()
	assert.True(t, ctx.Running())
	assert.Equal(t, "T2", ctx.CurrentTable().Name)

	ctx.AdvanceTable()
	assert.False(t, ctx.Running())
	assert.Nil(t, ctx.CurrentTable())
}

// ---------------------------------------------------------------------------
// Task 2: DeduplicationWindow
// ---------------------------------------------------------------------------

func TestDeduplicationWindowNoOverlap(t *testing.T) {
	t.Parallel()

	w := NewDeduplicationWindow("S", "T", NewCSN(100), NewCSN(200), []string{"ID"})
	w.AddRow(map[string]any{"ID": int64(1), "VAL": "a"})
	w.AddRow(map[string]any{"ID": int64(2), "VAL": "b"})

	events := w.Flush()
	assert.Len(t, events, 2)
	for _, e := range events {
		assert.Equal(t, OpTypeRead, e.Operation)
		assert.Equal(t, "S", e.Schema)
		assert.Equal(t, "T", e.Table)
	}
}

func TestDeduplicationWindowCDCEvicts(t *testing.T) {
	t.Parallel()

	w := NewDeduplicationWindow("S", "T", NewCSN(100), NewCSN(200), []string{"ID"})
	w.AddRow(map[string]any{"ID": int64(1), "VAL": "old"})
	w.AddRow(map[string]any{"ID": int64(2), "VAL": "b"})

	// CDC event for ID=1 within the window evicts the snapshot copy.
	evicted := w.Evict("S", "T", map[string]any{"ID": int64(1), "VAL": "new"}, NewCSN(150))
	assert.True(t, evicted)

	events := w.Flush()
	require.Len(t, events, 1)
	assert.Equal(t, int64(2), events[0].Data["ID"])
}

func TestDeduplicationWindowOutOfRangeCSNNotEvicted(t *testing.T) {
	t.Parallel()

	w := NewDeduplicationWindow("S", "T", NewCSN(100), NewCSN(200), []string{"ID"})
	w.AddRow(map[string]any{"ID": int64(1), "VAL": "snap"})

	// CDC event BEFORE lowCSN — already committed before snapshot opened.
	notEvicted := w.Evict("S", "T", map[string]any{"ID": int64(1)}, NewCSN(50))
	assert.False(t, notEvicted)

	// CDC event AFTER highCSN — happened after snapshot closed.
	notEvicted2 := w.Evict("S", "T", map[string]any{"ID": int64(1)}, NewCSN(300))
	assert.False(t, notEvicted2)

	// Exactly at lowCSN — boundary is exclusive on low side.
	notEvicted3 := w.Evict("S", "T", map[string]any{"ID": int64(1)}, NewCSN(100))
	assert.False(t, notEvicted3)

	// Exactly at highCSN — boundary is inclusive on high side.
	w2 := NewDeduplicationWindow("S", "T", NewCSN(100), NewCSN(200), []string{"ID"})
	w2.AddRow(map[string]any{"ID": int64(2)})
	evictedAtHigh := w2.Evict("S", "T", map[string]any{"ID": int64(2)}, NewCSN(200))
	assert.True(t, evictedAtHigh, "highCSN boundary must be inclusive")
	events2 := w2.Flush()
	assert.Empty(t, events2, "evicted row must not appear in flush")

	events := w.Flush()
	assert.Len(t, events, 1)
}

func TestDeduplicationWindowWrongTable(t *testing.T) {
	t.Parallel()

	w := NewDeduplicationWindow("S", "T", NewCSN(100), NewCSN(200), []string{"ID"})
	w.AddRow(map[string]any{"ID": int64(1)})

	notEvicted := w.Evict("S", "OTHER", map[string]any{"ID": int64(1)}, NewCSN(150))
	assert.False(t, notEvicted)

	assert.Len(t, w.Flush(), 1)
}

func TestDeduplicationWindowEmpty(t *testing.T) {
	t.Parallel()

	w := NewDeduplicationWindow("S", "T", NewCSN(0), NewCSN(100), []string{"ID"})
	assert.True(t, w.Empty())

	w.AddRow(map[string]any{"ID": int64(1)})
	assert.False(t, w.Empty())
}

func TestDeduplicationWindowLastPKValues(t *testing.T) {
	t.Parallel()

	w := NewDeduplicationWindow("S", "T", NewCSN(0), NewCSN(100), []string{"ID"})
	assert.Nil(t, w.LastPKValues())

	w.AddRow(map[string]any{"ID": int64(10)})
	w.AddRow(map[string]any{"ID": int64(20)})
	pkvs := w.LastPKValues()
	require.Len(t, pkvs, 1)
	// LastPKValues returns the direct row values (no JSON round-trip), so the
	// type is exactly what was stored via AddRow.
	assert.Equal(t, int64(20), pkvs[0])
}

// TestDeduplicationWindowByteVsStringPK verifies that Evict correctly matches a
// snapshot row whose PK was stored as []byte (DB2 ODBC returns CHAR/VARCHAR as
// []byte) against a CDC event with the same PK stored as string (after
// convertDB2Value). Without normalizePKVal this cross-type match always fails,
// causing duplicate delivery for every VARCHAR-PK table.
func TestDeduplicationWindowByteVsStringPK(t *testing.T) {
	t.Parallel()

	w := NewDeduplicationWindow("S", "T", NewCSN(100), NewCSN(200), []string{"ID"})
	// Snapshot row: DB2 ODBC driver returns VARCHAR as []byte.
	w.AddRow(map[string]any{"ID": []byte("FOO")})
	require.Equal(t, 1, w.Len())

	// CDC eviction event: convertDB2Value has converted the same column to string.
	evicted := w.Evict("S", "T", map[string]any{"ID": "FOO"}, NewCSN(150))
	assert.True(t, evicted, "Evict must match []byte snapshot PK against string CDC PK")
	assert.Equal(t, 0, w.Len(), "window must have 0 surviving rows after eviction")

	// LastPKValues must return string (not []byte) so JSON checkpoint round-trips correctly.
	pkvs := w.LastPKValues()
	require.Len(t, pkvs, 1)
	_, isString := pkvs[0].(string)
	assert.True(t, isString, "LastPKValues must return string, not []byte, for varchar PKs")
	assert.Equal(t, "FOO", pkvs[0])
}

// ---------------------------------------------------------------------------
// Task 3: ChunkReader
// ---------------------------------------------------------------------------

func TestChunkReaderFirstChunk(t *testing.T) {
	t.Parallel()

	queryCount := atomic.Int32{}
	db := openFakeDB(t, &replFakeHandlers{
		query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			n := queryCount.Add(1)
			switch n {
			case 1: // captureMaxSynchpoint (lowCSN = 100)
				return []string{"1"}, [][]driver.Value{{driver.Value(incTestCSNBytes(100))}}, nil
			case 2: // chunk SELECT
				return []string{"ID", "VAL"}, [][]driver.Value{
					{driver.Value(int64(1)), driver.Value("a")},
					{driver.Value(int64(2)), driver.Value("b")},
				}, nil
			case 3: // captureMaxSynchpoint (highCSN = 102)
				return []string{"1"}, [][]driver.Value{{driver.Value(incTestCSNBytes(102))}}, nil
			default:
				return nil, nil, nil
			}
		},
	})

	cr := NewChunkReader(db, "ASNCDC", "S", "T", []string{"ID"}, 1000)
	win, err := cr.ReadChunk(context.Background(), NewCSN(90), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), win.LowCSN.Uint64())
	assert.Equal(t, uint64(102), win.HighCSN.Uint64())
	events := win.Flush()
	assert.Len(t, events, 2)
}

func TestChunkReaderResumesFromLastKey(t *testing.T) {
	t.Parallel()

	var capturedQuery string
	queryCount := atomic.Int32{}
	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			n := queryCount.Add(1)
			switch n {
			case 1:
				return []string{"1"}, [][]driver.Value{{driver.Value(incTestCSNBytes(200))}}, nil
			case 2:
				capturedQuery = q
				return []string{"ID"}, [][]driver.Value{{driver.Value(int64(43))}}, nil
			case 3:
				return []string{"1"}, [][]driver.Value{{driver.Value(incTestCSNBytes(201))}}, nil
			default:
				return nil, nil, nil
			}
		},
	})

	cr := NewChunkReader(db, "ASNCDC", "S", "T", []string{"ID"}, 1000)
	_, err := cr.ReadChunk(context.Background(), NewCSN(190), []any{int64(42)}, nil)
	require.NoError(t, err)
	// Verify that the chunk query contains a WHERE clause for keyset pagination.
	assert.True(t, strings.Contains(capturedQuery, "WHERE"), "chunk query should contain WHERE clause, got: %s", capturedQuery)
}

func TestChunkReaderEmptyResult(t *testing.T) {
	t.Parallel()

	queryCount := atomic.Int32{}
	db := openFakeDB(t, &replFakeHandlers{
		query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			n := queryCount.Add(1)
			switch n {
			case 1:
				return []string{"1"}, [][]driver.Value{{driver.Value(incTestCSNBytes(300))}}, nil
			case 2:
				return []string{"ID"}, nil, nil // empty chunk
			case 3:
				return []string{"1"}, [][]driver.Value{{driver.Value(incTestCSNBytes(300))}}, nil
			default:
				return nil, nil, nil
			}
		},
	})

	cr := NewChunkReader(db, "ASNCDC", "S", "T", []string{"ID"}, 1000)
	win, err := cr.ReadChunk(context.Background(), NewCSN(290), []any{int64(99)}, nil)
	require.NoError(t, err)
	assert.True(t, win.Empty())
}

// ---------------------------------------------------------------------------
// Task 5: IncrementalSnapshotEngine
// ---------------------------------------------------------------------------

func TestEngineRunsSingleTableAllChunks(t *testing.T) {
	t.Parallel()

	// 3 rows, chunkSize=2 → chunk1=[1,2], chunk2=[3], chunk3=empty → done.
	// Engine must emit all 3 snapshot events and call checkpoint twice.

	chunkCall := atomic.Int32{}
	streamer := NewStreamer(nil, StreamConfig{}, Version{})
	checkpointCalled := atomic.Int32{}
	var allEvents []ChangeEvent

	db := openFakeDB(t, &replFakeHandlers{
		query: func(query string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(query, "KEYCOLUSE") {
				return []string{"COLNAME"}, [][]driver.Value{{driver.Value("ID")}}, nil
			}
			if strings.Contains(query, "SYSCAT.COLUMNS") {
				return []string{"COLNAME", "NULLS"}, [][]driver.Value{{driver.Value("ID"), driver.Value("N")}}, nil
			}
			if strings.Contains(query, "FETCH FIRST 1 ROW ONLY") {
				// selectMaxPK: return the lexicographic max row for the table.
				return []string{"ID"}, [][]driver.Value{{driver.Value(int64(3))}}, nil
			}
			if strings.Contains(query, "SYNCHPOINT") {
				return []string{"1"}, [][]driver.Value{{driver.Value(incTestCSNBytes(500))}}, nil
			}
			n := chunkCall.Add(1)
			switch n {
			case 1:
				return []string{"ID"}, [][]driver.Value{{driver.Value(int64(1))}, {driver.Value(int64(2))}}, nil
			case 2:
				return []string{"ID"}, [][]driver.Value{{driver.Value(int64(3))}}, nil
			default:
				return []string{"ID"}, nil, nil
			}
		},
	})

	engine := NewIncrementalSnapshotEngine(db, "ASNCDC", 2, streamer, func(_ context.Context, _ *IncrementalSnapshotContext) error {
		checkpointCalled.Add(1)
		return nil
	}, nil)

	ictx := &IncrementalSnapshotContext{
		Tables:        []IncrementalTable{{Schema: "S", Name: "ORDERS", SignalID: "sig1"}},
		LastEmittedPK: map[string][]any{},
		MaxPK:         map[string][]any{},
	}
	table := *ictx.CurrentTable()

	errCh := make(chan error, 1)
	go func() {
		errCh <- engine.RunTable(context.Background(), ictx, table, NewCSN(0))
	}()

	// Flush windows until the engine finishes. A window flush unblocks the engine
	// goroutine; when RunTable returns, errCh fires.
	flushUntilDone := func() error {
		for {
			select {
			case err := <-errCh:
				return err
			default:
			}
			streamer.windowMu.Lock()
			win := streamer.activeWindow
			streamer.windowMu.Unlock()
			if win != nil {
				allEvents = append(allEvents, win.Flush()...)
				streamer.windowMu.Lock()
				streamer.activeWindow = nil
				streamer.windowMu.Unlock()
			} else {
				runtime.Gosched()
			}
		}
	}

	require.NoError(t, flushUntilDone())
	assert.Len(t, allEvents, 3)
	assert.Equal(t, int32(2), checkpointCalled.Load())
}

func TestEngineResumesFromCheckpoint(t *testing.T) {
	t.Parallel()

	// LastEmittedPK = [1] → engine starts with chunk after ID=1, gets rows [2,3], then empty.
	chunkCall := atomic.Int32{}

	streamer := NewStreamer(nil, StreamConfig{}, Version{})

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(q, "KEYCOLUSE") {
				return []string{"COLNAME"}, [][]driver.Value{{driver.Value("ID")}}, nil
			}
			if strings.Contains(q, "SYSCAT.COLUMNS") {
				return []string{"COLNAME", "NULLS"}, [][]driver.Value{{driver.Value("ID"), driver.Value("N")}}, nil
			}
			if strings.Contains(q, "FETCH FIRST 1 ROW ONLY") {
				// selectMaxPK: return the lexicographic max row for the table.
				return []string{"ID"}, [][]driver.Value{{driver.Value(int64(3))}}, nil
			}
			if strings.Contains(q, "SYNCHPOINT") {
				return []string{"1"}, [][]driver.Value{{driver.Value(incTestCSNBytes(600))}}, nil
			}
			n := chunkCall.Add(1)
			switch n {
			case 1:
				return []string{"ID"}, [][]driver.Value{
					{driver.Value(int64(2))},
					{driver.Value(int64(3))},
				}, nil
			default:
				return []string{"ID"}, nil, nil
			}
		},
	})

	engine := NewIncrementalSnapshotEngine(db, "ASNCDC", 100, streamer, func(_ context.Context, _ *IncrementalSnapshotContext) error {
		return nil
	}, nil)

	ictx := &IncrementalSnapshotContext{
		Tables:        []IncrementalTable{{Schema: "S", Name: "T", SignalID: "sig1"}},
		LastEmittedPK: map[string][]any{"S.T": {json.Number("1")}},
		MaxPK:         map[string][]any{"S.T": {json.Number("3")}},
	}

	table := *ictx.CurrentTable()

	var allEvents []ChangeEvent
	errCh := make(chan error, 1)
	go func() {
		errCh <- engine.RunTable(context.Background(), ictx, table, NewCSN(0))
	}()

	for {
		select {
		case err := <-errCh:
			require.NoError(t, err)
			assert.Len(t, allEvents, 2)
			return
		default:
		}
		streamer.windowMu.Lock()
		win := streamer.activeWindow
		streamer.windowMu.Unlock()
		if win != nil {
			allEvents = append(allEvents, win.Flush()...)
			streamer.windowMu.Lock()
			streamer.activeWindow = nil
			streamer.windowMu.Unlock()
		} else {
			runtime.Gosched()
		}
	}
}

func TestEngineContextCancellation(t *testing.T) {
	t.Parallel()

	streamer := NewStreamer(nil, StreamConfig{}, Version{})

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(q, "KEYCOLUSE") {
				return []string{"COLNAME"}, [][]driver.Value{{driver.Value("ID")}}, nil
			}
			if strings.Contains(q, "SYSCAT.COLUMNS") {
				return []string{"COLNAME", "NULLS"}, [][]driver.Value{{driver.Value("ID"), driver.Value("N")}}, nil
			}
			if strings.Contains(q, "FETCH FIRST 1 ROW ONLY") {
				// selectMaxPK: return the lexicographic max row for the table.
				return []string{"ID"}, [][]driver.Value{{driver.Value(int64(100))}}, nil
			}
			if strings.Contains(q, "SYNCHPOINT") {
				return []string{"1"}, [][]driver.Value{{driver.Value(incTestCSNBytes(700))}}, nil
			}
			// Return a non-empty chunk so the engine sets a window and blocks on Wait.
			return []string{"ID"}, [][]driver.Value{{driver.Value(int64(1))}}, nil
		},
	})

	engine := NewIncrementalSnapshotEngine(db, "ASNCDC", 100, streamer, func(_ context.Context, _ *IncrementalSnapshotContext) error {
		return nil
	}, nil)

	ictx := &IncrementalSnapshotContext{
		Tables:        []IncrementalTable{{Schema: "S", Name: "T", SignalID: "sig1"}},
		LastEmittedPK: map[string][]any{},
		MaxPK:         map[string][]any{},
	}

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- engine.RunTable(ctx, ictx, *ictx.CurrentTable(), NewCSN(0))
	}()

	// Wait for engine to set window (it is now blocked on win.Wait).
	for {
		streamer.windowMu.Lock()
		win := streamer.activeWindow
		streamer.windowMu.Unlock()
		if win != nil {
			break
		}
		runtime.Gosched()
	}

	// Cancel the context — engine should unblock and return ctx.Err().
	cancel()

	err := <-errCh
	assert.ErrorIs(t, err, context.Canceled)
}

func TestDeduplicationWindowLen(t *testing.T) {
	t.Parallel()
	win := NewDeduplicationWindow("S", "T", NullCSN(), NullCSN(), []string{"ID"})
	require.Equal(t, 0, win.Len())
	win.AddRow(map[string]any{"ID": 1})
	win.AddRow(map[string]any{"ID": 2})
	require.Equal(t, 2, win.Len())
	win.AddRow(map[string]any{"ID": 1}) // duplicate key — overwrites, no new entry
	require.Equal(t, 2, win.Len())
}

type testLogger struct{ lines []string }

func (l *testLogger) Infof(f string, a ...any) { l.lines = append(l.lines, fmt.Sprintf(f, a...)) }
func (l *testLogger) Warnf(f string, a ...any) { l.lines = append(l.lines, fmt.Sprintf(f, a...)) }

func TestEngineSkipsEmptyTable(t *testing.T) {
	t.Parallel()

	log := &testLogger{}

	// empty-table scenario: selectMaxPK returns no rows → engine logs "empty, skipping"
	// and returns nil without issuing any SYNCHPOINT or chunk SELECT queries.
	chunkQueryCount := atomic.Int32{}
	db := openFakeDB(t, &replFakeHandlers{
		query: func(query string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(query, "KEYCOLUSE") {
				return []string{"COLNAME"}, [][]driver.Value{{driver.Value("ID")}}, nil
			}
			if strings.Contains(query, "SYSCAT.COLUMNS") {
				return []string{"COLNAME", "NULLS"}, [][]driver.Value{{driver.Value("ID"), driver.Value("N")}}, nil
			}
			if strings.Contains(query, "FETCH FIRST 1 ROW ONLY") {
				// selectMaxPK: empty table returns no rows.
				return []string{"ID"}, nil, nil
			}
			// Any other query (SYNCHPOINT or chunk SELECT) is unexpected.
			chunkQueryCount.Add(1)
			return []string{"ID"}, nil, nil
		},
	})

	streamer := NewStreamer(nil, StreamConfig{}, Version{})
	ictx := &IncrementalSnapshotContext{
		Tables:        []IncrementalTable{{Schema: "S", Name: "T"}},
		LastEmittedPK: map[string][]any{},
		MaxPK:         map[string][]any{},
	}

	engine := NewIncrementalSnapshotEngine(db, "ASNCDC", 10, streamer,
		func(context.Context, *IncrementalSnapshotContext) error { return nil },
		log,
	)

	err := engine.RunTable(context.Background(), ictx, ictx.Tables[0], NullCSN())
	require.NoError(t, err)
	assert.Equal(t, int32(0), chunkQueryCount.Load(), "no SYNCHPOINT or chunk queries should be issued for an empty table")

	require.NotEmpty(t, log.lines, "expected a log line about the empty table")
	combined := strings.Join(log.lines, "\n")
	require.Contains(t, combined, "empty", "expected 'empty' in log output for an empty table snapshot")
}

func TestEngineLogsChunkProgress(t *testing.T) {
	t.Parallel()

	log := &testLogger{}
	var chunkCall atomic.Int32

	streamer := NewStreamer(nil, StreamConfig{}, Version{})
	db := openFakeDB(t, &replFakeHandlers{
		query: func(query string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(query, "KEYCOLUSE") {
				return []string{"COLNAME"}, [][]driver.Value{{driver.Value("ID")}}, nil
			}
			if strings.Contains(query, "SYSCAT.COLUMNS") {
				return []string{"COLNAME", "NULLS"}, [][]driver.Value{{driver.Value("ID"), driver.Value("N")}}, nil
			}
			if strings.Contains(query, "FETCH FIRST 1 ROW ONLY") {
				return []string{"ID"}, [][]driver.Value{{driver.Value(int64(2))}}, nil
			}
			if strings.Contains(query, "SYNCHPOINT") {
				return []string{"1"}, [][]driver.Value{{driver.Value(incTestCSNBytes(100))}}, nil
			}
			n := chunkCall.Add(1)
			if n == 1 {
				return []string{"ID"}, [][]driver.Value{{driver.Value(int64(1))}, {driver.Value(int64(2))}}, nil
			}
			return []string{"ID"}, nil, nil
		},
	})

	ictx := &IncrementalSnapshotContext{
		Tables:        []IncrementalTable{{Schema: "S", Name: "T"}},
		LastEmittedPK: map[string][]any{},
		MaxPK:         map[string][]any{},
	}

	engine := NewIncrementalSnapshotEngine(db, "ASNCDC", 2, streamer,
		func(context.Context, *IncrementalSnapshotContext) error { return nil },
		log,
	)

	errCh := make(chan error, 1)
	go func() { errCh <- engine.RunTable(context.Background(), ictx, ictx.Tables[0], NullCSN()) }()

	// Flush windows until the engine finishes.
	for {
		streamer.windowMu.Lock()
		w := streamer.activeWindow
		streamer.windowMu.Unlock()
		if w != nil {
			w.Flush()
			streamer.windowMu.Lock()
			if streamer.activeWindow == w {
				streamer.activeWindow = nil
			}
			streamer.windowMu.Unlock()
		}
		select {
		case err := <-errCh:
			require.NoError(t, err)
			goto done
		default:
		}
	}
done:
	require.NotEmpty(t, log.lines, "expected at least one log line, got none")
	combined := strings.Join(log.lines, "\n")
	require.Contains(t, combined, "chunk", "expected chunk-level progress log")
}

func TestRunTableFailsOnNullablePK(t *testing.T) {
	t.Parallel()

	// Engine must fail fast when SYSCAT.COLUMNS reports NULLS='Y' for the PK column.
	queryCount := atomic.Int32{}
	db := openFakeDB(t, &replFakeHandlers{
		query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			n := queryCount.Add(1)
			switch n {
			case 1: // discoverPrimaryKeys
				return []string{"COLNAME"}, [][]driver.Value{{driver.Value("ID")}}, nil
			case 2: // checkPKNotNull — nullable
				return []string{"COLNAME", "NULLS"}, [][]driver.Value{
					{driver.Value("ID"), driver.Value("Y")},
				}, nil
			default:
				return nil, nil, nil
			}
		},
	})

	streamer := NewStreamer(nil, StreamConfig{}, Version{})
	engine := NewIncrementalSnapshotEngine(db, "ASNCDC", 10, streamer,
		func(context.Context, *IncrementalSnapshotContext) error { return nil }, nil)
	ictx := &IncrementalSnapshotContext{
		Tables:        []IncrementalTable{{Schema: "S", Name: "T"}},
		LastEmittedPK: map[string][]any{},
		MaxPK:         map[string][]any{},
	}
	err := engine.RunTable(context.Background(), ictx, ictx.Tables[0], NullCSN())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nullable")
}

func TestIncrementalContextBIGINTRoundtrip(t *testing.T) {
	t.Parallel()

	// 2^53 + 1 = 9007199254740993 — cannot be represented exactly as float64.
	// Verify that the json.Marshal + json.Decoder.UseNumber checkpoint path
	// preserves the full integer value (matching loadIncrementalContext).
	const bigintStr = "9007199254740993"
	ctx := &IncrementalSnapshotContext{
		Tables:        []IncrementalTable{{Schema: "S", Name: "T"}},
		LastEmittedPK: map[string][]any{"S.T": {json.Number(bigintStr)}},
		MaxPK:         map[string][]any{"S.T": {json.Number(bigintStr)}},
	}
	got := jsonRoundTrip(t, ctx)

	pk := got.LastEmittedPK["S.T"]
	require.Len(t, pk, 1)
	num, ok := pk[0].(json.Number)
	require.True(t, ok, "expected json.Number, got %T(%v)", pk[0], pk[0])
	assert.Equal(t, bigintStr, num.String())
}
