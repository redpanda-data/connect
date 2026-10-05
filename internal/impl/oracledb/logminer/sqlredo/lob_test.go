// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlredo

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeInlineLOBValues(t *testing.T) {
	tests := []struct {
		name              string
		lobData           map[string]any
		schema            string
		table             string
		pkValues          map[string]any
		events            []*DMLEvent
		expectedDataPerEv []map[string]any
		expectedMerged    bool
	}{
		{
			name:   "nil pkValues merges into all inserts for schema.table",
			schema: "HR", table: "EMPLOYEES",
			lobData:  map[string]any{"RESUME": "hello"},
			pkValues: nil,
			events: []*DMLEvent{
				{Schema: "HR", Table: "EMPLOYEES", Operation: OpInsert, Data: map[string]any{"ID": "1", "RESUME": nil}},
				{Schema: "HR", Table: "EMPLOYEES", Operation: OpInsert, Data: map[string]any{"ID": "2", "RESUME": nil}},
			},
			expectedDataPerEv: []map[string]any{
				{"ID": "1", "RESUME": "hello"},
				{"ID": "2", "RESUME": "hello"},
			},
			expectedMerged: true,
		},
		{
			name:   "pkValues matches first row only first insert updated",
			schema: "HR", table: "EMPLOYEES",
			lobData:  map[string]any{"RESUME": "row1 content"},
			pkValues: map[string]any{"ID": "1"},
			events: []*DMLEvent{
				{Schema: "HR", Table: "EMPLOYEES", Operation: OpInsert, Data: map[string]any{"ID": "1", "RESUME": nil}},
				{Schema: "HR", Table: "EMPLOYEES", Operation: OpInsert, Data: map[string]any{"ID": "2", "RESUME": nil}},
			},
			expectedDataPerEv: []map[string]any{
				{"ID": "1", "RESUME": "row1 content"},
				{"ID": "2", "RESUME": nil},
			},
			expectedMerged: true,
		},
		{
			name:   "pkValues matches second row only second insert updated",
			schema: "HR", table: "EMPLOYEES",
			lobData:  map[string]any{"RESUME": "row2 content"},
			pkValues: map[string]any{"ID": "2"},
			events: []*DMLEvent{
				{Schema: "HR", Table: "EMPLOYEES", Operation: OpInsert, Data: map[string]any{"ID": "1", "RESUME": nil}},
				{Schema: "HR", Table: "EMPLOYEES", Operation: OpInsert, Data: map[string]any{"ID": "2", "RESUME": nil}},
			},
			expectedDataPerEv: []map[string]any{
				{"ID": "1", "RESUME": nil},
				{"ID": "2", "RESUME": "row2 content"},
			},
			expectedMerged: true,
		},
		{
			name:   "empty byte slice is EMPTY_CLOB placeholder and is skipped",
			schema: "HR", table: "EMPLOYEES",
			lobData:  map[string]any{"RESUME": []byte{}},
			pkValues: nil,
			events: []*DMLEvent{
				{Schema: "HR", Table: "EMPLOYEES", Operation: OpInsert, Data: map[string]any{"ID": "1", "RESUME": "assembled data"}},
			},
			expectedDataPerEv: []map[string]any{
				{"ID": "1", "RESUME": "assembled data"},
			},
			expectedMerged: true,
		},
		{
			name:   "different schema is not modified",
			schema: "HR", table: "EMPLOYEES",
			lobData:  map[string]any{"RESUME": "should not apply"},
			pkValues: nil,
			events: []*DMLEvent{
				{Schema: "OTHER", Table: "EMPLOYEES", Operation: OpInsert, Data: map[string]any{"ID": "1", "RESUME": nil}},
			},
			expectedDataPerEv: []map[string]any{
				{"ID": "1", "RESUME": nil},
			},
			expectedMerged: false,
		},
		{
			name:   "different table is not modified",
			schema: "HR", table: "EMPLOYEES",
			lobData:  map[string]any{"RESUME": "should not apply"},
			pkValues: nil,
			events: []*DMLEvent{
				{Schema: "HR", Table: "OTHER_TABLE", Operation: OpInsert, Data: map[string]any{"ID": "1", "RESUME": nil}},
			},
			expectedDataPerEv: []map[string]any{
				{"ID": "1", "RESUME": nil},
			},
			expectedMerged: false,
		},
		{
			name:   "pkValues with no matching INSERT reports unmerged",
			schema: "HR", table: "EMPLOYEES",
			lobData:  map[string]any{"RESUME": "orphaned content"},
			pkValues: map[string]any{"ID": "999"},
			events: []*DMLEvent{
				{Schema: "HR", Table: "EMPLOYEES", Operation: OpInsert, Data: map[string]any{"ID": "1", "RESUME": nil}},
			},
			expectedDataPerEv: []map[string]any{
				{"ID": "1", "RESUME": nil},
			},
			expectedMerged: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := MergeInlineLOBValues(tt.lobData, tt.schema, tt.table, tt.pkValues, tt.events, nil)
			assert.Equal(t, tt.expectedMerged, merged)
			for i, ev := range tt.events {
				assert.Equal(t, tt.expectedDataPerEv[i], ev.Data, "event[%d]", i)
			}
		})
	}
}

func TestAssembleOffsetValidation(t *testing.T) {
	t.Run("valid 1-based offsets assemble correctly", func(t *testing.T) {
		acc := &LobAccumulator{IsBinary: true}
		acc.AddFragment(1, []byte{0x41, 0x42})
		acc.AddFragment(3, []byte{0x43})
		assert.Equal(t, []byte{0x41, 0x42, 0x43}, acc.Assemble())
	})

	t.Run("offset < 1 is skipped, does not panic", func(t *testing.T) {
		acc := &LobAccumulator{IsBinary: true}
		acc.AddFragment(0, []byte{0x41, 0x42}) // invalid: Oracle offsets are 1-based
		acc.AddFragment(1, []byte{0x58})
		assert.NotPanics(t, func() {
			assert.Equal(t, []byte{0x58}, acc.Assemble())
		})
	})

	t.Run("overflowing offset is skipped, does not panic", func(t *testing.T) {
		acc := &LobAccumulator{IsBinary: true}
		acc.AddFragment(math.MaxInt64, []byte{0x41, 0x42}) // corrupt: (offset-1)+len overflows int64
		acc.AddFragment(1, []byte{0x58})
		assert.NotPanics(t, func() {
			assert.Equal(t, []byte{0x58}, acc.Assemble())
		})
	})

	t.Run("CLOB gaps are space-filled", func(t *testing.T) {
		acc := &LobAccumulator{IsBinary: false}
		acc.AddFragment(1, []byte("ab"))
		acc.AddFragment(5, []byte("z"))
		assert.Equal(t, "ab  z", acc.Assemble())
	})
}

func TestPkMatches(t *testing.T) {
	t.Run("empty pkValues does not vacuously match", func(t *testing.T) {
		assert.False(t, pkMatches(map[string]any{"ID": "1", "VAL": "x"}, map[string]any{}))
	})
	t.Run("subset match", func(t *testing.T) {
		assert.True(t, pkMatches(map[string]any{"ID": "1", "VAL": "x"}, map[string]any{"ID": "1"}))
	})
	t.Run("value mismatch", func(t *testing.T) {
		assert.False(t, pkMatches(map[string]any{"ID": "2"}, map[string]any{"ID": "1"}))
	})
	t.Run("missing key", func(t *testing.T) {
		assert.False(t, pkMatches(map[string]any{"OTHER": "1"}, map[string]any{"ID": "1"}))
	})
}

// TestMergeLOBsEmptyPKNoMisroute verifies that a ROWID-only SELECT_LOB_LOCATOR
// (which yields an empty PK set) does NOT get merged into an arbitrary INSERT.
// Previously the empty PK matched the first event vacuously; now, with two
// candidate rows for the table, the accumulator is left unmerged (Pass 3 cannot
// disambiguate) so the caller synthesizes a separate event instead of corrupting
// the wrong row.
func TestMergeLOBsEmptyPKNoMisroute(t *testing.T) {
	state := NewTxnLOBState()
	acc := &LobAccumulator{Schema: "S", Table: "T", Column: "DOC", IsBinary: false, PKValues: map[string]any{}}
	acc.AddFragment(1, []byte("hello"))
	state.Add(acc)

	events := []*DMLEvent{
		{Operation: OpInsert, Schema: "S", Table: "T", Data: map[string]any{"ID": "1"}},
		{Operation: OpInsert, Schema: "S", Table: "T", Data: map[string]any{"ID": "2"}},
	}

	unmerged := MergeLOBsIntoDMLEvents(state, events, nil)

	// Neither INSERT should have had the LOB written into it.
	for _, ev := range events {
		_, has := ev.Data["DOC"]
		assert.Falsef(t, has, "LOB must not be misrouted into row ID=%v", ev.Data["ID"])
	}
	// The accumulator is returned as unmerged for the caller to synthesize.
	require.Len(t, unmerged, 1)
	assert.Equal(t, "DOC", unmerged[0].Column)
}

// TestMergeLOBsEmptyPKSingleCandidate confirms the Pass-3 single-candidate path
// still merges correctly when there is exactly one row for the table.
func TestMergeLOBsEmptyPKSingleCandidate(t *testing.T) {
	state := NewTxnLOBState()
	acc := &LobAccumulator{Schema: "S", Table: "T", Column: "DOC", IsBinary: false, PKValues: map[string]any{}}
	acc.AddFragment(1, []byte("hello"))
	state.Add(acc)

	events := []*DMLEvent{
		{Operation: OpInsert, Schema: "S", Table: "T", Data: map[string]any{"ID": "1"}},
	}

	unmerged := MergeLOBsIntoDMLEvents(state, events, nil)
	assert.Empty(t, unmerged)
	assert.Equal(t, "hello", events[0].Data["DOC"])
}

func afterImageAcc(state *TxnLOBState, col, content string, pks map[string]any) {
	acc := &LobAccumulator{Schema: "TESTDB", Table: "T_CKC_RUNTIME", Column: col, PKValues: pks}
	acc.AddFragment(1, []byte(content))
	state.Add(acc)
}

func afterImageUpdate(id, oldNum, newNum string) *DMLEvent {
	return &DMLEvent{
		Operation: OpUpdate, Schema: "TESTDB", Table: "T_CKC_RUNTIME",
		Data:      map[string]any{"VIOLATION_NUM": newNum},
		OldValues: map[string]any{"CALC2_ID": id, "VIOLATION_NUM": oldNum},
	}
}

func TestMergeLOBsUpdateAfterImage(t *testing.T) {
	t.Run("lob merges into update with changed non-lob column", func(t *testing.T) {
		state := NewTxnLOBState()
		afterImageAcc(state, "CKC_BLOB", "blobdata", map[string]any{"CALC2_ID": "1000002855", "VIOLATION_NUM": "1"})
		ev := afterImageUpdate("1000002855", "0", "1")

		unmerged := MergeLOBsIntoDMLEvents(state, []*DMLEvent{ev}, nil)
		assert.Empty(t, unmerged)
		assert.Equal(t, "blobdata", ev.Data["CKC_BLOB"])
		assert.Equal(t, "1", ev.Data["VIOLATION_NUM"])
	})

	t.Run("two rows each get their own lob", func(t *testing.T) {
		state := NewTxnLOBState()
		afterImageAcc(state, "CKC_BLOB", "row-a", map[string]any{"CALC2_ID": "A", "VIOLATION_NUM": "1"})
		afterImageAcc(state, "CKC_CLOB", "row-b", map[string]any{"CALC2_ID": "B", "VIOLATION_NUM": "1"})
		a := afterImageUpdate("A", "0", "1")
		b := afterImageUpdate("B", "0", "1")

		unmerged := MergeLOBsIntoDMLEvents(state, []*DMLEvent{a, b}, nil)
		assert.Empty(t, unmerged)
		assert.Equal(t, "row-a", a.Data["CKC_BLOB"])
		assert.NotContains(t, a.Data, "CKC_CLOB")
		assert.Equal(t, "row-b", b.Data["CKC_CLOB"])
		assert.NotContains(t, b.Data, "CKC_BLOB")
	})

	t.Run("same row updated twice most recent wins", func(t *testing.T) {
		state := NewTxnLOBState()
		afterImageAcc(state, "CKC_BLOB", "blobdata", map[string]any{"CALC2_ID": "A", "VIOLATION_NUM": "2"})
		first := afterImageUpdate("A", "0", "1")
		second := afterImageUpdate("A", "1", "2")

		unmerged := MergeLOBsIntoDMLEvents(state, []*DMLEvent{first, second}, nil)
		assert.Empty(t, unmerged)
		assert.NotContains(t, first.Data, "CKC_BLOB")
		assert.Equal(t, "blobdata", second.Data["CKC_BLOB"])
	})

	t.Run("mismatched after-image stays unmerged", func(t *testing.T) {
		state := NewTxnLOBState()
		afterImageAcc(state, "CKC_BLOB", "blobdata", map[string]any{"CALC2_ID": "A", "VIOLATION_NUM": "9"})
		x := afterImageUpdate("A", "0", "1")
		y := afterImageUpdate("B", "0", "1")

		unmerged := MergeLOBsIntoDMLEvents(state, []*DMLEvent{x, y}, nil)
		require.Len(t, unmerged, 1)
		assert.NotContains(t, x.Data, "CKC_BLOB")
		assert.NotContains(t, y.Data, "CKC_BLOB")
	})
}

func lobAcc(col string, isBinary bool, limit int, pks map[string]any, writes ...string) *LobAccumulator {
	acc := &LobAccumulator{Schema: "S", Table: "T", Column: col, IsBinary: isBinary, PKValues: pks, EventLimit: limit}
	for _, w := range writes {
		acc.AddFragment(1, []byte(w))
	}
	return acc
}

func lobUpdate(id, name string) *DMLEvent {
	return &DMLEvent{
		Operation: OpUpdate, Schema: "S", Table: "T",
		Data:      map[string]any{"NAME": name},
		OldValues: map[string]any{"ID": id, "NAME": name},
	}
}

func TestMergeLOBsSameRowWrittenTwice(t *testing.T) {
	pk := map[string]any{"ID": "1"}

	t.Run("longer then shorter", func(t *testing.T) {
		state := NewTxnLOBState()
		first := lobAcc("DOC", false, 1, pk, "aaaaaaaaaa")
		second := lobAcc("DOC", false, 2, pk, "bbb")
		state.Add(first)
		state.Add(second)
		ev1, ev2 := lobUpdate("1", "x"), lobUpdate("1", "x")

		unmerged := MergeLOBsIntoDMLEvents(state, []*DMLEvent{ev1, ev2}, nil)
		assert.Empty(t, unmerged)
		assert.Equal(t, "aaaaaaaaaa", ev1.Data["DOC"])
		assert.Equal(t, "bbb", ev2.Data["DOC"])
	})

	t.Run("shorter then longer", func(t *testing.T) {
		state := NewTxnLOBState()
		state.Add(lobAcc("DOC", true, 1, pk, "aa"))
		state.Add(lobAcc("DOC", true, 2, pk, "bbbbbbbb"))
		ev1, ev2 := lobUpdate("1", "x"), lobUpdate("1", "x")

		unmerged := MergeLOBsIntoDMLEvents(state, []*DMLEvent{ev1, ev2}, nil)
		assert.Empty(t, unmerged)
		assert.Equal(t, []byte("aa"), ev1.Data["DOC"])
		assert.Equal(t, []byte("bbbbbbbb"), ev2.Data["DOC"])
	})

	t.Run("second write also changes a non-LOB column", func(t *testing.T) {
		state := NewTxnLOBState()
		first := map[string]any{"ID": "1", "VIOLATION_NUM": "0"}
		second := map[string]any{"ID": "1", "VIOLATION_NUM": "1"}
		state.Add(lobAcc("DOC", false, 1, first, "one"))
		state.Add(lobAcc("DOC", false, 2, second, "two"))
		ev1 := &DMLEvent{
			Operation: OpUpdate, Schema: "S", Table: "T",
			Data: map[string]any{"VIOLATION_NUM": "0"}, OldValues: map[string]any{"ID": "1", "VIOLATION_NUM": "0"},
		}
		ev2 := &DMLEvent{
			Operation: OpUpdate, Schema: "S", Table: "T",
			Data: map[string]any{"VIOLATION_NUM": "1"}, OldValues: map[string]any{"ID": "1", "VIOLATION_NUM": "0"},
		}

		unmerged := MergeLOBsIntoDMLEvents(state, []*DMLEvent{ev1, ev2}, nil)
		assert.Empty(t, unmerged)
		assert.Equal(t, "one", ev1.Data["DOC"])
		assert.Equal(t, "two", ev2.Data["DOC"])
	})

	t.Run("interleaved rows", func(t *testing.T) {
		pkB := map[string]any{"ID": "2"}
		a1, b1, a2, b2 := lobUpdate("1", "x"), lobUpdate("2", "x"), lobUpdate("1", "x"), lobUpdate("2", "x")
		state := NewTxnLOBState()
		state.Add(lobAcc("DOC", false, 1, pk, "A1-long-value"))
		state.Add(lobAcc("DOC", false, 2, pkB, "B1"))
		state.Add(lobAcc("DOC", false, 3, pk, "A2"))
		state.Add(lobAcc("DOC", false, 4, pkB, "B2-long-value"))

		unmerged := MergeLOBsIntoDMLEvents(state, []*DMLEvent{a1, b1, a2, b2}, nil)
		assert.Empty(t, unmerged)
		assert.Equal(t, "A1-long-value", a1.Data["DOC"])
		assert.Equal(t, "B1", b1.Data["DOC"])
		assert.Equal(t, "A2", a2.Data["DOC"])
		assert.Equal(t, "B2-long-value", b2.Data["DOC"])
	})

	t.Run("second locator without its own update is synthesized not stolen", func(t *testing.T) {
		state := NewTxnLOBState()
		state.Add(lobAcc("DOC", false, 1, pk, "first"))
		state.Add(lobAcc("DOC", false, 1, pk, "second"))
		ev := lobUpdate("1", "x")

		unmerged := MergeLOBsIntoDMLEvents(state, []*DMLEvent{ev}, nil)
		assert.Equal(t, "first", ev.Data["DOC"])
		require.Len(t, unmerged, 1)
		assert.Equal(t, "second", unmerged[0].Assemble())
	})

	t.Run("events after the locator are out of scope", func(t *testing.T) {
		state := NewTxnLOBState()
		state.Add(lobAcc("DOC", false, 1, pk, "first"))
		early, late := lobUpdate("1", "x"), lobUpdate("1", "x")

		unmerged := MergeLOBsIntoDMLEvents(state, []*DMLEvent{early, late}, nil)
		assert.Empty(t, unmerged)
		assert.Equal(t, "first", early.Data["DOC"])
		assert.NotContains(t, late.Data, "DOC")
	})

	t.Run("unbounded accumulator keeps most recent match", func(t *testing.T) {
		state := NewTxnLOBState()
		state.Add(lobAcc("DOC", false, 0, pk, "replayed"))
		ev1, ev2 := lobUpdate("1", "x"), lobUpdate("1", "x")

		unmerged := MergeLOBsIntoDMLEvents(state, []*DMLEvent{ev1, ev2}, nil)
		assert.Empty(t, unmerged)
		assert.NotContains(t, ev1.Data, "DOC")
		assert.Equal(t, "replayed", ev2.Data["DOC"])
	})

	t.Run("single write still merges", func(t *testing.T) {
		state := NewTxnLOBState()
		state.Add(lobAcc("DOC", false, 1, pk, "only"))
		ev := lobUpdate("1", "x")

		assert.Empty(t, MergeLOBsIntoDMLEvents(state, []*DMLEvent{ev}, nil))
		assert.Equal(t, "only", ev.Data["DOC"])
	})
}

func TestLobAccumulatorTrim(t *testing.T) {
	t.Run("truncates a binary value", func(t *testing.T) {
		acc := lobAcc("DOC", true, 0, nil, "0123456789")
		acc.Trim(4)
		assert.Equal(t, []byte("0123"), acc.Assemble())
	})

	t.Run("counts characters for text", func(t *testing.T) {
		acc := lobAcc("DOC", false, 0, nil, "héllo wörld")
		acc.Trim(5)
		assert.Equal(t, "héllo", acc.Assemble())
	})

	t.Run("overlapping writes then trim", func(t *testing.T) {
		acc := lobAcc("DOC", true, 0, nil, "AAAAAAAA")
		acc.AddFragment(1, []byte("BB"))
		acc.Trim(5)
		assert.Equal(t, []byte("BBAAA"), acc.Assemble())
	})

	t.Run("longer than written leaves value as is", func(t *testing.T) {
		acc := lobAcc("DOC", false, 0, nil, "abc")
		acc.Trim(100)
		assert.Equal(t, "abc", acc.Assemble())
	})

	t.Run("zero and empty are no-ops", func(t *testing.T) {
		acc := lobAcc("DOC", false, 0, nil, "abc")
		acc.Trim(0)
		assert.Equal(t, "abc", acc.Assemble())

		empty := lobAcc("DOC", false, 0, nil)
		empty.Trim(5)
		assert.Nil(t, empty.Assemble())
	})

	t.Run("writes after a trim are kept", func(t *testing.T) {
		acc := lobAcc("DOC", false, 0, nil, "abcdef")
		acc.Trim(2)
		acc.AddFragment(3, []byte("XYZ"))
		assert.Equal(t, "abXYZ", acc.Assemble())
	})
}
