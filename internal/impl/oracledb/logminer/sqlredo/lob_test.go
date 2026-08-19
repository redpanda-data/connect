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
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			MergeInlineLOBValues(tt.lobData, tt.schema, tt.table, tt.pkValues, tt.events, nil)
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
	key := LobKey{Schema: "S", Table: "T", Column: "DOC"}
	acc := &LobAccumulator{Schema: "S", Table: "T", Column: "DOC", IsBinary: false, PKValues: map[string]any{}}
	acc.AddFragment(1, []byte("hello"))
	state.Accumulators[key] = acc

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
	key := LobKey{Schema: "S", Table: "T", Column: "DOC"}
	acc := &LobAccumulator{Schema: "S", Table: "T", Column: "DOC", IsBinary: false, PKValues: map[string]any{}}
	acc.AddFragment(1, []byte("hello"))
	state.Accumulators[key] = acc

	events := []*DMLEvent{
		{Operation: OpInsert, Schema: "S", Table: "T", Data: map[string]any{"ID": "1"}},
	}

	unmerged := MergeLOBsIntoDMLEvents(state, events, nil)
	assert.Empty(t, unmerged)
	assert.Equal(t, "hello", events[0].Data["DOC"])
}
