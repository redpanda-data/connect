// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/sqlredo"
)

func TestLobAccumulatorAssemble(t *testing.T) {
	tests := []struct {
		name    string
		binary  bool
		setup   func(a *LobAccumulator)
		wantNil bool
		want    any
	}{
		{
			name:  "single CLOB fragment",
			setup: func(a *LobAccumulator) { a.AddFragment(1, []byte("Hello")) },
			want:  "Hello",
		},
		{
			name: "multiple contiguous CLOB fragments",
			setup: func(a *LobAccumulator) {
				a.AddFragment(1, []byte("He"))
				a.AddFragment(3, []byte("llo"))
			},
			want: "Hello",
		},
		{
			name: "gap in CLOB filled with spaces",
			setup: func(a *LobAccumulator) {
				a.AddFragment(1, []byte("A"))
				a.AddFragment(3, []byte("C"))
			},
			want: "A C",
		},
		{
			name:   "BLOB gap filled with zeros",
			binary: true,
			setup: func(a *LobAccumulator) {
				a.AddFragment(1, []byte{0x01})
				a.AddFragment(3, []byte{0x03})
			},
			want: []byte{0x01, 0x00, 0x03},
		},
		{
			name: "out-of-order fragments",
			setup: func(a *LobAccumulator) {
				a.AddFragment(6, []byte(" World"))
				a.AddFragment(1, []byte("Hello"))
			},
			want: "Hello World",
		},
		{
			name:    "empty accumulator returns nil",
			setup:   func(a *LobAccumulator) {},
			wantNil: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := &LobAccumulator{IsBinary: tc.binary}
			tc.setup(a)
			got := a.Assemble()
			if tc.wantNil {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, tc.want, got)
		})
	}

	// NCLOB: Oracle delivers data as plain string literals in LOB_WRITE SQL.
	t.Run("NCLOB single fragment as string literal", func(t *testing.T) {
		a := &LobAccumulator{IsNational: true}
		a.AddFragment(1, []byte("Hello"))
		assert.Equal(t, "Hello", a.Assemble())
	})

	t.Run("NCLOB multi-fragment as string literals", func(t *testing.T) {
		a := &LobAccumulator{IsNational: true}
		a.AddFragment(1, []byte("Hello"))
		a.AddFragment(6, []byte(" World"))
		assert.Equal(t, "Hello World", a.Assemble())
	})
}

func TestMergeLOBsCLOBIntoInsert(t *testing.T) {
	event := &sqlredo.DMLEvent{
		Schema: "S",
		Table:  "T",
		Data:   map[string]any{"ID": "1", "CONTENT": ""},
	}

	acc := &LobAccumulator{
		Schema:   "S",
		Table:    "T",
		Column:   "CONTENT",
		IsBinary: false,
		PKValues: map[string]any{"ID": "1"},
	}
	acc.AddFragment(1, []byte("Hello World"))

	pkStr := fmt.Sprintf("%v", map[string]any{"ID": "1"})
	key := lobKey{Schema: "S", Table: "T", Column: "CONTENT", PKString: pkStr}
	state := newTxnLOBState()
	state.accumulators[key] = acc

	mergeLOBsIntoDMLEvents(state, []*sqlredo.DMLEvent{event}, nil)

	assert.Equal(t, "Hello World", event.Data["CONTENT"])
}

func TestMergeLOBsBLOBIntoInsert(t *testing.T) {
	event := &sqlredo.DMLEvent{
		Schema: "S",
		Table:  "T",
		Data:   map[string]any{"ID": "1", "CONTENT": []byte{}},
	}

	acc := &LobAccumulator{
		Schema:   "S",
		Table:    "T",
		Column:   "CONTENT",
		IsBinary: true,
		PKValues: map[string]any{"ID": "1"},
	}
	acc.AddFragment(1, []byte("Hello World"))

	pkStr := fmt.Sprintf("%v", map[string]any{"ID": "1"})
	key := lobKey{Schema: "S", Table: "T", Column: "CONTENT", PKString: pkStr}
	state := newTxnLOBState()
	state.accumulators[key] = acc

	mergeLOBsIntoDMLEvents(state, []*sqlredo.DMLEvent{event}, nil)

	assert.Equal(t, []byte("Hello World"), event.Data["CONTENT"])
}

func TestMergeLOBsNCLOBIntoInsert(t *testing.T) {
	event := &sqlredo.DMLEvent{
		Schema: "TESTDB",
		Table:  "PRODUCTS",
		Data:   map[string]any{"ID": "1", "DESCRIPTION": ""},
	}

	acc := &LobAccumulator{
		Schema:     "TESTDB",
		Table:      "PRODUCTS",
		Column:     "DESCRIPTION",
		IsNational: true,
		PKValues:   map[string]any{"ID": "1"},
	}
	acc.AddFragment(1, []byte("Hi"))

	pkStr := fmt.Sprintf("%v", map[string]any{"ID": "1"})
	key := lobKey{Schema: "TESTDB", Table: "PRODUCTS", Column: "DESCRIPTION", PKString: pkStr}
	state := newTxnLOBState()
	state.accumulators[key] = acc

	mergeLOBsIntoDMLEvents(state, []*sqlredo.DMLEvent{event}, nil)

	assert.Equal(t, "Hi", event.Data["DESCRIPTION"])
}

func TestMergeLOBsNoMatchDifferentTable(t *testing.T) {
	event := &sqlredo.DMLEvent{
		Schema: "S",
		Table:  "T",
		Data:   map[string]any{"ID": "1", "CONTENT": "original"},
	}

	acc := &LobAccumulator{
		Schema:   "S",
		Table:    "OTHER",
		Column:   "CONTENT",
		IsBinary: false,
		PKValues: map[string]any{"ID": "1"},
	}
	acc.AddFragment(1, []byte("New Value"))

	pkStr := fmt.Sprintf("%v", map[string]any{"ID": "1"})
	key := lobKey{Schema: "S", Table: "OTHER", Column: "CONTENT", PKString: pkStr}
	state := newTxnLOBState()
	state.accumulators[key] = acc

	mergeLOBsIntoDMLEvents(state, []*sqlredo.DMLEvent{event}, nil)

	assert.Equal(t, "original", event.Data["CONTENT"])
}

func TestMergeLOBsNoMatchDifferentPK(t *testing.T) {
	event := &sqlredo.DMLEvent{
		Schema: "S",
		Table:  "T",
		Data:   map[string]any{"ID": "1", "CONTENT": "original"},
	}

	acc := &LobAccumulator{
		Schema:   "S",
		Table:    "T",
		Column:   "CONTENT",
		IsBinary: false,
		PKValues: map[string]any{"ID": "99"},
	}
	acc.AddFragment(1, []byte("New Value"))

	pkStr := fmt.Sprintf("%v", map[string]any{"ID": "99"})
	key := lobKey{Schema: "S", Table: "T", Column: "CONTENT", PKString: pkStr}
	state := newTxnLOBState()
	state.accumulators[key] = acc

	mergeLOBsIntoDMLEvents(state, []*sqlredo.DMLEvent{event}, nil)

	assert.Equal(t, "original", event.Data["CONTENT"])
}

func TestMergeLOBsMostRecentEventWins(t *testing.T) {
	event1 := &sqlredo.DMLEvent{
		Schema: "S",
		Table:  "T",
		Data:   map[string]any{"ID": "1", "CONTENT": "first"},
	}
	event2 := &sqlredo.DMLEvent{
		Schema: "S",
		Table:  "T",
		Data:   map[string]any{"ID": "1", "CONTENT": "second"},
	}

	acc := &LobAccumulator{
		Schema:   "S",
		Table:    "T",
		Column:   "CONTENT",
		IsBinary: false,
		PKValues: map[string]any{"ID": "1"},
	}
	acc.AddFragment(1, []byte("LOB Value"))

	pkStr := fmt.Sprintf("%v", map[string]any{"ID": "1"})
	key := lobKey{Schema: "S", Table: "T", Column: "CONTENT", PKString: pkStr}
	state := newTxnLOBState()
	state.accumulators[key] = acc

	mergeLOBsIntoDMLEvents(state, []*sqlredo.DMLEvent{event1, event2}, nil)

	assert.Equal(t, "LOB Value", event2.Data["CONTENT"], "most recent event should receive LOB value")
	assert.Equal(t, "first", event1.Data["CONTENT"], "earlier event should be unchanged")
}

func TestMergeLOBsEmptyAccumulatorSkipped(t *testing.T) {
	event := &sqlredo.DMLEvent{
		Schema: "S",
		Table:  "T",
		Data:   map[string]any{"ID": "1", "CONTENT": "original"},
	}

	acc := &LobAccumulator{
		Schema:   "S",
		Table:    "T",
		Column:   "CONTENT",
		IsBinary: false,
		PKValues: map[string]any{"ID": "1"},
	}
	// No fragments added

	pkStr := fmt.Sprintf("%v", map[string]any{"ID": "1"})
	key := lobKey{Schema: "S", Table: "T", Column: "CONTENT", PKString: pkStr}
	state := newTxnLOBState()
	state.accumulators[key] = acc

	mergeLOBsIntoDMLEvents(state, []*sqlredo.DMLEvent{event}, nil)

	assert.Equal(t, "original", event.Data["CONTENT"])
}
