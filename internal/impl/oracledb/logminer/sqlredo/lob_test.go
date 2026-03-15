// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlredo

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
