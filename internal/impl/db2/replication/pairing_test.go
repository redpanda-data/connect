// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPairOpcodeEvents(t *testing.T) {
	csn100 := NewCSN(100)
	csn101 := NewCSN(101)

	tests := []struct {
		name   string
		input  []ChangeEvent
		expect []ChangeEvent
	}{
		{
			name: "update_pair_merged",
			input: []ChangeEvent{
				{CSN: csn100, IntentSeq: 1, Operation: opTypeUpdateBefore, Data: map[string]any{"ID": 1, "NAME": "old"}},
				{CSN: csn100, IntentSeq: 2, Operation: opTypeUpdateAfter, Data: map[string]any{"ID": 1, "NAME": "new"}},
			},
			expect: []ChangeEvent{
				{
					CSN: csn100, IntentSeq: 2, Operation: OpTypeUpdate,
					Data:       map[string]any{"ID": 1, "NAME": "new"},
					BeforeData: map[string]any{"ID": 1, "NAME": "old"},
				},
			},
		},
		{
			name: "standalone_delete",
			input: []ChangeEvent{
				{CSN: csn100, IntentSeq: 1, Operation: OpTypeDelete, Data: map[string]any{"ID": 2}},
			},
			expect: []ChangeEvent{
				{CSN: csn100, IntentSeq: 1, Operation: OpTypeDelete, Data: map[string]any{"ID": 2}},
			},
		},
		{
			name: "standalone_insert",
			input: []ChangeEvent{
				{CSN: csn100, IntentSeq: 1, Operation: OpTypeInsert, Data: map[string]any{"ID": 3}},
			},
			expect: []ChangeEvent{
				{CSN: csn100, IntentSeq: 1, Operation: OpTypeInsert, Data: map[string]any{"ID": 3}},
			},
		},
		{
			name: "delete_then_update_pair",
			input: []ChangeEvent{
				{CSN: csn100, IntentSeq: 1, Operation: OpTypeDelete, Data: map[string]any{"ID": 1}},
				{CSN: csn100, IntentSeq: 2, Operation: opTypeUpdateBefore, Data: map[string]any{"ID": 2, "NAME": "old"}},
				{CSN: csn100, IntentSeq: 3, Operation: opTypeUpdateAfter, Data: map[string]any{"ID": 2, "NAME": "new"}},
			},
			expect: []ChangeEvent{
				{CSN: csn100, IntentSeq: 1, Operation: OpTypeDelete, Data: map[string]any{"ID": 1}},
				{
					CSN: csn100, IntentSeq: 3, Operation: OpTypeUpdate,
					Data:       map[string]any{"ID": 2, "NAME": "new"},
					BeforeData: map[string]any{"ID": 2, "NAME": "old"},
				},
			},
		},
		{
			name: "two_update_pairs_same_csn",
			input: []ChangeEvent{
				{CSN: csn100, IntentSeq: 1, Operation: opTypeUpdateBefore, Data: map[string]any{"ID": 1, "V": "a"}},
				{CSN: csn100, IntentSeq: 2, Operation: opTypeUpdateAfter, Data: map[string]any{"ID": 1, "V": "b"}},
				{CSN: csn100, IntentSeq: 3, Operation: opTypeUpdateBefore, Data: map[string]any{"ID": 2, "V": "x"}},
				{CSN: csn100, IntentSeq: 4, Operation: opTypeUpdateAfter, Data: map[string]any{"ID": 2, "V": "y"}},
			},
			expect: []ChangeEvent{
				{
					CSN: csn100, IntentSeq: 2, Operation: OpTypeUpdate,
					Data: map[string]any{"ID": 1, "V": "b"}, BeforeData: map[string]any{"ID": 1, "V": "a"},
				},
				{
					CSN: csn100, IntentSeq: 4, Operation: OpTypeUpdate,
					Data: map[string]any{"ID": 2, "V": "y"}, BeforeData: map[string]any{"ID": 2, "V": "x"},
				},
			},
		},
		{
			// Cross-batch D+I pairs are handled upstream: pollChangeTable strips the trailing
			// opTypeUpdateBefore into pendingBeforeByTable. pairOpcodeEvents only sees complete
			// pairs. If an unmatched before-image arrives here (different CSNs, a DB2 anomaly
			// that shouldn't happen in production), the before-image is discarded and the
			// orphaned after-image is re-classified as an INSERT so no internal opcode leaks
			// downstream. Consumers see the current row value rather than an unknown opcode.
			name: "mismatched_csn_orphaned_after_becomes_insert",
			input: []ChangeEvent{
				{CSN: csn100, IntentSeq: 1, Operation: opTypeUpdateBefore, Data: map[string]any{"ID": 1}},
				{CSN: csn101, IntentSeq: 2, Operation: opTypeUpdateAfter, Data: map[string]any{"ID": 1}},
			},
			expect: []ChangeEvent{
				// Before-image discarded; after-image re-classified as INSERT.
				{CSN: csn101, IntentSeq: 2, Operation: OpTypeInsert, Data: map[string]any{"ID": 1}},
			},
		},
		{
			name:   "empty",
			input:  []ChangeEvent{},
			expect: []ChangeEvent{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pairOpcodeEvents(tc.input)
			assert.Equal(t, tc.expect, got)
		})
	}
}
