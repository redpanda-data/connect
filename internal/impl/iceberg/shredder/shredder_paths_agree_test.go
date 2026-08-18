// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package shredder

import (
	"fmt"
	"sort"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

// TestShredStructPathsAgree pins the case-sensitive fast path
// (shredStructExact) to the general folded path (shredStructFolded).
//
// shredStructExact exists purely to avoid two per-record map allocations when
// key matching is exact — which is the default (case_sensitive_columns: true)
// and therefore the path almost all traffic takes. It is an optimisation, so it
// must not change observable behaviour by even one emitted value, new-field
// notification, or error. This test drives the same schema and record through
// both implementations and demands identical output.
//
// It covers the cases the optimisation actually reasons about: every key
// matching (the allocation-free steady state), extra unknown keys (the fallback
// scan), missing fields, explicit nulls, required-field violations, and nesting
// — plus an empty record and an empty schema, where the matched-count shortcut
// is most likely to be wrong.
func TestShredStructPathsAgree(t *testing.T) {
	nested := iceberg.NestedField{
		ID:   10,
		Name: "inner",
		Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 11, Name: "a", Type: iceberg.PrimitiveTypes.Int64, Required: false},
			{ID: 12, Name: "b", Type: iceberg.PrimitiveTypes.String, Required: false},
		}},
		Required: false,
	}

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "flag", Type: iceberg.PrimitiveTypes.Bool, Required: false},
		nested,
	)

	emptySchema := iceberg.NewSchema(2)

	cases := []struct {
		name   string
		schema *iceberg.Schema
		record map[string]any
	}{
		{
			name:   "all fields present",
			schema: schema,
			record: map[string]any{
				"id": int64(1), "name": "a", "flag": true,
				"inner": map[string]any{"a": int64(2), "b": "c"},
			},
		},
		{
			name:   "exact match, no unknowns (allocation-free steady state)",
			schema: schema,
			record: map[string]any{"id": int64(1), "name": "a", "flag": false, "inner": nil},
		},
		{
			name:   "one unknown key",
			schema: schema,
			record: map[string]any{"id": int64(1), "surprise": "x"},
		},
		{
			name:   "several unknown keys",
			schema: schema,
			record: map[string]any{"id": int64(1), "x": 1, "y": "two", "z": nil},
		},
		{
			name:   "unknown key nested inside a known struct",
			schema: schema,
			record: map[string]any{
				"id":    int64(1),
				"inner": map[string]any{"a": int64(2), "nope": "surprise"},
			},
		},
		{
			name:   "missing optional fields",
			schema: schema,
			record: map[string]any{"id": int64(7)},
		},
		{
			name:   "explicit nulls",
			schema: schema,
			record: map[string]any{"id": int64(7), "name": nil, "flag": nil, "inner": nil},
		},
		{
			name:   "required field missing (error path)",
			schema: schema,
			record: map[string]any{"name": "no id here"},
		},
		{
			name:   "required field explicitly null (error path)",
			schema: schema,
			record: map[string]any{"id": nil},
		},
		{
			name:   "empty record",
			schema: schema,
			record: map[string]any{},
		},
		{
			name:   "empty schema, empty record",
			schema: emptySchema,
			record: map[string]any{},
		},
		{
			name:   "empty schema, all keys unknown",
			schema: emptySchema,
			record: map[string]any{"a": 1, "b": 2},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Both shredders are case-sensitive: the folded path is exercised
			// directly so the comparison isolates the implementations rather
			// than the matching mode.
			rs := NewRecordShredder(tc.schema, true)
			fields := tc.schema.Fields()

			exactSink := &testSink{}
			exactErr := rs.shredStructExact(fields, tc.record, nil, 0, 0, 0, exactSink)

			foldedSink := &testSink{}
			foldedErr := rs.shredStructFolded(fields, tc.record, nil, 0, 0, 0, foldedSink)

			if foldedErr != nil {
				require.EqualError(t, exactErr, foldedErr.Error(),
					"fast path must fail exactly as the general path does")
				return
			}
			require.NoError(t, exactErr, "fast path errored where the general path did not")

			require.Equal(t, foldedSink.values, exactSink.values,
				"emitted values must be identical (including order)")

			// New-field notification order follows Go map iteration, which is
			// randomised, so compare as sets.
			require.Equal(t, sortedNewFields(foldedSink.newFields), sortedNewFields(exactSink.newFields),
				"new-field notifications must be identical")
		})
	}
}

func sortedNewFields(in []newFieldRecord) []string {
	out := make([]string, 0, len(in))
	for _, nf := range in {
		out = append(out, fmt.Sprintf("path=%v name=%s value=%v", nf.path, nf.name, nf.value))
	}
	sort.Strings(out)
	return out
}
