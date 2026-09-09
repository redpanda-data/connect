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
// notification, or error.
//
// The comparison goes through the public Shred entry point with two shredders
// rather than reaching for the case-sensitive branch directly, because
// shredStruct dispatches on rs.caseSensitive on *every* recursion: a single
// case-sensitive shredder routes nested structs to shredStructExact regardless
// of how the top level was entered, so driving one shredder would compare the
// exact path against itself below the root and silently pass on any nested
// divergence. Two shredders makes one run exact all the way down and the other
// take the case-insensitive body all the way down.
//
// That comparison is only legitimate for a case-unambiguous corpus, so every
// schema field name and record key below is lower-case: folding then maps each
// key to itself and the two modes are *required* to agree. Inputs that differ
// in case are exactly where the modes are meant to diverge, and those belong in
// the case-sensitivity tests instead.
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

	// deep carries a REQUIRED leaf two levels down, so nested required-field
	// errors and nested unknown-field notifications are both reachable — the
	// divergences the earlier version of this test could not have seen.
	deep := iceberg.NestedField{
		ID:   20,
		Name: "deep",
		Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 21, Name: "mid", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: 22, Name: "leaf", Type: iceberg.PrimitiveTypes.String, Required: true},
			}}, Required: false},
		}},
		Required: false,
	}

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "flag", Type: iceberg.PrimitiveTypes.Bool, Required: false},
		nested,
		deep,
	)

	// Structs are also reached through list elements and map values, and those
	// recursions dispatch on case sensitivity exactly like the top level does.
	listOfStruct := iceberg.NestedField{
		ID:   30,
		Name: "items",
		Type: &iceberg.ListType{
			ElementID: 31,
			Element: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: 32, Name: "k", Type: iceberg.PrimitiveTypes.String, Required: true},
				{ID: 33, Name: "v", Type: iceberg.PrimitiveTypes.Int64, Required: false},
			}},
			ElementRequired: false,
		},
		Required: false,
	}
	mapOfStruct := iceberg.NestedField{
		ID:   40,
		Name: "byname",
		Type: &iceberg.MapType{
			KeyID:   41,
			KeyType: iceberg.PrimitiveTypes.String,
			ValueID: 42,
			ValueType: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: 43, Name: "n", Type: iceberg.PrimitiveTypes.Int64, Required: false},
			}},
			ValueRequired: false,
		},
		Required: false,
	}
	nestedSchema := iceberg.NewSchema(3,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		listOfStruct,
		mapOfStruct,
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
			name:   "unknown key in a doubly-nested struct",
			schema: schema,
			record: map[string]any{
				"id":   int64(1),
				"deep": map[string]any{"mid": map[string]any{"leaf": "ok", "extra": 1}},
			},
		},
		{
			name:   "required leaf missing two levels down (nested error path)",
			schema: schema,
			record: map[string]any{
				"id":   int64(1),
				"deep": map[string]any{"mid": map[string]any{}},
			},
		},
		{
			name:   "required leaf explicitly null two levels down",
			schema: schema,
			record: map[string]any{
				"id":   int64(1),
				"deep": map[string]any{"mid": map[string]any{"leaf": nil}},
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
			name:   "struct inside a list",
			schema: nestedSchema,
			record: map[string]any{"id": int64(1), "items": []any{
				map[string]any{"k": "a", "v": int64(1)},
				map[string]any{"k": "b"},
			}},
		},
		{
			name:   "unknown key in a struct inside a list",
			schema: nestedSchema,
			record: map[string]any{"id": int64(1), "items": []any{
				map[string]any{"k": "a", "surprise": true},
			}},
		},
		{
			name:   "required field missing in a struct inside a list",
			schema: nestedSchema,
			record: map[string]any{"id": int64(1), "items": []any{
				map[string]any{"v": int64(1)},
			}},
		},
		{
			name:   "struct inside a map value",
			schema: nestedSchema,
			record: map[string]any{"id": int64(1), "byname": map[string]any{
				"x": map[string]any{"n": int64(3)},
			}},
		},
		{
			name:   "unknown key in a struct inside a map value",
			schema: nestedSchema,
			record: map[string]any{"id": int64(1), "byname": map[string]any{
				"x": map[string]any{"n": int64(3), "extra": "?"},
			}},
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
			exactSink := &testSink{}
			exactErr := NewRecordShredder(tc.schema, true).Shred(tc.record, exactSink)

			foldedSink := &testSink{}
			foldedErr := NewRecordShredder(tc.schema, false).Shred(tc.record, foldedSink)

			if foldedErr != nil {
				require.EqualError(t, exactErr, foldedErr.Error(),
					"fast path must fail exactly as the general path does")
				// Still compare what was emitted before the error: an identical
				// error does not imply identical partial output, and a divergence
				// there would otherwise pass silently.
				require.Equal(t, foldedSink.values, exactSink.values,
					"values emitted before the error must be identical")
				require.Equal(t, sortedNewFields(foldedSink.newFields), sortedNewFields(exactSink.newFields),
					"new-field notifications before the error must be identical")
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

// TestShredStructPathsAgreeWithDuplicateFieldNames covers the one input class
// where the fast path's matched-count shortcut is unsound: Iceberg rejects
// duplicate field IDs but not duplicate field *names*, and matchedKeys counts
// fields rather than distinct claimed keys. Fields [a, a] against
// {"a":…, "b":…} therefore reach matchedKeys == len(value) while "b" is
// genuinely unknown, and without the duplicateFieldNames guard the fast path
// would skip the scan and never report it — losing the schema evolution of that
// column, silently.
//
// Kept separate from TestShredStructPathsAgree because the schema cannot be
// built with iceberg.NewSchema's validation in the same table as the others.
func TestShredStructPathsAgreeWithDuplicateFieldNames(t *testing.T) {
	fields := []iceberg.NestedField{
		{ID: 1, Name: "a", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		{ID: 2, Name: "a", Type: iceberg.PrimitiveTypes.Int64, Required: false},
	}
	record := map[string]any{"a": int64(1), "b": int64(2)}

	exactSink := &testSink{}
	exact := &RecordShredder{caseSensitive: true, duplicateFieldNames: hasDuplicateFieldNames(fields)}
	require.NoError(t, exact.shredStruct(fields, record, nil, 0, 0, 0, exactSink))

	foldedSink := &testSink{}
	folded := &RecordShredder{caseSensitive: false}
	require.NoError(t, folded.shredStruct(fields, record, nil, 0, 0, 0, foldedSink))

	require.Equal(t, foldedSink.values, exactSink.values)
	require.Equal(t, sortedNewFields(foldedSink.newFields), sortedNewFields(exactSink.newFields),
		"the unknown key must be reported by both paths")
	require.Len(t, exactSink.newFields, 1, "expected the unknown key to be reported")
	require.Equal(t, "b", exactSink.newFields[0].name)
}

// TestHasDuplicateFieldNames pins the detection itself, including through the
// list and map recursions.
func TestHasDuplicateFieldNames(t *testing.T) {
	str := iceberg.PrimitiveTypes.String
	dupStruct := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 10, Name: "x", Type: str}, {ID: 11, Name: "x", Type: str},
	}}

	require.False(t, hasDuplicateFieldNames([]iceberg.NestedField{
		{ID: 1, Name: "a", Type: str}, {ID: 2, Name: "b", Type: str},
	}), "distinct names")

	require.True(t, hasDuplicateFieldNames([]iceberg.NestedField{
		{ID: 1, Name: "a", Type: str}, {ID: 2, Name: "a", Type: str},
	}), "top-level duplicate")

	require.True(t, hasDuplicateFieldNames([]iceberg.NestedField{
		{ID: 1, Name: "s", Type: dupStruct},
	}), "duplicate nested in a struct")

	require.True(t, hasDuplicateFieldNames([]iceberg.NestedField{
		{ID: 1, Name: "l", Type: &iceberg.ListType{ElementID: 2, Element: dupStruct}},
	}), "duplicate nested in a list element")

	require.True(t, hasDuplicateFieldNames([]iceberg.NestedField{
		{ID: 1, Name: "m", Type: &iceberg.MapType{KeyID: 2, KeyType: str, ValueID: 3, ValueType: dupStruct}},
	}), "duplicate nested in a map value")
}
