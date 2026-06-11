// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
)

// TestBufferingSinkNewFieldDedup verifies the case-aware dedup behaviour of
// the buffering sink. Without folding, two messages reporting new fields that
// differ only in case (e.g. "FOO" and "foo") would each yield a separate
// UnknownFieldError; the router would then ask iceberg-go to add both, and
// case-insensitive UpdateSchema would reject the second. Folding the dedup
// key collapses them to a single schema-evolution attempt.
func TestBufferingSinkNewFieldDedup(t *testing.T) {
	t.Run("case-insensitive dedup folds case-only duplicates", func(t *testing.T) {
		sink := newBufferingSink(nil, 0, false)
		sink.OnNewField(icebergx.Path{}, "FOO", "x")
		sink.OnNewField(icebergx.Path{}, "foo", "y")

		errs := sink.newFieldErrors()
		require.Len(t, errs, 1)
		// Whichever arrived first wins; we only assert one survived.
		assert.Contains(t, []string{"FOO", "foo"}, errs[0].FieldName())
	})

	t.Run("case-sensitive dedup keeps both", func(t *testing.T) {
		sink := newBufferingSink(nil, 0, true)
		sink.OnNewField(icebergx.Path{}, "FOO", "x")
		sink.OnNewField(icebergx.Path{}, "foo", "y")

		errs := sink.newFieldErrors()
		assert.Len(t, errs, 2)
	})

	t.Run("case-insensitive dedup is path-aware", func(t *testing.T) {
		// Same name at different parents must not collapse.
		sink := newBufferingSink(nil, 0, false)
		sink.OnNewField(icebergx.Path{{Kind: icebergx.PathField, Name: "user"}}, "FOO", "x")
		sink.OnNewField(icebergx.Path{{Kind: icebergx.PathField, Name: "order"}}, "foo", "y")

		errs := sink.newFieldErrors()
		assert.Len(t, errs, 2)
	})
}

// TestSchemaEvolutionColumnNameNormalization verifies that when
// case_sensitive_columns=false, new column names are normalised to lowercase
// before being stored in the UnknownFieldError that drives AddColumn. Without
// this, a message keyed "EMAIL" would add an uppercase column to an otherwise
// lowercase schema, violating iceberg's recommended convention.
func TestSchemaEvolutionColumnNameNormalization(t *testing.T) {
	t.Run("buffering sink normalises to lowercase", func(t *testing.T) {
		sink := newBufferingSink(nil, 0, false)
		sink.OnNewField(icebergx.Path{}, "EMAIL", "a@x.z")

		errs := sink.newFieldErrors()
		require.Len(t, errs, 1)
		assert.Equal(t, "email", errs[0].FieldName())
	})

	t.Run("parquet sink normalises to lowercase", func(t *testing.T) {
		sink := &parquetSink{caseSensitive: false}
		sink.OnNewField(icebergx.Path{}, "EMAIL", "a@x.z")

		errs := sink.newFieldErrors()
		require.Len(t, errs, 1)
		assert.Equal(t, "email", errs[0].FieldName())
	})

	t.Run("case-sensitive mode preserves original capitalisation", func(t *testing.T) {
		sink := newBufferingSink(nil, 0, true)
		sink.OnNewField(icebergx.Path{}, "EMAIL", "a@x.z")

		errs := sink.newFieldErrors()
		require.Len(t, errs, 1)
		assert.Equal(t, "EMAIL", errs[0].FieldName())
	})
}

// TestParquetSinkNewFieldDedup mirrors TestBufferingSinkNewFieldDedup for the
// non-buffering parquet sink, which is used for unpartitioned writes.
func TestParquetSinkNewFieldDedup(t *testing.T) {
	t.Run("case-insensitive dedup folds case-only duplicates", func(t *testing.T) {
		// The buffer/writer state is unused by OnNewField, so a zero-value
		// sink with the caseSensitive flag set is sufficient for this test.
		sink := &parquetSink{caseSensitive: false}
		sink.OnNewField(icebergx.Path{}, "FOO", "x")
		sink.OnNewField(icebergx.Path{}, "foo", "y")

		require.Len(t, sink.newFieldErrors(), 1)
	})

	t.Run("case-sensitive dedup keeps both", func(t *testing.T) {
		sink := &parquetSink{caseSensitive: true}
		sink.OnNewField(icebergx.Path{}, "FOO", "x")
		sink.OnNewField(icebergx.Path{}, "foo", "y")

		assert.Len(t, sink.newFieldErrors(), 2)
	})
}

// TestBuildShredderFieldCommons verifies that the field-id → schema.Common
// map produced for the shredder correctly traverses iceberg containers
// (struct, list, map) so descendant leaf columns pick up unit metadata
// instead of being treated as the container's leaf.
func TestBuildShredderFieldCommons(t *testing.T) {
	tsCommon := schema.Common{
		Name:    "ts",
		Type:    schema.Timestamp,
		Logical: &schema.LogicalParams{Timestamp: &schema.TimestampParams{Unit: schema.TimeUnitMillis, AdjustToUTC: true}},
	}

	t.Run("nil root returns nil", func(t *testing.T) {
		s := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64},
		)
		assert.Nil(t, buildShredderFieldCommons(s, nil, false))
	})

	t.Run("flat schema registers leaf metadata", func(t *testing.T) {
		s := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.TimestampTzType{}},
		)
		root := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "id", Type: schema.Int64},
				tsCommon,
			},
		}
		got := buildShredderFieldCommons(s, &root, false)
		require.NotNil(t, got)
		require.Contains(t, got, 2)
		assert.Equal(t, schema.Timestamp, got[2].Type)
		require.NotNil(t, got[2].Logical)
		require.NotNil(t, got[2].Logical.Timestamp)
		assert.Equal(t, schema.TimeUnitMillis, got[2].Logical.Timestamp.Unit)
	})

	t.Run("nested struct registers descendant leaf", func(t *testing.T) {
		s := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "outer", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 2, Name: "inner_ts", Type: iceberg.TimestampTzType{}},
				},
			}},
		)
		root := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "outer", Type: schema.Object, Children: []schema.Common{
					{
						Name: "inner_ts", Type: schema.Timestamp,
						Logical: &schema.LogicalParams{Timestamp: &schema.TimestampParams{Unit: schema.TimeUnitMicros, AdjustToUTC: true}},
					},
				}},
			},
		}
		got := buildShredderFieldCommons(s, &root, false)
		require.Contains(t, got, 2, "inner_ts should be registered by element ID")
		assert.Equal(t, schema.TimeUnitMicros, got[2].Logical.Timestamp.Unit)
	})

	t.Run("list of timestamps registers element metadata", func(t *testing.T) {
		s := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "events", Type: &iceberg.ListType{
				ElementID: 100, Element: iceberg.TimestampTzType{}, ElementRequired: false,
			}},
		)
		root := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "events", Type: schema.Array, Children: []schema.Common{tsCommon}},
			},
		}
		got := buildShredderFieldCommons(s, &root, false)
		require.Contains(t, got, 100)
		assert.Equal(t, schema.TimeUnitMillis, got[100].Logical.Timestamp.Unit)
	})

	t.Run("map of string to timestamp registers value metadata", func(t *testing.T) {
		s := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "by_event", Type: &iceberg.MapType{
				KeyID: 100, KeyType: iceberg.PrimitiveTypes.String,
				ValueID: 101, ValueType: iceberg.TimestampTzType{}, ValueRequired: false,
			}},
		)
		root := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "by_event", Type: schema.Map, Children: []schema.Common{tsCommon}},
			},
		}
		got := buildShredderFieldCommons(s, &root, false)
		require.Contains(t, got, 101, "map value should be registered by ValueID")
		assert.NotContains(t, got, 100, "map key should not be registered")
		assert.Equal(t, schema.TimeUnitMillis, got[101].Logical.Timestamp.Unit)
	})

	t.Run("shape mismatch skips silently", func(t *testing.T) {
		// iceberg has a list, but metadata has scalar Int64 — skip the field.
		s := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "events", Type: &iceberg.ListType{
				ElementID: 100, Element: iceberg.TimestampTzType{},
			}},
		)
		root := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "events", Type: schema.Int64},
			},
		}
		got := buildShredderFieldCommons(s, &root, false)
		assert.Empty(t, got, "wrong-shape metadata should not register anything")
	})

	t.Run("case-insensitive matching", func(t *testing.T) {
		s := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "ts", Type: iceberg.TimestampTzType{}},
		)
		root := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{
					Name: "TS", Type: schema.Timestamp,
					Logical: &schema.LogicalParams{Timestamp: &schema.TimestampParams{Unit: schema.TimeUnitMicros, AdjustToUTC: true}},
				},
			},
		}
		got := buildShredderFieldCommons(s, &root, false)
		require.Contains(t, got, 1, "case-insensitive match should still register the metadata")
	})
}
