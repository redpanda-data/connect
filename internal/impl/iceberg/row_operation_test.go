// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestParseRowOperation(t *testing.T) {
	for _, in := range []string{"insert", "upsert", "delete"} {
		op, err := parseRowOperation(in)
		require.NoError(t, err)
		assert.Equal(t, rowOperation(in), op)
	}
	for _, in := range []string{"", "index", "c", "INSERT", "create"} {
		_, err := parseRowOperation(in)
		assert.Error(t, err, "expected %q to be rejected", in)
	}
}

func mustInterp(t testing.TB, expr string) *service.InterpolatedString {
	t.Helper()
	i, err := service.NewInterpolatedString(expr)
	require.NoError(t, err)
	return i
}

func TestRowOpConfigMutating(t *testing.T) {
	cases := []struct {
		name string
		cfg  RowOpConfig
		want bool
	}{
		{"nil operation", RowOpConfig{}, false},
		{"static insert", RowOpConfig{Operation: mustInterp(t, "insert")}, false},
		{"static empty", RowOpConfig{Operation: mustInterp(t, "")}, false},
		{"static upsert", RowOpConfig{Operation: mustInterp(t, "upsert")}, true},
		{"static delete", RowOpConfig{Operation: mustInterp(t, "delete")}, true},
		{"dynamic", RowOpConfig{Operation: mustInterp(t, `${! json("op") }`)}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.cfg.mutating())
		})
	}
}

func parseTestRowOpConfig(t *testing.T, snippet string) (RowOpConfig, error) {
	t.Helper()
	yaml := `
catalog:
  url: http://localhost:8181/api/catalog
namespace: ns
table: t
storage:
  aws_s3:
    bucket: bucket
` + snippet
	conf, err := icebergOutputConfig().ParseYAML(yaml, nil)
	require.NoError(t, err, "config should pass spec validation")
	return parseRowOpConfig(conf)
}

func TestParseRowOpConfigValidation(t *testing.T) {
	t.Run("default is insert and non-mutating", func(t *testing.T) {
		cfg, err := parseTestRowOpConfig(t, "")
		require.NoError(t, err)
		static, ok := cfg.Operation.Static()
		require.True(t, ok)
		assert.Equal(t, "insert", static)
		assert.False(t, cfg.mutating())
		assert.Empty(t, cfg.IdentifierFields)
	})

	t.Run("static upsert without identifier_fields errors", func(t *testing.T) {
		_, err := parseTestRowOpConfig(t, "row_operation: upsert\n")
		require.Error(t, err)
		assert.Contains(t, err.Error(), ioFieldIdentifierFields)
	})

	t.Run("static delete with identifier_fields ok", func(t *testing.T) {
		cfg, err := parseTestRowOpConfig(t, "row_operation: delete\nidentifier_fields: [id]\n")
		require.NoError(t, err)
		assert.Equal(t, []string{"id"}, cfg.IdentifierFields)
		assert.True(t, cfg.mutating())
	})

	t.Run("static unknown value errors", func(t *testing.T) {
		_, err := parseTestRowOpConfig(t, "row_operation: frobnicate\n")
		require.Error(t, err)
		assert.Contains(t, err.Error(), ioFieldRowOperation)
	})

	t.Run("dynamic operation defers validation to runtime", func(t *testing.T) {
		// No identifier_fields, but the operation is interpolated so it cannot be
		// validated at parse time — must not error here.
		cfg, err := parseTestRowOpConfig(t, "row_operation: '${! json(\"op\") }'\n")
		require.NoError(t, err)
		_, ok := cfg.Operation.Static()
		assert.False(t, ok)
	})
}

func structuredMsg(t testing.TB, v map[string]any) *service.Message {
	t.Helper()
	msg := service.NewMessage(nil)
	msg.SetStructuredMut(v)
	return msg
}

func TestSplitByOperation(t *testing.T) {
	w := &writer{
		caseSensitive: true,
		rowOpCfg: RowOpConfig{
			Operation:        mustInterp(t, `${! json("op") }`),
			IdentifierFields: []string{"id"},
		},
	}

	batch := service.MessageBatch{
		structuredMsg(t, map[string]any{"op": "insert", "id": 1}),
		structuredMsg(t, map[string]any{"op": "upsert", "id": 2}),
		structuredMsg(t, map[string]any{"op": "delete", "id": 3}),
		structuredMsg(t, map[string]any{"op": "", "id": 4}), // empty defaults to insert
	}

	inserts, deletes, counts, err := w.splitByOperation(batch)
	require.NoError(t, err)
	// insert(1) + upsert(2) + default-insert(4) => 3 inserts; upsert(2) + delete(3) => 2 deletes.
	assert.Len(t, inserts, 3)
	assert.Len(t, deletes, 2)
	assert.Equal(t, opCounts{inserted: 2, upserted: 1, deleted: 1}, counts)
}

func TestSplitByOperationUnknownValueErrors(t *testing.T) {
	w := &writer{
		caseSensitive: true,
		rowOpCfg: RowOpConfig{
			Operation:        mustInterp(t, `${! json("op") }`),
			IdentifierFields: []string{"id"},
		},
	}
	batch := service.MessageBatch{structuredMsg(t, map[string]any{"op": "c", "id": 1})}
	_, _, _, err := w.splitByOperation(batch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "c")
}

func TestSplitByOperationCollapsesPerKey(t *testing.T) {
	w := &writer{
		caseSensitive: true,
		rowOpCfg: RowOpConfig{
			Operation:        mustInterp(t, `${! json("op") }`),
			IdentifierFields: []string{"id"},
		},
	}
	batch := service.MessageBatch{
		structuredMsg(t, map[string]any{"op": "upsert", "id": 1, "v": "a"}),
		structuredMsg(t, map[string]any{"op": "upsert", "id": 1, "v": "b"}), // same key, later wins
		structuredMsg(t, map[string]any{"op": "insert", "id": 9, "v": "x"}), // non-keyed append
		structuredMsg(t, map[string]any{"op": "upsert", "id": 2, "v": "c"}),
		structuredMsg(t, map[string]any{"op": "delete", "id": 2}), // upsert-then-delete same key: delete wins
	}

	inserts, deletes, counts, err := w.splitByOperation(batch)
	require.NoError(t, err)

	// One equality delete per keyed key {1, 2}.
	require.Len(t, deletes, 2)

	// Counts are post-collapse: the two upserts to key 1 count as one upsert
	// (not two), key 2 collapses to a single delete, and id=9 is one insert.
	assert.Equal(t, opCounts{inserted: 1, upserted: 1, deleted: 1}, counts)

	// Data rows: the insert (id=9) and the surviving upsert for key 1 (v="b").
	// Key 2's final op is delete, so it contributes no data row.
	got := map[int]string{}
	for _, m := range inserts {
		s, err := m.AsStructured()
		require.NoError(t, err)
		row := s.(map[string]any)
		got[row["id"].(int)] = row["v"].(string)
	}
	assert.Equal(t, map[int]string{1: "b", 9: "x"}, got)
}

func TestSplitByOperationMissingKeyErrors(t *testing.T) {
	w := &writer{
		caseSensitive: true,
		rowOpCfg: RowOpConfig{
			Operation:        mustInterp(t, `${! json("op") }`),
			IdentifierFields: []string{"id"},
		},
	}
	batch := service.MessageBatch{structuredMsg(t, map[string]any{"op": "upsert", "other": 1})}
	_, _, _, err := w.splitByOperation(batch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing or null")
}

func TestSplitByOperationUpsertRequiresIdentifierFields(t *testing.T) {
	w := &writer{
		caseSensitive: true,
		rowOpCfg: RowOpConfig{
			Operation: mustInterp(t, `${! json("op") }`), // dynamic, so reaches the split
		},
	}
	batch := service.MessageBatch{structuredMsg(t, map[string]any{"op": "upsert", "id": 1})}
	_, _, _, err := w.splitByOperation(batch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), ioFieldIdentifierFields)
}

func TestDeleteKeyJSONValue(t *testing.T) {
	// int64 beyond float64 precision must survive as an exact string.
	v, err := deleteKeyJSONValue(iceberg.PrimitiveTypes.Int64, int64(9007199254740993))
	require.NoError(t, err)
	assert.Equal(t, "9007199254740993", v)

	// decimal is emitted as a string to avoid float rounding.
	v, err = deleteKeyJSONValue(iceberg.DecimalTypeOf(10, 2), 123.45)
	require.NoError(t, err)
	assert.Equal(t, "123.45", v)

	// time.Time into a timestamp column becomes RFC3339.
	tm := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	v, err = deleteKeyJSONValue(iceberg.PrimitiveTypes.Timestamp, tm)
	require.NoError(t, err)
	assert.Equal(t, "2026-06-24T12:00:00Z", v)

	// strings and other primitives pass through unchanged.
	v, err = deleteKeyJSONValue(iceberg.PrimitiveTypes.String, "abc")
	require.NoError(t, err)
	assert.Equal(t, "abc", v)

	// a non-integer value into an integer column is a hard error.
	_, err = deleteKeyJSONValue(iceberg.PrimitiveTypes.Int64, 1.5)
	require.Error(t, err)

	// an integer-valued float64 within float64's exact-integer range is fine.
	v, err = deleteKeyJSONValue(iceberg.PrimitiveTypes.Int64, float64(1_000_000_000_000_000))
	require.NoError(t, err)
	assert.Equal(t, "1000000000000000", v)

	// a float64 at/beyond 2^53 cannot be represented exactly as an integer, so
	// int64(n) would silently corrupt or overflow the delete key — reject it.
	_, err = deleteKeyJSONValue(iceberg.PrimitiveTypes.Int64, float64(uint64(1)<<53))
	require.Error(t, err)
	_, err = deleteKeyJSONValue(iceberg.PrimitiveTypes.Int64, 9.3e18) // > 2^63, would overflow int64
	require.Error(t, err)

	// a bare numeric value into a temporal column is a hard error: its unit is
	// ambiguous and would silently mismatch the data written by the insert path.
	_, err = deleteKeyJSONValue(iceberg.PrimitiveTypes.Timestamp, int64(1700000000000))
	require.Error(t, err)

	// timestamptz formats identically to timestamp, normalised to UTC.
	v, err = deleteKeyJSONValue(iceberg.PrimitiveTypes.TimestampTz, tm)
	require.NoError(t, err)
	assert.Equal(t, "2026-06-24T12:00:00Z", v)

	// a date column keeps only the calendar date (UTC), dropping any time-of-day.
	v, err = deleteKeyJSONValue(iceberg.PrimitiveTypes.Date, time.Date(2026, 6, 24, 18, 30, 0, 0, time.UTC))
	require.NoError(t, err)
	assert.Equal(t, "2026-06-24", v)

	// a time column keeps only the wall-clock time (UTC), to nanosecond precision.
	v, err = deleteKeyJSONValue(iceberg.PrimitiveTypes.Time, time.Date(1970, 1, 1, 15, 4, 5, 123456789, time.UTC))
	require.NoError(t, err)
	assert.Equal(t, "15:04:05.123456789", v)

	// bare numbers into date/time columns are rejected for the same ambiguity reason.
	_, err = deleteKeyJSONValue(iceberg.PrimitiveTypes.Date, int64(20000))
	require.Error(t, err)
	_, err = deleteKeyJSONValue(iceberg.PrimitiveTypes.Time, int64(54_000_000_000))
	require.Error(t, err)
}

func TestDeleteRecordFieldsAcceptsPrimitivesRejectsNested(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		iceberg.NestedField{ID: 2, Name: "uid", Type: iceberg.PrimitiveTypes.UUID, Required: true},
		iceberg.NestedField{ID: 3, Name: "amt", Type: iceberg.DecimalTypeOf(10, 2), Required: true},
		iceberg.NestedField{ID: 4, Name: "tags", Type: &iceberg.ListType{ElementID: 5, Element: iceberg.PrimitiveTypes.String, ElementRequired: false}, Required: false},
	)
	unpartitioned := iceberg.UnpartitionedSpec

	// Previously-gated primitive types are now accepted as delete keys.
	for _, name := range []string{"ts", "uid", "amt"} {
		w := &writer{caseSensitive: true, rowOpCfg: RowOpConfig{IdentifierFields: []string{name}}}
		_, _, err := w.deleteRecordFields(sc, unpartitioned)
		require.NoErrorf(t, err, "%s should be accepted as a delete key", name)
	}

	// A non-primitive (list) column is rejected — a fundamental Iceberg constraint.
	w := &writer{caseSensitive: true, rowOpCfg: RowOpConfig{IdentifierFields: []string{"tags"}}}
	_, _, err := w.deleteRecordFields(sc, unpartitioned)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-primitive")
}

func eqDelTestSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp},
	)
}

func identitySpec(name string, sourceID, fieldID int) iceberg.PartitionSpec {
	return iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{sourceID}, FieldID: fieldID, Name: name, Transform: iceberg.IdentityTransform{},
	})
}

func TestDeleteRecordFields(t *testing.T) {
	sc := eqDelTestSchema()
	unpartitioned := iceberg.UnpartitionedSpec

	t.Run("unpartitioned identifier only", func(t *testing.T) {
		w := &writer{caseSensitive: true, rowOpCfg: RowOpConfig{IdentifierFields: []string{"id"}}}
		fields, eq, err := w.deleteRecordFields(sc, unpartitioned)
		require.NoError(t, err)
		require.Len(t, fields, 1)
		assert.Equal(t, "id", fields[0].Name)
		assert.Equal(t, []int{1}, eq)
	})

	t.Run("partition source outside identifier_fields is rejected", func(t *testing.T) {
		// Partitioned by region (field 2) but identifier is only id (field 1):
		// region can change between insert and delete, so deletes would miss.
		w := &writer{caseSensitive: true, rowOpCfg: RowOpConfig{IdentifierFields: []string{"id"}}}
		spec := identitySpec("region", 2, 1000)
		_, _, err := w.deleteRecordFields(sc, &spec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), ioFieldIdentifierFields)
	})

	t.Run("partition source that is an identifier field is allowed", func(t *testing.T) {
		w := &writer{caseSensitive: true, rowOpCfg: RowOpConfig{IdentifierFields: []string{"id"}}}
		spec := identitySpec("id_part", 1, 1000) // partition derived from the id key
		fields, eq, err := w.deleteRecordFields(sc, &spec)
		require.NoError(t, err)
		require.Len(t, fields, 1)
		assert.Equal(t, []int{1}, eq)
	})

	t.Run("floating-point identifier is rejected", func(t *testing.T) {
		floatSc := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "score", Type: iceberg.PrimitiveTypes.Float64, Required: true},
		)
		w := &writer{caseSensitive: true, rowOpCfg: RowOpConfig{IdentifierFields: []string{"score"}}}
		_, _, err := w.deleteRecordFields(floatSc, unpartitioned)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "floating-point")
	})

	t.Run("missing identifier column errors", func(t *testing.T) {
		w := &writer{caseSensitive: true, rowOpCfg: RowOpConfig{IdentifierFields: []string{"nope"}}}
		_, _, err := w.deleteRecordFields(sc, unpartitioned)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("case-insensitive resolves, case-sensitive does not", func(t *testing.T) {
		wCI := &writer{caseSensitive: false, rowOpCfg: RowOpConfig{IdentifierFields: []string{"ID"}}}
		fields, _, err := wCI.deleteRecordFields(sc, unpartitioned)
		require.NoError(t, err)
		require.Len(t, fields, 1)
		assert.Equal(t, "id", fields[0].Name)

		wCS := &writer{caseSensitive: true, rowOpCfg: RowOpConfig{IdentifierFields: []string{"ID"}}}
		_, _, err = wCS.deleteRecordFields(sc, unpartitioned)
		require.Error(t, err)
	})

	t.Run("multiple identifier fields preserve order", func(t *testing.T) {
		w := &writer{caseSensitive: true, rowOpCfg: RowOpConfig{IdentifierFields: []string{"region", "id"}}}
		fields, eq, err := w.deleteRecordFields(sc, unpartitioned)
		require.NoError(t, err)
		require.Len(t, fields, 2)
		assert.Equal(t, "region", fields[0].Name)
		assert.Equal(t, "id", fields[1].Name)
		assert.Equal(t, []int{2, 1}, eq)
	})
}

func TestLookupField(t *testing.T) {
	row := map[string]any{"ID": 7, "name": "x"}

	v, ok := lookupField(row, "ID", true)
	assert.True(t, ok)
	assert.Equal(t, 7, v)

	_, ok = lookupField(row, "id", true)
	assert.False(t, ok, "case-sensitive lookup should not match different case")

	v, ok = lookupField(row, "id", false)
	assert.True(t, ok, "case-insensitive lookup should match")
	assert.Equal(t, 7, v)

	_, ok = lookupField(row, "missing", false)
	assert.False(t, ok)
}

func TestSchemaWithIdentifierFields(t *testing.T) {
	strField := func(id int, name string) iceberg.NestedField {
		return iceberg.NestedField{ID: id, Name: name, Type: iceberg.PrimitiveTypes.String, Required: false}
	}

	t.Run("no identifier fields leaves schema unchanged", func(t *testing.T) {
		r := &Router{caseSensitive: true}
		sc, err := r.schemaWithIdentifierFields([]iceberg.NestedField{strField(1, "id"), strField(2, "v")})
		require.NoError(t, err)
		assert.Empty(t, sc.IdentifierFieldIDs)
		f, ok := sc.FindFieldByName("id")
		require.True(t, ok)
		assert.False(t, f.Required, "columns stay optional when no identifier is configured")
	})

	t.Run("marks identifier required and registers its id", func(t *testing.T) {
		r := &Router{caseSensitive: true, rowOpCfg: RowOpConfig{IdentifierFields: []string{"id"}}}
		sc, err := r.schemaWithIdentifierFields([]iceberg.NestedField{strField(1, "id"), strField(2, "v")})
		require.NoError(t, err)
		assert.Equal(t, []int{1}, sc.IdentifierFieldIDs)
		id, ok := sc.FindFieldByName("id")
		require.True(t, ok)
		assert.True(t, id.Required, "identifier column must be required")
		v, ok := sc.FindFieldByName("v")
		require.True(t, ok)
		assert.False(t, v.Required, "non-identifier columns stay optional")
	})

	t.Run("composite keys register every id", func(t *testing.T) {
		r := &Router{caseSensitive: true, rowOpCfg: RowOpConfig{IdentifierFields: []string{"tenant", "id"}}}
		sc, err := r.schemaWithIdentifierFields([]iceberg.NestedField{strField(1, "tenant"), strField(2, "id"), strField(3, "v")})
		require.NoError(t, err)
		assert.ElementsMatch(t, []int{1, 2}, sc.IdentifierFieldIDs)
	})

	t.Run("absent identifier column is a hard error", func(t *testing.T) {
		r := &Router{caseSensitive: true, rowOpCfg: RowOpConfig{IdentifierFields: []string{"missing"}}}
		_, err := r.schemaWithIdentifierFields([]iceberg.NestedField{strField(1, "id")})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not present in the table being created")
	})

	t.Run("non-primitive identifier is rejected", func(t *testing.T) {
		structType := &iceberg.StructType{FieldList: []iceberg.NestedField{strField(2, "inner")}}
		r := &Router{caseSensitive: true, rowOpCfg: RowOpConfig{IdentifierFields: []string{"obj"}}}
		_, err := r.schemaWithIdentifierFields([]iceberg.NestedField{{ID: 1, Name: "obj", Type: structType}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-primitive")
	})

	t.Run("floating-point identifier is rejected", func(t *testing.T) {
		r := &Router{caseSensitive: true, rowOpCfg: RowOpConfig{IdentifierFields: []string{"amt"}}}
		_, err := r.schemaWithIdentifierFields([]iceberg.NestedField{{ID: 1, Name: "amt", Type: iceberg.PrimitiveTypes.Float64}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "floating-point")
	})

	t.Run("case-insensitive identifier match", func(t *testing.T) {
		r := &Router{caseSensitive: false, rowOpCfg: RowOpConfig{IdentifierFields: []string{"ID"}}}
		sc, err := r.schemaWithIdentifierFields([]iceberg.NestedField{strField(1, "id")})
		require.NoError(t, err)
		assert.Equal(t, []int{1}, sc.IdentifierFieldIDs)
		f, ok := sc.FindFieldByName("id")
		require.True(t, ok)
		assert.True(t, f.Required)
	})
}

// TestMaxInFlightOrderingLint pins the cross-field lint rule that protects keyed
// (upsert/delete) workloads from out-of-order commits: a non-insert row_operation
// combined with max_in_flight > 1 must produce a lint error, while append-only
// configs and max_in_flight: 1 must not.
func TestMaxInFlightOrderingLint(t *testing.T) {
	linter := service.GlobalEnvironment().NewComponentConfigLinter()
	base := func(extra string) string {
		return `
iceberg:
  catalog:
    url: http://localhost:8181/api/catalog
  namespace: ns
  table: t
  storage:
    aws_s3:
      bucket: b
` + extra
	}

	cases := []struct {
		name     string
		extra    string
		wantLint bool
	}{
		{"append-only default", "", false},
		{"static insert with high in-flight", "  row_operation: insert\n  max_in_flight: 8\n", false},
		{"static upsert at default in-flight", "  row_operation: upsert\n  identifier_fields: [id]\n", true},
		{"static upsert with in-flight 1", "  row_operation: upsert\n  identifier_fields: [id]\n  max_in_flight: 1\n", false},
		{"dynamic operation at default in-flight", "  row_operation: '${! metadata(\"op\") }'\n  identifier_fields: [id]\n", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lints, err := linter.LintOutputYAML([]byte(base(tc.extra)))
			require.NoError(t, err)

			var ordering []string
			for _, l := range lints {
				if strings.Contains(l.Error(), "max_in_flight") {
					ordering = append(ordering, l.Error())
				}
			}
			if tc.wantLint {
				assert.NotEmpty(t, ordering, "expected a max_in_flight ordering lint, got lints: %v", lints)
			} else {
				assert.Empty(t, ordering, "expected no ordering lint")
			}
		})
	}
}
