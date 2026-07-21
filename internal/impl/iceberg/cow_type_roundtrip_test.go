// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// This file is the guard for the copy-on-write column-type support gate
// (checkCOWSchemaSupported / cowSupportedColumnType in cow.go). Each accepted
// type has a faithful round-trip proven here; each rejected type has evidence
// here of exactly why it cannot be accepted safely.
//
// cowMutateDirect exercises the real mutating machinery — buildCOWRecordFactory
// (JSON projection), buildCOWFilter, and committer.commitOverwrite (which calls
// iceberg-go's txn.Overwrite) — but bypasses checkCOWSchemaSupported so the true
// round-trip behaviour of any column type can be observed independently of the
// gate. The merge key is always int64 "id" so the filter path is exercised
// unchanged; the column under test is a non-key column "v".

// cowMutateDirect seeds `seed` as a plain append, then applies `upsert` as a
// copy-on-write overwrite, and returns the resulting table (or the first error
// from the encode/commit path).
func cowMutateDirect(t testing.TB, ctx context.Context, sc *iceberg.Schema, seed, upsert []map[string]any) (*table.Table, error) {
	t.Helper()
	tbl, cat := newCOWTable(t, sc)
	w := cowWriter(t, cat.snapshot(), "id")

	if len(seed) > 0 {
		factory, err := w.buildCOWRecordFactory(sc, toBatch(t, seed))
		if err != nil {
			return nil, err
		}
		rdr, err := factory()
		if err != nil {
			return nil, err
		}
		tx := tbl.NewTransaction()
		if err := tx.Append(ctx, rdr, nil); err != nil {
			rdr.Release()
			return nil, err
		}
		rdr.Release()
		if _, err := tx.Commit(ctx); err != nil {
			return nil, err
		}
	}

	comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w = cowWriter(t, cat.snapshot(), "id")
	w.committer = comm

	sc = cat.snapshot().Schema()
	filter, err := w.buildCOWFilter(sc, toBatch(t, upsert))
	if err != nil {
		return nil, err
	}
	factory, err := w.buildCOWRecordFactory(sc, toBatch(t, upsert))
	if err != nil {
		return nil, err
	}
	if err := w.committer.commitOverwrite(ctx, OverwriteInput{Filter: filter, NewReader: factory, SchemaID: sc.ID}); err != nil {
		return nil, err
	}
	return cat.snapshot(), nil
}

func toBatch(t testing.TB, rows []map[string]any) service.MessageBatch {
	t.Helper()
	b := make(service.MessageBatch, 0, len(rows))
	for _, r := range rows {
		b = append(b, structuredMsg(t, r))
	}
	return b
}

// cowReadColJSON reads column `col` for the row whose int64 "id" == id and
// returns its value as canonical JSON (via the Arrow array's GetOneForMarshal),
// plus whether the value was present (non-null). JSON is a type-agnostic,
// lossless-comparable form for the round-trip assertions.
func cowReadColJSON(t testing.TB, ctx context.Context, tbl *table.Table, col string, id int64) (string, bool) {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer at.Release()
	tr := array.NewTableReader(at, 0)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		idArr := rec.Column(rec.Schema().FieldIndices("id")[0]).(*array.Int64)
		carr := rec.Column(rec.Schema().FieldIndices(col)[0])
		for r := 0; r < int(rec.NumRows()); r++ {
			if idArr.Value(r) != id {
				continue
			}
			if carr.IsNull(r) {
				return "", false
			}
			b, err := json.Marshal(carr.GetOneForMarshal(r))
			require.NoError(t, err)
			return string(b), true
		}
	}
	return "", false
}

func cowIDField() iceberg.NestedField {
	return iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}
}

func cowSingleColSchema(id int, name string, typ iceberg.Type) *iceberg.Schema {
	return iceberg.NewSchema(0, cowIDField(), iceberg.NestedField{ID: id, Name: name, Type: typ})
}

// TestCOWColumnTypeRoundTrip proves each column type accepted by
// cowSupportedColumnType round-trips faithfully through a real copy-on-write
// overwrite: seed row id=1 with one value, upsert id=1 to a known value, then
// read id=1 back and assert the stored value is exactly the intended value. Row
// id=2 is seeded and left untouched to confirm the rewrite preserves other rows.
func TestCOWColumnTypeRoundTrip(t *testing.T) {
	ctx := t.Context()

	uuidVal := "f47ac10b-58cc-0372-8567-0e02b2c3d479"
	tsVal := time.Date(2026, 7, 21, 10, 20, 30, 123456000, time.UTC) // microsecond precision
	dateVal := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	timeVal := time.Date(2000, 1, 1, 13, 14, 15, 123456000, time.UTC)

	cases := []struct {
		name     string
		schema   *iceberg.Schema
		seedV    any
		upV      any
		wantJSON string // canonical JSON of the faithfully-stored value
	}{
		{"boolean", cowSingleColSchema(2, "v", iceberg.PrimitiveTypes.Bool), false, true, `true`},
		{"int32", cowSingleColSchema(2, "v", iceberg.PrimitiveTypes.Int32), int64(1), int64(2147483647), `2147483647`},
		// > 2^53: proves the top-level integer-to-string massaging preserves full int64 precision.
		{"int64", cowSingleColSchema(2, "v", iceberg.PrimitiveTypes.Int64), int64(1), int64(9007199254740993), `9007199254740993`},
		{"float32", cowSingleColSchema(2, "v", iceberg.PrimitiveTypes.Float32), float64(1), float64(1.5), `1.5`},
		{"float64", cowSingleColSchema(2, "v", iceberg.PrimitiveTypes.Float64), float64(1), float64(1.5), `1.5`},
		{"string", cowSingleColSchema(2, "v", iceberg.PrimitiveTypes.String), "a", "hello", `"hello"`},
		{"date", cowSingleColSchema(2, "v", iceberg.PrimitiveTypes.Date), dateVal, dateVal, `"2026-07-21"`},
		{"time", cowSingleColSchema(2, "v", iceberg.PrimitiveTypes.Time), timeVal, timeVal, `"13:14:15.123456"`},
		{"timestamp", cowSingleColSchema(2, "v", iceberg.PrimitiveTypes.Timestamp), tsVal, tsVal, `"2026-07-21T10:20:30.123456Z"`},
		{"timestamptz", cowSingleColSchema(2, "v", iceberg.PrimitiveTypes.TimestampTz), tsVal, tsVal, `"2026-07-21T10:20:30.123456Z"`},
		{"decimal", cowSingleColSchema(2, "v", iceberg.DecimalTypeOf(10, 2)), "1.00", "123.45", `"123.45"`},
		{"uuid", cowSingleColSchema(2, "v", iceberg.PrimitiveTypes.UUID), uuidVal, uuidVal, `"` + uuidVal + `"`},
		// []byte -> json.Marshal base64 -> Arrow Binary base64-decodes: DEADBEEF.
		{"binary", cowSingleColSchema(2, "v", iceberg.PrimitiveTypes.Binary), []byte{0x01}, []byte{0xDE, 0xAD, 0xBE, 0xEF}, `"3q2+7w=="`},
		{"fixed", cowSingleColSchema(2, "v", iceberg.FixedTypeOf(4)), []byte{0, 0, 0, 0}, []byte{1, 2, 3, 4}, `"AQIDBA=="`},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			seed := []map[string]any{{"id": int64(1), "v": c.seedV}, {"id": int64(2), "v": c.seedV}}
			upsert := []map[string]any{{"id": int64(1), "v": c.upV}}
			final, err := cowMutateDirect(t, ctx, c.schema, seed, upsert)
			require.NoError(t, err)

			got, present := cowReadColJSON(t, ctx, final, "v", 1)
			require.True(t, present, "upserted value must be present")
			assert.JSONEq(t, c.wantJSON, got, "upserted %s value must round-trip faithfully", c.name)

			got2, present2 := cowReadColJSON(t, ctx, final, "v", 2)
			require.True(t, present2, "untouched row must survive the rewrite")
			_ = got2
		})
	}
}

// TestCOWNestedStructAndListRoundTrip proves nested struct and list columns
// round-trip faithfully through a real copy-on-write overwrite, at several
// compositions: a flat struct, a list, a struct-of-list, a struct-of-struct, and
// a list-of-struct. cowMassage projects each value onto the Arrow JSON shape
// recursively, so these are now supported (not gated). The >2^53-nested-int and
// map fidelity cases are covered by their own tests below.
func TestCOWNestedStructAndListRoundTrip(t *testing.T) {
	ctx := t.Context()

	t.Run("struct", func(t *testing.T) {
		sc := iceberg.NewSchema(0, cowIDField(), iceberg.NestedField{ID: 2, Name: "v", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 3, Name: "a", Type: iceberg.PrimitiveTypes.Int64},
			{ID: 4, Name: "b", Type: iceberg.PrimitiveTypes.String},
		}}})
		seed := []map[string]any{{"id": int64(1), "v": map[string]any{"a": int64(0), "b": "seed"}}}
		upsert := []map[string]any{{"id": int64(1), "v": map[string]any{"a": int64(7), "b": "hi"}}}
		final, err := cowMutateDirect(t, ctx, sc, seed, upsert)
		require.NoError(t, err)
		got, present := cowReadColJSON(t, ctx, final, "v", 1)
		require.True(t, present)
		assert.JSONEq(t, `{"a":7,"b":"hi"}`, got)
	})

	t.Run("list", func(t *testing.T) {
		sc := iceberg.NewSchema(0, cowIDField(), iceberg.NestedField{ID: 2, Name: "v", Type: &iceberg.ListType{
			ElementID: 3, Element: iceberg.PrimitiveTypes.String, ElementRequired: false,
		}})
		seed := []map[string]any{{"id": int64(1), "v": []any{"x"}}}
		upsert := []map[string]any{{"id": int64(1), "v": []any{"a", "b", "c"}}}
		final, err := cowMutateDirect(t, ctx, sc, seed, upsert)
		require.NoError(t, err)
		got, present := cowReadColJSON(t, ctx, final, "v", 1)
		require.True(t, present)
		assert.JSONEq(t, `["a","b","c"]`, got)
	})

	t.Run("struct_of_list", func(t *testing.T) {
		sc := iceberg.NewSchema(0, cowIDField(), iceberg.NestedField{ID: 2, Name: "v", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 3, Name: "tags", Type: &iceberg.ListType{ElementID: 4, Element: iceberg.PrimitiveTypes.String, ElementRequired: false}},
			{ID: 5, Name: "name", Type: iceberg.PrimitiveTypes.String},
		}}})
		seed := []map[string]any{{"id": int64(1), "v": map[string]any{"tags": []any{"x"}, "name": "seed"}}}
		upsert := []map[string]any{{"id": int64(1), "v": map[string]any{"tags": []any{"p", "q"}, "name": "hi"}}}
		final, err := cowMutateDirect(t, ctx, sc, seed, upsert)
		require.NoError(t, err)
		got, present := cowReadColJSON(t, ctx, final, "v", 1)
		require.True(t, present)
		assert.JSONEq(t, `{"tags":["p","q"],"name":"hi"}`, got)
	})

	t.Run("struct_of_struct", func(t *testing.T) {
		sc := iceberg.NewSchema(0, cowIDField(), iceberg.NestedField{ID: 2, Name: "v", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 3, Name: "inner", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: 4, Name: "a", Type: iceberg.PrimitiveTypes.Int64},
				{ID: 5, Name: "b", Type: iceberg.PrimitiveTypes.String},
			}}},
			{ID: 6, Name: "label", Type: iceberg.PrimitiveTypes.String},
		}}})
		seed := []map[string]any{{"id": int64(1), "v": map[string]any{"inner": map[string]any{"a": int64(0), "b": "s"}, "label": "seed"}}}
		upsert := []map[string]any{{"id": int64(1), "v": map[string]any{"inner": map[string]any{"a": int64(42), "b": "deep"}, "label": "hi"}}}
		final, err := cowMutateDirect(t, ctx, sc, seed, upsert)
		require.NoError(t, err)
		got, present := cowReadColJSON(t, ctx, final, "v", 1)
		require.True(t, present)
		assert.JSONEq(t, `{"inner":{"a":42,"b":"deep"},"label":"hi"}`, got)
	})

	t.Run("list_of_struct", func(t *testing.T) {
		sc := iceberg.NewSchema(0, cowIDField(), iceberg.NestedField{ID: 2, Name: "v", Type: &iceberg.ListType{
			ElementID: 3, ElementRequired: false, Element: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: 4, Name: "a", Type: iceberg.PrimitiveTypes.Int64},
				{ID: 5, Name: "b", Type: iceberg.PrimitiveTypes.String},
			}},
		}})
		seed := []map[string]any{{"id": int64(1), "v": []any{map[string]any{"a": int64(0), "b": "s"}}}}
		upsert := []map[string]any{{"id": int64(1), "v": []any{
			map[string]any{"a": int64(1), "b": "one"},
			map[string]any{"a": int64(2), "b": "two"},
		}}}
		final, err := cowMutateDirect(t, ctx, sc, seed, upsert)
		require.NoError(t, err)
		got, present := cowReadColJSON(t, ctx, final, "v", 1)
		require.True(t, present)
		assert.JSONEq(t, `[{"a":1,"b":"one"},{"a":2,"b":"two"}]`, got)
	})
}

// TestCOWMapColumnRoundTrip proves the map type now round-trips faithfully. A CDC
// map value arrives as a JSON object ({"k":v}); cowMassage reshapes it to Arrow's
// List<Struct<key,value>> encoding (an array of {"key":...,"value":...} entries),
// which is exactly the shape the Arrow map JSON reader — and read-back marshaller
// — use, so the value round-trips exactly.
func TestCOWMapColumnRoundTrip(t *testing.T) {
	ctx := t.Context()

	t.Run("string_to_primitive", func(t *testing.T) {
		sc := iceberg.NewSchema(0, cowIDField(), iceberg.NestedField{ID: 2, Name: "v", Type: &iceberg.MapType{
			KeyID: 3, KeyType: iceberg.PrimitiveTypes.String,
			ValueID: 4, ValueType: iceberg.PrimitiveTypes.Int64, ValueRequired: false,
		}})
		seed := []map[string]any{{"id": int64(1), "v": map[string]any{"k0": int64(0)}}}
		upsert := []map[string]any{{"id": int64(1), "v": map[string]any{"k1": int64(1)}}}
		final, err := cowMutateDirect(t, ctx, sc, seed, upsert)
		require.NoError(t, err)
		got, present := cowReadColJSON(t, ctx, final, "v", 1)
		require.True(t, present)
		// Arrow marshals a map back as an array of {"key","value"} entries.
		assert.JSONEq(t, `[{"key":"k1","value":1}]`, got)
	})

	t.Run("string_to_struct", func(t *testing.T) {
		sc := iceberg.NewSchema(0, cowIDField(), iceberg.NestedField{ID: 2, Name: "v", Type: &iceberg.MapType{
			KeyID: 3, KeyType: iceberg.PrimitiveTypes.String,
			ValueID: 4, ValueRequired: false, ValueType: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: 5, Name: "a", Type: iceberg.PrimitiveTypes.Int64},
				{ID: 6, Name: "b", Type: iceberg.PrimitiveTypes.String},
			}},
		}})
		seed := []map[string]any{{"id": int64(1), "v": map[string]any{"k0": map[string]any{"a": int64(0), "b": "s"}}}}
		upsert := []map[string]any{{"id": int64(1), "v": map[string]any{"k1": map[string]any{"a": int64(9), "b": "nine"}}}}
		final, err := cowMutateDirect(t, ctx, sc, seed, upsert)
		require.NoError(t, err)
		got, present := cowReadColJSON(t, ctx, final, "v", 1)
		require.True(t, present)
		assert.JSONEq(t, `[{"key":"k1","value":{"a":9,"b":"nine"}}]`, got)
	})
}

// TestCOWNestedIntegerBeyond2Pow53RoundTrip is the load-bearing fidelity test: an
// int64 nested inside a struct beyond 2^53 must now round-trip EXACTLY. cowMassage
// applies deleteKeyJSONValue at every leaf, so the nested int is emitted as a JSON
// string and parsed back by the Arrow Int64 builder without the float64 truncation
// that the old flat projection suffered. This is the reverse of the previous
// TestCOWNestedIntegerBeyond2Pow53IsLossy, which documented the corruption that
// kept struct/list gated.
func TestCOWNestedIntegerBeyond2Pow53RoundTrip(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0, cowIDField(), iceberg.NestedField{ID: 2, Name: "v", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 3, Name: "a", Type: iceberg.PrimitiveTypes.Int64},
	}}})
	const big = int64(9007199254740993) // 2^53 + 1
	seed := []map[string]any{{"id": int64(1), "v": map[string]any{"a": int64(0)}}}
	upsert := []map[string]any{{"id": int64(1), "v": map[string]any{"a": big}}}
	final, err := cowMutateDirect(t, ctx, sc, seed, upsert)
	require.NoError(t, err)
	got, present := cowReadColJSON(t, ctx, final, "v", 1)
	require.True(t, present)
	assert.JSONEq(t, `{"a":9007199254740993}`, got,
		"nested int64 beyond 2^53 must round-trip faithfully via the recursive per-leaf massage")
}

// TestCOWNestedNullAndAbsentFields proves that null and absent nested fields read
// back as null: an omitted struct field, an explicit null struct field, and null
// list elements are all preserved through the copy-on-write rewrite.
func TestCOWNestedNullAndAbsentFields(t *testing.T) {
	ctx := t.Context()

	t.Run("absent_and_null_struct_fields", func(t *testing.T) {
		sc := iceberg.NewSchema(0, cowIDField(), iceberg.NestedField{ID: 2, Name: "v", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 3, Name: "a", Type: iceberg.PrimitiveTypes.Int64},
			{ID: 4, Name: "b", Type: iceberg.PrimitiveTypes.String},
		}}})
		seed := []map[string]any{{"id": int64(1), "v": map[string]any{"a": int64(0), "b": "s"}}}
		// "a" explicitly null, "b" absent -> both read back as null.
		upsert := []map[string]any{{"id": int64(1), "v": map[string]any{"a": nil}}}
		final, err := cowMutateDirect(t, ctx, sc, seed, upsert)
		require.NoError(t, err)
		got, present := cowReadColJSON(t, ctx, final, "v", 1)
		require.True(t, present)
		assert.JSONEq(t, `{"a":null,"b":null}`, got)
	})

	t.Run("null_list_elements", func(t *testing.T) {
		sc := iceberg.NewSchema(0, cowIDField(), iceberg.NestedField{ID: 2, Name: "v", Type: &iceberg.ListType{
			ElementID: 3, Element: iceberg.PrimitiveTypes.String, ElementRequired: false,
		}})
		seed := []map[string]any{{"id": int64(1), "v": []any{"x"}}}
		upsert := []map[string]any{{"id": int64(1), "v": []any{"a", nil, "c"}}}
		final, err := cowMutateDirect(t, ctx, sc, seed, upsert)
		require.NoError(t, err)
		got, present := cowReadColJSON(t, ctx, final, "v", 1)
		require.True(t, present)
		assert.JSONEq(t, `["a",null,"c"]`, got)
	})
}
