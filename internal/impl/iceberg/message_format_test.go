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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestArrayToRow(t *testing.T) {
	columns := []string{"id", "symbol", "price"}
	cases := []struct {
		name    string
		arr     []any
		want    map[string]any
		wantErr string
	}{
		{name: "all columns", arr: []any{1, "IBM", "1.5"}, want: map[string]any{"id": 1, "symbol": "IBM", "price": "1.5"}},
		{name: "trailing columns absent", arr: []any{1}, want: map[string]any{"id": 1}},
		{name: "empty", arr: []any{}, want: map[string]any{}},
		{name: "too many elements", arr: []any{1, "IBM", "1.5", "extra"}, wantErr: "array has 4 elements but the table has 3 columns"},
		{name: "explicit null", arr: []any{nil, "IBM"}, want: map[string]any{"id": nil, "symbol": "IBM"}},
		{name: "bytes become text", arr: []any{[]byte("1"), []byte("IBM"), []any{[]byte("n")}}, want: map[string]any{"id": "1", "symbol": "IBM", "price": []any{[]byte("n")}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := arrayToRow(tc.arr, columns)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestArrayColumnsRemapped(t *testing.T) {
	cases := []struct {
		name      string
		prev, cur []string
		want      bool
	}{
		{name: "first writer", prev: nil, cur: []string{"a", "b"}, want: false},
		{name: "unchanged", prev: []string{"a", "b"}, cur: []string{"a", "b"}, want: false},
		{name: "column appended", prev: []string{"a", "b"}, cur: []string{"a", "b", "c"}, want: false},
		{name: "reordered", prev: []string{"a", "b"}, cur: []string{"b", "a"}, want: true},
		{name: "dropped then added", prev: []string{"a", "b", "c"}, cur: []string{"a", "c", "d"}, want: true},
		{name: "last column dropped", prev: []string{"a", "b"}, cur: []string{"a"}, want: true},
		{name: "renamed", prev: []string{"a", "b"}, cur: []string{"a", "x"}, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, arrayColumnsRemapped(tc.prev, tc.cur))
		})
	}
}

func arrayModeSchema() (*iceberg.Schema, iceberg.PartitionSpec) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "symbol", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "price", Type: iceberg.DecimalTypeOf(18, 6)},
		iceberg.NestedField{ID: 4, Name: "pos", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 5, Name: "x", Type: iceberg.PrimitiveTypes.Int64},
		}}},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "symbol", Transform: iceberg.IdentityTransform{},
	})
	return sc, spec
}

// newArrayModeWriter builds an array-mode writer over a fresh table, with its
// column order taken from the table schema exactly as createWriter does. A
// non-empty strategy makes the row operation come from the "op" metadata key,
// keyed on id, and leaves the table unpartitioned: merge-on-read rejects
// upserts when a partition source column is not an identifier field.
func newArrayModeWriter(t *testing.T, strategy mergeStrategy) (*writer, *memCatalog) {
	t.Helper()
	sc, spec := arrayModeSchema()
	if strategy != "" {
		spec = *iceberg.UnpartitionedSpec
	}
	tbl, cat := newPartitionedCOWTable(t, sc, spec)
	require.NoError(t, os.MkdirAll(filepath.Join(tbl.Location(), "data"), 0o755))

	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	t.Cleanup(comm.Close)

	w := &writer{
		table:         cat.snapshot(),
		committer:     comm,
		caseSensitive: true,
		logger:        service.MockResources().Logger(),
		messageFormat: MessageFormatArray,
		arrayColumns:  topLevelColumnNames(sc),
	}
	if strategy != "" {
		w.rowOpCfg = RowOpConfig{
			Operation:        mustInterp(t, `${! metadata("op") }`),
			IdentifierFields: []string{"id"},
			MergeStrategy:    strategy,
		}
	}
	return w, cat
}

func arrayMsg(op, body string) *service.Message {
	msg := service.NewMessage([]byte(body))
	if op != "" {
		msg.MetaSetMut("op", op)
	}
	return msg
}

// scanArrayModeRows renders every row as "id,symbol,price,pos", sorted. The
// decimal is printed at its column scale and the rest with arrow's own value
// formatting, so struct nulls are visible.
func scanArrayModeRows(t *testing.T, ctx context.Context, tbl *table.Table) []string {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer at.Release()

	var out []string
	tr := array.NewTableReader(at, 0)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		for r := 0; r < int(rec.NumRows()); r++ {
			cells := make([]string, 0, rec.NumCols())
			for _, name := range []string{"id", "symbol", "price", "pos"} {
				col := rec.Column(rec.Schema().FieldIndices(name)[0])
				if dec, ok := col.(*array.Decimal128); ok && dec.IsValid(r) {
					cells = append(cells, dec.Value(r).ToString(dec.DataType().(*arrow.Decimal128Type).Scale))
					continue
				}
				cells = append(cells, col.ValueStr(r))
			}
			out = append(out, strings.Join(cells, ","))
		}
	}
	sort.Strings(out)
	return out
}

func TestWriterArrayMode(t *testing.T) {
	ctx := t.Context()
	seed := []string{`["1","IBM","123.456789",{"x":"5"}]`, `["2","AAPL","1"]`}
	seedRows := []string{
		`1,IBM,123.456789,{"x":5}`,
		`2,AAPL,1.000000,(null)`,
	}

	t.Run("insert", func(t *testing.T) {
		w, cat := newArrayModeWriter(t, "")
		batch := service.MessageBatch{arrayMsg("", seed[0]), arrayMsg("", seed[1])}
		require.NoError(t, w.Write(ctx, batch))
		assert.Equal(t, seedRows, scanArrayModeRows(t, ctx, cat.snapshot()))
	})

	t.Run("non-array message", func(t *testing.T) {
		w, _ := newArrayModeWriter(t, "")
		err := w.Write(ctx, service.MessageBatch{arrayMsg("", `{"id":1}`)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "message_format is array but message is map[string]interface {}")
	})

	t.Run("non-json message", func(t *testing.T) {
		w, _ := newArrayModeWriter(t, "")
		err := w.Write(ctx, service.MessageBatch{arrayMsg("", "1|IBM|1.5")})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "message_format is array: ")
	})

	t.Run("null required column", func(t *testing.T) {
		w, _ := newArrayModeWriter(t, "")
		err := w.Write(ctx, service.MessageBatch{arrayMsg("", `[null,"IBM"]`)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `missing required field "id"`)
	})

	for _, strategy := range []mergeStrategy{mergeStrategyMOR, mergeStrategyCOW} {
		t.Run(string(strategy), func(t *testing.T) {
			t.Run("upsert replaces row", func(t *testing.T) {
				w, cat := newArrayModeWriter(t, strategy)
				require.NoError(t, w.Write(ctx, service.MessageBatch{
					arrayMsg("insert", seed[0]), arrayMsg("insert", seed[1]),
				}))
				require.NoError(t, w.Write(ctx, service.MessageBatch{arrayMsg("upsert", `["1","IBM","9"]`)}))
				assert.Equal(t, []string{
					`1,IBM,9.000000,(null)`,
					`2,AAPL,1.000000,(null)`,
				}, scanArrayModeRows(t, ctx, cat.snapshot()))
			})

			t.Run("empty array has no key", func(t *testing.T) {
				w, _ := newArrayModeWriter(t, strategy)
				err := w.Write(ctx, service.MessageBatch{arrayMsg("upsert", `[]`)})
				require.Error(t, err)
				assert.Contains(t, err.Error(), `identifier_fields "id" is missing or null`)
			})
		})
	}
}

// TestWriterArrayModeFromBloblangSplit drives the mapping users write for
// delimited text. content().split yields []byte elements, which must be parsed
// as text: a decimal(18,2) column is 8 bytes wide, so the 8-character
// "12345.67" would otherwise be stored verbatim as the unscaled value.
func TestWriterArrayModeFromBloblangSplit(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "symbol", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "price", Type: iceberg.DecimalTypeOf(18, 2)},
		iceberg.NestedField{ID: 4, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp},
	)
	tbl, cat := newPartitionedCOWTable(t, sc, *iceberg.UnpartitionedSpec)
	require.NoError(t, os.MkdirAll(filepath.Join(tbl.Location(), "data"), 0o755))
	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	t.Cleanup(comm.Close)
	w := &writer{
		table:           cat.snapshot(),
		committer:       comm,
		caseSensitive:   true,
		logger:          service.MockResources().Logger(),
		messageFormat:   MessageFormatArray,
		arrayColumns:    topLevelColumnNames(sc),
		timestampLayout: "2006-01-02 15:04:05.999999999",
	}

	exec, err := bloblang.Parse(`root = content().split("|")`)
	require.NoError(t, err)
	msg, err := service.NewMessage([]byte("1|IBM|12345.67|2025-05-29 11:54:26.721451319")).BloblangQuery(exec)
	require.NoError(t, err)
	structured, err := msg.AsStructured()
	require.NoError(t, err)
	require.IsType(t, []byte(nil), structured.([]any)[0], "the mapping must yield []byte elements for this test to mean anything")

	require.NoError(t, w.Write(ctx, service.MessageBatch{msg}))

	at, err := cat.snapshot().Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer at.Release()
	require.Equal(t, int64(1), at.NumRows())
	tr := array.NewTableReader(at, 0)
	defer tr.Release()
	require.True(t, tr.Next())
	rec := tr.RecordBatch()
	col := func(name string) arrow.Array { return rec.Column(rec.Schema().FieldIndices(name)[0]) }

	assert.Equal(t, int64(1), col("id").(*array.Int64).Value(0))
	assert.Equal(t, "IBM", col("symbol").(*array.String).Value(0))
	price := col("price").(*array.Decimal128)
	assert.Equal(t, "12345.67", price.Value(0).ToString(price.DataType().(*arrow.Decimal128Type).Scale))
	wantTS := time.Date(2025, 5, 29, 11, 54, 26, 721451000, time.UTC).UnixMicro()
	assert.Equal(t, wantTS, int64(col("ts").(*array.Timestamp).Value(0)))
}
