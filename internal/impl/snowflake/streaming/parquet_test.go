// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package streaming

import (
	"bytes"
	"io"
	"testing"

	"github.com/aws/smithy-go/ptr"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming/int128"
)

func msg(s string) *service.Message {
	return service.NewMessage([]byte(s))
}

func TestWriteParquet(t *testing.T) {
	batch := service.MessageBatch{
		msg(`{"a":2}`),
		msg(`{"a":12353}`),
	}
	inputDataSchema := parquet.Group{
		"A": parquet.Decimal(0, 18, parquet.Int64Type),
	}
	transformers := []*dataTransformer{
		{
			name: "A",
			converter: numberConverter{
				nullable:  true,
				scale:     0,
				precision: 38,
			},
			column: &columnMetadata{
				Name:         "A",
				Ordinal:      1,
				Type:         "NUMBER(18,0)",
				LogicalType:  "fixed",
				PhysicalType: "SB8",
				Precision:    ptr.Int32(18),
				Scale:        ptr.Int32(0),
				Nullable:     true,
			},
			bufferFactory: int64TypedBufferFactory,
		},
	}
	schema := parquet.NewSchema("bdec", inputDataSchema)
	w := newParquetWriter("latest", schema)

	// Ensure that a parquet writer correctly resets it's state
	for range 4 {
		w.Reset(nil)

		// Create a concurrent row group, write to it, and flush (all in one call)
		rg := w.BeginRowGroup()
		stats, err := writeRowGroupFromObject(
			batch,
			schema,
			transformers,
			SchemaModeIgnoreExtra,
			rg,
		)
		require.NoError(t, err)

		// Commit the row group
		_, err = rg.Commit()
		require.NoError(t, err)

		// Close the writer and get the bytes
		b, _, err := w.Close()
		require.NoError(t, err)

		actual, err := readGeneric(
			bytes.NewReader(b),
			int64(len(b)),
			parquet.NewSchema("bdec", inputDataSchema),
		)
		require.NoError(t, err)
		require.Equal(t, []map[string]any{
			{"A": float64(2)},
			{"A": float64(12353)},
		}, actual)
		require.Equal(t, []*statsBuffer{
			{
				minIntVal: int128.FromInt64(2),
				maxIntVal: int128.FromInt64(12353),
				hasData:   true,
			},
		}, stats)
	}
}

func readGeneric(r io.ReaderAt, size int64, schema *parquet.Schema) (rows []map[string]any, err error) {
	config, err := parquet.NewReaderConfig(schema)
	if err != nil {
		return nil, err
	}
	file, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, err
	}
	reader := parquet.NewGenericReader[map[string]any](file, config)
	rows = make([]map[string]any, file.NumRows())
	for i := range rows {
		rows[i] = map[string]any{}
	}
	n, err := reader.Read(rows)
	if err == io.EOF {
		err = nil
	}
	reader.Close()
	return rows[:n], err
}

func TestVerifyRowCounts(t *testing.T) {
	// Build a real two-row-group file through the same write path as
	// constructBdecPart so the happy path checks genuine footer metadata.
	batches := []service.MessageBatch{
		{msg(`{"a":2}`), msg(`{"a":12353}`), msg(`{"a":7}`)},
		{msg(`{"a":42}`)},
	}
	inputDataSchema := parquet.Group{
		"A": parquet.Decimal(0, 18, parquet.Int64Type),
	}
	transformers := []*dataTransformer{
		{
			name: "A",
			converter: numberConverter{
				nullable:  true,
				scale:     0,
				precision: 38,
			},
			column: &columnMetadata{
				Name:         "A",
				Ordinal:      1,
				Type:         "NUMBER(18,0)",
				LogicalType:  "fixed",
				PhysicalType: "SB8",
				Precision:    ptr.Int32(18),
				Scale:        ptr.Int32(0),
				Nullable:     true,
			},
			bufferFactory: int64TypedBufferFactory,
		},
	}
	schema := parquet.NewSchema("bdec", inputDataSchema)
	w := newParquetWriter("latest", schema)
	w.Reset(nil)
	rowGroups := make([]*parquet.ConcurrentRowGroupWriter, len(batches))
	for i, batch := range batches {
		rowGroups[i] = w.BeginRowGroup()
		_, err := writeRowGroupFromObject(batch, schema, transformers, SchemaModeIgnoreExtra, rowGroups[i])
		require.NoError(t, err)
	}
	for _, rg := range rowGroups {
		_, err := rg.Commit()
		require.NoError(t, err)
	}
	_, md, err := w.Close()
	require.NoError(t, err)

	require.NoError(t, verifyRowCounts([]int64{3, 1}, md))

	// Every mismatch between what was serialized and what the footer claims
	// must be rejected.
	require.ErrorContains(t, verifyRowCounts([]int64{3}, md), "expected 3")
	require.ErrorContains(t, verifyRowCounts([]int64{2, 2}, md), "row group 0")
	require.ErrorContains(t, verifyRowCounts([]int64{3, 2}, md), "expected 5")
	require.ErrorContains(t, verifyRowCounts([]int64{2, 1, 1}, md), "row groups")

	corrupt := *md
	corrupt.NumRows = 5
	require.ErrorContains(t, verifyRowCounts([]int64{3, 1}, &corrupt), "5")

	corrupt = *md
	corrupt.RowGroups = append([]format.RowGroup{}, md.RowGroups...)
	corrupt.RowGroups[1].NumRows = 9
	err = verifyRowCounts([]int64{3, 1}, &corrupt)
	require.ErrorContains(t, err, "9")

	corrupt = *md
	corrupt.RowGroups = append([]format.RowGroup{}, md.RowGroups...)
	corrupt.RowGroups[0].Columns = append([]format.ColumnChunk{}, md.RowGroups[0].Columns...)
	corrupt.RowGroups[0].Columns[0].MetaData.NumValues = 2
	err = verifyRowCounts([]int64{3, 1}, &corrupt)
	require.ErrorContains(t, err, "column")
}
