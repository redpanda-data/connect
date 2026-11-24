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

		// Convert the batch, then write it into a single row group
		chunk, err := convertObjectChunk(
			batch,
			schema,
			transformers,
			SchemaModeIgnoreExtra,
		)
		require.NoError(t, err)
		stats := chunk.stats
		rg := w.BeginRowGroup()
		require.NoError(t, writeRowGroup(rg, []*convertedChunk{chunk}))

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
	// Build real files through the same write path as constructBdecPart so
	// the checks run against genuine footer metadata.
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
	chunks := make([]*convertedChunk, len(batches))
	for i, batch := range batches {
		var err error
		chunks[i], err = convertObjectChunk(batch, schema, transformers, SchemaModeIgnoreExtra)
		require.NoError(t, err)
	}
	// writeFile writes each element of groups as its own row group.
	writeFile := func(groups ...[]*convertedChunk) *format.FileMetaData {
		w := newParquetWriter("latest", schema)
		w.Reset(nil)
		for _, group := range groups {
			rg := w.BeginRowGroup()
			require.NoError(t, writeRowGroup(rg, group))
			_, err := rg.Commit()
			require.NoError(t, err)
		}
		_, md, err := w.Close()
		require.NoError(t, err)
		return md
	}

	// All chunks in one row group is the only layout Snowflake can read.
	md := writeFile(chunks)
	require.Len(t, md.RowGroups, 1)
	require.NoError(t, verifyRowCounts(4, md))

	// A file with one row group per chunk must be rejected even though its
	// footer is consistent: Snowflake would only read the first row group.
	multi := writeFile(chunks[:1], chunks[1:])
	require.Len(t, multi.RowGroups, 2)
	require.EqualValues(t, 4, multi.NumRows)
	require.ErrorContains(t, verifyRowCounts(4, multi), "2 row groups")

	// Every mismatch between what was serialized and what the footer claims
	// must be rejected.
	require.ErrorContains(t, verifyRowCounts(3, md), "expected 3")
	require.ErrorContains(t, verifyRowCounts(5, md), "expected 5")

	corrupt := *md
	corrupt.NumRows = 5
	require.ErrorContains(t, verifyRowCounts(4, &corrupt), "5")

	corrupt = *md
	corrupt.RowGroups = append([]format.RowGroup{}, md.RowGroups...)
	corrupt.RowGroups[0].NumRows = 9
	require.ErrorContains(t, verifyRowCounts(4, &corrupt), "9")

	corrupt = *md
	corrupt.RowGroups = append([]format.RowGroup{}, md.RowGroups...)
	corrupt.RowGroups[0].Columns = append([]format.ColumnChunk{}, md.RowGroups[0].Columns...)
	corrupt.RowGroups[0].Columns[0].MetaData.NumValues = 2
	require.ErrorContains(t, verifyRowCounts(4, &corrupt), "column")

	corrupt = *md
	corrupt.RowGroups = nil
	require.ErrorContains(t, verifyRowCounts(4, &corrupt), "0 row groups")
}
