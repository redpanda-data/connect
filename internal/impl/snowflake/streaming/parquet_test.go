/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package streaming

import (
	"bytes"
	"io"
	"testing"

	"github.com/aws/smithy-go/ptr"
	"github.com/parquet-go/parquet-go"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"

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
		"A": parquet.Decimal(0, 18, parquet.Int32Type),
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
			bufferFactory: int32TypedBufferFactory,
		},
	}
	schema := parquet.NewSchema("bdec", inputDataSchema)
	rows, stats, err := constructRowGroup(
		batch,
		schema,
		transformers,
		false,
	)
	require.NoError(t, err)
	b, err := writeParquetFile("latest", parquetFileData{
		schema, rows, nil,
	})
	require.NoError(t, err)
	actual, err := readGeneric(
		bytes.NewReader(b),
		int64(len(b)),
		parquet.NewSchema("bdec", inputDataSchema),
	)
	require.NoError(t, err)
	require.Equal(t, []map[string]any{
		{"A": int32(2)},
		{"A": int32(12353)},
	}, actual)
	require.Equal(t, []*statsBuffer{
		{
			minIntVal: int128.FromInt64(2),
			maxIntVal: int128.FromInt64(12353),
			hasData:   true,
		},
	}, stats)
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
