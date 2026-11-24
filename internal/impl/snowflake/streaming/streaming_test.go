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
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/aws/smithy-go/ptr"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming/int128"
)

func TestDebugModeDisabled(t *testing.T) {
	// So I can't forget to disable this!
	require.False(t, debug)
}

// Snowflake's scanner reads a single row group per BDEC file, so a batch
// larger than the build chunk size must still produce a file with one row
// group that holds every row in order.
func TestConstructBdecPartSingleRowGroup(t *testing.T) {
	// The ordinals are deliberately not in name order: parquet orders the
	// columns by name, while the transformers follow the ordinals.
	columns := []columnMetadata{
		{Name: "SEQ", Ordinal: 1, Type: "NUMBER(18,0)", LogicalType: "fixed", PhysicalType: "SB8", Precision: ptr.Int32(18), Scale: ptr.Int32(0)},
		{Name: "BIG", Ordinal: 2, Type: "NUMBER(38,0)", LogicalType: "fixed", PhysicalType: "SB16", Precision: ptr.Int32(38), Scale: ptr.Int32(0)},
		{Name: "LABEL", Ordinal: 3, Type: "VARCHAR", LogicalType: "text", PhysicalType: "LOB", Nullable: true},
		{Name: "FLAG", Ordinal: 4, Type: "BOOLEAN", LogicalType: "boolean", PhysicalType: "SB1"},
		{Name: "AMOUNT", Ordinal: 5, Type: "DOUBLE", LogicalType: "real", PhysicalType: "DOUBLE"},
	}
	schema, transformers, _, err := constructParquetSchema(columns, dataConverterOptions{})
	require.NoError(t, err)

	const numRows = 1037
	label := func(i int) string { return fmt.Sprintf("row_%04d", i) }
	hasLabel := func(i int) bool { return i%10 != 3 }

	tests := []struct {
		name   string
		format MessageFormat
		row    func(i int) string
	}{
		{
			name:   "object",
			format: MessageFormatObject,
			row: func(i int) string {
				s := fmt.Sprintf(`{"seq":%d,"big":%d,"flag":%t,"amount":%d.5`, i, i*3, i%2 == 0, i)
				if hasLabel(i) {
					s += fmt.Sprintf(`,"label":%q`, label(i))
				}
				return s + "}"
			},
		},
		{
			name:   "array",
			format: MessageFormatArray,
			row: func(i int) string {
				l := "null"
				if hasLabel(i) {
					l = fmt.Sprintf("%q", label(i))
				}
				return fmt.Sprintf(`[%d,%d,%s,%t,%d.5]`, i, i*3, l, i%2 == 0, i)
			},
		},
	}
	builds := []BuildOptions{
		{Parallelism: 1, ChunkSize: 50_000},
		{Parallelism: 1, ChunkSize: 100},
		{Parallelism: 4, ChunkSize: 100},
		{Parallelism: 8, ChunkSize: 1},
	}
	for _, tc := range tests {
		for _, build := range builds {
			t.Run(fmt.Sprintf("%s/parallelism_%d_chunk_%d", tc.name, build.Parallelism, build.ChunkSize), func(t *testing.T) {
				batch := make(service.MessageBatch, numRows)
				for i := range batch {
					batch[i] = msg(tc.row(i))
				}
				channel := &SnowflakeIngestionChannel{
					schema:         schema,
					transformers:   transformers,
					connectVersion: "latest",
				}
				channel.BuildOptions = build
				channel.MessageFormat = tc.format
				part, err := channel.constructBdecPart(batch, map[string]string{"primaryFileId": "test.bdec"})
				require.NoError(t, err)

				require.Len(t, part.parquetMetadata.RowGroups, 1, "a BDEC file must have exactly one row group")
				require.EqualValues(t, numRows, part.parquetMetadata.NumRows)
				require.EqualValues(t, numRows, part.parquetMetadata.RowGroups[0].NumRows)

				// The stats registered with Snowflake must cover the whole batch.
				stat := func(name string) *statsBuffer {
					for i, tr := range transformers {
						if tr.name == name {
							return part.stats[i]
						}
					}
					t.Fatalf("no column %s", name)
					return nil
				}
				require.Equal(t, int128.FromInt64(0), stat("SEQ").minIntVal)
				require.Equal(t, int128.FromInt64(numRows-1), stat("SEQ").maxIntVal)
				require.Equal(t, int128.FromInt64(0), stat("BIG").minIntVal)
				require.Equal(t, int128.FromInt64((numRows-1)*3), stat("BIG").maxIntVal)
				require.Equal(t, 0.5, stat("AMOUNT").minRealVal)
				require.Equal(t, float64(numRows-1)+0.5, stat("AMOUNT").maxRealVal)
				require.EqualValues(t, 104, stat("LABEL").nullCount)
				require.Equal(t, label(0), string(stat("LABEL").minStrVal))
				require.Equal(t, label(numRows-1), string(stat("LABEL").maxStrVal))

				// Every row must be readable, in order, from the only row group.
				file, err := parquet.OpenFile(bytes.NewReader(part.parquetFile), int64(len(part.parquetFile)))
				require.NoError(t, err)
				require.Len(t, file.RowGroups(), 1)
				column := func(name string) int {
					leaf, ok := file.Schema().Lookup(name)
					require.True(t, ok, "no column %s", name)
					return leaf.ColumnIndex
				}
				seqCol, bigCol, labelCol, flagCol, amountCol := column("SEQ"), column("BIG"), column("LABEL"), column("FLAG"), column("AMOUNT")
				reader := file.RowGroups()[0].Rows()
				defer reader.Close()
				rows := make([]parquet.Row, numRows+1)
				read := 0
				for read < len(rows) {
					n, err := reader.ReadRows(rows[read:])
					read += n
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(t, err)
				}
				require.Equal(t, numRows, read)
				for i, row := range rows[:read] {
					require.EqualValues(t, i, row[seqCol].Int64(), "row %d", i)
					require.Equal(t, int128.FromInt64(int64(i*3)).AppendBigEndian(nil), row[bigCol].ByteArray(), "row %d", i)
					if hasLabel(i) {
						require.Equal(t, label(i), string(row[labelCol].ByteArray()), "row %d", i)
					} else {
						require.True(t, row[labelCol].IsNull(), "row %d", i)
					}
					require.Equal(t, i%2 == 0, row[flagCol].Boolean(), "row %d", i)
					require.Equal(t, float64(i)+0.5, row[amountCol].Double(), "row %d", i)
				}
			})
		}
	}
}
