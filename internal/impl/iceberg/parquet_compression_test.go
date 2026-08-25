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
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/shredder"
)

// TestResolveParquetCompression pins the resolution order: configured value,
// then the table's own property, then uncompressed.
func TestResolveParquetCompression(t *testing.T) {
	logger := service.MockResources().Logger()

	tests := []struct {
		name       string
		configured string
		props      iceberg.Properties
		want       format.CompressionCodec
	}{
		{
			name: "nothing set writes uncompressed",
			want: format.Uncompressed,
		},
		{
			name:       "configured value is used",
			configured: compressionZstd,
			want:       format.Zstd,
		},
		{
			name:  "table property is used when unset",
			props: iceberg.Properties{table.ParquetCompressionKey: "snappy"},
			want:  format.Snappy,
		},
		{
			name:       "configured value beats the table property",
			configured: compressionSnappy,
			props:      iceberg.Properties{table.ParquetCompressionKey: "zstd"},
			want:       format.Snappy,
		},
		{
			// "none" is Iceberg's spelling for no compression; it must not be
			// mistaken for an unrecognised value.
			name:  "table property none is recognised",
			props: iceberg.Properties{table.ParquetCompressionKey: "none"},
			want:  format.Uncompressed,
		},
		{
			name:  "table property uncompressed is recognised",
			props: iceberg.Properties{table.ParquetCompressionKey: "uncompressed"},
			want:  format.Uncompressed,
		},
		{
			name:  "gzip from the table property",
			props: iceberg.Properties{table.ParquetCompressionKey: "gzip"},
			want:  format.Gzip,
		},
		{
			// The codecs deliberately not offered: honouring them would risk
			// files some reader refuses, so they degrade to uncompressed rather
			// than failing the write.
			name:  "lz4 from the table property degrades to uncompressed",
			props: iceberg.Properties{table.ParquetCompressionKey: "lz4"},
			want:  format.Uncompressed,
		},
		{
			name:  "brotli from the table property degrades to uncompressed",
			props: iceberg.Properties{table.ParquetCompressionKey: "brotli"},
			want:  format.Uncompressed,
		},
		{
			name:  "unknown table property value degrades to uncompressed",
			props: iceberg.Properties{table.ParquetCompressionKey: "nonsense"},
			want:  format.Uncompressed,
		},
		{
			name:  "empty table property value is treated as unset",
			props: iceberg.Properties{table.ParquetCompressionKey: ""},
			want:  format.Uncompressed,
		},
		{
			name:  "an unrelated property does not interfere",
			props: iceberg.Properties{"write.metadata.compression-codec": "gzip"},
			want:  format.Uncompressed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveParquetCompression(tc.configured, tc.props, logger)
			require.Equal(t, tc.want, got.CompressionCodec())
		})
	}
}

// TestParquetCompressionReachesWrittenFile checks the resolved codec actually
// governs the bytes on disk, not merely what the resolver returns: the option
// has to survive being handed to the sink and applied per column chunk.
func TestParquetCompressionReachesWrittenFile(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "s", Type: iceberg.PrimitiveTypes.String},
	)

	for _, tc := range []struct {
		name       string
		configured string
		props      iceberg.Properties
		want       format.CompressionCodec
	}{
		{name: "default is uncompressed", want: format.Uncompressed},
		{name: "configured zstd", configured: compressionZstd, want: format.Zstd},
		{name: "property snappy", props: iceberg.Properties{table.ParquetCompressionKey: "snappy"}, want: format.Snappy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pqSchema, fieldToCol, err := icebergx.BuildParquetSchema(sc, icebergx.TimestampEncoding(0))
			require.NoError(t, err)

			codec := resolveParquetCompression(tc.configured, tc.props, service.MockResources().Logger())
			sink := newParquetSink(pqSchema, fieldToCol, true, parquet.Compression(codec))

			// Enough rows of repetitive data that a codec has something to bite
			// on, driven the way the writer drives it: shred a row, then flush.
			for i := range 500 {
				require.NoError(t, sink.EmitValue(shredder.ShreddedValue{FieldID: 1, Value: parquet.ValueOf(int64(i))}))
				require.NoError(t, sink.EmitValue(shredder.ShreddedValue{FieldID: 2, Value: parquet.ValueOf("a highly compressible repeated string value")}))
				require.NoError(t, sink.flush())
			}
			res, err := sink.Close()
			require.NoError(t, err)

			require.NotEmpty(t, res.footer.RowGroups, "expected at least one row group")
			for _, rg := range res.footer.RowGroups {
				for _, col := range rg.Columns {
					require.Equal(t, tc.want, col.MetaData.Codec,
						"column %v written with the wrong codec", col.MetaData.PathInSchema)
				}
			}
		})
	}
}
