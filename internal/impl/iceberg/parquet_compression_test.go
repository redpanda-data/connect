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
	tests := []struct {
		name        string
		configured  string
		props       iceberg.Properties
		want        format.CompressionCodec
		wantWarning string // substring; empty means no warning expected
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
			// The config enum linter folds case before validating, so an
			// upper-case value passes validation and arrives here verbatim. It
			// must be honoured, not silently written uncompressed.
			name:       "upper-case configured value is honoured",
			configured: "ZSTD",
			want:       format.Zstd,
		},
		{
			name:       "mixed-case configured value is honoured",
			configured: "Snappy",
			want:       format.Snappy,
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
			name:        "lz4 from the table property degrades to uncompressed",
			wantWarning: "does not write",
			props:       iceberg.Properties{table.ParquetCompressionKey: "lz4"},
			want:        format.Uncompressed,
		},
		{
			name:        "brotli from the table property degrades to uncompressed",
			wantWarning: "does not write",
			props:       iceberg.Properties{table.ParquetCompressionKey: "brotli"},
			want:        format.Uncompressed,
		},
		{
			// The property belongs to whoever owns the table, so its casing is
			// not this output's to dictate — "ZSTD" plainly means zstd.
			name:  "upper-case table property value is honoured",
			props: iceberg.Properties{table.ParquetCompressionKey: "ZSTD"},
			want:  format.Zstd,
		},
		{
			name:  "mixed-case table property value is honoured",
			props: iceberg.Properties{table.ParquetCompressionKey: "Snappy"},
			want:  format.Snappy,
		},
		{
			name:  "upper-case NONE is honoured",
			props: iceberg.Properties{table.ParquetCompressionKey: "NONE"},
			want:  format.Uncompressed,
		},
		{
			name:  "surrounding whitespace is tolerated",
			props: iceberg.Properties{table.ParquetCompressionKey: "  gzip  "},
			want:  format.Gzip,
		},
		{
			name:        "a declined codec is declined regardless of casing",
			wantWarning: "does not write",
			props:       iceberg.Properties{table.ParquetCompressionKey: "LZ4"},
			want:        format.Uncompressed,
		},
		{
			name:        "unknown table property value degrades to uncompressed",
			wantWarning: "not a compression codec",
			props:       iceberg.Properties{table.ParquetCompressionKey: "nonsense"},
			want:        format.Uncompressed,
		},
		{
			name:  "empty table property value is treated as unset",
			props: iceberg.Properties{table.ParquetCompressionKey: ""},
			want:  format.Uncompressed,
		},
		{
			name:  "whitespace-only table property value is treated as unset",
			props: iceberg.Properties{table.ParquetCompressionKey: "   "},
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
			got, warning := resolveParquetCompression(tc.configured, tc.props)
			require.Equal(t, tc.want, got.CompressionCodec())
			if tc.wantWarning == "" {
				require.Empty(t, warning, "did not expect a warning")
			} else {
				require.Contains(t, warning, tc.wantWarning)
			}
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

			codec, _ := resolveParquetCompression(tc.configured, tc.props)
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

// TestWriterOptsForIsolatesTables pins the reason writerOptsFor uses
// slices.Concat rather than append: two tables resolving to different codecs
// must each get their own option, not share a backing array.
//
// The base slice is deliberately given spare capacity, because that is the only
// condition under which the append version of this bug bites — with a full
// slice, append would allocate and the aliasing would be invisible.
func TestWriterOptsForIsolatesTables(t *testing.T) {
	base := make([]parquet.WriterOption, 0, 8)
	base = append(base, parquet.DefaultEncodingFor(parquet.ByteArray, &parquet.Plain))

	r := &Router{writerOpts: base, logger: service.MockResources().Logger()}

	zstdOpts := r.writerOptsFor(iceberg.Properties{table.ParquetCompressionKey: "zstd"})
	snappyOpts := r.writerOptsFor(iceberg.Properties{table.ParquetCompressionKey: "snappy"})

	// Both carry the base option plus their own codec.
	require.Len(t, zstdOpts, 2)
	require.Len(t, snappyOpts, 2)

	// Resolving the second table must not have rewritten the first table's
	// option. Compare the codecs the options actually apply, by configuring a
	// writer with each and reading back what it would use.
	require.Equal(t, format.Zstd, codecFromOptions(t, zstdOpts))
	require.Equal(t, format.Snappy, codecFromOptions(t, snappyOpts))

	// The output-level slice itself must be untouched.
	require.Len(t, r.writerOpts, 1)
}

// codecFromOptions reports the compression codec a set of writer options
// resolves to, by applying them to a writer config.
func codecFromOptions(t *testing.T, opts []parquet.WriterOption) format.CompressionCodec {
	t.Helper()
	cfg, err := parquet.NewWriterConfig(opts...)
	require.NoError(t, err)
	require.NotNil(t, cfg.Compression, "options did not set a compression codec")
	return cfg.Compression.CompressionCodec()
}

// minimalIcebergYAML is the smallest config this output accepts, so the tests
// below can append only the parquet block they care about.
const minimalIcebergYAML = `
catalog:
  url: http://localhost:8181/api/catalog
namespace: ns
table: t
storage:
  aws_s3:
    bucket: bucket
`

// TestParquetCompressionConfigParsing pins the design claim that an unset
// `parquet.compression` is NOT the same as an explicit "uncompressed": unset
// must leave the router's value empty so the table property remains reachable,
// while an explicit value must be carried through even when it happens to name
// the same codec as the fallback.
func TestParquetCompressionConfigParsing(t *testing.T) {
	tests := []struct {
		name string
		conf string
		want string
	}{
		{
			name: "no parquet block at all leaves it unset",
			conf: "",
			want: "",
		},
		{
			name: "a parquet block without compression leaves it unset",
			conf: "parquet:\n  string_encoding: plain\n",
			want: "",
		},
		{
			name: "an explicit uncompressed is not the same as unset",
			conf: "parquet:\n  compression: uncompressed\n",
			want: compressionUncompressed,
		},
		{
			name: "an explicit codec is carried through",
			conf: "parquet:\n  compression: zstd\n",
			want: compressionZstd,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := icebergOutputConfig().ParseYAML(minimalIcebergYAML+tc.conf, nil)
			require.NoError(t, err)

			var got string
			if conf.Contains(ioFieldParquet, ioFieldParquetCompression) {
				got, err = conf.FieldString(ioFieldParquet, ioFieldParquetCompression)
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, got)

			// And the resolved codec follows from it: unset defers to the table
			// property, explicit does not.
			codec, _ := resolveParquetCompression(got,
				iceberg.Properties{table.ParquetCompressionKey: "snappy"})
			// Unset falls through to the property (snappy); anything explicit
			// wins over it.
			wantCodec := format.Snappy
			switch tc.want {
			case compressionUncompressed:
				wantCodec = format.Uncompressed
			case compressionZstd:
				wantCodec = format.Zstd
			}
			require.Equal(t, wantCodec, codec.CompressionCodec())
		})
	}
}
