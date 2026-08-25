// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Compression codec names accepted by the `parquet.compression` field. These
// are deliberately a subset of the codecs parquet permits: every one of them is
// read by all the query engines this output targets. The codecs left out are
// interoperability hazards rather than technical gaps —
//
//   - lz4: the original LZ4 codec was ambiguously specified (Hadoop framing vs.
//     raw blocks) and readers disagree on which one `LZ4` means, which is why
//     LZ4_RAW was later added to the format. Writing either risks files a given
//     engine refuses.
//   - brotli, lzo: read support across engines is patchy.
//
// A table property may still name one of those (see resolveParquetCompression),
// in which case this output declines to write it rather than produce files some
// reader cannot open.
const (
	compressionUncompressed = "uncompressed"
	compressionSnappy       = "snappy"
	compressionGzip         = "gzip"
	compressionZstd         = "zstd"
)

// parquetCompressionCodecs maps an accepted codec name to its parquet-go codec.
// "none" is included because that is the spelling Iceberg's own table property
// uses for no compression, and the property is a valid source of this value.
var parquetCompressionCodecs = map[string]compress.Codec{
	compressionUncompressed: &parquet.Uncompressed,
	"none":                  &parquet.Uncompressed,
	compressionSnappy:       &parquet.Snappy,
	compressionGzip:         &parquet.Gzip,
	compressionZstd:         &parquet.Zstd,
}

// resolveParquetCompression decides which compression codec this output writes
// data files with for one table, resolving in a fixed order:
//
//  1. `parquet.compression`, when set — an explicit operator instruction wins.
//  2. the table's own `write.parquet.compression-codec` property, so a table
//     configured by its owner (or another writer) is honoured without needing
//     connector configuration. Also the only way to make the copy-on-write
//     rewrite path agree, since that path is inside iceberg-go and reads this
//     property itself.
//  3. uncompressed, preserving this output's historical behaviour when neither
//     is specified.
//
// A property naming a codec this output declines to write (see the constants
// above) is reported and treated as unset rather than failing the write: the
// property is not this output's configuration to validate, and uncompressed is
// readable everywhere, so refusing to start would be a worse outcome than
// writing data every engine can read. An invalid *configured* value cannot
// reach here — the config field is an enum, validated at startup.
//
// Called once per table when its writer is built, not per batch.
func resolveParquetCompression(configured string, props iceberg.Properties, logger *service.Logger) compress.Codec {
	if configured != "" {
		if codec, ok := parquetCompressionCodecs[configured]; ok {
			return codec
		}
		// Unreachable via config validation; be explicit rather than silently
		// writing something the operator did not ask for.
		if logger != nil {
			logger.Warnf("Unsupported %s.%s value %q; writing uncompressed data files.", ioFieldParquet, ioFieldParquetCompression, configured)
		}
		return &parquet.Uncompressed
	}

	fromTable, ok := props[table.ParquetCompressionKey]
	if !ok || fromTable == "" {
		return &parquet.Uncompressed
	}

	if codec, ok := parquetCompressionCodecs[fromTable]; ok {
		return codec
	}

	if logger != nil {
		logger.Warnf("Table property %s is %q, which this output does not write (readers disagree on it or engine support is patchy); writing uncompressed data files instead. Set %s.%s to choose a supported codec explicitly.",
			table.ParquetCompressionKey, fromTable, ioFieldParquet, ioFieldParquetCompression)
	}
	return &parquet.Uncompressed
}
