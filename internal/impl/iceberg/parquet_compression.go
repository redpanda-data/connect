// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

// Compression codec names this output will write. Deliberately a subset of what
// parquet permits and Iceberg's table property accepts: each of these is read by
// every query engine this output targets. The omissions are interoperability
// judgements rather than technical gaps — parquet-go can write brotli and
// LZ4_RAW, and declining them is a choice:
//
//   - lz4: the original LZ4 codec was ambiguously specified (Hadoop framing vs.
//     raw blocks) and readers disagree on which one `LZ4` means. parquet-go
//     cannot write it at all.
//   - lz4raw: the unambiguous replacement, so the objection above does not
//     apply — but reader support for it is younger and less universal than
//     snappy or zstd, which is reason enough not to write it by default.
//   - brotli, lzo: read support across engines is patchy.
//
// A table property may still name any of these; see resolveParquetCompression.
const (
	compressionUncompressed = "uncompressed"
	compressionSnappy       = "snappy"
	compressionGzip         = "gzip"
	compressionZstd         = "zstd"
)

// parquetCompressionCodecs maps an accepted codec name to its parquet-go codec.
// "none" is included because that is the spelling Iceberg's table property uses
// for no compression, and the property is a valid source of this value.
var parquetCompressionCodecs = map[string]compress.Codec{
	compressionUncompressed: &parquet.Uncompressed,
	"none":                  &parquet.Uncompressed,
	compressionSnappy:       &parquet.Snappy,
	compressionGzip:         &parquet.Gzip,
	compressionZstd:         &parquet.Zstd,
}

// declinedCompressionCodecs are codec names parquet defines and Iceberg's table
// property accepts, but that this output will not write. Kept distinct from
// values that are simply unrecognised, so the operator is told which of the two
// happened.
var declinedCompressionCodecs = map[string]struct{}{
	"lz4":    {},
	"lz4raw": {},
	"brotli": {},
	"lzo":    {},
}

// normaliseCodecName folds a codec name for lookup. Codec names are ASCII, so
// lower-casing is sufficient.
//
// Both the configured value and the table property need this. The property
// obviously does — it is set by whoever owns the table, so its casing is not
// this output's to dictate. The configured value needs it too, and less
// obviously: the config framework's enum linter lower-cases before comparing
// against the option set, so `compression: ZSTD` passes validation and arrives
// here verbatim. Without folding it would miss the map and silently write
// uncompressed.
func normaliseCodecName(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// resolveParquetCompression decides which compression codec this output writes
// data files with for one table, resolving in a fixed order:
//
//  1. `parquet.compression`, when set — an explicit operator instruction wins.
//  2. otherwise the table's own `write.parquet.compression-codec` property, so a
//     table configured by its owner (or another writer) is honoured without
//     needing connector configuration.
//  3. otherwise uncompressed, preserving this output's historical behaviour.
//
// A property naming a codec this output declines is treated as unset rather than
// failing the write: the property is not this output's configuration to
// validate, and uncompressed is readable everywhere, so refusing to start would
// be a worse outcome than writing data every engine can read.
//
// Returns the codec and, when something needs saying, a warning for the caller
// to log. The warning is returned rather than logged here so the caller can
// suppress repeats — writers are rebuilt on every write failure, so logging
// directly would spam a retrying pipeline.
func resolveParquetCompression(configured string, props iceberg.Properties) (codec compress.Codec, warning string) {
	if configured != "" {
		if codec, ok := parquetCompressionCodecs[normaliseCodecName(configured)]; ok {
			return codec, ""
		}
		// Reachable: the enum linter folds case before validating, so a
		// differently-spelled-but-valid value passes config validation. A
		// genuinely invalid value is rejected before it gets here.
		return &parquet.Uncompressed, fmt.Sprintf(
			"Unsupported %s.%s value %q; writing uncompressed data files. Supported values are uncompressed, snappy, gzip and zstd.",
			ioFieldParquet, ioFieldParquetCompression, configured)
	}

	fromTable := normaliseCodecName(props[table.ParquetCompressionKey])
	if fromTable == "" {
		return &parquet.Uncompressed, ""
	}

	if codec, ok := parquetCompressionCodecs[fromTable]; ok {
		return codec, ""
	}

	raw := props[table.ParquetCompressionKey]
	if _, declined := declinedCompressionCodecs[fromTable]; declined {
		// Deliberately does not claim the table is now safe: this governs only
		// the files THIS writer produces. Copy-on-write rewrites and
		// equality-delete files are written inside iceberg-go, which honours the
		// same property and does map these codecs, so a table carrying one can
		// still gain files in it.
		return &parquet.Uncompressed, fmt.Sprintf(
			"Table property %s is %q, which this output does not write: engine read support for it is not universal. The data files this output writes will be uncompressed instead — set %s.%s to choose a codec for them. Other writers, including this output's own copy-on-write rewrites and equality-delete files, may still honour the property, so change the property itself if nothing should write that codec.",
			table.ParquetCompressionKey, raw, ioFieldParquet, ioFieldParquetCompression)
	}

	return &parquet.Uncompressed, fmt.Sprintf(
		"Table property %s is %q, which is not a compression codec this output recognises; writing uncompressed data files. Supported values are uncompressed, snappy, gzip and zstd.",
		table.ParquetCompressionKey, raw)
}
