// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"strings"

	"gopkg.in/yaml.v3"
)

// mapBatching emits a single `batching:` block on body from whichever of the
// given Kafka Connect keys are present. It consumes all keys it inspects.
//
//   - countKey maps to batching.count (unquoted integer).
//   - byteSizeKey maps to batching.byte_size (unquoted integer).
//   - periodMsKeys: the FIRST key that is present becomes batching.period
//     (millisecond value converted to a duration string, e.g. "5000ms"); all
//     remaining periodMsKeys are also consumed so they do not surface as TODO
//     noise.
//
// At most ONE batching: mapping is emitted regardless of how many periodMsKeys
// are supplied. If none of the keys are present, nothing is emitted.
//
// The sub-field names/shape (count/byte_size/period) match the common benthos
// batch policy exposed by aws_s3 and friends — verified via assertValidRPCN.
func mapBatching(body *yaml.Node, ctx *MapCtx, countKey, byteSizeKey string, periodMsKeys ...string) {
	batch := mapping()

	if countKey != "" {
		if v, ok := ctx.String(countKey); ok {
			kv(batch, "count", intScalar(v))
		}
	}
	if byteSizeKey != "" {
		if v, ok := ctx.String(byteSizeKey); ok {
			kv(batch, "byte_size", intScalar(v))
		}
	}
	// Use the first present periodMsKey; consume all of them regardless.
	periodSet := false
	for _, key := range periodMsKeys {
		if key == "" {
			continue
		}
		v, ok := ctx.String(key) // always marks consumed
		if ok && !periodSet {
			kv(batch, "period", scalar(v+"ms"))
			periodSet = true
		}
	}

	if len(batch.Content) == 0 {
		return
	}
	kv(body, "batching", batch)
}

// objectFormatExtension reads and consumes `format.class` (side effect: the key
// is marked consumed on ctx) and returns the object file extension implied by
// the Kafka Connect format. Defaults to ".json" when the format is absent or
// unrecognized.
func objectFormatExtension(ctx *MapCtx) string {
	cls, ok := ctx.String("format.class")
	if !ok {
		return ".json"
	}
	switch {
	case strings.Contains(cls, "AvroFormat"):
		return ".avro"
	case strings.Contains(cls, "JsonFormat"):
		return ".json"
	case strings.Contains(cls, "ParquetFormat"):
		return ".parquet"
	case strings.Contains(cls, "ByteArrayFormat"):
		return ".bin"
	default:
		return ".json"
	}
}

// consumeIgnored marks recognized-but-irrelevant Kafka Connect plumbing keys
// consumed WITHOUT recording a warning, so they don't surface as TODO noise.
func consumeIgnored(ctx *MapCtx, keys ...string) {
	for _, k := range keys {
		ctx.consume(k)
	}
}
