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
//   - countKeys: the FIRST present key becomes batching.count (unquoted
//     integer); all remaining countKeys are also consumed so they do not
//     surface as TODO noise (e.g. ["flush.size", "file.max.records"]).
//   - byteSizeKey maps to batching.byte_size (unquoted integer).
//   - periodMsKeys: the FIRST key that is present becomes batching.period
//     (millisecond value converted to a duration string, e.g. "5000ms"); all
//     remaining periodMsKeys are also consumed so they do not surface as TODO
//     noise.
//
// At most ONE batching: mapping is emitted regardless of how many count/period
// keys are supplied. If none of the keys are present, nothing is emitted.
//
// The sub-field names/shape (count/byte_size/period) match the common benthos
// batch policy exposed by aws_s3 and friends — verified via assertValidRPCN.
func mapBatching(body *yaml.Node, ctx *MapCtx, countKeys []string, byteSizeKey string, periodMsKeys ...string) {
	batch := mapping()

	// Use the first present countKey; consume all of them regardless.
	countSet := false
	for _, key := range countKeys {
		if key == "" {
			continue
		}
		v, ok := ctx.String(key) // always marks consumed
		if ok && !countSet {
			kv(batch, "count", intScalar(v))
			countSet = true
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

// objectFormatExtension reads and consumes the output-format keys (side effect:
// the keys are marked consumed on ctx) and returns the object file extension
// implied by the Kafka Connect format. It reads Confluent's `format.class`
// first, then falls back to Aiven's `format.output.type`. Defaults to ".json"
// when the format is absent or unrecognized.
func objectFormatExtension(ctx *MapCtx) string {
	cls, ok := ctx.String("format.class")
	if !ok {
		// Aiven S3/GCS sinks use format.output.type instead of format.class.
		if typ, ok := ctx.String("format.output.type"); ok {
			switch strings.ToLower(strings.TrimSpace(typ)) {
			case "jsonl":
				return ".jsonl"
			case "csv":
				return ".csv"
			case "parquet":
				return ".parquet"
			case "avro":
				return ".avro"
			case "json":
				return ".json"
			default:
				return ".json"
			}
		}
		return ".json"
	}
	// Always consume format.output.type if present alongside format.class so it
	// is not flagged as unmapped.
	ctx.consume("format.output.type")
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

// compressionSuffix returns the object-path extension suffix for a Kafka
// Connect file.compression.type value, plus the matching benthos compress
// algorithm name. Returns ("", "") for none/empty/unrecognized values.
func compressionSuffix(typ string) (suffix, algorithm string) {
	switch strings.ToLower(strings.TrimSpace(typ)) {
	case "gzip":
		return ".gz", "gzip"
	case "snappy":
		return ".snappy", "snappy"
	case "zstd":
		return ".zst", "zstd"
	default:
		return "", ""
	}
}

// applyObjectCompression reads and consumes `file.compression.type` (Aiven). For
// a real compression algorithm it appends the matching suffix to the path's
// extension and notes (inline) that a `compress` processor must be added to the
// pipeline to actually compress the data, since the connector layer cannot
// inject a processor. For none/absent/unrecognized, nothing changes.
func applyObjectCompression(ctx *MapCtx, path *yaml.Node, ext string) {
	typ, ok := ctx.String("file.compression.type")
	if !ok {
		return
	}
	suffix, algorithm := compressionSuffix(typ)
	if suffix == "" {
		return
	}
	// Replace the trailing ext in the path value with the compressed ext.
	path.Value = strings.TrimSuffix(path.Value, ext) + ext + suffix
	todo := "TODO: add a compress processor (algorithm: " + algorithm + ") to the pipeline before this output"
	if path.LineComment != "" {
		path.LineComment += "; " + todo
	} else {
		path.LineComment = todo
	}
}

// jodaToGoLayoutForPath translates a Kafka Connect TimeBasedPartitioner
// `path.format` (a Joda pattern, e.g. 'year'=YYYY/'month'=MM/'day'=dd) into a
// Bloblang-interpolated path prefix. Literal runs (quoted 'segments' and
// separators like = and /) pass through verbatim; date-token runs are
// translated via jodaToGoLayout and wrapped in a record-timestamp
// interpolation, e.g.
//
//	${! metadata("kafka_timestamp_unix").number().ts_format("2006") }
//
// The record timestamp is epoch SECONDS under the kafka_timestamp_unix metadata
// key (set by the redpanda/kafka_franz input via record.Timestamp.Unix()).
// NOTE: Do NOT use kafka_timestamp_ms here — Bloblang's ts_format interprets a
// bare number as epoch SECONDS, so feeding it epoch milliseconds would produce
// wildly wrong years (~55000 instead of ~2024). kafka_timestamp_unix is the
// correct key. The emitted form has been verified against the benthos linter.
func jodaToGoLayoutForPath(pattern string) string {
	isLetter := func(b byte) bool {
		return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
	}
	var sb strings.Builder
	i := 0
	for i < len(pattern) {
		ch := pattern[i]
		switch {
		case ch == '\'':
			// Quoted literal: emit the contents verbatim.
			i++
			for i < len(pattern) && pattern[i] != '\'' {
				sb.WriteByte(pattern[i])
				i++
			}
			if i < len(pattern) {
				i++ // closing quote
			}
		case isLetter(ch):
			// Collect a date-token run and translate it as a whole so
			// multi-char tokens (YYYY, MM, dd) map correctly. Kafka Connect's
			// TimeBasedPartitioner uses uppercase YYYY for the calendar year;
			// jodaToGoLayout expects lowercase yyyy, so normalize Y→y here.
			start := i
			for i < len(pattern) && isLetter(pattern[i]) {
				i++
			}
			token := strings.ReplaceAll(pattern[start:i], "Y", "y")
			goLayout := jodaToGoLayout(token)
			// kafka_timestamp_unix is epoch SECONDS — ts_format requires seconds.
			// Using kafka_timestamp_ms (epoch ms) here would produce wrong dates.
			sb.WriteString(`${! metadata("kafka_timestamp_unix").number().ts_format("`)
			sb.WriteString(goLayout)
			sb.WriteString(`") }`)
		default:
			// Separator / literal char.
			sb.WriteByte(ch)
			i++
		}
	}
	return sb.String()
}

// applyTimeBasedPartitioner handles a Kafka Connect TimeBasedPartitioner by
// translating its `path.format` into a time-bucketed prefix prepended to the
// existing topic-based object path. It consumes partitioner.class plus the
// partitioner tuning keys (partition.duration.ms, locale, timezone, path.format)
// so they do not surface as unmapped TODO noise.
//
// When path.format is absent (TimeBasedPartitioner defaults) or cannot be
// translated, it emits a best-effort path with an inline TODO instead.
func applyTimeBasedPartitioner(ctx *MapCtx, path *yaml.Node) {
	ctx.consume("partitioner.class")
	ctx.consume("partition.duration.ms")
	ctx.consume("locale")
	ctx.consume("timezone")

	format, ok := ctx.String("path.format")
	if !ok || strings.TrimSpace(format) == "" {
		todo := "TODO: TimeBasedPartitioner uses default time-bucketing — add a time-based path prefix and verify"
		if path.LineComment != "" {
			path.LineComment += "; " + todo
		} else {
			path.LineComment = todo
		}
		return
	}

	prefix := jodaToGoLayoutForPath(format)
	if strings.TrimSpace(prefix) == "" {
		todo := "TODO: could not translate TimeBasedPartitioner path.format=" + format + " — add a time-based path prefix and verify"
		if path.LineComment != "" {
			path.LineComment += "; " + todo
		} else {
			path.LineComment = todo
		}
		return
	}

	path.Value = strings.TrimSuffix(prefix, "/") + "/" + path.Value
}

// consumeIgnored marks recognized-but-irrelevant Kafka Connect plumbing keys
// consumed WITHOUT recording a warning, so they don't surface as TODO noise.
func consumeIgnored(ctx *MapCtx, keys ...string) {
	for _, k := range keys {
		ctx.consume(k)
	}
}

// sinkInputFromTopics builds a redpanda input consuming the connector's topics,
// or returns nil if no topics are configured. seed_brokers uses a placeholder
// since broker addresses are worker-level config in Kafka Connect.
//
// Two cases are handled:
//   - `topics` (CSV of literal topic names): topics list is passed through as-is.
//   - `topics.regex` (a single Java regex pattern): the pattern is placed in the
//     topics list and regexp_topics is set to true so RPCN performs regex matching.
//
// When neither key is present (or both are empty) nil is returned and the caller
// falls back to a stdin stub.
func sinkInputFromTopics(cfg ConnectConfig, ctx *MapCtx) *yaml.Node {
	// Prefer literal `topics` over `topics.regex`.
	v, ok := ctx.Lookup("topics")
	if ok && strings.TrimSpace(v) != "" {
		ctx.consume("topics")

		body := mapping()

		brokers := scalar("localhost:9092")
		brokers.LineComment = "TODO: set your Redpanda/Kafka broker(s)"
		kv(body, "seed_brokers", seq(brokers))

		kv(body, "topics", seq(scalarsFromCSV(v)...))

		cg := scalar("connect-" + cfg.Name)
		cg.LineComment = "TODO: confirm consumer group"
		kv(body, "consumer_group", cg)

		return component("redpanda", body)
	}

	// Fall back to topics.regex when literal topics is absent/empty.
	re, ok := ctx.Lookup("topics.regex")
	if !ok || strings.TrimSpace(re) == "" {
		return nil
	}
	ctx.consume("topics.regex")

	body := mapping()

	brokers := scalar("localhost:9092")
	brokers.LineComment = "TODO: set your Redpanda/Kafka broker(s)"
	kv(body, "seed_brokers", seq(brokers))

	kv(body, "topics", seq(scalar(strings.TrimSpace(re))))
	kv(body, "regexp_topics", boolScalar(true))

	cg := scalar("connect-" + cfg.Name)
	cg.LineComment = "TODO: confirm consumer group"
	kv(body, "consumer_group", cg)

	return component("redpanda", body)
}
