// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

func init() {
	// Unwrap SMTs — no-op for *_cdc inputs.
	registerSMT("io.debezium.transforms.ExtractNewRecordState", extractNewRecordStateSMT{})
	registerSMT("io.debezium.connector.mongodb.transforms.ExtractNewDocumentState", extractNewDocumentStateSMT{})

	// Outbox pattern — best-effort mapping with TODO.
	registerSMT("io.debezium.transforms.outbox.EventRouter", eventRouterSMT{})
	registerSMT("io.debezium.connector.mongodb.transforms.outbox.MongoEventRouter", eventRouterSMT{})

	// Timezone conversion — best-effort passthrough with tailored TODO.
	registerSMT("io.debezium.transforms.TimezoneConverter", timezoneConverterSMT{})

	// Scripted transforms — guidance only.
	registerSMT("io.debezium.transforms.Filter", debeziumFilterSMT{})
	registerSMT("io.debezium.transforms.ContentBasedRouter", contentBasedRouterSMT{})

	// Partition transforms — guidance only.
	registerSMT("io.debezium.transforms.partitions.PartitionRouting", partitionRoutingSMT{})
	registerSMT("io.debezium.transforms.partitions.ComputePartition", computePartitionSMT{})

	// Record state and schema — guidance only.
	registerSMT("io.debezium.transforms.ExtractChangedRecordState", extractChangedRecordStateSMT{})
	registerSMT("io.debezium.transforms.SchemaChangeEventFilter", schemaChangeEventFilterSMT{})

	// Geometry — guidance only.
	registerSMT("io.debezium.transforms.GeometryFormatTransformer", geometryFormatTransformerSMT{})

	// PostgreSQL-specific — guidance only.
	registerSMT("io.debezium.connector.postgresql.transforms.DecodeLogicalDecodingMessageContent", decodeLogicalDecodingMessageContentSMT{})
	registerSMT("io.debezium.connector.postgresql.transforms.timescaledb.TimescaleDb", timescaleDbSMT{})
}

// ── helpers ───────────────────────────────────────────────────────────────────

// debeziumNoOp emits no processor nodes and issues an informational warning.
// Use for SMTs that are functionally a no-op for RPCN *_cdc inputs.
func debeziumNoOp(smt SMTConfig, ctx *MapCtx, msg string) ([]*yaml.Node, error) {
	ctx.Warn(smt.Alias, msg)
	return nil, nil
}

// debeziumGuidance emits a passthrough `root = this` mapping whose inline
// comment is the tailored TODO message, and issues a warning with the same text.
// Use for SMTs that cannot be auto-translated but benefit from specific guidance.
func debeziumGuidance(smt SMTConfig, ctx *MapCtx, todoMsg string) ([]*yaml.Node, error) {
	expr := scalar("root = this")
	expr.LineComment = "TODO: " + todoMsg
	ctx.Warn(smt.Alias, todoMsg)
	return []*yaml.Node{mappingProc(expr)}, nil
}

// ── ExtractNewRecordState ─────────────────────────────────────────────────────

// extractNewRecordStateSMT handles the canonical Debezium "unwrap envelope" SMT.
// Redpanda Connect *_cdc inputs already deliver unwrapped row state directly
// from the WAL/binlog, so this SMT is a no-op in an RPCN pipeline. We register
// it to avoid the generic "unsupported SMT — map manually" stub and instead
// emit an informational warning explaining why no processor is generated.
type extractNewRecordStateSMT struct{}

func (extractNewRecordStateSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	msg := "ExtractNewRecordState is a no-op for RPCN *_cdc inputs — records are already unwrapped"
	if opts := debeziumUnwrapOpts(smt); opts != "" {
		msg += "; the row-shaping options [" + opts + "] are NOT reproduced — add equivalent mapping/filter processors if you depend on them"
	}
	ctx.Warn("transforms."+smt.Alias+".type", msg)
	// Emit no processor nodes.
	return nil, nil
}

// debeziumUnwrapOpts returns a comma-separated list of the row-shaping
// ExtractNewRecordState options that are actually present (add.fields,
// add.headers, delete.handling.mode, drop.tombstones), or "" if none are set.
// These change the output record shape and are not reproduced by *_cdc inputs.
func debeziumUnwrapOpts(smt SMTConfig) string {
	var present []string
	for _, k := range []string{"add.fields", "add.headers", "delete.handling.mode", "drop.tombstones"} {
		if v, ok := smt.Props[k]; ok {
			if s := strings.TrimSpace(fmt.Sprint(v)); s != "" {
				present = append(present, k+"="+s)
			}
		}
	}
	return strings.Join(present, ", ")
}

// ── ExtractNewDocumentState (MongoDB) ─────────────────────────────────────────

// extractNewDocumentStateSMT handles the MongoDB variant of the Debezium unwrap
// envelope SMT. Same rationale as ExtractNewRecordState: no-op for *_cdc inputs.
type extractNewDocumentStateSMT struct{}

func (extractNewDocumentStateSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	return debeziumNoOp(smt, ctx,
		"ExtractNewDocumentState is a no-op for RPCN *_cdc inputs — records are already "+
			"unwrapped; advanced options (add.fields, flatten.struct) are not translated",
	)
}

// ── EventRouter / MongoEventRouter (outbox pattern) ──────────────────────────

// eventRouterSMT handles the Debezium EventRouter and MongoEventRouter outbox
// pattern SMTs. It emits a best-effort starting mapping using the default outbox
// column names (aggregatetype → topic, payload → body) with a TODO comment
// instructing the user to adjust the column names and payload parsing.
type eventRouterSMT struct{}

func (eventRouterSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	expr := scalar("meta kafka_topic = this.aggregatetype\nroot = this.payload")
	expr.LineComment = "TODO: adjust outbox column names (route.by.field / fields...) and payload parsing as needed"
	ctx.Warn(
		smt.Alias,
		"EventRouter outbox pattern: emitted a best-effort starting mapping using default column "+
			"names (aggregatetype, payload) — adjust route.by.field / payload field names as needed",
	)
	return []*yaml.Node{mappingProc(expr)}, nil
}

// ── TimezoneConverter ─────────────────────────────────────────────────────────

// timezoneConverterSMT handles io.debezium.transforms.TimezoneConverter.
// It emits a passthrough `root = this` with a tailored TODO that includes the
// configured target timezone and field list from the connector config.
type timezoneConverterSMT struct{}

func (timezoneConverterSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	tz, _ := smt.Props["converted.timezone"].(string)
	includeList, _ := smt.Props["include.list"].(string)
	excludeList, _ := smt.Props["exclude.list"].(string)

	var parts []string
	if tz != "" {
		parts = append(parts, "timezone "+tz)
	}
	if includeList != "" {
		parts = append(parts, "fields "+includeList)
	} else if excludeList != "" {
		parts = append(parts, "excluding fields "+excludeList)
	}

	var suffix string
	if len(parts) > 0 {
		suffix = " (" + strings.Join(parts, ", ") + ")"
	}

	todoMsg := fmt.Sprintf(
		"shift timestamp field(s)%s to target timezone — apply per-field timestamp conversion manually",
		suffix,
	)
	return debeziumGuidance(smt, ctx, todoMsg)
}

// ── Filter (scripted) ─────────────────────────────────────────────────────────

// debeziumFilterSMT handles io.debezium.transforms.Filter — a scripted filter
// using JSR-223 or GraalVM. The script cannot be auto-translated; emit guidance.
type debeziumFilterSMT struct{}

func (debeziumFilterSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	language, _ := smt.Props["language"].(string)
	condition, _ := smt.Props["condition"].(string)

	var parts []string
	if language != "" {
		parts = append(parts, "language="+language)
	}
	if condition != "" {
		parts = append(parts, "condition `"+condition+"`")
	}

	var detail string
	if len(parts) > 0 {
		detail = " (" + strings.Join(parts, ", ") + ")"
	}

	todoMsg := fmt.Sprintf(
		"Debezium scripted Filter%s: port the condition to a Bloblang `mapping` + drop "+
			"(root = if !(<cond>) { deleted() })",
		detail,
	)
	return debeziumGuidance(smt, ctx, todoMsg)
}

// ── ContentBasedRouter (scripted) ────────────────────────────────────────────

// contentBasedRouterSMT handles io.debezium.transforms.ContentBasedRouter — a
// scripted topic router. The script expression cannot be auto-translated.
type contentBasedRouterSMT struct{}

func (contentBasedRouterSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	language, _ := smt.Props["language"].(string)
	topicExpr, _ := smt.Props["topic.expression"].(string)

	var parts []string
	if language != "" {
		parts = append(parts, "language="+language)
	}
	if topicExpr != "" {
		parts = append(parts, "topic.expression `"+topicExpr+"`")
	}

	var detail string
	if len(parts) > 0 {
		detail = " (" + strings.Join(parts, ", ") + ")"
	}

	todoMsg := fmt.Sprintf(
		"scripted ContentBasedRouter%s: port the topic.expression script to "+
			"`meta kafka_topic = <bloblang>`",
		detail,
	)
	return debeziumGuidance(smt, ctx, todoMsg)
}

// ── PartitionRouting ──────────────────────────────────────────────────────────

// partitionRoutingSMT handles io.debezium.transforms.partitions.PartitionRouting.
// Partitioning is an output concern in RPCN, not a pipeline processor.
type partitionRoutingSMT struct{}

func (partitionRoutingSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	return debeziumGuidance(smt, ctx,
		"partition routing is an output concern in RPCN — set the message key / partitioning "+
			"on the redpanda output, not in the pipeline",
	)
}

// ── ComputePartition (removed in Debezium 2.5) ───────────────────────────────

// computePartitionSMT handles io.debezium.transforms.partitions.ComputePartition,
// which was removed in Debezium 2.5 in favour of PartitionRouting.
type computePartitionSMT struct{}

func (computePartitionSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	return debeziumGuidance(smt, ctx,
		"ComputePartition was removed in Debezium 2.5 — use PartitionRouting; "+
			"partitioning is an output concern in RPCN",
	)
}

// ── ExtractChangedRecordState ─────────────────────────────────────────────────

// extractChangedRecordStateSMT handles io.debezium.transforms.ExtractChangedRecordState,
// which adds changed/unchanged field headers to the event envelope.
type extractChangedRecordStateSMT struct{}

func (extractChangedRecordStateSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	return debeziumGuidance(smt, ctx,
		"adds changed/unchanged field headers — derive with a mapping over before/after if needed",
	)
}

// ── SchemaChangeEventFilter ───────────────────────────────────────────────────

// schemaChangeEventFilterSMT handles io.debezium.transforms.SchemaChangeEventFilter,
// which filters DDL schema-change events from the change stream.
type schemaChangeEventFilterSMT struct{}

func (schemaChangeEventFilterSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	return debeziumGuidance(smt, ctx,
		"filters DDL schema-change events — filter by metadata/content with a mapping + filter",
	)
}

// ── GeometryFormatTransformer ─────────────────────────────────────────────────

// geometryFormatTransformerSMT handles io.debezium.transforms.GeometryFormatTransformer,
// which converts WKB/EWKB geometry binary data to alternative formats.
type geometryFormatTransformerSMT struct{}

func (geometryFormatTransformerSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	return debeziumGuidance(smt, ctx,
		"WKB/EWKB geometry conversion not translated — convert geometry fields manually",
	)
}

// ── DecodeLogicalDecodingMessageContent (PostgreSQL) ─────────────────────────

// decodeLogicalDecodingMessageContentSMT handles
// io.debezium.connector.postgresql.transforms.DecodeLogicalDecodingMessageContent,
// which decodes Postgres logical-decoding message content.
type decodeLogicalDecodingMessageContentSMT struct{}

func (decodeLogicalDecodingMessageContentSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	return debeziumGuidance(smt, ctx,
		"decoding of Postgres logical-decoding message content not translated",
	)
}

// ── TimescaleDb (PostgreSQL) ──────────────────────────────────────────────────

// timescaleDbSMT handles
// io.debezium.connector.postgresql.transforms.timescaledb.TimescaleDb,
// which routes TimescaleDB hypertable chunk events to the parent hypertable topic.
type timescaleDbSMT struct{}

func (timescaleDbSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	return debeziumGuidance(smt, ctx,
		"TimescaleDB hypertable chunk→hypertable routing not translated",
	)
}
