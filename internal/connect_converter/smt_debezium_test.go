// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── ExtractNewRecordState (existing) ─────────────────────────────────────────

func TestSMTExtractNewRecordStateNoOp(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.ExtractNewRecordState"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// No-op: must NOT emit a mapping: processor.
	assert.NotContains(t, y, "mapping:")
	// Must emit an informational warning (field key uses the legacy transforms.<alias>.type format).
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "transforms.t.type" {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning for ExtractNewRecordState")
}

// ── ExtractNewDocumentState (MongoDB no-op) ───────────────────────────────────

func TestSMTExtractNewDocumentStateNoOp(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.connector.mongodb.transforms.ExtractNewDocumentState"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// No-op: must NOT emit a mapping: processor.
	assert.NotContains(t, y, "mapping:")
	// Must emit an informational warning.
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning for ExtractNewDocumentState")
}

// ── EventRouter (outbox best-effort) ─────────────────────────────────────────

func TestSMTEventRouterBestEffort(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.outbox.EventRouter"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// Must emit a mapping: processor.
	assert.Contains(t, y, "mapping:")
	// Must contain the outbox default column references.
	assert.Contains(t, y, "meta kafka_topic = this.aggregatetype")
	assert.Contains(t, y, "root = this.payload")
	// Must contain the TODO comment.
	assert.Contains(t, y, "TODO")
	// Must emit a warning.
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning for EventRouter")
}

// ── MongoEventRouter (MongoDB outbox best-effort) ─────────────────────────────

func TestSMTMongoEventRouterBestEffort(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.connector.mongodb.transforms.outbox.MongoEventRouter"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// Must emit a mapping: processor.
	assert.Contains(t, y, "mapping:")
	// Must contain the outbox default column references.
	assert.Contains(t, y, "meta kafka_topic = this.aggregatetype")
	assert.Contains(t, y, "root = this.payload")
	// Must contain the TODO comment.
	assert.Contains(t, y, "TODO")
	// Must emit a warning.
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning for MongoEventRouter")
}

// ── TimezoneConverter (best-effort, passthrough + TODO) ───────────────────────

func TestSMTTimezoneConverterBestEffort(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.TimezoneConverter",
	  "transforms.t.converted.timezone":"UTC",
	  "transforms.t.include.list":"created_at,updated_at"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// Must emit a mapping: processor with passthrough.
	assert.Contains(t, y, "mapping:")
	assert.Contains(t, y, "root = this")
	// Must contain TODO with timezone and field list.
	assert.Contains(t, y, "TODO")
	assert.Contains(t, y, "UTC")
	assert.Contains(t, y, "created_at,updated_at")
	// Must emit a warning.
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning for TimezoneConverter")
}

func TestSMTTimezoneConverterNoProps(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.TimezoneConverter"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "mapping:")
	assert.Contains(t, y, "root = this")
	assert.Contains(t, y, "TODO")
}

// ── Filter / scripted (guidance-only) ─────────────────────────────────────────

func TestSMTDebeziumFilterGuidance(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.Filter",
	  "transforms.t.language":"jsr223.groovy",
	  "transforms.t.condition":"value.after != null"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "mapping:")
	assert.Contains(t, y, "root = this")
	assert.Contains(t, y, "TODO")
	// condition and language should appear in the TODO comment.
	assert.Contains(t, y, "jsr223.groovy")
	assert.Contains(t, y, "value.after != null")
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning for Debezium Filter")
}

// ── ContentBasedRouter / scripted (guidance-only) ────────────────────────────

func TestSMTContentBasedRouterGuidance(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.ContentBasedRouter",
	  "transforms.t.language":"jsr223.groovy",
	  "transforms.t.topic.expression":"'topic-'+value.after.category"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "mapping:")
	assert.Contains(t, y, "root = this")
	assert.Contains(t, y, "TODO")
	// topic.expression should appear in the TODO comment.
	assert.Contains(t, y, "topic-")
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning for ContentBasedRouter")
}

// ── PartitionRouting (guidance-only) ──────────────────────────────────────────

func TestSMTPartitionRoutingGuidance(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.partitions.PartitionRouting",
	  "transforms.t.partition.payload.fields":"account_id"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "mapping:")
	assert.Contains(t, y, "root = this")
	assert.Contains(t, y, "TODO")
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning for PartitionRouting")
}

// ── ComputePartition (removed in Debezium 2.5, guidance-only) ─────────────────

func TestSMTComputePartitionGuidance(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.partitions.ComputePartition"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "mapping:")
	assert.Contains(t, y, "root = this")
	assert.Contains(t, y, "TODO")
	// Must mention it was removed in 2.5.
	assert.Contains(t, y, "2.5")
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning for ComputePartition")
}

// ── ExtractChangedRecordState (guidance-only) ─────────────────────────────────

func TestSMTExtractChangedRecordStateGuidance(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.ExtractChangedRecordState"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "mapping:")
	assert.Contains(t, y, "root = this")
	assert.Contains(t, y, "TODO")
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning for ExtractChangedRecordState")
}

// ── SchemaChangeEventFilter (guidance-only) ────────────────────────────────────

func TestSMTSchemaChangeEventFilterGuidance(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.SchemaChangeEventFilter"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "mapping:")
	assert.Contains(t, y, "root = this")
	assert.Contains(t, y, "TODO")
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning for SchemaChangeEventFilter")
}

// ── GeometryFormatTransformer (guidance-only) ──────────────────────────────────

func TestSMTGeometryFormatTransformerGuidance(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.GeometryFormatTransformer"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "mapping:")
	assert.Contains(t, y, "root = this")
	assert.Contains(t, y, "TODO")
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning for GeometryFormatTransformer")
}

// ── DecodeLogicalDecodingMessageContent (guidance-only) ───────────────────────

func TestSMTDecodeLogicalDecodingMessageContentGuidance(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.connector.postgresql.transforms.DecodeLogicalDecodingMessageContent"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "mapping:")
	assert.Contains(t, y, "root = this")
	assert.Contains(t, y, "TODO")
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning for DecodeLogicalDecodingMessageContent")
}

// ── TimescaleDb (guidance-only) ───────────────────────────────────────────────

func TestSMTTimescaleDbGuidance(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.connector.postgresql.transforms.timescaledb.TimescaleDb"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "mapping:")
	assert.Contains(t, y, "root = this")
	assert.Contains(t, y, "TODO")
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning for TimescaleDb")
}
