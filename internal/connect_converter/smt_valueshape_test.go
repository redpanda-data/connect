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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── SetSchemaMetadata ─────────────────────────────────────────────────────────

func TestSMTSetSchemaMetadataValue(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.SetSchemaMetadata$Value",
	  "transforms.t.schema.name":"com.example.MyRecord",
	  "transforms.t.schema.version":"1"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	// No processors should be emitted (no-op).
	assert.NotContains(t, string(res.YAML), "mapping:")
	// A warning must be present.
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" && strings.Contains(w.Message, "SetSchemaMetadata") {
			warned = true
		}
	}
	assert.True(t, warned, "expected SetSchemaMetadata informational warning")
}

func TestSMTSetSchemaMetadataKey(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.SetSchemaMetadata$Key",
	  "transforms.t.schema.name":"com.example.MyKey"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.NotContains(t, string(res.YAML), "mapping:")
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" && strings.Contains(w.Message, "SetSchemaMetadata") {
			warned = true
		}
	}
	assert.True(t, warned, "expected SetSchemaMetadata informational warning for $Key variant")
}

// ── Flatten ───────────────────────────────────────────────────────────────────

func TestSMTFlattenValueDefaultDelimiter(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.Flatten$Value"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "root = this")
	assert.Contains(t, y, "TODO")
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" && strings.Contains(w.Message, "Flatten") {
			warned = true
		}
	}
	assert.True(t, warned, "expected Flatten best-effort warning")
}

func TestSMTFlattenValueCustomDelimiter(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.Flatten$Value",
	  "transforms.t.delimiter":"_"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// Should mention the delimiter in the TODO comment.
	assert.Contains(t, y, `"_"`)
	assert.Contains(t, y, "TODO")
}

func TestSMTFlattenKey(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.Flatten$Key"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	// $Key variant should also produce KEY warning.
	var found bool
	for _, w := range res.Warnings {
		if w.Field == "t" && strings.Contains(w.Message, "KEY") {
			found = true
		}
	}
	assert.True(t, found, "expected $Key variant warning")
}

// ── Confluent ReplaceField alias ──────────────────────────────────────────────

func TestSMTConfluentReplaceFieldInclude(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.ReplaceField$Value",
	  "transforms.t.include":"id,name"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "root.id = this.id")
	assert.Contains(t, y, "root.name = this.name")
}

func TestSMTConfluentReplaceFieldExclude(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.ReplaceField$Value",
	  "transforms.t.exclude":"secret"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "root.secret = deleted()")
}

func TestSMTConfluentReplaceFieldRenames(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.ReplaceField$Key",
	  "transforms.t.renames":"old:new"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "root.new = this.old")
	assert.Contains(t, y, "root.old = deleted()")
}

// ── GzipDecompress ────────────────────────────────────────────────────────────

func TestSMTGzipDecompressValue(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.GzipDecompress$Value"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `decompress("gzip")`)
}

func TestSMTGzipDecompressKey(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.GzipDecompress$Key"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `decompress("gzip")`)
	// $Key variant should warn.
	var found bool
	for _, w := range res.Warnings {
		if w.Field == "t" && strings.Contains(w.Message, "KEY") {
			found = true
		}
	}
	assert.True(t, found, "expected $Key variant warning for GzipDecompress")
}

// ── Drop ──────────────────────────────────────────────────────────────────────

func TestSMTDropValue(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.Drop$Value"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "root = null")
	assert.Contains(t, y, "TODO")
}

func TestSMTDropKey(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.Drop$Key"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "root = this")
	assert.Contains(t, y, "TODO")
	// $Key variant should warn.
	var found bool
	for _, w := range res.Warnings {
		if w.Field == "t" && strings.Contains(w.Message, "KEY") {
			found = true
		}
	}
	assert.True(t, found, "expected $Key variant warning for Drop$Key")
}

// ── TombstoneHandler ─────────────────────────────────────────────────────────

func TestSMTTombstoneHandlerDrop(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.TombstoneHandler",
	  "transforms.t.behavior":"drop"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "deleted()")
	assert.Contains(t, y, "null")
}

func TestSMTTombstoneHandlerIgnore(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.TombstoneHandler",
	  "transforms.t.behavior":"ignore"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "root = this")
}

func TestSMTTombstoneHandlerWarn(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.TombstoneHandler",
	  "transforms.t.behavior":"warn"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "root = this")
}

func TestSMTTombstoneHandlerFail(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.TombstoneHandler",
	  "transforms.t.behavior":"fail"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "root = this")
	assert.Contains(t, y, "TODO")
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" && strings.Contains(w.Message, "fail") {
			warned = true
		}
	}
	assert.True(t, warned, "expected TombstoneHandler fail warning")
}

func TestSMTTombstoneHandlerNoBehavior(t *testing.T) {
	// No behavior property → default passthrough.
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.TombstoneHandler"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), "root = this")
}

// ── FromXml ───────────────────────────────────────────────────────────────────

func TestSMTFromXmlValue(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.cloud.transforms.xml.FromXml$Value"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "parse_xml()")
}
