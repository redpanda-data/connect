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

// filterCarrier builds a Convert() input with an S3 sink and optionally a
// predicate declaration. Use smtCarrier for single-SMT tests that don't need
// top-level predicate keys.
func filterCarrier(smtExtra, predicateBlock string) []byte {
	pred := ""
	if predicateBlock != "" {
		pred = "," + predicateBlock
	}
	return []byte(`{"name":"s3","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b",
	  "topics":"orders",
	  "transforms":"t",
	  ` + smtExtra + pred + `
	}}`)
}

// ── Filter SMT: no predicate ─────────────────────────────────────────────────

// TestSMTFilterNoPredicate: Filter with no predicate drops all records.
func TestSMTFilterNoPredicate(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.Filter"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// Must drop all records.
	assert.Contains(t, y, "root = deleted()")
}

// ── Filter SMT: RecordIsTombstone predicate ──────────────────────────────────

// TestSMTFilterRecordIsTombstone: Filter with RecordIsTombstone drops null-value records.
func TestSMTFilterRecordIsTombstone(t *testing.T) {
	in := filterCarrier(
		`"transforms.t.type":"org.apache.kafka.connect.transforms.Filter",
		  "transforms.t.predicate":"isTombstone",
		  "predicates":"isTombstone",
		  "predicates.isTombstone.type":"org.apache.kafka.connect.transforms.predicates.RecordIsTombstone"`,
		"",
	)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// Drops records where value is null (negate=false → drop when predicate true).
	assert.Contains(t, y, "this == null")
	assert.Contains(t, y, "deleted()")
	// Should not have unmapped-field warnings for predicates keys.
	for _, w := range res.Warnings {
		assert.NotContains(t, w.Message, "unmapped field", "unexpected unmapped warning: %s=%s", w.Field, w.Message)
	}
}

// TestSMTFilterRecordIsTombstoneNegate: negate=true keeps only tombstones (drops non-null).
func TestSMTFilterRecordIsTombstoneNegate(t *testing.T) {
	in := filterCarrier(
		`"transforms.t.type":"org.apache.kafka.connect.transforms.Filter",
		  "transforms.t.predicate":"isTombstone",
		  "transforms.t.negate":"true",
		  "predicates":"isTombstone",
		  "predicates.isTombstone.type":"org.apache.kafka.connect.transforms.predicates.RecordIsTombstone"`,
		"",
	)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// negate=true → drop when predicate is FALSE → `if !(this == null) { deleted() }`
	assert.Contains(t, y, "!(this == null)")
	assert.Contains(t, y, "deleted()")
}

// ── Filter SMT: TopicNameMatches predicate ───────────────────────────────────

// TestSMTFilterTopicNameMatches: drops records from topics matching the pattern.
func TestSMTFilterTopicNameMatches(t *testing.T) {
	in := filterCarrier(
		`"transforms.t.type":"org.apache.kafka.connect.transforms.Filter",
		  "transforms.t.predicate":"topicMatch",
		  "predicates":"topicMatch",
		  "predicates.topicMatch.type":"org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
		  "predicates.topicMatch.pattern":".*-cdc"`,
		"",
	)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `@kafka_topic.re_match(".*-cdc")`)
	assert.Contains(t, y, "deleted()")
	for _, w := range res.Warnings {
		assert.NotContains(t, w.Message, "unmapped field", "unexpected unmapped warning: %s=%s", w.Field, w.Message)
	}
}

// TestSMTFilterTopicNameMatchesNegate: negate=true keeps only matching topics.
func TestSMTFilterTopicNameMatchesNegate(t *testing.T) {
	in := filterCarrier(
		`"transforms.t.type":"org.apache.kafka.connect.transforms.Filter",
		  "transforms.t.predicate":"topicMatch",
		  "transforms.t.negate":"true",
		  "predicates":"topicMatch",
		  "predicates.topicMatch.type":"org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
		  "predicates.topicMatch.pattern":"orders-.*"`,
		"",
	)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `!(@kafka_topic.re_match("orders-.*"))`)
	assert.Contains(t, y, "deleted()")
}

// ── Filter SMT: HasHeaderKey predicate ──────────────────────────────────────

// TestSMTFilterHasHeaderKey: drops records that have the specified header key.
func TestSMTFilterHasHeaderKey(t *testing.T) {
	in := filterCarrier(
		`"transforms.t.type":"org.apache.kafka.connect.transforms.Filter",
		  "transforms.t.predicate":"hasHeader",
		  "predicates":"hasHeader",
		  "predicates.hasHeader.type":"org.apache.kafka.connect.transforms.predicates.HasHeaderKey",
		  "predicates.hasHeader.name":"x-dead-letter"`,
		"",
	)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `metadata("x-dead-letter") != null`)
	assert.Contains(t, y, "deleted()")
	for _, w := range res.Warnings {
		assert.NotContains(t, w.Message, "unmapped field", "unexpected unmapped warning: %s=%s", w.Field, w.Message)
	}
}

// ── Filter SMT: unknown predicate class ─────────────────────────────────────

// TestSMTFilterUnknownPredicate: unknown predicate class emits passthrough + TODO + warning.
func TestSMTFilterUnknownPredicate(t *testing.T) {
	in := filterCarrier(
		`"transforms.t.type":"org.apache.kafka.connect.transforms.Filter",
		  "transforms.t.predicate":"myPred",
		  "predicates":"myPred",
		  "predicates.myPred.type":"com.example.CustomPredicate",
		  "predicates.myPred.threshold":"100"`,
		"",
	)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// Passthrough with TODO comment.
	assert.Contains(t, y, "root = this")
	assert.Contains(t, y, "TODO")
	// Warning should mention unsupported predicate.
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning for the unsupported predicate")
	// predicates keys should NOT appear as unmapped fields.
	for _, w := range res.Warnings {
		assert.NotContains(t, w.Message, "unmapped field", "unexpected unmapped warning: %s=%s", w.Field, w.Message)
	}
}

// ── Filter SMT: multiple predicates in predicates list ──────────────────────

// TestSMTFilterMultiplePredicatesDeclared: Filter only uses one; the other
// predicate's keys must also be consumed (no unmapped-field warnings).
func TestSMTFilterMultiplePredicatesDeclared(t *testing.T) {
	in := filterCarrier(
		`"transforms.t.type":"org.apache.kafka.connect.transforms.Filter",
		  "transforms.t.predicate":"topicMatch",
		  "predicates":"topicMatch,isTombstone",
		  "predicates.topicMatch.type":"org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
		  "predicates.topicMatch.pattern":".*-raw",
		  "predicates.isTombstone.type":"org.apache.kafka.connect.transforms.predicates.RecordIsTombstone"`,
		"",
	)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	// No unmapped field warnings.
	for _, w := range res.Warnings {
		assert.NotContains(t, w.Message, "unmapped field", "unexpected unmapped warning: %s=%s", w.Field, w.Message)
	}
}

// ── Filter SMT: predicate referenced but not declared in predicates list ──────

// TestSMTFilterPredicateNotDeclared: predicate name referenced in transforms
// but missing from predicates list — should produce passthrough + warning.
func TestSMTFilterPredicateNotDeclared(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.Filter",
	  "transforms.t.predicate":"ghost"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// Predicate not found → passthrough or drop-all.
	assert.Contains(t, y, "TODO")
}
