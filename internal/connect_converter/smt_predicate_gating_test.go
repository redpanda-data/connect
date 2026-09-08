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

// predicateCarrier builds a Convert() input for a non-Filter SMT with optional
// predicate/negate keys and a predicates block.
func predicateCarrier(smtFields, predicateBlock string) []byte {
	pred := ""
	if predicateBlock != "" {
		pred = "," + predicateBlock
	}
	return []byte(`{"name":"s3","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b",
	  "topics":"orders",
	  "transforms":"t",
	  ` + smtFields + pred + `
	}}`)
}

// ── Non-Filter SMT: no predicate → no switch ─────────────────────────────────

// TestSMTGatingNoPredicate: an InsertField SMT with no predicate emits a plain
// mapping processor, not a switch.
func TestSMTGatingNoPredicate(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.InsertField$Value",
	  "transforms.t.static.field":"src",
	  "transforms.t.static.value":"kafka"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `root.src = "kafka"`)
	assert.NotContains(t, y, "switch:")
}

// ── Non-Filter SMT: RecordIsTombstone predicate → switch wrapper ─────────────

// TestSMTGatingInsertFieldRecordIsTombstone: an InsertField with a
// RecordIsTombstone predicate emits a switch processor whose check is
// `this == null` and whose nested processors contain the mapping.
func TestSMTGatingInsertFieldRecordIsTombstone(t *testing.T) {
	in := predicateCarrier(
		`"transforms.t.type":"org.apache.kafka.connect.transforms.InsertField$Value",
		  "transforms.t.static.field":"src",
		  "transforms.t.static.value":"kafka",
		  "transforms.t.predicate":"isTombstone",
		  "predicates":"isTombstone",
		  "predicates.isTombstone.type":"org.apache.kafka.connect.transforms.predicates.RecordIsTombstone"`,
		"",
	)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// Must have a switch processor.
	assert.Contains(t, y, "switch:")
	// Check expression must be the predicate bloblang.
	assert.Contains(t, y, "this == null")
	// The InsertField mapping must be nested inside the switch.
	assert.Contains(t, y, `root.src = "kafka"`)
	// No unmapped-field warnings for predicates keys.
	for _, w := range res.Warnings {
		assert.NotContains(t, w.Message, "unmapped field",
			"unexpected unmapped warning: %s=%s", w.Field, w.Message)
	}
}

// ── negate=true → negated check ──────────────────────────────────────────────

// TestSMTGatingNegate: negate=true wraps the predicate expression in `!(...)`.
func TestSMTGatingNegate(t *testing.T) {
	in := predicateCarrier(
		`"transforms.t.type":"org.apache.kafka.connect.transforms.InsertField$Value",
		  "transforms.t.static.field":"env",
		  "transforms.t.static.value":"prod",
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
	assert.Contains(t, y, "switch:")
	// The check must negate the predicate expression.
	assert.Contains(t, y, "!(this == null)")
	assert.Contains(t, y, `root.env = "prod"`)
	for _, w := range res.Warnings {
		assert.NotContains(t, w.Message, "unmapped field",
			"unexpected unmapped warning: %s=%s", w.Field, w.Message)
	}
}

// ── TopicNameMatches predicate ────────────────────────────────────────────────

// TestSMTGatingTopicNameMatches: a TopicNameMatches predicate generates the
// correct re_match Bloblang expression inside the switch check.
func TestSMTGatingTopicNameMatches(t *testing.T) {
	in := predicateCarrier(
		`"transforms.t.type":"org.apache.kafka.connect.transforms.InsertField$Value",
		  "transforms.t.static.field":"env",
		  "transforms.t.static.value":"prod",
		  "transforms.t.predicate":"topicMatch",
		  "predicates":"topicMatch",
		  "predicates.topicMatch.type":"org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
		  "predicates.topicMatch.pattern":"orders-.*"`,
		"",
	)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "switch:")
	assert.Contains(t, y, `@kafka_topic.re_match("orders-.*")`)
	assert.Contains(t, y, `root.env = "prod"`)
	for _, w := range res.Warnings {
		assert.NotContains(t, w.Message, "unmapped field",
			"unexpected unmapped warning: %s=%s", w.Field, w.Message)
	}
}

// ── HasHeaderKey predicate ────────────────────────────────────────────────────

// TestSMTGatingHasHeaderKey: a HasHeaderKey predicate generates the correct
// metadata Bloblang expression inside the switch check.
func TestSMTGatingHasHeaderKey(t *testing.T) {
	in := predicateCarrier(
		`"transforms.t.type":"org.apache.kafka.connect.transforms.InsertField$Value",
		  "transforms.t.static.field":"flagged",
		  "transforms.t.static.value":"yes",
		  "transforms.t.predicate":"hasHeader",
		  "predicates":"hasHeader",
		  "predicates.hasHeader.type":"org.apache.kafka.connect.transforms.predicates.HasHeaderKey",
		  "predicates.hasHeader.name":"x-flag"`,
		"",
	)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "switch:")
	assert.Contains(t, y, `metadata("x-flag") != null`)
	assert.Contains(t, y, `root.flagged = "yes"`)
	for _, w := range res.Warnings {
		assert.NotContains(t, w.Message, "unmapped field",
			"unexpected unmapped warning: %s=%s", w.Field, w.Message)
	}
}

// ── Unknown predicate class on non-Filter SMT → no switch, warn ──────────────

// TestSMTGatingUnknownPredicate: an SMT with an unknown predicate class emits
// the processor ungated with a warning (no switch wrapper).
func TestSMTGatingUnknownPredicate(t *testing.T) {
	in := predicateCarrier(
		`"transforms.t.type":"org.apache.kafka.connect.transforms.InsertField$Value",
		  "transforms.t.static.field":"src",
		  "transforms.t.static.value":"kafka",
		  "transforms.t.predicate":"myPred",
		  "predicates":"myPred",
		  "predicates.myPred.type":"com.example.CustomPredicate"`,
		"",
	)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// The InsertField mapping must still appear (ungated).
	assert.Contains(t, y, `root.src = "kafka"`)
	// A warning must be emitted for the unsupported predicate.
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning for the unsupported predicate on SMT t")
	// predicates keys must not surface as unmapped fields.
	for _, w := range res.Warnings {
		assert.NotContains(t, w.Message, "unmapped field",
			"unexpected unmapped warning: %s=%s", w.Field, w.Message)
	}
}

// ── Filter SMT is NOT double-gated ───────────────────────────────────────────

// TestSMTFilterNotDoubleGated: the Filter SMT handles its own predicate
// internally; it must NOT be wrapped in an extra switch by the generic gating.
func TestSMTFilterNotDoubleGated(t *testing.T) {
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
	// The Filter generates a mapping proc; there must be NO switch wrapper.
	assert.NotContains(t, y, "switch:")
	// The tombstone drop expression must still be present.
	assert.Contains(t, y, "deleted()")
}
