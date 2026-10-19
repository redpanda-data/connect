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

// ── InsertHeader ──────────────────────────────────────────────────────────────

func TestSMTInsertHeader(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.InsertHeader",
	  "transforms.t.header":"x-source",
	  "transforms.t.value.literal":"kafka"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `meta "x-source" = "kafka"`)
}

func TestSMTInsertHeaderEmpty(t *testing.T) {
	// No header/value.literal → passthrough stub with TODO.
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.InsertHeader"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), "TODO")
}

// ── DropHeaders ───────────────────────────────────────────────────────────────

func TestSMTDropHeaders(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.DropHeaders",
	  "transforms.t.headers":"x-trace,x-span"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `meta "x-trace" = deleted()`)
	assert.Contains(t, y, `meta "x-span" = deleted()`)
}

func TestSMTDropHeadersSingle(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.DropHeaders",
	  "transforms.t.headers":"x-only"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), `meta "x-only" = deleted()`)
}

func TestSMTDropHeadersEmpty(t *testing.T) {
	// No headers property → passthrough stub with TODO.
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.DropHeaders"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), "TODO")
}

// ── HeaderFrom$Value ──────────────────────────────────────────────────────────

func TestSMTHeaderFromValueCopy(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.HeaderFrom$Value",
	  "transforms.t.fields":"user_id,region",
	  "transforms.t.headers":"x-user-id,x-region",
	  "transforms.t.operation":"copy"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `meta "x-user-id" = this.user_id`)
	assert.Contains(t, y, `meta "x-region" = this.region`)
	// copy does NOT delete fields
	assert.NotContains(t, y, "root.user_id = deleted()")
	assert.NotContains(t, y, "root.region = deleted()")
}

func TestSMTHeaderFromValueMove(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.HeaderFrom$Value",
	  "transforms.t.fields":"user_id",
	  "transforms.t.headers":"x-user-id",
	  "transforms.t.operation":"move"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `meta "x-user-id" = this.user_id`)
	// move deletes the source field
	assert.Contains(t, y, "root.user_id = deleted()")
}

func TestSMTHeaderFromKeyWarns(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.HeaderFrom$Key",
	  "transforms.t.fields":"id",
	  "transforms.t.headers":"x-id",
	  "transforms.t.operation":"copy"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	// $Key variant must emit a KEY warning.
	var found bool
	for _, w := range res.Warnings {
		if w.Field == "t" && strings.Contains(w.Message, "KEY") {
			found = true
		}
	}
	assert.True(t, found, "expected $Key variant warning mentioning KEY")
}

func TestSMTHeaderFromMismatchedLengths(t *testing.T) {
	// fields has 2 entries but headers has 1 → common prefix (1 pair), TODO + warning.
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.HeaderFrom$Value",
	  "transforms.t.fields":"a,b",
	  "transforms.t.headers":"x-a",
	  "transforms.t.operation":"copy"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// First pair should still be mapped.
	assert.Contains(t, y, `meta "x-a" = this.a`)
	// A TODO comment must appear.
	assert.Contains(t, y, "TODO")
	// A warning must be emitted.
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" && strings.Contains(strings.ToLower(w.Message), "mismatch") {
			warned = true
		}
	}
	assert.True(t, warned, "expected a mismatch warning")
}

func TestSMTHeaderFromEmpty(t *testing.T) {
	// No fields/headers → passthrough stub.
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.HeaderFrom$Value",
	  "transforms.t.operation":"copy"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), "TODO")
}

// ── HeaderToValue ─────────────────────────────────────────────────────────────

func TestSMTHeaderToValueCopy(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.HeaderToValue",
	  "transforms.t.headers":"x-user-id,x-region",
	  "transforms.t.fields":"user_id,region",
	  "transforms.t.operation":"copy"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `root.user_id = metadata("x-user-id")`)
	assert.Contains(t, y, `root.region = metadata("x-region")`)
	// copy does NOT delete headers
	assert.NotContains(t, y, "meta x-user-id = deleted()")
	assert.NotContains(t, y, "meta x-region = deleted()")
}

func TestSMTHeaderToValueMove(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.HeaderToValue",
	  "transforms.t.headers":"x-trace",
	  "transforms.t.fields":"trace_id",
	  "transforms.t.operation":"move"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `root.trace_id = metadata("x-trace")`)
	// move deletes the header
	assert.Contains(t, y, `meta "x-trace" = deleted()`)
}

func TestSMTHeaderToValueMismatchedLengths(t *testing.T) {
	// headers has 1, fields has 2 → common prefix (1 pair), TODO + warning.
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.HeaderToValue",
	  "transforms.t.headers":"x-a",
	  "transforms.t.fields":"a,b",
	  "transforms.t.operation":"copy"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `root.a = metadata("x-a")`)
	assert.Contains(t, y, "TODO")
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" && strings.Contains(strings.ToLower(w.Message), "mismatch") {
			warned = true
		}
	}
	assert.True(t, warned, "expected a mismatch warning")
}

func TestSMTHeaderToValueEmpty(t *testing.T) {
	// No headers/fields → passthrough stub.
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.HeaderToValue",
	  "transforms.t.operation":"copy"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), "TODO")
}
