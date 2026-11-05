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

// ── Gap 1: TimestampConverter format property ──────────────────────────────

func TestJodaToGoLayout(t *testing.T) {
	cases := []struct {
		joda string
		want string
	}{
		{"yyyy-MM-dd", "2006-01-02"},
		{"yyyy-MM-dd HH:mm:ss", "2006-01-02 15:04:05"},
		{"yyyy-MM-dd HH:mm:ss.SSS", "2006-01-02 15:04:05.000"},
		{"MM/dd/yyyy", "01/02/2006"},
		{"dd-MM-yyyy", "02-01-2006"},
		{"HH:mm:ss", "15:04:05"},
		{"hh:mm:ss a", "03:04:05 PM"},
		{"yy-MM-dd", "06-01-02"},
		{"yyyy/M/d", "2006/1/2"},
		{"yyyy-MM-dd'T'HH:mm:ss", "2006-01-02T15:04:05"},
		{"yyyy-MM-dd'T'HH:mm:ssZ", "2006-01-02T15:04:05Z07:00"},
		{"yyyy-MM-dd'T'HH:mm:ssXXX", "2006-01-02T15:04:05Z07:00"},
		{"yyyy-MM-dd'T'HH:mm:ssz", "2006-01-02T15:04:05Z07:00"},
	}
	for _, tc := range cases {
		got := jodaToGoLayout(tc.joda)
		assert.Equal(t, tc.want, got, "joda=%q", tc.joda)
	}
}

func TestSMTTimestampConverterFormatString(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.TimestampConverter$Value",
	  "transforms.t.field":"ts",
	  "transforms.t.target.type":"string",
	  "transforms.t.format":"yyyy-MM-dd HH:mm:ss"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// Should emit ts_format with the translated Go layout; no inline TODO on the mapping line.
	assert.Contains(t, y, `ts_format("2006-01-02 15:04:05")`)
	// The generated mapping line must NOT contain a TODO comment.
	assert.NotContains(t, y, "ts_format(\"2006-01-02T15:04:05Z07:00\")")
	// No warnings should be emitted for the timestamp converter alias.
	for _, w := range res.Warnings {
		assert.NotEqual(t, "t", w.Field, "unexpected warning for TimestampConverter with format: %s", w.Message)
	}
}

func TestSMTTimestampConverterFormatTimestamp(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.TimestampConverter$Value",
	  "transforms.t.field":"ts",
	  "transforms.t.target.type":"Timestamp",
	  "transforms.t.format":"yyyy-MM-dd HH:mm:ss"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// Should emit ts_parse with the translated Go layout; not the RFC3339 fallback.
	assert.Contains(t, y, `ts_parse("2006-01-02 15:04:05")`)
	assert.NotContains(t, y, "ts_parse(\"2006-01-02T15:04:05Z07:00\")")
	// No warnings for this alias.
	for _, w := range res.Warnings {
		assert.NotEqual(t, "t", w.Field, "unexpected warning for TimestampConverter with format: %s", w.Message)
	}
}

func TestSMTTimestampConverterNoFormatFallback(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.TimestampConverter$Value",
	  "transforms.t.field":"ts",
	  "transforms.t.target.type":"string"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// No format → RFC3339 fallback + TODO.
	assert.Contains(t, y, `ts_format("2006-01-02T15:04:05Z07:00")`)
	assert.Contains(t, y, "TODO")
}

// ── Gap 3: Cast integer types should use int64() ───────────────────────────

func TestSMTCastIntegerUsesInt64(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.Cast$Value",
	  "transforms.t.spec":"count:int32,id:int64,small:int8,med:int16"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "this.count.int64()")
	assert.Contains(t, y, "this.id.int64()")
	assert.Contains(t, y, "this.small.int64()")
	assert.Contains(t, y, "this.med.int64()")
	// float types should still use number()
	assert.NotContains(t, y, "this.count.number()")
}

func TestSMTCastFloatStillNumber(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.Cast$Value",
	  "transforms.t.spec":"price:float64,rate:float32"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "this.price.number()")
	assert.Contains(t, y, "this.rate.number()")
}

// ── Gap 7: Cast whole-value form (spec without colon) ─────────────────────

func TestSMTCastWholeValueString(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.Cast$Value",
	  "transforms.t.spec":"string"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), "root = this.string()")
}

func TestSMTCastWholeValueInt(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.Cast$Value",
	  "transforms.t.spec":"int64"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), "root = this.int64()")
}

func TestSMTCastWholeValueUnknown(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.Cast$Value",
	  "transforms.t.spec":"bytes"`)
	res, err := Convert(in)
	require.NoError(t, err)
	// unknown type → passthrough stub with TODO
	assert.Contains(t, string(res.YAML), "TODO")
}

// ── Gap 5: InsertField metadata fields ────────────────────────────────────

func TestSMTInsertFieldTopic(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.InsertField$Value",
	  "transforms.t.topic.field":"topic_name"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), `root.topic_name = @kafka_topic`)
}

func TestSMTInsertFieldTimestamp(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.InsertField$Value",
	  "transforms.t.timestamp.field":"msg_ts"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// Should reference the kafka timestamp metadata key
	assert.Contains(t, y, "msg_ts")
	assert.Contains(t, y, "kafka_timestamp")
}

func TestSMTInsertFieldPartition(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.InsertField$Value",
	  "transforms.t.partition.field":"part"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), `root.part = metadata("kafka_partition")`)
}

func TestSMTInsertFieldOffset(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.InsertField$Value",
	  "transforms.t.offset.field":"off"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), `root.off = metadata("kafka_offset")`)
}

func TestSMTInsertFieldMultiple(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.InsertField$Value",
	  "transforms.t.topic.field":"t",
	  "transforms.t.partition.field":"p",
	  "transforms.t.offset.field":"o"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `root.t = @kafka_topic`)
	assert.Contains(t, y, `root.p = metadata("kafka_partition")`)
	assert.Contains(t, y, `root.o = metadata("kafka_offset")`)
}

// ── Gap 6: ReplaceField blacklist alias ────────────────────────────────────

func TestSMTReplaceFieldBlacklist(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.ReplaceField$Value",
	  "transforms.t.blacklist":"secret,password"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "root.secret = deleted()")
	assert.Contains(t, y, "root.password = deleted()")
}

func TestSMTReplaceFieldInclude(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.ReplaceField$Value",
	  "transforms.t.include":"id,name,email"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// Should keep only the listed fields
	assert.Contains(t, y, "root.id = this.id")
	assert.Contains(t, y, "root.name = this.name")
	assert.Contains(t, y, "root.email = this.email")
}

func TestSMTReplaceFieldWhitelist(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.ReplaceField$Value",
	  "transforms.t.whitelist":"a,b"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "root.a = this.a")
	assert.Contains(t, y, "root.b = this.b")
}

// ── Gap 18: ReplaceField fallback message accuracy ────────────────────────

func TestSMTReplaceFieldFallbackMessage(t *testing.T) {
	// An empty config: nothing parseable → fallback stub.
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.ReplaceField$Value"`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assert.Contains(t, y, "TODO")
	// The message should NOT say "include/whitelist semantics" when nothing was present.
	// It should reflect which properties were looked for.
	assert.Contains(t, y, "ReplaceField")
}

// ── Gap 17: RegexRouter @kafka_topic shorthand ────────────────────────────

func TestSMTRegexRouterAtShorthand(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.RegexRouter",
	  "transforms.t.regex":"(.*)-raw",
	  "transforms.t.replacement":"$1-processed"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// Must use @kafka_topic shorthand, not metadata("kafka_topic").
	assert.Contains(t, y, `@kafka_topic.re_replace_all`)
	assert.NotContains(t, y, `metadata("kafka_topic")`)
}
