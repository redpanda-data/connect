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

// ---------------------------------------------------------------------------
// 1. TimestampRouter
// ---------------------------------------------------------------------------

func TestTimestampRouterDefault(t *testing.T) {
	// Default topic.format="${topic}-${timestamp}" with default timestamp.format="yyyyMMdd"
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.TimestampRouter"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// Must reference kafka_topic and use ts_format with the Go layout for yyyyMMdd
	assert.Contains(t, y, "@kafka_topic")
	assert.Contains(t, y, `ts_format("20060102")`)
	assert.Contains(t, y, `meta kafka_topic`)
}

func TestTimestampRouterCustomFormat(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.TimestampRouter",
	  "transforms.t.topic.format":"archive-${topic}-${timestamp}",
	  "transforms.t.timestamp.format":"yyyy-MM-dd"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `"archive-"`)
	assert.Contains(t, y, "@kafka_topic")
	assert.Contains(t, y, `ts_format("2006-01-02")`)
}

func TestTimestampRouterTopicOnly(t *testing.T) {
	// topic.format with only ${topic} (no timestamp placeholder)
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.TimestampRouter",
	  "transforms.t.topic.format":"${topic}"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "meta kafka_topic = @kafka_topic")
}

func TestTimestampRouterWarns(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.TimestampRouter"`)
	res, err := Convert(in)
	require.NoError(t, err)
	var found bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected a TimestampRouter warning")
}

// ---------------------------------------------------------------------------
// 2. MessageTimestampRouter (Confluent)
// ---------------------------------------------------------------------------

func TestMessageTimestampRouterBasic(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.MessageTimestampRouter",
	  "transforms.t.topic.format":"${topic}-${timestamp}",
	  "transforms.t.timestamp.format":"yyyyMMdd"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "@kafka_topic")
	assert.Contains(t, y, `ts_format("20060102")`)
}

func TestMessageTimestampRouterCustomTimestampKeys(t *testing.T) {
	// message.timestamp.keys present → TODO comment + warning
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.MessageTimestampRouter",
	  "transforms.t.topic.format":"${topic}-${timestamp}",
	  "transforms.t.timestamp.format":"yyyyMMdd",
	  "transforms.t.message.timestamp.keys":"created_at,updated_at"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// TODO comment about custom timestamp keys must appear
	assert.Contains(t, y, "TODO")
	assert.Contains(t, y, "created_at")
	// Warning must exist
	var found bool
	for _, w := range res.Warnings {
		if w.Field == "t" && strings.Contains(w.Message, "created_at") {
			found = true
		}
	}
	assert.True(t, found, "expected warning about message.timestamp.keys")
}

// ---------------------------------------------------------------------------
// 3. ExtractTopic
// ---------------------------------------------------------------------------

func TestExtractTopicValue(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.ExtractTopic$Value",
	  "transforms.t.field":"event_type"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "meta kafka_topic = this.event_type.string()")
}

func TestExtractTopicValueEmptyField(t *testing.T) {
	// No field → use whole value
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.ExtractTopic$Value"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "meta kafka_topic = this.string()")
}

func TestExtractTopicHeader(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.ExtractTopic$Header",
	  "transforms.t.field":"target_topic"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `meta kafka_topic = metadata("target_topic")`)
}

func TestExtractTopicKey(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.ExtractTopic$Key",
	  "transforms.t.field":"event_type"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "meta kafka_topic = this.event_type.string()")
	// $Key variant must warn
	var found bool
	for _, w := range res.Warnings {
		if w.Field == "t" && strings.Contains(w.Message, "KEY") {
			found = true
		}
	}
	assert.True(t, found, "expected KEY variant warning")
}

func TestExtractTopicSkipMissingConsumed(t *testing.T) {
	// skip.missing.or.null is consumed without error
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.transforms.ExtractTopic$Value",
	  "transforms.t.field":"topic_field",
	  "transforms.t.skip.missing.or.null":"true"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
}

// ---------------------------------------------------------------------------
// 4. TopicRegexRouter (Confluent cloud)
// ---------------------------------------------------------------------------

func TestTopicRegexRouter(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.cloud.transforms.TopicRegexRouter",
	  "transforms.t.regex":"(.*)-events",
	  "transforms.t.replacement":"$1"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "re_replace_all")
	assert.Contains(t, y, `"(.*)-events"`)
}

func TestTopicRegexRouterWarns(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.confluent.connect.cloud.transforms.TopicRegexRouter",
	  "transforms.t.regex":"x",
	  "transforms.t.replacement":"y"`)
	res, err := Convert(in)
	require.NoError(t, err)
	var found bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected a TopicRegexRouter warning")
}

// ---------------------------------------------------------------------------
// 5. ByLogicalTableRouter (Debezium)
// ---------------------------------------------------------------------------

func TestByLogicalTableRouterBasic(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.ByLogicalTableRouter",
	  "transforms.t.topic.regex":"(.*)_shard[0-9]+",
	  "transforms.t.topic.replacement":"$1"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "re_replace_all")
	assert.Contains(t, y, `"(.*)_shard[0-9]+"`)
	assert.Contains(t, y, `"$1"`)
}

func TestByLogicalTableRouterKeyFields(t *testing.T) {
	// key.field.* present → TODO + warning
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.ByLogicalTableRouter",
	  "transforms.t.topic.regex":"(.*)_[0-9]+",
	  "transforms.t.topic.replacement":"$1",
	  "transforms.t.key.field.name":"source_table",
	  "transforms.t.key.enforce.uniqueness":"true"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "TODO")
	var found bool
	for _, w := range res.Warnings {
		if w.Field == "t" && strings.Contains(w.Message, "key-field") {
			found = true
		}
	}
	assert.True(t, found, "expected key-field rewriting warning")
}

func TestByLogicalTableRouterWarns(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"io.debezium.transforms.ByLogicalTableRouter",
	  "transforms.t.topic.regex":"(.*)_shard[0-9]+",
	  "transforms.t.topic.replacement":"$1"`)
	res, err := Convert(in)
	require.NoError(t, err)
	var found bool
	for _, w := range res.Warnings {
		if w.Field == "t" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected a ByLogicalTableRouter topic-rewrite warning")
}
