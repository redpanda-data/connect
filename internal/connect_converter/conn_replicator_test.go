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

func TestConvertReplicatorWhitelist(t *testing.T) {
	y := gapConvert(t, `{"name":"r","config":{
	  "connector.class":"io.confluent.connect.replicator.ReplicatorSourceConnector",
	  "src.kafka.bootstrap.servers":"src:9092",
	  "dest.kafka.bootstrap.servers":"dst:9092",
	  "topic.whitelist":"orders,payments",
	  "topic.rename.format":"${topic}.replica",
	  "src.consumer.group.id":"repl-orders",
	  "value.converter":"io.confluent.connect.replicator.util.ByteArrayConverter"
	}}`)
	// kafka→kafka maps to redpanda input + output.
	assert.Contains(t, y, "input:")
	assert.Contains(t, y, "output:")
	assert.Contains(t, y, "- src:9092")
	assert.Contains(t, y, "- dst:9092")
	assert.Contains(t, y, "- orders")
	assert.Contains(t, y, "- payments")
	assert.Contains(t, y, "consumer_group: repl-orders")
	// topic.rename.format ${topic}.replica → interpolation.
	assert.Contains(t, y, "topic: ${! @kafka_topic }.replica")
	// Replicator copies bytes — no schema_registry_decode.
	assert.NotContains(t, y, "schema_registry_decode")
}

func TestConvertReplicatorRegexAndBlacklist(t *testing.T) {
	res, err := Convert([]byte(`{"name":"r","config":{
	  "connector.class":"io.confluent.connect.replicator.ReplicatorSourceConnector",
	  "src.kafka.bootstrap.servers":"s:9092",
	  "dest.kafka.bootstrap.servers":"d:9092",
	  "topic.regex":"prod\\..*",
	  "topic.blacklist":"prod.internal",
	  "src.kafka.security.protocol":"SASL_SSL"
	}}`))
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "regexp_topics: true")
	assert.Contains(t, y, `prod\..*`)
	// SASL_SSL source → tls + sasl stubs.
	assert.Contains(t, y, "sasl:")
	assert.Contains(t, y, "tls:")
	// topic.blacklist can't be expressed in the input — expect a warning.
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "topic.blacklist" {
			warned = true
		}
	}
	assert.True(t, warned, "expected topic.blacklist warning")
}

// MirrorMaker2 auxiliary connectors are recognized as operational (no data).
func TestConvertMirrorCheckpointInformational(t *testing.T) {
	res, err := Convert([]byte(`{"name":"cp","config":{"connector.class":"org.apache.kafka.connect.mirror.MirrorCheckpointConnector","source.cluster.alias":"a","target.cluster.alias":"b"}}`))
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "connector.class" {
			warned = true
		}
	}
	assert.True(t, warned, "expected informational warning for MirrorCheckpoint")
	assert.Contains(t, y, "drop")
}
