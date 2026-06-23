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

func TestConvertMirror(t *testing.T) {
	in := []byte(`{"name":"mm","config":{
	  "connector.class":"org.apache.kafka.connect.mirror.MirrorSourceConnector",
	  "source.cluster.bootstrap.servers":"src:9092",
	  "target.cluster.bootstrap.servers":"dst:9092",
	  "topics":"orders"
	}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	// Must use redpanda, not kafka_franz.
	assert.Contains(t, y, "redpanda:")
	assert.NotContains(t, y, "kafka_franz:")
	assert.Contains(t, y, "src:9092")
	assert.Contains(t, y, "dst:9092")
	// both sides populated — no TODO broker stub.
	assert.NotContains(t, y, "TODO: set source cluster brokers")
	assert.NotContains(t, y, "TODO: set target cluster brokers")
}

func TestConvertMirrorFull(t *testing.T) {
	in := []byte(`{"name":"mm-full","config":{
	  "connector.class":"org.apache.kafka.connect.mirror.MirrorSourceConnector",
	  "source.cluster.bootstrap.servers":"src1:9092,src2:9092",
	  "target.cluster.bootstrap.servers":"dst:9093",
	  "topics":"orders,events",
	  "source.cluster.security.protocol":"SASL_SSL",
	  "source.cluster.sasl.mechanism":"SCRAM-SHA-512",
	  "source.cluster.sasl.jaas.config":"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"u\" password=\"p\";",
	  "target.cluster.security.protocol":"SSL",
	  "replication.factor":"3",
	  "sync.topic.configs.enabled":"true",
	  "refresh.topics.enabled":"true",
	  "emit.heartbeats.enabled":"true",
	  "emit.checkpoints.enabled":"true",
	  "refresh.groups.enabled":"true",
	  "sync.group.offsets.enabled":"false"
	}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)

	// Must use redpanda.
	assert.Contains(t, y, "redpanda:")
	assert.NotContains(t, y, "kafka_franz:")

	// Both broker sets present.
	assert.Contains(t, y, "src1:9092")
	assert.Contains(t, y, "src2:9092")
	assert.Contains(t, y, "dst:9093")

	// Security TODO stubs present (SASL_SSL source → tls + sasl; SSL target → tls).
	assert.Contains(t, y, "TODO: configure source cluster TLS")
	assert.Contains(t, y, "TODO: set SASL mechanism")
	assert.Contains(t, y, "TODO: configure target cluster TLS")

	// Plumbing keys must NOT surface as unmapped TODO warnings.
	assert.NotContains(t, y, "replication.factor")
	assert.NotContains(t, y, "sync.topic.configs.enabled")
	assert.NotContains(t, y, "emit.heartbeats.enabled")
}
