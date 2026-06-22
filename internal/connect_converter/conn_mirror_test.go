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
	assert.Contains(t, y, "kafka_franz:")
	assert.Contains(t, y, "src:9092")
	assert.Contains(t, y, "dst:9092")
	// both sides populated — no TODO input stub.
	assert.NotContains(t, y, "TODO: set the input")
	assert.NotContains(t, y, "TODO: set the output")
}
