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

func TestSMTInsertFieldStatic(t *testing.T) {
	in := []byte(`{"name":"s3","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b",
	  "transforms":"ins",
	  "transforms.ins.type":"org.apache.kafka.connect.transforms.InsertField$Value",
	  "transforms.ins.static.field":"src",
	  "transforms.ins.static.value":"kafka",
	  "topics":"orders"
	}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "mapping:")
	assert.Contains(t, y, `root.src = "kafka"`)
}

func TestSMTRegexRouter(t *testing.T) {
	in := []byte(`{"name":"s3","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b",
	  "transforms":"r",
	  "transforms.r.type":"org.apache.kafka.connect.transforms.RegexRouter",
	  "transforms.r.regex":"(.*)",
	  "transforms.r.replacement":"prefix_$1",
	  "topics":"orders"
	}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "re_replace_all")
}

func TestSMTOrderPreserved(t *testing.T) {
	c := newTestCtx(map[string]any{
		"transforms":                "a,b",
		"transforms.a.type":         "org.apache.kafka.connect.transforms.RegexRouter",
		"transforms.a.regex":        "x",
		"transforms.a.replacement":  "y",
		"transforms.b.type":         "org.apache.kafka.connect.transforms.InsertField$Value",
		"transforms.b.static.field": "f",
		"transforms.b.static.value": "v",
	})
	nodes := mapSMTs(c)
	require.Len(t, nodes, 2)
	// first node is the regex router (alias "a"), second is insertfield (alias "b").
	assert.Equal(t, "mapping", nodes[0].Content[0].Value)
	assert.Equal(t, "mapping", nodes[1].Content[0].Value)
}
