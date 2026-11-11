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
	"gopkg.in/yaml.v3"
)

// renderInput marshals the given input node into a minimal RPCN pipeline YAML
// suitable for assertValidRPCN, using a stdout output to satisfy the linter.
func renderInput(t *testing.T, inputNode *yaml.Node) []byte {
	t.Helper()
	inputYAML, err := yaml.Marshal(inputNode)
	require.NoError(t, err, "marshalling input node")

	var sb strings.Builder
	sb.WriteString("input:\n")
	for line := range strings.SplitSeq(strings.TrimRight(string(inputYAML), "\n"), "\n") {
		sb.WriteString("    ")
		sb.WriteString(line)
		sb.WriteByte('\n')
	}
	sb.WriteString("output:\n    stdout: {}\n")
	return []byte(sb.String())
}

func TestSinkInputFromTopics_LiteralTopics(t *testing.T) {
	cfg := ConnectConfig{
		Name:  "my-sink",
		Class: "io.confluent.connect.s3.S3SinkConnector",
		Props: map[string]any{
			"topics": "orders,events",
		},
	}
	ctx := newMapCtx(cfg)
	node := sinkInputFromTopics(cfg, ctx)

	require.NotNil(t, node, "expected a redpanda input node for literal topics")
	out, err := yaml.Marshal(node)
	require.NoError(t, err)
	y := string(out)

	assert.Contains(t, y, "redpanda:")
	assert.Contains(t, y, "orders")
	assert.Contains(t, y, "events")
	assert.Contains(t, y, "consumer_group: connect-my-sink")
	assert.NotContains(t, y, "regexp_topics")

	// topics should be consumed; topics.regex was not present.
	assert.Empty(t, ctx.Unmapped())

	assertValidRPCN(t, renderInput(t, node))
}

func TestSinkInputFromTopics_RegexTopics(t *testing.T) {
	cfg := ConnectConfig{
		Name:  "regex-sink",
		Class: "io.confluent.connect.s3.S3SinkConnector",
		Props: map[string]any{
			"topics.regex": "orders.*",
		},
	}
	ctx := newMapCtx(cfg)
	node := sinkInputFromTopics(cfg, ctx)

	require.NotNil(t, node, "expected a redpanda input node for topics.regex")
	out, err := yaml.Marshal(node)
	require.NoError(t, err)
	y := string(out)

	assert.Contains(t, y, "redpanda:")
	assert.Contains(t, y, "orders.*")
	assert.Contains(t, y, "regexp_topics: true")
	assert.Contains(t, y, "consumer_group: connect-regex-sink")

	// topics.regex should be consumed.
	assert.Empty(t, ctx.Unmapped())

	assertValidRPCN(t, renderInput(t, node))
}

func TestSinkInputFromTopics_NoTopics(t *testing.T) {
	cfg := ConnectConfig{
		Name:  "no-topics-sink",
		Class: "io.confluent.connect.s3.S3SinkConnector",
		Props: map[string]any{},
	}
	ctx := newMapCtx(cfg)
	node := sinkInputFromTopics(cfg, ctx)

	assert.Nil(t, node, "expected nil when neither topics nor topics.regex is present")
}

func TestSinkInputFromTopics_LiteralTopicsTakesPrecedence(t *testing.T) {
	// When both keys are present, literal `topics` wins.
	cfg := ConnectConfig{
		Name:  "both-sink",
		Class: "io.confluent.connect.s3.S3SinkConnector",
		Props: map[string]any{
			"topics":       "payments",
			"topics.regex": "orders.*",
		},
	}
	ctx := newMapCtx(cfg)
	node := sinkInputFromTopics(cfg, ctx)

	require.NotNil(t, node)
	out, err := yaml.Marshal(node)
	require.NoError(t, err)
	y := string(out)

	// Literal topics path: no regexp_topics; topics.regex untouched (left for unmapped).
	assert.Contains(t, y, "payments")
	assert.NotContains(t, y, "regexp_topics")
	// topics.regex was NOT consumed — it should surface as unmapped.
	assert.Contains(t, ctx.Unmapped(), "topics.regex")
}

// TestConvertS3SinkTopicsRegex exercises the full Convert path to ensure
// a sink connector configured with topics.regex synthesizes a redpanda input
// with regexp_topics: true and passes the benthos linter.
func TestConvertS3SinkTopicsRegex(t *testing.T) {
	in := []byte(`{
	  "name": "s3-regex-sink",
	  "config": {
	    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
	    "s3.bucket.name":  "my-bucket",
	    "s3.region":       "us-east-1",
	    "topics.regex":    "orders.*"
	  }
	}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)

	assert.Contains(t, y, "redpanda:")
	assert.Contains(t, y, "orders.*")
	assert.Contains(t, y, "regexp_topics: true")
	assert.Contains(t, y, "consumer_group: connect-s3-regex-sink")
	assert.NotContains(t, y, "stdin:")

	assertValidRPCN(t, res.YAML)
}
