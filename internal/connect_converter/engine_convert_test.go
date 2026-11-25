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

func TestConvertUnknownConnector(t *testing.T) {
	in := []byte(`{"name":"mystery","config":{"connector.class":"com.acme.Mystery","topics":"orders"}}`)
	res, err := Convert(in)
	require.NoError(t, err)

	y := string(res.YAML)
	assert.Contains(t, y, "Converted from Kafka Connect connector \"mystery\"")
	assert.Contains(t, y, "TODO: unsupported connector.class=com.acme.Mystery")
	// "topics" is an unmapped field for the fallback connector.
	assert.Contains(t, y, "topics")
	assert.NotEmpty(t, res.Warnings)

	assertValidRPCN(t, res.YAML)
}

func TestSinkSynthesizesRedpandaInput(t *testing.T) {
	in := []byte(`{"name":"my-sink","config":{"connector.class":"io.confluent.connect.s3.S3SinkConnector","s3.bucket.name":"bkt","s3.region":"us-east-1","topics":"orders"}}`)
	res, err := Convert(in)
	require.NoError(t, err)

	y := string(res.YAML)
	assert.Contains(t, y, "redpanda:")
	assert.Contains(t, y, "orders")
	assert.Contains(t, y, "consumer_group: connect-my-sink")
	assert.NotContains(t, y, "stdin:")

	assertValidRPCN(t, res.YAML)
}

func TestUnknownConnectorKeepsStdinStub(t *testing.T) {
	in := []byte(`{"name":"mystery","config":{"connector.class":"com.acme.Mystery","topics":"orders"}}`)
	res, err := Convert(in)
	require.NoError(t, err)

	y := string(res.YAML)
	assert.Contains(t, y, "stdin:")
	assert.NotContains(t, y, "consumer_group: connect-mystery")

	assertValidRPCN(t, res.YAML)
}

func TestConvertMalformedReturnsError(t *testing.T) {
	_, err := Convert([]byte(`{bad`))
	require.Error(t, err)
}

func TestConvertMissingClassReturnsError(t *testing.T) {
	_, err := Convert([]byte(`{"name":"x","config":{"topics":"orders"}}`))
	require.Error(t, err)
}
