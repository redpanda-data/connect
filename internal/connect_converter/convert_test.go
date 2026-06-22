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

func TestConvertMalformedReturnsError(t *testing.T) {
	_, err := Convert([]byte(`{bad`))
	require.Error(t, err)
}

func TestConvertMissingClassReturnsError(t *testing.T) {
	_, err := Convert([]byte(`{"name":"x","config":{"topics":"orders"}}`))
	require.Error(t, err)
}
