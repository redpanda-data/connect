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

func TestConvertWithAvroValueConverter(t *testing.T) {
	in := []byte(`{"name":"s3","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b",
	  "value.converter":"io.confluent.connect.avro.AvroConverter",
	  "value.converter.schema.registry.url":"http://sr:8081",
	  "topics":"orders"
	}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "processors:")
	assert.Contains(t, y, "schema_registry_decode:")
	assert.Contains(t, y, "url: http://sr:8081")
}

func TestConvertWithJSONValueConverter_NoProcessor(t *testing.T) {
	in := []byte(`{"name":"s3","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b",
	  "value.converter":"org.apache.kafka.connect.json.JsonConverter",
	  "topics":"orders"
	}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.NotContains(t, string(res.YAML), "schema_registry_decode")
}

func TestSnowflakeJsonConverterNoProcessor(t *testing.T) {
	in := []byte(`{"name":"s3","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b",
	  "value.converter":"com.snowflake.kafka.connector.records.SnowflakeJsonConverter",
	  "topics":"orders"
	}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.NotContains(t, string(res.YAML), "schema_registry_decode")
	// Confirm no warning about unsupported value converter.
	for _, w := range res.Warnings {
		assert.NotContains(t, w.Field, "unsupported value converter", "SnowflakeJsonConverter must not produce an unsupported-converter warning")
	}
}
