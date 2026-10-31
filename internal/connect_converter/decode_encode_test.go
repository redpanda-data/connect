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

// JsonConverter with schemas.enable=true → unwrap the Connect {schema,payload}
// envelope.
func TestDecodeJSONConverterEnvelope(t *testing.T) {
	y := gapConvert(t, `{"name":"s","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b","s3.region":"us-east-1","topics":"t",
	  "value.converter":"org.apache.kafka.connect.json.JsonConverter",
	  "value.converter.schemas.enable":"true"
	}}`)
	assert.Contains(t, y, "root = this.payload")
}

// JsonConverter with schemas.enable=false → no decode processor (benthos
// auto-parses JSON bytes on structured access).
func TestDecodeJSONConverterSchemaless(t *testing.T) {
	res, err := Convert([]byte(`{"name":"s","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b","s3.region":"us-east-1","topics":"t",
	  "value.converter":"org.apache.kafka.connect.json.JsonConverter",
	  "value.converter.schemas.enable":"false"
	}}`))
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.NotContains(t, y, "this.payload")
}

// Avro value converter → schema_registry_decode (decode side).
func TestDecodeAvroSchemaRegistry(t *testing.T) {
	y := gapConvert(t, `{"name":"s","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b","s3.region":"us-east-1","topics":"t",
	  "value.converter":"io.confluent.connect.avro.AvroConverter",
	  "value.converter.schema.registry.url":"http://sr:8081"
	}}`)
	assert.Contains(t, y, "schema_registry_decode")
}

// Object-store Avro format → an `avro` encode processor runs before the output.
func TestEncodeObjectStoreAvro(t *testing.T) {
	y := gapConvert(t, `{"name":"s","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b","s3.region":"us-east-1","topics":"t",
	  "format.class":"io.confluent.connect.s3.format.avro.AvroFormat"
	}}`)
	assert.Contains(t, y, "avro:")
	assert.Contains(t, y, "operator: from_json")
	assert.Contains(t, y, ".avro")
}

// Object-store Parquet format → a path TODO directing the user to add
// parquet_encode (it can't lint without a schema, so no processor is emitted).
func TestEncodeObjectStoreParquetTODO(t *testing.T) {
	y := gapConvert(t, `{"name":"s","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b","s3.region":"us-east-1","topics":"t",
	  "format.class":"io.confluent.connect.s3.format.parquet.ParquetFormat"
	}}`)
	assert.Contains(t, y, "parquet_encode")
	// No avro encoder emitted for parquet.
	assert.NotContains(t, y, "operator: from_json")
}
