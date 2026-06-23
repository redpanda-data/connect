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

func TestConvertGCSSink(t *testing.T) {
	in := []byte(`{"name":"gcs","config":{"connector.class":"io.confluent.connect.gcs.GcsSinkConnector","gcs.bucket.name":"bkt","topics":"orders"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "gcp_cloud_storage:")
	assert.Contains(t, y, "bucket: bkt")
	assert.Contains(t, y, "path:")
}

// TestConvertAivenGCSSinkConnector verifies that the Aiven GCS connector alias
// routes to the same gcp_cloud_storage output with the standard bucket field.
func TestConvertAivenGCSSinkConnector(t *testing.T) {
	in := []byte(`{"name":"aiven-gcs","config":{"connector.class":"io.aiven.kafka.connect.gcs.GcsSinkConnector","gcs.bucket.name":"my-gcs-bucket","topics":"orders"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "gcp_cloud_storage:")
	assert.Contains(t, y, "bucket: my-gcs-bucket")
	assert.NotContains(t, y, "drop:")
	assert.NotContains(t, y, "unsupported")
}

// TestConvertAivenGCSSinkFormatOutputType verifies Aiven format.output.type
// drives the GCS object extension when format.class is absent.
func TestConvertAivenGCSSinkFormatOutputType(t *testing.T) {
	cases := []struct {
		name    string
		fmtVal  string
		wantExt string
	}{
		{"json", "json", ".json"},
		{"jsonl", "jsonl", ".jsonl"},
		{"csv", "csv", ".csv"},
		{"avro", "avro", ".avro"},
		{"parquet", "parquet", ".parquet"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(`{"name":"gcs","config":{"connector.class":"io.aiven.kafka.connect.gcs.GcsSinkConnector","gcs.bucket.name":"b","topics":"t","format.output.type":"` + tc.fmtVal + `"}}`)
			res, err := Convert(in)
			require.NoError(t, err)
			y := string(res.YAML)
			assertValidRPCN(t, res.YAML)
			assert.Contains(t, y, tc.wantExt)
			assert.NotContains(t, y, "format.output.type")
			if tc.fmtVal == "avro" || tc.fmtVal == "parquet" {
				assert.Contains(t, y, "add an encode step")
			}
		})
	}
}

// TestConvertGCSSinkFileMaxRecords verifies Aiven file.max.records maps to
// batching.count on GCS.
func TestConvertGCSSinkFileMaxRecords(t *testing.T) {
	in := []byte(`{"name":"gcs","config":{"connector.class":"io.aiven.kafka.connect.gcs.GcsSinkConnector","gcs.bucket.name":"b","topics":"t","file.max.records":"25"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "count: 25")
	assert.NotContains(t, y, "file.max.records")
	assert.NotContains(t, y, "unmapped field")
}

// TestConvertGCSSinkCompression verifies file.compression.type appends a suffix
// and attaches a compress-processor TODO on GCS.
func TestConvertGCSSinkCompression(t *testing.T) {
	in := []byte(`{"name":"gcs","config":{"connector.class":"io.aiven.kafka.connect.gcs.GcsSinkConnector","gcs.bucket.name":"b","topics":"t","format.output.type":"json","file.compression.type":"gzip"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, ".json.gz")
	assert.Contains(t, y, "compress")
	assert.NotContains(t, y, "file.compression.type")
}

// TestConvertGCSSinkTimeBasedPartitioner verifies TimeBasedPartitioner path.format
// translation on GCS.
func TestConvertGCSSinkTimeBasedPartitioner(t *testing.T) {
	in := []byte(`{"name":"gcs","config":{"connector.class":"io.confluent.connect.gcs.GcsSinkConnector","gcs.bucket.name":"b","topics":"t","partitioner.class":"io.confluent.connect.storage.partitioner.TimeBasedPartitioner","path.format":"'year'=YYYY/'month'=MM/'day'=dd"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	// kafka_timestamp_unix is epoch SECONDS; ts_format requires seconds.
	assert.Contains(t, y, `metadata("kafka_timestamp_unix").number().ts_format("2006")`)
	// Verify the old (broken) millisecond form is NOT emitted.
	assert.NotContains(t, y, `metadata("kafka_timestamp_ms").number().ts_format`)
	assert.Contains(t, y, "@kafka_topic")
	assert.NotContains(t, y, "partitioner.class")
	assert.NotContains(t, y, "path.format")
	assert.NotContains(t, y, "unmapped field")
}

func TestConvertGCSSinkFull(t *testing.T) {
	in := []byte(`{
		"name": "gcs-sink-full",
		"config": {
			"connector.class": "io.confluent.connect.gcs.GcsSinkConnector",
			"gcs.bucket.name": "my-gcs-bucket",
			"topics": "orders,shipments",
			"flush.size": "3",
			"format.class": "io.confluent.connect.gcs.format.avro.AvroFormat",
			"gcs.part.size": "5242880",
			"storage.class": "io.confluent.connect.gcs.storage.GCSStorage",
			"schema.compatibility": "NONE",
			"schema.generator.class": "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",
			"partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner"
		}
	}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "gcp_cloud_storage:")
	assert.Contains(t, y, "bucket: my-gcs-bucket")
	// batching.count must be an unquoted integer
	assert.Contains(t, y, "count: 3")
	// path must end with .avro (format derived from AvroFormat)
	assert.Contains(t, y, ".avro")
	// ignored KC plumbing must NOT appear as unmapped TODO
	assert.NotContains(t, y, "gcs.part.size")
	assert.NotContains(t, y, "storage.class")
	assert.NotContains(t, y, "schema.compatibility")
	assert.NotContains(t, y, "schema.generator.class")
	assert.NotContains(t, y, "partitioner.class")
}
