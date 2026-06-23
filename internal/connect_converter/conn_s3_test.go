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
)

func TestConvertS3Sink(t *testing.T) {
	in := []byte(`{
	  "name":"s3-sink",
	  "config":{
	    "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	    "s3.bucket.name":"my-bucket",
	    "s3.region":"us-east-1",
	    "topics":"orders"
	  }
	}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)

	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "aws_s3:")
	assert.Contains(t, y, "bucket: my-bucket")
	assert.Contains(t, y, "region: us-east-1")
	assert.Contains(t, y, "path:")
	// input side synthesized from topics.
	assert.Contains(t, y, "redpanda:")
	assert.Contains(t, y, "orders")
	assert.Contains(t, y, "consumer_group: connect-s3-sink")
}

func TestConvertS3SinkFull(t *testing.T) {
	in := []byte(`{
	  "name":"s3-sink",
	  "config":{
	    "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	    "tasks.max":"1",
	    "topics":"test-topic",
	    "s3.region":"us-west-2",
	    "s3.bucket.name":"confluent-kafka-connect-s3-testing",
	    "flush.size":"3",
	    "format.class":"io.confluent.connect.s3.format.avro.AvroFormat",
	    "partitioner.class":"io.confluent.connect.storage.partitioner.DefaultPartitioner",
	    "s3.part.size":"5242880",
	    "schema.compatibility":"NONE",
	    "schema.generator.class":"io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",
	    "storage.class":"io.confluent.connect.s3.storage.S3Storage"
	  }
	}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)

	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "aws_s3:")
	assert.Contains(t, y, "bucket: confluent-kafka-connect-s3-testing")
	assert.Contains(t, y, "region: us-west-2")
	assert.Contains(t, y, "batching:")
	assert.Contains(t, y, "count: 3") // unquoted integer
	// Avro format drives the object extension.
	assert.Contains(t, y, ".avro")

	// Recognized KC plumbing must be dropped silently, not surfaced as TODOs.
	for _, k := range []string{
		"storage.class", "schema.generator.class",
		"schema.compatibility", "s3.part.size", "partitioner.class",
		"s3.compression.type",
	} {
		assert.NotContains(t, y, k, "ignored key %q should not appear as an unmapped field", k)
	}
	assert.NotContains(t, y, "unmapped field")
}

// TestConvertAivenS3SinkConnector verifies that the Aiven S3 connector alias
// routes to the same aws_s3 output and that aws.s3.bucket.name is used as a
// fallback bucket source when s3.bucket.name is absent.
func TestConvertAivenS3SinkConnector(t *testing.T) {
	in := []byte(`{
	  "name":"aiven-s3-sink",
	  "config":{
	    "connector.class":"io.aiven.kafka.connect.s3.S3SinkConnector",
	    "aws.s3.bucket.name":"my-bucket",
	    "topics":"orders"
	  }
	}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "aws_s3:")
	assert.Contains(t, y, "bucket: my-bucket")
	assert.NotContains(t, y, "drop:")
	assert.NotContains(t, y, "unsupported")
}

// TestConvertAivenKafkaConnectS3SinkConnector verifies the longer Aiven class
// name alias also routes correctly with the aws.s3.bucket.name fallback.
func TestConvertAivenKafkaConnectS3SinkConnector(t *testing.T) {
	in := []byte(`{
	  "name":"aiven-s3-sink-long",
	  "config":{
	    "connector.class":"io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector",
	    "aws.s3.bucket.name":"my-bucket",
	    "topics":"orders"
	  }
	}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "aws_s3:")
	assert.Contains(t, y, "bucket: my-bucket")
	assert.NotContains(t, y, "drop:")
	assert.NotContains(t, y, "unsupported")
}

// TestConvertAivenS3SinkFormatOutputType verifies Aiven format.output.type
// drives the object extension when format.class is absent.
func TestConvertAivenS3SinkFormatOutputType(t *testing.T) {
	cases := []struct {
		name    string
		fmtVal  string
		wantExt string
	}{
		{"json", "json", ".json"},
		{"jsonl", "jsonl", ".jsonl"},
		{"csv", "csv", ".csv"},
		{"parquet", "parquet", ".parquet"},
		{"avro", "avro", ".avro"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(`{"name":"s3","config":{"connector.class":"io.aiven.kafka.connect.s3.S3SinkConnector","aws.s3.bucket.name":"b","topics":"t","format.output.type":"` + tc.fmtVal + `"}}`)
			res, err := Convert(in)
			require.NoError(t, err)
			y := string(res.YAML)
			assertValidRPCN(t, res.YAML)
			assert.Contains(t, y, tc.wantExt)
			assert.NotContains(t, y, "format.output.type")
			// avro/parquet must still surface the encode TODO.
			if tc.fmtVal == "avro" || tc.fmtVal == "parquet" {
				assert.Contains(t, y, "add an encode step")
			}
		})
	}
}

// TestConvertS3SinkFileMaxRecords verifies Aiven file.max.records maps to
// batching.count.
func TestConvertS3SinkFileMaxRecords(t *testing.T) {
	in := []byte(`{"name":"s3","config":{"connector.class":"io.aiven.kafka.connect.s3.S3SinkConnector","aws.s3.bucket.name":"b","topics":"t","file.max.records":"50"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "count: 50")
	assert.NotContains(t, y, "file.max.records")
	assert.NotContains(t, y, "unmapped field")
}

// TestConvertS3SinkCompression verifies file.compression.type appends a suffix
// to the extension and attaches a compress-processor TODO.
func TestConvertS3SinkCompression(t *testing.T) {
	cases := []struct{ compression, suffix string }{
		{"gzip", ".gz"},
		{"snappy", ".snappy"},
		{"zstd", ".zst"},
	}
	for _, tc := range cases {
		t.Run(tc.compression, func(t *testing.T) {
			in := []byte(`{"name":"s3","config":{"connector.class":"io.aiven.kafka.connect.s3.S3SinkConnector","aws.s3.bucket.name":"b","topics":"t","format.output.type":"json","file.compression.type":"` + tc.compression + `"}}`)
			res, err := Convert(in)
			require.NoError(t, err)
			y := string(res.YAML)
			assertValidRPCN(t, res.YAML)
			assert.Contains(t, y, ".json"+tc.suffix)
			assert.Contains(t, y, "compress")
			assert.NotContains(t, y, "file.compression.type")
		})
	}
}

// TestConvertS3SinkCompressionNone verifies none/absent compression adds no
// suffix and no TODO.
func TestConvertS3SinkCompressionNone(t *testing.T) {
	in := []byte(`{"name":"s3","config":{"connector.class":"io.aiven.kafka.connect.s3.S3SinkConnector","aws.s3.bucket.name":"b","topics":"t","format.output.type":"json","file.compression.type":"none"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.NotContains(t, y, ".json.")
	assert.NotContains(t, y, "file.compression.type")
}

// TestConvertS3SinkTimeBasedPartitioner verifies TimeBasedPartitioner path.format
// translates into a time-bucketed object path prefix.
func TestConvertS3SinkTimeBasedPartitioner(t *testing.T) {
	in := []byte(`{"name":"s3","config":{"connector.class":"io.confluent.connect.s3.S3SinkConnector","s3.bucket.name":"b","s3.region":"us-east-1","topics":"t","partitioner.class":"io.confluent.connect.storage.partitioner.TimeBasedPartitioner","path.format":"'year'=YYYY/'month'=MM/'day'=dd","partition.duration.ms":"3600000","locale":"en-US","timezone":"UTC"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	// Time-bucketed prefix using the record timestamp metadata.
	assert.Contains(t, y, `metadata("kafka_timestamp_ms").number().ts_format("2006")`)
	assert.Contains(t, y, `year=`)
	assert.Contains(t, y, `month=`)
	assert.Contains(t, y, `day=`)
	// The topic-based suffix is still present.
	assert.Contains(t, y, "@kafka_topic")
	// All time-partitioner keys consumed — no TODO noise.
	for _, k := range []string{"partitioner.class", "path.format", "partition.duration.ms", "locale", "timezone"} {
		assert.NotContains(t, y, k)
	}
	assert.NotContains(t, y, "unmapped field")
}

// TestConvertS3SinkTimeBasedPartitionerNoFormat verifies TimeBasedPartitioner
// without path.format consumes the keys and emits a TODO.
func TestConvertS3SinkTimeBasedPartitionerNoFormat(t *testing.T) {
	in := []byte(`{"name":"s3","config":{"connector.class":"io.confluent.connect.s3.S3SinkConnector","s3.bucket.name":"b","s3.region":"us-east-1","topics":"t","partitioner.class":"io.confluent.connect.storage.partitioner.TimeBasedPartitioner"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "TODO")
	assert.NotContains(t, y, "partitioner.class")
	assert.NotContains(t, y, "unmapped field")
}

// TestConvertS3SinkFieldPartitioner verifies a non-time, non-default partitioner
// still surfaces as a TODO (left for manual review).
func TestConvertS3SinkFieldPartitioner(t *testing.T) {
	in := []byte(`{"name":"s3","config":{"connector.class":"io.confluent.connect.s3.S3SinkConnector","s3.bucket.name":"b","s3.region":"us-east-1","topics":"t","partitioner.class":"io.confluent.connect.storage.partitioner.FieldPartitioner"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	// FieldPartitioner is NOT consumed — surfaces in the unmapped sweep.
	assert.Contains(t, y, "partitioner.class")
}

// TestConvertS3SinkRotateSchedule verifies that flush.size AND
// rotate.schedule.interval.ms (with no rotate.interval.ms) produce exactly ONE
// batching: block containing both count and period.
func TestConvertS3SinkRotateSchedule(t *testing.T) {
	in := []byte(`{
	  "name":"s3-sink-sched",
	  "config":{
	    "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	    "s3.bucket.name":"my-bucket",
	    "s3.region":"us-east-1",
	    "topics":"orders",
	    "flush.size":"100",
	    "rotate.schedule.interval.ms":"60000"
	  }
	}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)

	assertValidRPCN(t, res.YAML)

	// Exactly one batching: block — count the occurrences.
	batchingCount := strings.Count(y, "batching:")
	assert.Equal(t, 1, batchingCount, "expected exactly one batching: block, got %d\n%s", batchingCount, y)

	// Both count and period must appear.
	assert.Contains(t, y, "count: 100")
	assert.Contains(t, y, "period: 60000ms")

	// No unmapped field TODOs.
	assert.NotContains(t, y, "unmapped field")
}
