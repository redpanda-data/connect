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
	// input side is a TODO stub for a sink.
	assert.Contains(t, y, "TODO: set the input")
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
