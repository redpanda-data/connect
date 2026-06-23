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
