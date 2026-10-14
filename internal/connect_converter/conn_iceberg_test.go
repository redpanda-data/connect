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

func TestConvertIcebergSinkRESTCatalog(t *testing.T) {
	y := gapConvert(t, `{"name":"i","config":{
	  "connector.class":"org.apache.iceberg.connect.IcebergSinkConnector",
	  "topics":"events",
	  "iceberg.tables":"analytics.web.events",
	  "iceberg.catalog.type":"rest",
	  "iceberg.catalog.uri":"https://polaris.example.com/api/catalog",
	  "iceberg.catalog.credential":"cid:csecret",
	  "iceberg.catalog.warehouse":"s3://bucket1/wh"
	}}`)
	assert.Contains(t, y, "iceberg:")
	assert.Contains(t, y, "url: https://polaris.example.com/api/catalog")
	// namespace is everything before the final dot.
	assert.Contains(t, y, "namespace: analytics.web")
	assert.Contains(t, y, "table: events")
	// credential "cid:csecret" → oauth2 client_id/client_secret.
	assert.Contains(t, y, "client_id: cid")
	assert.Contains(t, y, "client_secret: csecret")
	// storage bucket parsed from the s3:// warehouse.
	assert.Contains(t, y, "aws_s3:")
	assert.Contains(t, y, "bucket: bucket1")
	// the engine synthesizes a redpanda input from topics.
	assert.Contains(t, y, "redpanda:")
}

// The legacy Tabular class is an alias for the same mapper.
func TestConvertIcebergSinkTabularAlias(t *testing.T) {
	y := gapConvert(t, `{"name":"i","config":{
	  "connector.class":"io.tabular.iceberg.connect.IcebergSinkConnector",
	  "topics":"events",
	  "iceberg.tables":"db.events",
	  "iceberg.catalog.uri":"https://cat/api",
	  "iceberg.catalog.warehouse":"s3://b/wh",
	  "iceberg.tables.upsert-mode-enabled":"true"
	}}`)
	assert.Contains(t, y, "iceberg:")
	// upsert mode is not reproduced — expect a warning.
	res, _ := Convert([]byte(`{"name":"i","config":{"connector.class":"io.tabular.iceberg.connect.IcebergSinkConnector","topics":"events","iceberg.tables":"db.events","iceberg.catalog.uri":"https://cat/api","iceberg.catalog.warehouse":"s3://b/wh","iceberg.tables.upsert-mode-enabled":"true"}}`))
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "iceberg.tables.upsert-mode-enabled" {
			warned = true
		}
	}
	assert.True(t, warned, "expected upsert-mode warning")
}

// AWS Glue (catalog-impl) maps to the Glue REST endpoint with SigV4 auth and a
// derived table location.
func TestConvertIcebergSinkGlue(t *testing.T) {
	y := gapConvert(t, `{"name":"i","config":{
	  "connector.class":"org.apache.iceberg.connect.IcebergSinkConnector",
	  "topics":"events",
	  "iceberg.tables":"db.events",
	  "iceberg.catalog.catalog-impl":"org.apache.iceberg.aws.glue.GlueCatalog",
	  "iceberg.catalog.io-impl":"org.apache.iceberg.aws.s3.S3FileIO",
	  "iceberg.catalog.warehouse":"s3://b/wh",
	  "iceberg.catalog.client.region":"us-west-2",
	  "iceberg.tables.evolve-schema-enabled":"true"
	}}`)
	assert.Contains(t, y, "url: https://glue.us-west-2.amazonaws.com/iceberg")
	assert.Contains(t, y, "aws_sigv4:")
	assert.Contains(t, y, "service: glue")
	assert.Contains(t, y, "region: us-west-2")
	assert.Contains(t, y, "enabled: true")
	assert.Contains(t, y, "table_location: s3://b/wh/")
}

// A non-REST catalog (Hive) can't be expressed by the REST-only iceberg output;
// expect a warning and a TODO instead of a silent wrong mapping.
func TestConvertIcebergSinkHiveWarns(t *testing.T) {
	res, err := Convert([]byte(`{"name":"i","config":{
	  "connector.class":"org.apache.iceberg.connect.IcebergSinkConnector",
	  "topics":"events",
	  "iceberg.tables":"db.events",
	  "iceberg.catalog.type":"hive",
	  "iceberg.catalog.uri":"thrift://hive:9083",
	  "iceberg.catalog.warehouse":"s3a://b/wh"
	}}`))
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "iceberg.catalog.type" {
			warned = true
		}
	}
	assert.True(t, warned, "expected non-REST catalog warning")
	assert.Contains(t, y, "REST")
	// s3a:// warehouse still resolves the S3 storage bucket.
	assert.Contains(t, y, "bucket: b")
}

// Dynamic routing becomes a table interpolation over the route field.
func TestConvertIcebergSinkDynamicRouting(t *testing.T) {
	y := gapConvert(t, `{"name":"i","config":{
	  "connector.class":"org.apache.iceberg.connect.IcebergSinkConnector",
	  "topics":"events",
	  "iceberg.tables.dynamic-enabled":"true",
	  "iceberg.tables.route-field":"dest_table",
	  "iceberg.catalog.uri":"https://cat/api",
	  "iceberg.catalog.warehouse":"gs://gbucket/wh"
	}}`)
	assert.Contains(t, y, "table: ${! this.dest_table }")
	// gs:// warehouse selects the GCS storage backend.
	assert.Contains(t, y, "gcp_cloud_storage:")
	assert.Contains(t, y, "bucket: gbucket")
}
