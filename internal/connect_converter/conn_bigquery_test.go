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

func TestConvertBigQuerySink(t *testing.T) {
	in := []byte(`{"name":"bq","config":{"connector.class":"com.wepay.kafka.connect.bigquery.BigQuerySinkConnector","project":"proj","defaultDataset":"ds","topics":"orders"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "gcp_bigquery:")
	assert.Contains(t, y, "project: proj")
	assert.Contains(t, y, "dataset: ds")
	assert.Contains(t, y, "table:")
}

func TestConvertBigQuerySinkFull(t *testing.T) {
	in := []byte(`{
		"name": "bq-full",
		"config": {
			"connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
			"project": "my-project",
			"defaultDataset": "my_dataset",
			"topics": "orders,events",
			"credentials": "{\"type\":\"service_account\"}",
			"queueSize": "100",
			"autoCreateTables": "false",
			"sanitizeTopics": "true",
			"allBQFieldsNullable": "true",
			"schemaRetriever": "com.wepay.kafka.connect.bigquery.schemaregistry.SchemaRegistrySchemaRetriever",
			"bigQueryRetry": "3",
			"bigQueryRetryWait": "1000",
			"allowNewBigQueryFields": "false",
			"allowBigQueryRequiredFieldRelaxation": "false",
			"kafkaDataFieldName": "kafkaData",
			"kafkaKeyFieldName": "kafkaKey"
		}
	}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)

	// Mapped fields should appear.
	assert.Contains(t, y, "gcp_bigquery:")
	assert.Contains(t, y, "project: my-project")
	assert.Contains(t, y, "dataset: my_dataset")
	assert.Contains(t, y, "credentials_json:")
	assert.Contains(t, y, "create_disposition: CREATE_NEVER")
	assert.Contains(t, y, "count: 100")

	// Confluent-internal plumbing must NOT surface as unmapped TODOs.
	assert.NotContains(t, y, "sanitizeTopics")
	assert.NotContains(t, y, "allBQFieldsNullable")
	assert.NotContains(t, y, "schemaRetriever")
	assert.NotContains(t, y, "bigQueryRetry")
	assert.NotContains(t, y, "allowNewBigQueryFields")
	assert.NotContains(t, y, "kafkaDataFieldName")
	assert.NotContains(t, y, "kafkaKeyFieldName")
}
