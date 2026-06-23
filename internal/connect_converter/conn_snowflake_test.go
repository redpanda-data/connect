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

func TestConvertSnowflakeSink(t *testing.T) {
	in := []byte(`{"name":"sf","config":{
	  "connector.class":"com.snowflake.kafka.connector.SnowflakeSinkConnector",
	  "snowflake.url.name":"acct.snowflakecomputing.com",
	  "snowflake.user.name":"svc",
	  "snowflake.database.name":"DB",
	  "snowflake.schema.name":"PUBLIC",
	  "snowflake.private.key":"KEY",
	  "topics":"orders"
	}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "snowflake_streaming:")
	assert.Contains(t, y, "user: svc")
	assert.Contains(t, y, "database: DB")
	assert.Contains(t, y, "schema: PUBLIC")
	// role is absent from the input — expect a TODO stub and a warning.
	assert.Contains(t, y, "role: ")
	assert.Contains(t, y, "TODO: set the Snowflake role")
	var roleWarned bool
	for _, w := range res.Warnings {
		if w.Field == "role" {
			roleWarned = true
			break
		}
	}
	assert.True(t, roleWarned, "expected a warning for missing role field")
}

func TestConvertSnowflakeStreamingSink(t *testing.T) {
	in := []byte(`{"name":"sf","config":{
	  "connector.class":"com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector",
	  "snowflake.url.name":"org-acct.snowflakecomputing.com:443",
	  "snowflake.user.name":"u",
	  "snowflake.database.name":"DB",
	  "snowflake.schema.name":"PUBLIC",
	  "snowflake.role.name":"R",
	  "snowflake.private.key":"KEY",
	  "snowflake.ingestion.method":"SNOWPIPE_STREAMING",
	  "topics":"iot"
	}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "snowflake_streaming:")
	assert.NotContains(t, y, "drop:")
	assert.NotContains(t, y, "unsupported connector.class")
}

func TestConvertSnowflakeTopic2Table(t *testing.T) {
	baseConfig := `"connector.class":"com.snowflake.kafka.connector.SnowflakeSinkConnector",` +
		`"snowflake.url.name":"acct.snowflakecomputing.com",` +
		`"snowflake.user.name":"svc",` +
		`"snowflake.role.name":"R",` +
		`"snowflake.database.name":"DB",` +
		`"snowflake.schema.name":"PUBLIC",` +
		`"topics":"orders_topic"`

	t.Run("single mapping sets table without TODO", func(t *testing.T) {
		in := []byte(`{"name":"sf","config":{` + baseConfig + `,"snowflake.topic2table.map":"orders_topic:ORDERS_RAW"}}`)
		res, err := Convert(in)
		require.NoError(t, err)
		y := string(res.YAML)
		assertValidRPCN(t, res.YAML)
		assert.Contains(t, y, "table: ORDERS_RAW")
		assert.NotContains(t, y, "unmapped field snowflake.topic2table.map")
		for _, w := range res.Warnings {
			assert.NotEqual(t, "snowflake.topic2table.map", w.Field, "topic2table.map must not surface as unmapped")
		}
	})

	t.Run("multi mapping sets first table with TODO comment", func(t *testing.T) {
		in := []byte(`{"name":"sf","config":{` + baseConfig + `,"snowflake.topic2table.map":"a:TA,b:TB"}}`)
		res, err := Convert(in)
		require.NoError(t, err)
		y := string(res.YAML)
		assertValidRPCN(t, res.YAML)
		assert.Contains(t, y, "table: TA")
		assert.Contains(t, y, "TODO")
		for _, w := range res.Warnings {
			assert.NotEqual(t, "snowflake.topic2table.map", w.Field, "topic2table.map must not surface as unmapped")
		}
	})
}

func TestConvertSnowflakeSinkFull(t *testing.T) {
	in := []byte(`{"name":"sf-full","config":{
	  "connector.class":"com.snowflake.kafka.connector.SnowflakeSinkConnector",
	  "snowflake.url.name":"myorg-myacct.snowflakecomputing.com",
	  "snowflake.user.name":"loader",
	  "snowflake.role.name":"LOADER_ROLE",
	  "snowflake.database.name":"PROD",
	  "snowflake.schema.name":"PUBLIC",
	  "snowflake.private.key":"MYPRIVKEY",
	  "snowflake.private.key.passphrase":"s3cr3t",
	  "topics":"events",
	  "buffer.count.records":"10000",
	  "buffer.flush.time":"10",
	  "buffer.size.bytes":"5000000",
	  "behavior.on.null.values":"IGNORE",
	  "snowflake.metadata.all":"true"
	}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	// Batching: count is an unquoted int, period uses the seconds suffix.
	assert.Contains(t, y, "batching:")
	assert.Contains(t, y, "count: 10000")
	assert.Contains(t, y, "period: 10s")
	assert.Contains(t, y, "byte_size: 5000000")
	// Key passphrase mapped.
	assert.Contains(t, y, "private_key_pass: s3cr3t")
	// Plumbing keys are silently consumed: neither surfaced as unmapped warnings
	// nor emitted as TODO noise in the YAML.
	assert.NotContains(t, y, "behavior.on.null.values")
	assert.NotContains(t, y, "snowflake.metadata.all")
	for _, w := range res.Warnings {
		assert.NotEqual(t, "behavior.on.null.values", w.Field, "plumbing key must not be unmapped")
		assert.NotEqual(t, "snowflake.metadata.all", w.Field, "plumbing key must not be unmapped")
	}
}
