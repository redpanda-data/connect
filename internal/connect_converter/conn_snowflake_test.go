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
