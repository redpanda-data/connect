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

func TestJDBCDriver(t *testing.T) {
	assert.Equal(t, "postgres", jdbcDriver("jdbc:postgresql://h:5432/db"))
	assert.Equal(t, "mysql", jdbcDriver("jdbc:mysql://h:3306/db"))
	assert.Equal(t, "", jdbcDriver("weird"))
	assert.Equal(t, "mssql", jdbcDriver("jdbc:sqlserver://h:1433;databaseName=db"))
	assert.Equal(t, "clickhouse", jdbcDriver("jdbc:clickhouse://h:8123/db"))
	assert.Equal(t, "postgresql://h:5432/db", dsnFromURL("jdbc:postgresql://h:5432/db"))
	assert.Equal(t, "weird", dsnFromURL("weird"))
}

func TestConvertJDBCSource(t *testing.T) {
	in := []byte(`{"name":"jdbc-src","config":{"connector.class":"io.confluent.connect.jdbc.JdbcSourceConnector","connection.url":"jdbc:postgresql://h:5432/db","table.whitelist":"orders"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "sql_select:")
	assert.Contains(t, y, "driver: postgres")
	assert.Contains(t, y, "table: orders")
}

func TestConvertJDBCSink(t *testing.T) {
	in := []byte(`{"name":"jdbc-sink","config":{"connector.class":"io.confluent.connect.jdbc.JdbcSinkConnector","connection.url":"jdbc:mysql://h:3306/db","table.name.format":"orders"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "sql_insert:")
	assert.Contains(t, y, "driver: mysql")
	assert.Contains(t, y, "table: orders")
}
