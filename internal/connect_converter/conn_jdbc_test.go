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
	assert.Empty(t, jdbcDriver("weird"))
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

func TestConvertJDBCSourceNoURL(t *testing.T) {
	in := []byte(`{"name":"jdbc-src-nourl","config":{"connector.class":"io.confluent.connect.jdbc.JdbcSourceConnector"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	// Both driver and dsn stubs must be present so the config passes linting.
	assert.Contains(t, y, "driver:")
	assert.Contains(t, y, "dsn:")
	assertValidRPCN(t, res.YAML)
}

func TestConvertJDBCSinkNoURL(t *testing.T) {
	in := []byte(`{"name":"jdbc-sink-nourl","config":{"connector.class":"io.confluent.connect.jdbc.JdbcSinkConnector"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	// Both driver and dsn stubs must be present so the config passes linting.
	assert.Contains(t, y, "driver:")
	assert.Contains(t, y, "dsn:")
	assertValidRPCN(t, res.YAML)
}

// TestConvertAivenJDBCSourceConnector verifies that the Aiven JDBC source
// connector alias routes to the sql_select input.
func TestConvertAivenJDBCSourceConnector(t *testing.T) {
	in := []byte(`{"name":"aiven-jdbc-src","config":{"connector.class":"io.aiven.connect.jdbc.JdbcSourceConnector","connection.url":"jdbc:postgresql://h:5432/db","table.whitelist":"orders"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "sql_select:")
	assert.Contains(t, y, "driver: postgres")
	assert.Contains(t, y, "table: orders")
	assert.NotContains(t, y, "drop:")
	assert.NotContains(t, y, "unsupported")
}

// TestConvertAivenJDBCSinkConnector verifies that the Aiven JDBC sink
// connector alias routes to the sql_insert output.
func TestConvertAivenJDBCSinkConnector(t *testing.T) {
	in := []byte(`{"name":"aiven-jdbc-sink","config":{"connector.class":"io.aiven.connect.jdbc.JdbcSinkConnector","connection.url":"jdbc:mysql://h:3306/db","table.name.format":"orders"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "sql_insert:")
	assert.Contains(t, y, "driver: mysql")
	assert.Contains(t, y, "table: orders")
	assert.NotContains(t, y, "drop:")
	assert.NotContains(t, y, "unsupported")
}

func TestConvertJDBCSourceFull(t *testing.T) {
	in := []byte(`{"name":"jdbc-src-full","config":{` +
		`"connector.class":"io.confluent.connect.jdbc.JdbcSourceConnector",` +
		`"connection.url":"jdbc:postgresql://h:5432/db",` +
		`"table.whitelist":"orders",` +
		`"mode":"incrementing",` +
		`"incrementing.column.name":"id",` +
		`"poll.interval.ms":"5000",` +
		`"validate.non.null":"false"` +
		`}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)

	// Mapped fields present.
	assert.Contains(t, y, "sql_select:")
	assert.Contains(t, y, "driver: postgres")
	assert.Contains(t, y, "table: orders")
	// Mode TODO surfaced as suffix comment.
	assert.Contains(t, y, "KC mode=incrementing")
	assert.Contains(t, y, "id")

	// Ignored keys must NOT surface as unmapped TODOs.
	assert.NotContains(t, y, "poll.interval.ms")
	assert.NotContains(t, y, "validate.non.null")
	assert.NotContains(t, y, "unmapped field mode")
	assert.NotContains(t, y, "unmapped field poll")
}

func TestConvertJDBCSinkFull(t *testing.T) {
	in := []byte(`{"name":"jdbc-sink-full","config":{` +
		`"connector.class":"io.confluent.connect.jdbc.JdbcSinkConnector",` +
		`"connection.url":"jdbc:postgresql://h:5432/db",` +
		`"table.name.format":"orders",` +
		`"insert.mode":"upsert",` +
		`"batch.size":"500",` +
		`"pk.fields":"id",` +
		`"auto.create":"true"` +
		`}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)

	// Mapped fields present.
	assert.Contains(t, y, "sql_insert:")
	assert.Contains(t, y, "driver: postgres")
	assert.Contains(t, y, "table: orders")
	// insert.mode upsert surfaces as suffix TODO.
	assert.Contains(t, y, "KC insert.mode=upsert")
	// batch.size maps to batching.count.
	assert.Contains(t, y, "batching:")
	assert.Contains(t, y, "count: 500")
	// pk.fields informs columns TODO.
	assert.Contains(t, y, "pk.fields=id")

	// Ignored keys must NOT surface as unmapped TODOs.
	assert.NotContains(t, y, "unmapped field auto.create")
	assert.NotContains(t, y, "unmapped field insert.mode")
	assert.NotContains(t, y, "unmapped field batch.size")
	assert.NotContains(t, y, "unmapped field pk.fields")
}
