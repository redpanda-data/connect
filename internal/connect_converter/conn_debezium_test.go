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

func TestDebeziumPostgres(t *testing.T) {
	in := []byte(`{
		"name": "debezium-pg",
		"config": {
			"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
			"database.hostname": "pghost",
			"database.port": "5432",
			"database.user": "pguser",
			"database.password": "pgpass",
			"database.dbname": "mydb",
			"table.include.list": "public.orders,public.customers",
			"slot.name": "my_slot",
			"publication.name": "my_pub",
			"snapshot.mode": "initial",
			"topic.prefix": "myprefix",
			"tombstones.on.delete": "false",
			"decimal.handling.mode": "precise"
		}
	}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)

	assert.Contains(t, y, "postgres_cdc:")
	assert.Contains(t, y, "dsn: postgres://pguser:pgpass@pghost:5432/mydb")
	assert.Contains(t, y, "public.orders")
	assert.Contains(t, y, "public.customers")
	assert.Contains(t, y, "slot_name: my_slot")
	assert.Contains(t, y, "stream_snapshot:")
	assert.NotContains(t, y, "drop:")

	assertValidRPCN(t, res.YAML)
}

func TestDebeziumMySQL(t *testing.T) {
	in := []byte(`{
		"name": "debezium-mysql",
		"config": {
			"connector.class": "io.debezium.connector.mysql.MySqlConnector",
			"database.hostname": "mysqlhost",
			"database.port": "3306",
			"database.user": "mysqluser",
			"database.password": "mysqlpass",
			"database.dbname": "shopdb",
			"table.include.list": "shopdb.orders,shopdb.products",
			"snapshot.mode": "initial",
			"topic.prefix": "shop",
			"database.server.id": "12345",
			"tombstones.on.delete": "true",
			"heartbeat.interval.ms": "10000"
		}
	}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)

	assert.Contains(t, y, "mysql_cdc:")
	assert.Contains(t, y, "dsn: mysqluser:mysqlpass@tcp(mysqlhost:3306)/shopdb")
	assert.Contains(t, y, "shopdb.orders")
	assert.Contains(t, y, "shopdb.products")
	assert.Contains(t, y, "checkpoint_cache:")
	assert.Contains(t, y, "stream_snapshot:")
	assert.NotContains(t, y, "drop:")

	assertValidRPCN(t, res.YAML)
}

func TestDebeziumSQLServer(t *testing.T) {
	in := []byte(`{
		"name": "debezium-sqlserver",
		"config": {
			"connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
			"database.hostname": "sqlhost",
			"database.port": "1433",
			"database.user": "sqluser",
			"database.password": "sqlpass",
			"database.dbname": "salesdb",
			"table.include.list": "dbo.orders,dbo.customers",
			"snapshot.mode": "schema_only",
			"topic.prefix": "sales",
			"tombstones.on.delete": "false"
		}
	}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)

	assert.Contains(t, y, "microsoft_sql_server_cdc:")
	assert.Contains(t, y, "connection_string: sqlserver://sqluser:sqlpass@sqlhost:1433?database=salesdb")
	assert.Contains(t, y, "dbo.orders")
	assert.Contains(t, y, "dbo.customers")
	assert.Contains(t, y, "stream_snapshot:")
	assert.NotContains(t, y, "drop:")

	assertValidRPCN(t, res.YAML)
}

func TestDebeziumOracle(t *testing.T) {
	in := []byte(`{
		"name": "debezium-oracle",
		"config": {
			"connector.class": "io.debezium.connector.oracle.OracleConnector",
			"database.hostname": "oraclehost",
			"database.port": "1521",
			"database.user": "orauser",
			"database.password": "orapass",
			"database.dbname": "ORCLPDB1",
			"table.include.list": "INVENTORY.ORDERS,INVENTORY.CUSTOMERS",
			"snapshot.mode": "initial",
			"topic.prefix": "oracle.server",
			"tombstones.on.delete": "false",
			"lob.enabled": "true"
		}
	}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)

	assert.Contains(t, y, "oracledb_cdc:")
	assert.Contains(t, y, "connection_string: oracle://orauser:orapass@oraclehost:1521/ORCLPDB1")
	assert.Contains(t, y, "INVENTORY.ORDERS")
	assert.Contains(t, y, "INVENTORY.CUSTOMERS")
	assert.Contains(t, y, "stream_snapshot:")
	assert.NotContains(t, y, "drop:")

	assertValidRPCN(t, res.YAML)
}

// TestDebeziumMongoUnsupported verifies that the MongoDB connector is not
// registered and falls through to the drop stub.
func TestDebeziumMongoUnsupported(t *testing.T) {
	in := []byte(`{
		"name": "debezium-mongo",
		"config": {
			"connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
			"mongodb.connection.string": "mongodb://mongohost:27017",
			"collection.include.list": "mydb.orders"
		}
	}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)

	assert.Contains(t, y, "drop:")
	assert.Contains(t, y, "unsupported connector.class")
	assert.NotContains(t, y, "mongo_cdc")
	assert.NotContains(t, y, "mongodb_cdc")
}
