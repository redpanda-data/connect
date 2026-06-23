// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

func init() {
	registerConnector("io.debezium.connector.postgresql.PostgresConnector", debeziumPostgresConnector{})
	registerConnector("io.debezium.connector.mysql.MySqlConnector", debeziumMySQLConnector{})
	registerConnector("io.debezium.connector.sqlserver.SqlServerConnector", debeziumSQLServerConnector{})
	registerConnector("io.debezium.connector.oracle.OracleConnector", debeziumOracleConnector{})
	// io.debezium.connector.mongodb.MongoDbConnector is intentionally not
	// registered — there is no RPCN mongodb_cdc equivalent; it falls through
	// to the drop stub.
}

// debeziumHostPort reads database.hostname and database.port (defaulting to
// the supplied defaultPort) and returns "host:port".
func debeziumHostPort(ctx *MapCtx, defaultPort string) string {
	host, _ := ctx.String("database.hostname")
	if host == "" {
		host = "localhost"
	}
	port, _ := ctx.String("database.port")
	if port == "" {
		port = defaultPort
	}
	return host + ":" + port
}

// debeziumCreds returns (user, password) from the Debezium config.
func debeziumCreds(ctx *MapCtx) (string, string) {
	user, _ := ctx.String("database.user")
	password, _ := ctx.String("database.password")
	return user, password
}

// debeziumDBName reads database.dbname (used by Postgres) and database.dbname
// variant; Debezium also uses database.dbname for Postgres.
func debeziumDBName(ctx *MapCtx) string {
	db, _ := ctx.String("database.dbname")
	return db
}

// debeziumTables emits the tables field (list) from table.include.list (CSV).
// Returns the emitted node for further use.
func debeziumTables(body *yaml.Node, ctx *MapCtx, fieldName string) {
	if v, ok := ctx.String("table.include.list"); ok && v != "" {
		kv(body, fieldName, seq(scalarsFromCSV(v)...))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: list tables to capture (e.g. schema.table)"
		kv(body, fieldName, seq(stub))
	}
}

// debeziumSnapshotModeToInitial returns true when Debezium snapshot.mode
// implies a full initial snapshot (modes "initial", "always",
// "initial_only"). Returns false for "schema_only", "never", etc.
func debeziumSnapshotModeToInitial(mode string) bool {
	switch mode {
	case "initial", "always", "initial_only":
		return true
	default:
		return false
	}
}

// consumeDebeziumCommon silently drops Debezium plumbing fields that have no
// RPCN equivalent. topic.prefix and database.server.name are intentionally NOT
// consumed here — they fall through to the engine's Unmapped() sweep so they
// appear as inline "# TODO: unmapped field …" comments in the generated YAML.
//
// Converter keys (value.converter, key.converter and their sub-keys) are
// consumed here so that mapConverters() detects the early consumption and skips
// emitting a schema_registry_decode processor — CDC inputs deliver structured
// rows directly from the WAL/binlog; they do not read Avro/JSON-encoded bytes.
func consumeDebeziumCommon(ctx *MapCtx) {
	consumeIgnored(ctx,
		"tombstones.on.delete",
		"decimal.handling.mode",
		"time.precision.mode",
		"heartbeat.interval.ms",
		"max.batch.size",
		"max.queue.size",
		"poll.interval.ms",
		"connect.timeout.ms",
		"snapshot.locking.mode",
		"snapshot.isolation.mode",
		"include.schema.changes",
		"include.unknown.datatypes",
		"schema.name.adjustment.mode",
		"column.exclude.list",
		"column.include.list",
		"message.key.columns",
		"event.processing.failure.handling.mode",
		"errors.max.retries",
		"skipped.operations",
		"signal.data.collection",
		"notification.enabled.channels",
		"binary.handling.mode",
		"interval.handling.mode",
		"lob.enabled",
		// Converter keys: CDC inputs don't decode Avro/JSON-encoded Kafka bytes.
		"value.converter",
		"value.converter.schema.registry.url",
		"value.converter.schemas.enable",
		"key.converter",
		"key.converter.schema.registry.url",
		"key.converter.schemas.enable",
	)
}

// -------------------------------------------------------------------
// PostgreSQL
// -------------------------------------------------------------------

type debeziumPostgresConnector struct{}

func (debeziumPostgresConnector) Map(_ ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()

	// DSN: postgres://user:password@host:port/dbname
	user, password := debeziumCreds(ctx)
	hostPort := debeziumHostPort(ctx, "5432")
	dbName := debeziumDBName(ctx)
	dsnVal := fmt.Sprintf("postgres://%s:%s@%s/%s", user, password, hostPort, dbName)
	dsnNode := scalar(dsnVal)
	dsnNode.LineComment = "TODO: password is inlined — move to a secret/env-var reference"
	kv(body, "dsn", dsnNode)

	// tables (required list)
	debeziumTables(body, ctx, "tables")

	// schema (required) — first entry from schema.include.list, else stub
	if v, ok := ctx.String("schema.include.list"); ok && v != "" {
		parts := scalarsFromCSV(v)
		kv(body, "schema", parts[0])
	} else {
		stub := scalar("public")
		stub.LineComment = "TODO: set the PostgreSQL schema to replicate from"
		kv(body, "schema", stub)
	}

	// slot_name — REQUIRED by postgres_cdc runtime (errors if empty)
	if v, ok := ctx.String("slot.name"); ok && v != "" {
		kv(body, "slot_name", scalar(v))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: slot_name is required by postgres_cdc — provide a replication slot name"
		kv(body, "slot_name", stub)
		ctx.Warn("slot.name", "slot_name is required by postgres_cdc; no Debezium slot.name provided")
	}

	// publication.name has no field in postgres_cdc (the component derives it from slot_name).
	ctx.consume("publication.name")

	// snapshot.mode → stream_snapshot
	if v, ok := ctx.String("snapshot.mode"); ok {
		snapshotNode := boolScalar(debeziumSnapshotModeToInitial(v))
		snapshotNode.LineComment = fmt.Sprintf("TODO: Debezium snapshot.mode=%s — verify this maps to stream_snapshot correctly", v)
		kv(body, "stream_snapshot", snapshotNode)
	}

	// topic.prefix and database.server.name are intentionally NOT consumed here.
	// They fall through to the engine's Unmapped() sweep which emits them as
	// inline "# TODO: unmapped field …" comments in the generated YAML.

	consumeDebeziumCommon(ctx)

	return Component{Input: component("postgres_cdc", body)}, nil
}

// -------------------------------------------------------------------
// MySQL
// -------------------------------------------------------------------

type debeziumMySQLConnector struct{}

func (debeziumMySQLConnector) Map(_ ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()

	// DSN: user:password@tcp(host:port)/dbname
	// database.dbname may be absent for MySQL (Debezium captures all DBs unless filtered).
	user, password := debeziumCreds(ctx)
	hostPort := debeziumHostPort(ctx, "3306")
	dbName, _ := ctx.String("database.dbname")
	// If database.include.list is set, use the first entry as dbname.
	if dbName == "" {
		if v, ok := ctx.String("database.include.list"); ok && v != "" {
			dbName = scalarsFromCSV(v)[0].Value
		}
	} else {
		ctx.consume("database.include.list")
	}
	dsnVal := fmt.Sprintf("%s:%s@tcp(%s)/%s", user, password, hostPort, dbName)
	dsnNode := scalar(dsnVal)
	dsnNode.LineComment = "TODO: password is inlined — move to a secret/env-var reference"
	kv(body, "dsn", dsnNode)

	// tables (required list)
	debeziumTables(body, ctx, "tables")

	// checkpoint_cache is REQUIRED by mysql_cdc with no Debezium source.
	stub := scalar("")
	stub.LineComment = "TODO: checkpoint_cache is required — provide a cache resource name for BinLog position storage"
	kv(body, "checkpoint_cache", stub)
	ctx.Warn("checkpoint_cache", "checkpoint_cache is required by mysql_cdc; no Debezium equivalent — set a cache resource")

	// stream_snapshot is REQUIRED (no default in spec)
	if v, ok := ctx.String("snapshot.mode"); ok {
		snapshotNode := boolScalar(debeziumSnapshotModeToInitial(v))
		snapshotNode.LineComment = fmt.Sprintf("TODO: Debezium snapshot.mode=%s — verify this maps to stream_snapshot correctly", v)
		kv(body, "stream_snapshot", snapshotNode)
	} else {
		snapshotStub := boolScalar(false)
		snapshotStub.LineComment = "TODO: set stream_snapshot=true to replay existing rows first"
		kv(body, "stream_snapshot", snapshotStub)
	}

	// database.server.id — no equivalent field in mysql_cdc
	if v, ok := ctx.String("database.server.id"); ok {
		ctx.Warn("database.server.id", "database.server.id="+v+" has no mysql_cdc equivalent — remove or handle at the MySQL server level")
	}

	// topic.prefix and database.server.name are intentionally NOT consumed here.
	// They fall through to the engine's Unmapped() sweep which emits them as
	// inline "# TODO: unmapped field …" comments in the generated YAML.

	consumeDebeziumCommon(ctx)

	return Component{Input: component("mysql_cdc", body)}, nil
}

// -------------------------------------------------------------------
// Microsoft SQL Server
// -------------------------------------------------------------------

type debeziumSQLServerConnector struct{}

func (debeziumSQLServerConnector) Map(_ ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()

	// connection_string: sqlserver://user:password@host:port?database=dbname
	user, password := debeziumCreds(ctx)
	hostPort := debeziumHostPort(ctx, "1433")
	dbName := debeziumDBName(ctx)
	if dbName == "" {
		if v, ok := ctx.String("database.names"); ok && v != "" {
			dbName = scalarsFromCSV(v)[0].Value
		}
	} else {
		ctx.consume("database.names")
	}
	connStr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", user, password, hostPort, dbName)
	connNode := scalar(connStr)
	connNode.LineComment = "TODO: password is inlined — move to a secret/env-var reference"
	kv(body, "connection_string", connNode)

	// include (required list) — maps from table.include.list (CSV)
	debeziumTables(body, ctx, "include")

	// snapshot.mode → stream_snapshot (has default false so optional)
	if v, ok := ctx.String("snapshot.mode"); ok {
		snapshotNode := boolScalar(debeziumSnapshotModeToInitial(v))
		snapshotNode.LineComment = fmt.Sprintf("TODO: Debezium snapshot.mode=%s — verify this maps to stream_snapshot correctly", v)
		kv(body, "stream_snapshot", snapshotNode)
	}

	// topic.prefix and database.server.name are intentionally NOT consumed here.
	// They fall through to the engine's Unmapped() sweep which emits them as
	// inline "# TODO: unmapped field …" comments in the generated YAML.

	consumeDebeziumCommon(ctx)

	return Component{Input: component("microsoft_sql_server_cdc", body)}, nil
}

// -------------------------------------------------------------------
// Oracle
// -------------------------------------------------------------------

type debeziumOracleConnector struct{}

func (debeziumOracleConnector) Map(_ ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()

	// connection_string: oracle://user:password@host:port/service_name
	user, password := debeziumCreds(ctx)
	hostPort := debeziumHostPort(ctx, "1521")
	// Oracle uses database.dbname as service name, or database.service.name.
	serviceName := debeziumDBName(ctx)
	if serviceName == "" {
		if v, ok := ctx.String("database.service.name"); ok && v != "" {
			serviceName = v
		} else {
			ctx.consume("database.service.name")
		}
	} else {
		ctx.consume("database.service.name")
	}
	connStr := fmt.Sprintf("oracle://%s:%s@%s/%s", user, password, hostPort, serviceName)
	connNode := scalar(connStr)
	if serviceName == "" {
		connNode.LineComment = "TODO: set the Oracle service name; password is inlined — move to a secret/env-var reference"
	} else {
		connNode.LineComment = "TODO: password is inlined — move to a secret/env-var reference"
	}
	kv(body, "connection_string", connNode)

	// include (required list) — maps from table.include.list (CSV)
	debeziumTables(body, ctx, "include")

	// snapshot.mode → stream_snapshot (has default false so optional)
	if v, ok := ctx.String("snapshot.mode"); ok {
		snapshotNode := boolScalar(debeziumSnapshotModeToInitial(v))
		snapshotNode.LineComment = fmt.Sprintf("TODO: Debezium snapshot.mode=%s — verify this maps to stream_snapshot correctly", v)
		kv(body, "stream_snapshot", snapshotNode)
	}

	// topic.prefix and database.server.name are intentionally NOT consumed here.
	// They fall through to the engine's Unmapped() sweep which emits them as
	// inline "# TODO: unmapped field …" comments in the generated YAML.

	consumeDebeziumCommon(ctx)

	return Component{Input: component("oracledb_cdc", body)}, nil
}
