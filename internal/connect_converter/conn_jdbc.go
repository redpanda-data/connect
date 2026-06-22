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

	"gopkg.in/yaml.v3"
)

func init() {
	registerConnector("io.confluent.connect.jdbc.JdbcSourceConnector", jdbcSourceConnector{})
	registerConnector("io.confluent.connect.jdbc.JdbcSinkConnector", jdbcSinkConnector{})
}

// jdbcDriver maps a JDBC URL to the RPCN sql driver name.
func jdbcDriver(url string) string {
	switch {
	case strings.HasPrefix(url, "jdbc:postgresql:"):
		return "postgres"
	case strings.HasPrefix(url, "jdbc:mysql:"):
		return "mysql"
	case strings.HasPrefix(url, "jdbc:sqlserver:"):
		return "mssql"
	case strings.HasPrefix(url, "jdbc:clickhouse:"):
		return "clickhouse"
	default:
		return ""
	}
}

// dsnFromURL strips the leading "jdbc:" so the remainder can be used as a DSN.
func dsnFromURL(url string) string {
	return strings.TrimPrefix(url, "jdbc:")
}

func driverAndDSN(ctx *MapCtx, body *yaml.Node) {
	url, ok := ctx.String("connection.url")
	if !ok {
		ctx.Warn("connection.url", "missing JDBC connection URL")
		driverStub := scalar("postgres")
		driverStub.LineComment = "TODO: set the database driver (e.g. postgres, mysql, mssql)"
		kv(body, "driver", driverStub)
		dsnStub := scalar("")
		dsnStub.LineComment = "TODO: set the database DSN"
		kv(body, "dsn", dsnStub)
		return
	}
	driver := jdbcDriver(url)
	dn := scalar(driver)
	if driver == "" {
		dn.LineComment = "TODO: unrecognized JDBC URL — set the driver manually"
	}
	kv(body, "driver", dn)
	dsn := scalar(dsnFromURL(url))
	dsn.LineComment = "TODO: verify DSN format for the chosen driver"
	kv(body, "dsn", dsn)
}

type jdbcSourceConnector struct{}

func (jdbcSourceConnector) Map(_ ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()
	driverAndDSN(ctx, body)

	if v, ok := ctx.String("table.whitelist"); ok {
		kv(body, "table", scalar(v))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set the source table (or use a query)"
		kv(body, "table", stub)
	}

	// columns is a required string list field in sql_select.
	colNode := scalar("*")
	colNode.LineComment = "TODO: list specific columns if needed"
	kv(body, "columns", seq(colNode))

	return Component{Input: component("sql_select", body)}, nil
}

type jdbcSinkConnector struct{}

func (jdbcSinkConnector) Map(_ ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()
	driverAndDSN(ctx, body)

	if v, ok := ctx.String("table.name.format"); ok {
		kv(body, "table", scalar(v))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set the destination table"
		kv(body, "table", stub)
	}

	// columns is a required string list field in sql_insert.
	colNode := scalar("id")
	colNode.LineComment = "TODO: list destination columns matching your message fields"
	kv(body, "columns", seq(colNode))

	// args_mapping is a required Bloblang field in sql_insert (not optional).
	args := scalar(`root = [ this.id ]`)
	args.LineComment = "TODO: map message fields to column values"
	kv(body, "args_mapping", args)

	return Component{Output: component("sql_insert", body)}, nil
}
