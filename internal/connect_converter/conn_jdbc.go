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
	// Aiven JDBC connector class names.
	registerConnector("io.aiven.connect.jdbc.JdbcSourceConnector", jdbcSourceConnector{})
	registerConnector("io.aiven.connect.jdbc.JdbcSinkConnector", jdbcSinkConnector{})
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

// injectUserInfo inserts "user[:password]@" into dsn right after the first "://"
// separator. If dsn contains no "://", it is returned unchanged (caller should
// emit a TODO noting that credentials could not be inlined). An empty user is a
// no-op.
func injectUserInfo(dsn, user, password string) string {
	if user == "" {
		return dsn
	}
	idx := strings.Index(dsn, "://")
	if idx < 0 {
		// TODO: no scheme separator found — credentials could not be inlined into DSN
		return dsn
	}
	var userinfo string
	if password != "" {
		userinfo = user + ":" + password + "@"
	} else {
		userinfo = user + "@"
	}
	return dsn[:idx+3] + userinfo + dsn[idx+3:]
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
		// Consume credential keys so they don't surface as unmapped-field warnings.
		ctx.consume("connection.user")
		ctx.consume("connection.password")
		return
	}
	driver := jdbcDriver(url)
	dn := scalar(driver)
	if driver == "" {
		dn.LineComment = "TODO: unrecognized JDBC URL — set the driver manually"
	}
	kv(body, "driver", dn)

	// Read (and consume) credentials so they do not surface as unmapped-field warnings.
	user, _ := ctx.String("connection.user")
	password, _ := ctx.String("connection.password")

	dsnVal := injectUserInfo(dsnFromURL(url), user, password)
	dsn := scalar(dsnVal)
	comment := "TODO: verify DSN format for the chosen driver"
	if password != "" {
		comment += " # TODO: password is inlined — move to a secret/env-var reference"
	}
	dsn.LineComment = comment
	kv(body, "dsn", dsn)
}

type jdbcSourceConnector struct{}

func (jdbcSourceConnector) Map(_ ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()
	driverAndDSN(ctx, body)

	// mode + incrementing/timestamp column — surface intent as a suffix TODO.
	// sql_select polls once and exits; KC mode implies ongoing polling with
	// ordering. We reflect that intent in a suffix comment rather than guessing
	// a wrong query.
	mode, hasMode := ctx.Lookup("mode")
	incrCol, _ := ctx.Lookup("incrementing.column.name")
	tsCol, _ := ctx.Lookup("timestamp.column.name")

	// Track whether a suffix was already emitted by the query branch.
	suffixEmitted := false

	if v, ok := ctx.String("query"); ok {
		// KC custom query takes precedence over table.
		suffix := scalar(v)
		suffix.LineComment = "TODO: custom KC query — verify it works as a sql_select suffix or place in a raw query processor"
		kv(body, "suffix", suffix)
		suffixEmitted = true
		ctx.consume("table.whitelist")
		stub := scalar("")
		stub.LineComment = "TODO: set table (required by sql_select even when using suffix)"
		kv(body, "table", stub)
	} else if v, ok := ctx.String("table.whitelist"); ok {
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

	// If a mode was specified, add a suffix TODO to reflect ordering intent.
	if hasMode && !suffixEmitted {
		var modeNote string
		switch mode {
		case "incrementing":
			modeNote = "KC mode=incrementing on column " + incrCol + "; add ORDER BY " + incrCol + " and a WHERE clause to replicate incremental polling"
		case "timestamp":
			modeNote = "KC mode=timestamp on column " + tsCol + "; add ORDER BY " + tsCol + " and a WHERE clause for timestamp-based polling"
		case "timestamp+incrementing":
			modeNote = "KC mode=timestamp+incrementing (ts=" + tsCol + ", id=" + incrCol + "); add combined WHERE/ORDER BY for incremental+timestamp polling"
		default:
			modeNote = "KC mode=" + mode + "; configure equivalent polling strategy for sql_select"
		}
		suffixStub := scalar("")
		suffixStub.LineComment = "TODO: " + modeNote
		kv(body, "suffix", suffixStub)
	}

	consumeIgnored(ctx,
		"mode",
		"incrementing.column.name",
		"timestamp.column.name",
		"poll.interval.ms",
		"batch.max.rows",
		"validate.non.null",
		"numeric.mapping",
		"table.types",
	)

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

	// pk.fields / pk.mode — read them so we can inform the columns/args TODO.
	pkFields, hasPKFields := ctx.Lookup("pk.fields")
	pkMode, _ := ctx.Lookup("pk.mode")

	// columns is a required string list field in sql_insert.
	colNode := scalar("id")
	var colComment string
	if hasPKFields {
		colComment = "TODO: list destination columns matching your message fields; KC pk.fields=" + pkFields + " (pk.mode=" + pkMode + ")"
	} else {
		colComment = "TODO: list destination columns matching your message fields"
	}
	colNode.LineComment = colComment
	kv(body, "columns", seq(colNode))

	// args_mapping is a required Bloblang field in sql_insert (not optional).
	args := scalar(`root = [ this.id ]`)
	args.LineComment = "TODO: map message fields to column values"
	kv(body, "args_mapping", args)

	// insert.mode: sql_insert always emits a plain INSERT. For upsert/update,
	// surface the intent as a suffix TODO so the user knows to add ON CONFLICT.
	insertMode, hasInsertMode := ctx.Lookup("insert.mode")
	if hasInsertMode && insertMode != "insert" {
		suffix := scalar("")
		suffix.LineComment = "TODO: KC insert.mode=" + insertMode + "; sql_insert emits plain INSERT — write an ON CONFLICT or UPDATE suffix manually"
		kv(body, "suffix", suffix)
	}

	// batch.size → batching.count (sql_insert supports the batching policy).
	mapBatching(body, ctx, "batch.size", "", "")

	consumeIgnored(ctx,
		"insert.mode",
		"pk.fields",
		"pk.mode",
		"auto.create",
		"auto.evolve",
		"quote.sql.identifiers",
		"max.retries",
		"retry.backoff.ms",
		"db.timezone",
	)

	return Component{Output: component("sql_insert", body)}, nil
}
