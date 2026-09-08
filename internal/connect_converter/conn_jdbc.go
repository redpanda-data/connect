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
	case strings.HasPrefix(url, "jdbc:mysql:"), strings.HasPrefix(url, "jdbc:mariadb:"):
		return "mysql"
	case strings.HasPrefix(url, "jdbc:sqlserver:"):
		return "mssql"
	case strings.HasPrefix(url, "jdbc:clickhouse:"):
		return "clickhouse"
	case strings.HasPrefix(url, "jdbc:oracle:"):
		return "oracle"
	default:
		return ""
	}
}

// dsnFromURL strips the leading "jdbc:" so the remainder can be used as a DSN.
func dsnFromURL(url string) string {
	return strings.TrimPrefix(url, "jdbc:")
}

// oracleDSN converts a JDBC Oracle URL into the benthos oracle DSN form
// (oracle://host:port/service_name, before user-info is injected). It handles
// the common Oracle thin/oci forms:
//
//	jdbc:oracle:thin:@//host:port/service   → oracle://host:port/service
//	jdbc:oracle:thin:@host:port/service     → oracle://host:port/service
//	jdbc:oracle:thin:@host:port:SID         → oracle://host:port/SID
//
// It returns "" when the form can't be expressed as an EZConnect DSN (e.g. a
// TNS descriptor or LDAP URL), so the caller can fall back to a verify TODO.
func oracleDSN(url string) string {
	_, after, ok := strings.Cut(url, "@")
	if !ok {
		return ""
	}
	conn := strings.TrimSpace(after)
	conn = strings.TrimPrefix(conn, "//")
	// TNS descriptors and LDAP URLs cannot be expressed as EZConnect DSNs.
	if conn == "" || strings.HasPrefix(conn, "(") || strings.HasPrefix(conn, "ldap") {
		return ""
	}
	// SID form host:port:SID (two colons, no slash) → host:port/SID.
	if strings.Count(conn, ":") == 2 && !strings.Contains(conn, "/") {
		i := strings.LastIndex(conn, ":")
		conn = conn[:i] + "/" + conn[i+1:]
	}
	return "oracle://" + conn
}

// injectUserInfo inserts "user[:password]@" into dsn right after the first "://"
// separator. If dsn contains no "://", it is returned unchanged (caller should
// emit a TODO noting that credentials could not be inlined). An empty user is a
// no-op.
//
// NOTE: credentials that contain special characters such as '@', ':', '/', or '#'
// are inserted verbatim and will corrupt the DSN. The verify-DSN TODO emitted by
// the caller already prompts the user to review the output before use.
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

// driverAndDSN emits the driver and dsn fields and returns the resolved driver
// name (empty string when the JDBC URL was unrecognized) so callers can build
// dialect-specific SQL.
func driverAndDSN(ctx *MapCtx, body *yaml.Node) string {
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
		return ""
	}
	driver := jdbcDriver(url)
	// A driver of "" fails the sql_* driver enum lint, so fall back to a
	// non-empty placeholder and flag it.
	dn := scalar(driver)
	if driver == "" {
		dn.Value = "postgres"
		dn.LineComment = "TODO: unrecognized JDBC URL — set the driver manually (e.g. postgres, mysql, mssql, oracle)"
	}
	kv(body, "driver", dn)

	// Read (and consume) credentials so they do not surface as unmapped-field warnings.
	user, _ := ctx.String("connection.user")
	password, _ := ctx.String("connection.password")

	// Oracle JDBC URLs need bespoke DSN reshaping; everything else is the URL
	// minus the leading "jdbc:".
	dsnBase := dsnFromURL(url)
	if driver == "oracle" {
		if od := oracleDSN(url); od != "" {
			dsnBase = od
		}
	}
	dsnVal := injectUserInfo(dsnBase, user, password)
	dsn := scalar(dsnVal)
	comment := "TODO: verify DSN format for the chosen driver"
	if password != "" {
		comment = "TODO: verify DSN format for the chosen driver; password is inlined — move to a secret/env-var reference"
	}
	dsn.LineComment = comment
	kv(body, "dsn", dsn)
	return driver
}

type jdbcSourceConnector struct{}

func (jdbcSourceConnector) Map(_ ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()
	driverAndDSN(ctx, body)

	// sql_select reads structured rows directly from the database — there are
	// no Avro/JSON-encoded Kafka bytes to deserialize. Consume the converter
	// keys so the engine does not insert a (broken) schema_registry_decode.
	consumeConverterKeys(ctx)

	// mode + incrementing/timestamp column. sql_select runs one query per poll;
	// KC mode implies ordered, cursor-based incremental polling. We emit an
	// ORDER BY suffix (a valid SQL fragment that reflects intent) and note that
	// the incremental WHERE-cursor must be added manually.
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

	// If a mode was specified, emit an ORDER BY suffix reflecting the cursor.
	if hasMode && !suffixEmitted {
		var orderCol, note string
		switch mode {
		case "incrementing":
			orderCol = incrCol
			note = "KC mode=incrementing; add a WHERE " + incrCol + " > <last-seen> cursor for incremental polling"
		case "timestamp":
			orderCol = tsCol
			note = "KC mode=timestamp; add a WHERE " + tsCol + " > <last-seen> cursor for timestamp polling"
		case "timestamp+incrementing":
			orderCol = strings.TrimSpace(tsCol + ", " + incrCol)
			note = "KC mode=timestamp+incrementing; add a combined WHERE cursor for incremental+timestamp polling"
		default:
			note = "KC mode=" + mode + "; configure equivalent polling strategy for sql_select"
		}
		suffixStub := scalar("")
		if orderCol != "" && strings.TrimSpace(orderCol) != "," {
			suffixStub.Value = "ORDER BY " + orderCol
		}
		suffixStub.LineComment = "TODO: " + note
		kv(body, "suffix", suffixStub)
	}

	consumeIgnored(ctx,
		"mode",
		"incrementing.column.name",
		"timestamp.column.name",
		"timestamp.initial",
		"timestamp.granularity",
		"poll.interval.ms",
		"batch.max.rows",
		"validate.non.null",
		"numeric.mapping",
		"numeric.precision.mapping",
		"table.types",
		"table.blacklist",
		"catalog.pattern",
		"schema.pattern",
		"db.timezone",
		"quote.sql.identifiers",
		"connection.attempts",
		"connection.backoff.ms",
		// topic.prefix names the produced Kafka topic in KC; sql_select has no
		// topic concept, so it is irrelevant here.
		"topic.prefix",
	)

	return Component{Input: component("sql_select", body)}, nil
}

// csvFields splits a comma-separated KC value into trimmed, non-empty strings.
func csvFields(v string) []string {
	var out []string
	for p := range strings.SplitSeq(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// excludeFields returns cols with any member of drop removed (order preserved).
func excludeFields(cols, drop []string) []string {
	dropSet := map[string]bool{}
	for _, d := range drop {
		dropSet[d] = true
	}
	var out []string
	for _, c := range cols {
		if !dropSet[c] {
			out = append(out, c)
		}
	}
	return out
}

// upsertSuffix builds a dialect-aware ON CONFLICT / ON DUPLICATE KEY suffix for
// a KC insert.mode of upsert/update. It returns (suffix, todo). When it cannot
// safely build SQL (unknown driver, or missing pk.fields/columns) it returns an
// empty suffix and a manual-mapping TODO.
func upsertSuffix(driver, mode string, pk, cols []string) (string, string) {
	manual := "KC insert.mode=" + mode + "; sql_insert emits a plain INSERT — write an ON CONFLICT/UPDATE suffix manually"
	if len(pk) == 0 || len(cols) == 0 {
		return "", manual + " (needs pk.fields and the column list / fields.whitelist)"
	}
	setCols := excludeFields(cols, pk)
	switch driver {
	case "postgres", "pgx", "sqlite", "clickhouse":
		if len(setCols) == 0 {
			return "ON CONFLICT (" + strings.Join(pk, ", ") + ") DO NOTHING",
				"verify upsert suffix generated from KC insert.mode=" + mode
		}
		var sets []string
		for _, c := range setCols {
			sets = append(sets, c+" = EXCLUDED."+c)
		}
		return "ON CONFLICT (" + strings.Join(pk, ", ") + ") DO UPDATE SET " + strings.Join(sets, ", "),
			"verify upsert suffix generated from KC insert.mode=" + mode + " (pk.fields + fields.whitelist)"
	case "mysql":
		var sets []string
		for _, c := range setCols {
			sets = append(sets, c+" = VALUES("+c+")")
		}
		if len(sets) == 0 {
			sets = append(sets, pk[0]+" = "+pk[0])
		}
		return "ON DUPLICATE KEY UPDATE " + strings.Join(sets, ", "),
			"verify upsert suffix generated from KC insert.mode=" + mode + " (fields.whitelist)"
	default:
		return "", manual + " (driver " + driver + " needs a MERGE statement)"
	}
}

type jdbcSinkConnector struct{}

func (jdbcSinkConnector) Map(_ ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()
	driver := driverAndDSN(ctx, body)

	if v, ok := ctx.String("table.name.format"); ok {
		kv(body, "table", scalar(v))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set the destination table"
		kv(body, "table", stub)
	}

	// pk.fields / pk.mode — read them so we can inform columns/args and upsert.
	pkFieldsRaw, hasPKFields := ctx.Lookup("pk.fields")
	pkMode, _ := ctx.Lookup("pk.mode")
	pkFields := csvFields(pkFieldsRaw)

	// columns: prefer the explicit destination column list (fields.whitelist);
	// fall back to a placeholder. args_mapping is derived from the columns.
	var cols []string
	if fw, ok := ctx.String("fields.whitelist"); ok && fw != "" {
		cols = csvFields(fw)
	}
	if len(cols) > 0 {
		colNodes := make([]*yaml.Node, 0, len(cols))
		args := make([]string, 0, len(cols))
		for _, c := range cols {
			colNodes = append(colNodes, scalar(c))
			args = append(args, "this."+c)
		}
		kv(body, "columns", seq(colNodes...))
		am := scalar("root = [ " + strings.Join(args, ", ") + " ]")
		am.LineComment = "TODO: verify column→field mapping (derived from fields.whitelist)"
		kv(body, "args_mapping", am)
	} else {
		colNode := scalar("id")
		if hasPKFields {
			colNode.LineComment = "TODO: list destination columns matching your message fields; KC pk.fields=" + pkFieldsRaw + " (pk.mode=" + pkMode + ")"
		} else {
			colNode.LineComment = "TODO: list destination columns matching your message fields"
		}
		kv(body, "columns", seq(colNode))
		args := scalar(`root = [ this.id ]`)
		args.LineComment = "TODO: map message fields to column values"
		kv(body, "args_mapping", args)
	}

	// insert.mode: sql_insert emits a plain INSERT. For upsert/update, build a
	// dialect-aware ON CONFLICT/UPDATE suffix when we have enough information.
	insertMode, hasInsertMode := ctx.Lookup("insert.mode")
	if hasInsertMode && insertMode != "insert" {
		sfx, todo := upsertSuffix(driver, insertMode, pkFields, cols)
		suffix := scalar(sfx)
		suffix.LineComment = "TODO: " + todo
		kv(body, "suffix", suffix)
	}

	// batch.size → batching.count (sql_insert supports the batching policy).
	mapBatching(body, ctx, []string{"batch.size"}, "", "")

	consumeIgnored(ctx,
		"insert.mode",
		"pk.fields",
		"pk.mode",
		"fields.whitelist",
		"auto.create",
		"auto.evolve",
		"quote.sql.identifiers",
		"dialect.name",
		"max.retries",
		"retry.backoff.ms",
		"connection.attempts",
		"connection.backoff.ms",
		"db.timezone",
	)

	return Component{Output: component("sql_insert", body)}, nil
}
