// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import "strings"

func init() {
	registerConnector("com.snowflake.kafka.connector.SnowflakeSinkConnector", snowflakeSinkConnector{})
	registerConnector("com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector", snowflakeSinkConnector{})
}

type snowflakeSinkConnector struct{}

func (snowflakeSinkConnector) Map(_ ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()

	// mapRequired emits the value when the KC key is present; otherwise it
	// emits a TODO stub AND records a warning so the caller knows the field
	// needs attention before the config is production-ready.
	mapRequired := func(kcKey, rpKey, todoMsg, warnMsg string) {
		if v, ok := ctx.String(kcKey); ok {
			kv(body, rpKey, scalar(v))
		} else {
			stub := scalar("")
			stub.LineComment = "TODO: " + todoMsg
			kv(body, rpKey, stub)
			ctx.Warn(rpKey, warnMsg)
		}
	}

	// mapOptional emits the value only when the KC key is present; absent
	// optional fields are simply omitted with no stub or warning.
	mapOptional := func(kcKey, rpKey string) {
		if v, ok := ctx.String(kcKey); ok {
			kv(body, rpKey, scalar(v))
		}
	}

	// account: KC provides a full URL/host; RPCN wants the account identifier.
	if v, ok := ctx.String("snowflake.url.name"); ok {
		acc := scalar(v)
		acc.LineComment = "TODO: RPCN expects the account identifier, not the full URL"
		kv(body, "account", acc)
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set the Snowflake account identifier (e.g. ORG-ACCOUNT)"
		kv(body, "account", stub)
		ctx.Warn("account", "no snowflake.url.name specified; emitted TODO stub — set account before deploying")
	}

	// user, role, database, schema are all required fields with no default.
	mapRequired("snowflake.user.name", "user",
		"set the Snowflake user name",
		"no snowflake.user.name specified; emitted TODO stub — set user before deploying")
	mapRequired("snowflake.role.name", "role",
		"set the Snowflake role",
		"no snowflake.role.name specified; emitted TODO stub — set role before deploying")
	mapRequired("snowflake.database.name", "database",
		"set the Snowflake database",
		"no snowflake.database.name specified; emitted TODO stub — set database before deploying")
	mapRequired("snowflake.schema.name", "schema",
		"set the Snowflake schema",
		"no snowflake.schema.name specified; emitted TODO stub — set schema before deploying")

	// private_key is optional in the spec (private_key_file is the alternative).
	mapOptional("snowflake.private.key", "private_key")
	// private_key_pass decrypts an encrypted RSA key.
	mapOptional("snowflake.private.key.passphrase", "private_key_pass")

	ctx.consume("topics")

	// topic2table.map has the form "topic1:table1,topic2:table2".
	// RPCN snowflake_streaming has a single table per output, so we use the
	// first entry's table value. If multiple mappings are present we note that
	// only the first was applied.
	if t2t, ok := ctx.String("snowflake.topic2table.map"); ok {
		entries := strings.Split(t2t, ",")
		// Each entry is "topic:table"; split on the first colon only.
		firstParts := strings.SplitN(strings.TrimSpace(entries[0]), ":", 2)
		tableVal := ""
		if len(firstParts) == 2 {
			tableVal = strings.TrimSpace(firstParts[1])
		}
		tableNode := scalar(tableVal)
		if len(entries) > 1 {
			tableNode.LineComment = "TODO: multiple topic→table mappings found; only the first was applied — RPCN snowflake_streaming has a single table per output"
		}
		kv(body, "table", tableNode)
	} else {
		table := scalar("")
		table.LineComment = "TODO: set the destination table (KC derives it from the topic)"
		kv(body, "table", table)
	}

	// Batching: the Snowflake KC connector buffers by record count, byte size and
	// flush time. NOTE: buffer.flush.time is in SECONDS (unlike the *.ms period
	// keys handled by mapBatching), so we build the block inline here and format
	// the period as "<n>s". mapBatching is deliberately not reused.
	batch := mapping()
	if v, ok := ctx.String("buffer.count.records"); ok {
		kv(batch, "count", intScalar(v))
	}
	if v, ok := ctx.String("buffer.size.bytes"); ok {
		kv(batch, "byte_size", intScalar(v))
	}
	if v, ok := ctx.String("buffer.flush.time"); ok {
		kv(batch, "period", scalar(v+"s"))
	}
	if len(batch.Content) > 0 {
		kv(body, "batching", batch)
	}

	// Recognized Snowflake-Kafka-connector plumbing with no snowflake_streaming
	// equivalent — drop quietly so they don't surface as TODO noise.
	// (key.converter*/value.converter* are already treated as meta.)
	consumeIgnored(ctx,
		"behavior.on.null.values",
		"snowflake.metadata.createtime",
		"snowflake.metadata.topic",
		"snowflake.metadata.offset.and.partition",
		"snowflake.metadata.all",
		"jvm.proxy.host",
		"jvm.proxy.port",
		// The KC connector's only streaming ingestion method maps onto the
		// snowflake_streaming output itself; the key carries no extra config.
		"snowflake.ingestion.method",
	)

	return Component{Output: component("snowflake_streaming", body)}, nil
}
