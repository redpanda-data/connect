// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

func init() {
	registerConnector("com.snowflake.kafka.connector.SnowflakeSinkConnector", snowflakeSinkConnector{})
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

	ctx.consume("topics")
	table := scalar("")
	table.LineComment = "TODO: set the destination table (KC derives it from the topic)"
	kv(body, "table", table)

	return Component{Output: component("snowflake_streaming", body)}, nil
}
