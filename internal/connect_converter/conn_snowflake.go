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

func (snowflakeSinkConnector) Map(cfg ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()

	// account: KC provides a full URL/host; RPCN wants the account identifier.
	if v, ok := ctx.String("snowflake.url.name"); ok {
		acc := scalar(v)
		acc.LineComment = "TODO: RPCN expects the account identifier, not the full URL"
		kv(body, "account", acc)
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set the Snowflake account identifier (e.g. ORG-ACCOUNT)"
		kv(body, "account", stub)
	}

	mapStr := func(kcKey, rpKey string) {
		if v, ok := ctx.String(kcKey); ok {
			kv(body, rpKey, scalar(v))
		}
	}
	mapStr("snowflake.user.name", "user")

	if v, ok := ctx.String("snowflake.role.name"); ok {
		kv(body, "role", scalar(v))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set the Snowflake role"
		kv(body, "role", stub)
	}

	mapStr("snowflake.database.name", "database")
	mapStr("snowflake.schema.name", "schema")
	mapStr("snowflake.private.key", "private_key")

	ctx.consume("topics")
	table := scalar("")
	table.LineComment = "TODO: set the destination table (KC derives it from the topic)"
	kv(body, "table", table)

	return Component{Output: component("snowflake_streaming", body)}, nil
}
