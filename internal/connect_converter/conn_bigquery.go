// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

func init() {
	registerConnector("com.wepay.kafka.connect.bigquery.BigQuerySinkConnector", bigQuerySinkConnector{})
}

type bigQuerySinkConnector struct{}

func (bigQuerySinkConnector) Map(cfg ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()

	if v, ok := ctx.String("project"); ok {
		kv(body, "project", scalar(v))
	} else {
		ctx.Warn("project", "missing required project")
		stub := scalar("")
		stub.LineComment = "TODO: set the GCP project"
		kv(body, "project", stub)
	}

	if v, ok := ctx.String("defaultDataset"); ok {
		kv(body, "dataset", scalar(v))
	} else if v, ok := ctx.String("datasets"); ok {
		kv(body, "dataset", scalar(v))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set the BigQuery dataset"
		kv(body, "dataset", stub)
	}

	ctx.consume("topics")
	table := scalar("")
	table.LineComment = "TODO: set the destination table (KC derives it from the topic)"
	kv(body, "table", table)

	return Component{Output: component("gcp_bigquery", body)}, nil
}
