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

func (bigQuerySinkConnector) Map(_ ConnectConfig, ctx *MapCtx) (Component, error) {
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

	// Credentials: `credentials` contains inline JSON → credentials_json.
	// `keyfile` is a filesystem path; gcp_bigquery has no path field, so
	// surface it as a TODO rather than silently drop.
	if v, ok := ctx.String("credentials"); ok {
		kv(body, "credentials_json", scalar(v))
	} else if _, ok := ctx.Lookup("keyfile"); ok {
		ctx.consume("keyfile")
		// TODO: keyfile is a path to a service-account JSON file; credentials_json
		// expects the JSON content inline — load the file manually and paste here.
	}

	// autoCreateTables → create_disposition.
	// KC true  → CREATE_IF_NEEDED (BigQuery default, also gcp_bigquery default).
	// KC false → CREATE_NEVER (table must already exist).
	if v, ok := ctx.String("autoCreateTables"); ok {
		if v == "false" {
			kv(body, "create_disposition", scalar("CREATE_NEVER"))
		}
		// true is gcp_bigquery's default (CREATE_IF_NEEDED); omit for brevity.
	}

	// queueSize → batching.count. No KC byte-size or period knob for BigQuery
	// sink, so byteSizeKey and periodMsKeys are left empty.
	mapBatching(body, ctx, "queueSize", "", "")

	// Recognized Confluent-internal plumbing with no gcp_bigquery equivalent.
	consumeIgnored(ctx,
		"sanitizeTopics",
		"sanitizeFieldNames",
		"allBQFieldsNullable",
		"schemaRetriever",
		"bigQueryRetry",
		"bigQueryRetryWait",
		"allowNewBigQueryFields",
		"allowBigQueryRequiredFieldRelaxation",
		"kafkaDataFieldName",
		"kafkaKeyFieldName",
	)

	return Component{Output: component("gcp_bigquery", body)}, nil
}
