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
	// KC derives the destination table from the topic (optionally remapped via
	// topic2TableMap). gcp_bigquery's `table` accepts Bloblang interpolation, so
	// emit a topic-derived value rather than an empty stub.
	if t2t, ok := ctx.String("topic2TableMap"); ok && t2t != "" {
		if expr, parsed := topicTableMatchExpr(t2t); parsed {
			tn := scalar(expr)
			tn.LineComment = "TODO: table derived from topic2TableMap — verify"
			kv(body, "table", tn)
		} else {
			tn := scalar(`${! @kafka_topic }`)
			tn.LineComment = "TODO: could not parse topic2TableMap — verify the destination table"
			kv(body, "table", tn)
		}
	} else {
		ctx.consume("defaultTopicToTableMap")
		tn := scalar(`${! @kafka_topic }`)
		tn.LineComment = "TODO: table derived from the topic — verify topic-name casing rules match BigQuery naming"
		kv(body, "table", tn)
	}

	// Credentials: `credentials` / inline `keyfile` (keySource=JSON) contain the
	// service-account JSON → credentials_json. A file path (keySource=FILE) has
	// no gcp_bigquery field; APPLICATION_DEFAULT uses ambient credentials.
	keySource, _ := ctx.String("keySource")
	if v, ok := ctx.String("credentials"); ok {
		kv(body, "credentials_json", scalar(v))
		ctx.consume("keyfile")
	} else if v, ok := ctx.String("keyfile"); ok && v != "" {
		if keySource == "JSON" {
			kv(body, "credentials_json", scalar(v))
		} else if keySource != "APPLICATION_DEFAULT" {
			cj := scalar("")
			cj.LineComment = "TODO: keyfile is a path (keySource=" + keySource + "); credentials_json expects inline JSON — load the file contents here"
			kv(body, "credentials_json", cj)
		}
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
	mapBatching(body, ctx, []string{"queueSize"}, "", "")

	// gcp_bigquery performs streaming inserts only — it has no MERGE/upsert or
	// delete path. Warn when the KC config relied on them.
	if v, ok := ctx.String("upsertEnabled"); ok && v == "true" {
		ctx.Warn("upsertEnabled", "gcp_bigquery streams inserts only — KC upsertEnabled merge semantics are not reproduced")
	}
	if v, ok := ctx.String("deleteEnabled"); ok && v == "true" {
		ctx.Warn("deleteEnabled", "gcp_bigquery streams inserts only — KC deleteEnabled (CDC delete) is not reproduced")
	}

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
		"keySource",
		"upsertEnabled",
		"deleteEnabled",
		"mergeIntervalMs",
		"mergeRecordsThreshold",
		"timePartitioningType",
		"partitionExpirationMs",
		"bigQueryPartitionDecorator",
		"bigQueryMessageTimePartitioning",
		"clusteringPartitionFieldNames",
		"convertDoubleSpecialValues",
		"allowSchemaUnionization",
		"avroDataCacheSize",
		"threadPoolSize",
		"schemaRegistryLocation",
	)

	return Component{Output: component("gcp_bigquery", body)}, nil
}
