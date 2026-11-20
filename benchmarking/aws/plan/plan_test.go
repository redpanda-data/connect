// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package plan

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var testNow = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

func pgToSnowflake() map[string]any {
	return map[string]any{
		"input": map[string]any{
			"postgres_cdc": map[string]any{
				"dsn":             "postgres://admin:s3cretpw@db.customer-corp.internal:5432/prod",
				"stream_snapshot": false,
				"schema":          "public",
				"tables":          []any{"customers"},
				"slot_name":       "cust_slot",
				"batching":        map[string]any{"count": 5000, "period": "1s"},
			},
		},
		"pipeline": map[string]any{"processors": []any{
			map[string]any{"mapping": "root = this"},
		}},
		"output": map[string]any{
			"snowflake_streaming": map[string]any{
				"account":     "CUSTORG-ACCT",
				"user":        "svc_loader",
				"role":        "LOADER",
				"database":    "PROD",
				"schema":      "PUBLIC",
				"table":       "CUSTOMERS",
				"private_key": "-----BEGIN PRIVATE KEY-----MIIEvQ-----END PRIVATE KEY-----",
			},
		},
	}
}

func build(t *testing.T, cfg map[string]any) Plan {
	t.Helper()
	p, err := Build(cfg, Workload{MessageSizeBytes: 1000, TargetMBps: 10}, SlackRef{Channel: "C1", ThreadTS: "1.2"}, testNow)
	require.NoError(t, err)
	return p
}

func TestBuildPostgresToSnowflake(t *testing.T) {
	p := build(t, pgToSnowflake())
	require.True(t, p.Runnable)
	require.Equal(t, SourcePostgres, p.Source)
	require.Equal(t, SinkSnowflake, p.Sink)
	require.Equal(t, "XSMALL", p.SinkSize)
	require.Empty(t, p.Caveats)
	require.Equal(t, "postgres_cdc", p.Input)
	require.Equal(t, "snowflake_streaming", p.Output)
	require.Equal(t, "cdc", p.Workload.Mode)
	in := p.Pipeline["input"].(map[string]any)["postgres_cdc"].(map[string]any)
	require.Equal(t, "${POSTGRES_DSN}", in["dsn"])
	require.Equal(t, "bench_slot", in["slot_name"])
	require.Equal(t, "public", in["schema"])
	require.Equal(t, 5000, in["batching"].(map[string]any)["count"])
	out := p.Pipeline["output"].(map[string]any)["snowflake_streaming"].(map[string]any)
	require.Equal(t, "${BENCH_SNOWFLAKE_ACCOUNT}", out["account"])
}

func TestBuildDoesNotMutateInput(t *testing.T) {
	cfg := pgToSnowflake()
	build(t, cfg)
	require.Equal(t, pgToSnowflake(), cfg)
}

func TestBuildUnsupportedCDC(t *testing.T) {
	for _, name := range []string{"oracledb_cdc", "aws_dynamodb_cdc"} {
		cfg := pgToSnowflake()
		cfg["input"] = map[string]any{name: map[string]any{"connection_string": "secret"}}
		p := build(t, cfg)
		require.False(t, p.Runnable)
		require.Equal(t, "no bench stack for "+name+" yet", p.Reason)
		b, _ := json.Marshal(p)
		require.NotContains(t, string(b), "secret")
	}
}

func TestBuildApproximatesBulkInput(t *testing.T) {
	cfg := pgToSnowflake()
	cfg["input"] = map[string]any{"http_server": map[string]any{"path": "/post", "cors": map[string]any{"enabled": true}}}
	p := build(t, cfg)
	require.True(t, p.Runnable)
	require.Equal(t, SourceApproximated, p.Source)
	require.Equal(t, []string{"source_approximated"}, p.Caveats)
	require.Equal(t, "bulk", p.Workload.Mode)
}

func TestBuildFallbackSink(t *testing.T) {
	cfg := pgToSnowflake()
	cfg["output"] = map[string]any{"elasticsearch_v8": map[string]any{"urls": []any{"https://es.customer-corp.internal"}}}
	p := build(t, cfg)
	require.Equal(t, SinkDrop, p.Sink)
	require.Equal(t, []string{"sink_not_measured"}, p.Caveats)
	require.Equal(t, map[string]any{"drop": map[string]any{}}, p.Pipeline["output"])
	require.Equal(t, "elasticsearch_v8", p.Output)
	require.Empty(t, p.SinkSize)
}

func TestBuildStubsExternalCalls(t *testing.T) {
	cfg := pgToSnowflake()
	cfg["pipeline"] = map[string]any{"processors": []any{
		map[string]any{"http": map[string]any{"url": "https://api.customer-corp.com", "verb": "POST"}},
		map[string]any{"mapping": "root.a = 1"},
	}}
	p := build(t, cfg)
	require.Equal(t, []string{"external_calls_stubbed"}, p.Caveats)
	procs := p.Pipeline["pipeline"].(map[string]any)["processors"].([]any)
	require.Equal(t, map[string]any{"mapping": "root = this"}, procs[0])
	require.Equal(t, map[string]any{"mapping": "root.a = 1"}, procs[1])
	require.Equal(t, []string{"http", "mapping"}, p.Processors)
}

func TestBuildPureProcessorsNoCaveat(t *testing.T) {
	cfg := pgToSnowflake()
	cfg["pipeline"] = map[string]any{"processors": []any{
		map[string]any{"branch": map[string]any{
			"request_map": "root = this",
			"processors":  []any{map[string]any{"mapping": "root = this"}},
		}},
	}}
	p := build(t, cfg)
	require.Empty(t, p.Caveats)
}

func TestBuildStubsNestedExternalCalls(t *testing.T) {
	ext := func() any { return map[string]any{"redis": map[string]any{"url": "redis://h:6379"}} }
	cfg := pgToSnowflake()
	cfg["pipeline"] = map[string]any{"processors": []any{
		map[string]any{"branch": map[string]any{
			"request_map": "root = this",
			"processors":  []any{ext()},
			"result_map":  "root.x = this",
		}},
		map[string]any{"switch": []any{map[string]any{"check": "true", "processors": []any{ext()}}}},
		map[string]any{"try": []any{ext()}},
		map[string]any{"for_each": []any{ext()}},
		map[string]any{"while": map[string]any{"check": "true", "processors": []any{ext()}}},
		map[string]any{"catch": []any{ext()}},
	}}
	p := build(t, cfg)
	require.Equal(t, []string{"external_calls_stubbed"}, p.Caveats)
	b, _ := json.Marshal(p.Pipeline)
	require.NotContains(t, string(b), "redis")
	procs := p.Pipeline["pipeline"].(map[string]any)["processors"].([]any)
	br := procs[0].(map[string]any)["branch"].(map[string]any)
	require.Equal(t, "root.x = this", br["result_map"])
	require.Equal(t, []any{map[string]any{"mapping": "root = this"}}, br["processors"])
	require.Equal(t, []any{map[string]any{"mapping": "root = this"}}, procs[2].(map[string]any)["try"])
}

func TestBuildStubsInputAndOutputProcessors(t *testing.T) {
	cfg := pgToSnowflake()
	cfg["output"].(map[string]any)["processors"] = []any{map[string]any{"aws_lambda": map[string]any{"function": "f"}}}
	p := build(t, cfg)
	require.Equal(t, []string{"external_calls_stubbed"}, p.Caveats)
	require.Equal(t, []any{map[string]any{"mapping": "root = this"}}, p.Pipeline["output"].(map[string]any)["processors"])
}

func TestBuildSnapshotCaveat(t *testing.T) {
	cfg := pgToSnowflake()
	cfg["input"].(map[string]any)["postgres_cdc"].(map[string]any)["stream_snapshot"] = true
	p := build(t, cfg)
	require.Equal(t, []string{"snapshot_not_measured"}, p.Caveats)
	in := p.Pipeline["input"].(map[string]any)["postgres_cdc"].(map[string]any)
	require.Equal(t, false, in["stream_snapshot"])
}

func TestBuildCaveatsSortedAndDeduped(t *testing.T) {
	cfg := pgToSnowflake()
	cfg["input"].(map[string]any)["postgres_cdc"].(map[string]any)["stream_snapshot"] = true
	cfg["output"] = map[string]any{"elasticsearch_v8": map[string]any{}}
	cfg["pipeline"] = map[string]any{"processors": []any{
		map[string]any{"http": map[string]any{}}, map[string]any{"cache": map[string]any{}},
	}}
	p := build(t, cfg)
	require.Equal(t, []string{"external_calls_stubbed", "sink_not_measured", "snapshot_not_measured"}, p.Caveats)
}

func TestBuildNeverLeaksSecrets(t *testing.T) {
	secrets := []string{
		"s3cretpw", "customer-corp", "MIIEvQ", "svc_loader", "CUSTORG-ACCT",
		"cust_slot", "customers", "SASLPASS", "saslusername", "bq-private-key", "kafka-prod-1",
		"AKIAFAKEKEY", "hunter2", "my-secret-bucket", "SELECT secretcol", "tok_abc123",
	}
	cfg := pgToSnowflake()
	cfg["input"] = map[string]any{
		"kafka_franz": map[string]any{
			"seed_brokers":   []any{"kafka-prod-1.customer-corp.internal:9092"},
			"topics":         []any{"customers"},
			"consumer_group": "cust-group",
			"client_id":      "cust-client",
			"sasl":           []any{map[string]any{"mechanism": "PLAIN", "username": "saslusername", "password": "SASLPASS"}},
			"tls":            map[string]any{"enabled": true, "root_cas": "-----cust-----"},
			"batching":       map[string]any{"count": 100},
		},
	}
	cfg["output"] = map[string]any{
		"gcp_bigquery": map[string]any{
			"project":          "customer-corp-proj",
			"dataset":          "customers",
			"table":            "customers",
			"credentials_json": "{\"private_key\":\"bq-private-key\",\"client_email\":\"x@customer-corp.iam\"}",
			"max_in_flight":    8,
		},
	}
	cfg["pipeline"] = map[string]any{"processors": []any{
		map[string]any{"http": map[string]any{"url": "https://customer-corp.example", "headers": map[string]any{"Authorization": "Bearer tok_abc123"}}},
		map[string]any{"sql_select": map[string]any{"dsn": "mysql://root:hunter2@h/db", "query": "SELECT secretcol FROM t"}},
	}}
	cfg["cache_resources"] = []any{map[string]any{"redis": map[string]any{"url": "redis://customer-corp:6379"}}}
	cfg["http"] = map[string]any{"address": "0.0.0.0:4195"}
	p := build(t, cfg)
	b, err := json.Marshal(p)
	require.NoError(t, err)
	for _, s := range secrets {
		require.NotContains(t, string(b), s)
	}
	require.NotContains(t, p.Pipeline, "cache_resources")
	// Performance knobs survive.
	in := p.Pipeline["input"].(map[string]any)["kafka_franz"].(map[string]any)
	require.Equal(t, 100, in["batching"].(map[string]any)["count"])
	out := p.Pipeline["output"].(map[string]any)["gcp_bigquery"].(map[string]any)
	require.Equal(t, 8, out["max_in_flight"])
	require.Equal(t, "${BENCH_BQ_PROJECT}", out["project"])

	// Same for an approximated source and a fallback sink.
	cfg2 := pgToSnowflake()
	cfg2["input"] = map[string]any{"aws_sqs": map[string]any{"url": "https://sqs.customer-corp", "queue_url": "https://sqs/customer-corp", "max_number_of_messages": 10}}
	b2, _ := json.Marshal(build(t, cfg2))
	require.NotContains(t, string(b2), "customer-corp")
	require.Contains(t, string(b2), "max_number_of_messages")
}

func TestBuildIDUnique(t *testing.T) {
	re := regexp.MustCompile("^[0-9a-f]{12}$")
	a, b := build(t, pgToSnowflake()), build(t, pgToSnowflake())
	require.Regexp(t, re, a.ID)
	require.Regexp(t, re, b.ID)
	require.NotEqual(t, a.ID, b.ID)
	require.Equal(t, testNow, a.CreatedAt)
	require.Equal(t, "C1", a.Slack.Channel)
}

func TestBuildMalformedConfig(t *testing.T) {
	_, err := Build(map[string]any{"input": map[string]any{}}, Workload{}, SlackRef{}, testNow)
	require.Error(t, err)
}

func TestBuildJSONRoundTrip(t *testing.T) {
	p := build(t, pgToSnowflake())
	b, err := json.Marshal(p)
	require.NoError(t, err)
	require.True(t, strings.Contains(string(b), "\"plan_id\""))
	var back Plan
	require.NoError(t, json.Unmarshal(b, &back))
	require.Equal(t, p.ID, back.ID)
}
