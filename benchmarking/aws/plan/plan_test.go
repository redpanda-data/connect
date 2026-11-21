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

	"github.com/redpanda-data/connect/v4/benchmarking/aws/catalog"
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
				// On already, so the bench's forced schema evolution adds
				// no caveat to the baseline fixture.
				"schema_evolution": map[string]any{"enabled": true},
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
	withInput := func(in map[string]any) map[string]any {
		cfg := pgToSnowflake()
		cfg["input"] = in
		return cfg
	}
	withOutput := func(out map[string]any) map[string]any {
		cfg := pgToSnowflake()
		cfg["output"] = out
		return cfg
	}
	withProcs := func(procs ...any) map[string]any {
		cfg := pgToSnowflake()
		cfg["pipeline"] = map[string]any{"processors": procs}
		return cfg
	}
	cases := []struct {
		name    string
		cfg     map[string]any
		secrets []string
	}{
		{"dsn password", pgToSnowflake(), []string{"s3cretpw", "customer-corp", "cust_slot", "customers"}},
		{"snowflake key user account", pgToSnowflake(), []string{"MIIEvQ", "svc_loader", "CUSTORG-ACCT", "LOADER"}},
		{"bigquery credentials", withOutput(map[string]any{"gcp_bigquery": map[string]any{
			"project": "cust-proj", "dataset": "cust-ds", "table": "cust-tbl",
			"credentials_json": "{\"private_key\":\"bq-private-key\"}",
		}}), []string{"bq-private-key", "cust-proj", "cust-ds", "cust-tbl"}},
		{"sasl", withInput(map[string]any{"kafka_franz": map[string]any{
			"seed_brokers": []any{"kafka-prod-1.customer-corp.internal:9092"},
			"topics":       []any{"customers"},
			"sasl":         []any{map[string]any{"mechanism": "PLAIN", "username": "saslusername", "password": "SASLPASS"}},
		}}), []string{"saslusername", "SASLPASS", "kafka-prod-1", "customer-corp"}},
		{"postgres aws block", withInput(map[string]any{"postgres_cdc": map[string]any{
			"dsn": "postgres://u@h/db",
			"aws": map[string]any{
				"enabled": true, "region": "eu-west-9", "endpoint": "https://rds-endpoint-xyz",
				"id": "AKIAFAKEKEY", "secret": "awssecret123",
				"roles": []any{map[string]any{"role": "arn:aws:iam::999:role/x", "role_external_id": "extid-777"}},
			},
		}}), []string{"AKIAFAKEKEY", "awssecret123", "extid-777", "rds-endpoint-xyz", "arn:aws:iam::999"}},
		{"schema registry oauth", withProcs(map[string]any{"schema_registry_decode": map[string]any{
			"url": "https://sr.customer-corp.io",
			"oauth": map[string]any{
				"enabled": true, "consumer_key": "ckey-1", "consumer_secret": "csecret-2",
				"access_token": "atok-3", "access_token_secret": "atoksecret-4",
			},
			"oauth2": map[string]any{"client_key": "clientkey-5", "token_url": "https://tok.customer-corp.io"},
		}}), []string{"csecret-2", "atok-3", "atoksecret-4", "clientkey-5", "tok.customer-corp", "ckey-1", "sr.customer-corp"}},
		{"http bearer", withProcs(map[string]any{"http": map[string]any{
			"url": "https://api.customer-corp.com", "headers": map[string]any{"Authorization": "Bearer tok_abc123"},
		}}), []string{"tok_abc123", "customer-corp"}},
		{"cosmosdb connection string", withProcs(map[string]any{"azure_cosmosdb": map[string]any{
			"connection_string": "AccountEndpoint=https://cosmos-acct.documents.azure.com;AccountKey=cosmoskey==",
			"account_key":       "acctkey-9", "endpoint": "https://cosmos-acct.documents.azure.com",
		}}), []string{"cosmoskey", "acctkey-9", "cosmos-acct"}},
		{
			"ollama server", withProcs(map[string]any{"ollama_chat": map[string]any{"server_address": "http://ollama.customer-corp:11434"}}),
			[]string{"ollama.customer-corp"},
		},
		{"resources section", func() map[string]any {
			cfg := pgToSnowflake()
			cfg["resources"] = map[string]any{"caches": []any{map[string]any{"redis": map[string]any{"url": "redis://res-host", "password": "respw"}}}}
			cfg["cache_resources"] = []any{map[string]any{"redis": map[string]any{"url": "redis://cr-host", "password": "crpw"}}}
			return cfg
		}(), []string{"res-host", "respw", "cr-host", "crpw"}},
		{
			"unknown output", withOutput(map[string]any{"mystery_sink": map[string]any{"api_key": "apikey-xyz", "password": "mystpw", "host": "mystery-host"}}),
			[]string{"apikey-xyz", "mystpw", "mystery-host"},
		},
		{
			"unknown input", withInput(map[string]any{"mystery_src": map[string]any{"api_key": "apikey-abc", "queue_url": "https://q.customer-corp", "max": 5}}),
			[]string{"apikey-abc", "customer-corp"},
		},
		{"protobuf bsr", withProcs(map[string]any{"protobuf": map[string]any{
			"operator": "to_json", "message": "x.Y",
			"bsr": []any{map[string]any{"url": "https://buf.customer-corp.io", "api_key": "BSRKEY-1", "module": "buf.build/cust/mod"}},
		}}), []string{"BSRKEY-1", "buf.customer-corp", "buf.build/cust"}},
		{"json_schema http schema_path", withProcs(map[string]any{"json_schema": map[string]any{
			"schema_path": "http://u:SCHEMAPW@schemas.customer-corp.io/s.json",
		}}), []string{"SCHEMAPW", "schemas.customer-corp"}},
		{"bloblang literal bearer", withProcs(map[string]any{"mapping": "root.auth = \"Bearer LITERALTOK\""}), []string{"LITERALTOK"}},
		{"bloblang literal aws key", withProcs(map[string]any{"mutation": "root.k = \"AKIAIOSFODNN7EXAMPLE\""}), []string{"AKIAIOSFODNN7EXAMPLE"}},
		{"log message password", withProcs(map[string]any{"log": map[string]any{"message": "connecting password=hunter22 now"}}), []string{"hunter22"}},
		{"request_map jwt", withProcs(map[string]any{"branch": map[string]any{
			"request_map": "root.t = \"eyJhbGciOi.eyJzdWIiOi.SflKxwRJSM\"", "processors": []any{map[string]any{"mapping": "root = this"}},
		}}), []string{"eyJhbGciOi"}},
		{"real output unknown secret-ish keys", withOutput(map[string]any{"aws_s3": map[string]any{
			"bucket": "my-secret-bucket", "path": "x", "client_secret": "cs-1", "session_token": "st-2", "custom_cert": "cc-3",
			"force_path_style_urls": true,
		}}), []string{"my-secret-bucket", "cs-1", "st-2", "cc-3"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := build(t, tc.cfg)
			b, err := json.Marshal(p)
			require.NoError(t, err)
			for _, s := range tc.secrets {
				require.NotContains(t, string(b), s)
			}
		})
	}
}

func TestBuildAWSBlockNotLeftEnabled(t *testing.T) {
	cfg := pgToSnowflake()
	cfg["input"].(map[string]any)["postgres_cdc"].(map[string]any)["aws"] = map[string]any{"enabled": true, "region": "eu-west-9"}
	p := build(t, cfg)
	in := p.Pipeline["input"].(map[string]any)["postgres_cdc"].(map[string]any)
	require.NotContains(t, in, "aws")
}

func TestBuildKeepsPureProcessorsVerbatim(t *testing.T) {
	cfg := pgToSnowflake()
	procs := []any{
		map[string]any{"json_schema": map[string]any{"schema": "{\"type\":\"object\"}"}},
		map[string]any{"parquet_encode": map[string]any{"schema": []any{map[string]any{"name": "id", "type": "INT64"}}, "default_compression": "zstd"}},
		map[string]any{"mapping": "root = this"},
	}
	cfg["pipeline"] = map[string]any{"processors": procs}
	p := build(t, cfg)
	require.Empty(t, p.Caveats)
	require.Equal(t, procs, p.Pipeline["pipeline"].(map[string]any)["processors"])
}

func TestBuildStubsUnknownProcessors(t *testing.T) {
	cfg := pgToSnowflake()
	cfg["pipeline"] = map[string]any{"processors": []any{map[string]any{"some_new_ai_thing": map[string]any{"x": 1}}}}
	p := build(t, cfg)
	require.Equal(t, []string{"external_calls_stubbed"}, p.Caveats)
	require.Equal(t, []any{map[string]any{"mapping": "root = this"}}, p.Pipeline["pipeline"].(map[string]any)["processors"])
}

func TestBuildWorkflowRecursed(t *testing.T) {
	cfg := pgToSnowflake()
	cfg["pipeline"] = map[string]any{"processors": []any{map[string]any{"workflow": map[string]any{
		"order": []any{[]any{"a"}},
		"branches": map[string]any{"a": map[string]any{
			"request_map": "root = this",
			"processors":  []any{map[string]any{"http": map[string]any{"url": "https://x"}}},
		}},
	}}}}
	p := build(t, cfg)
	require.Equal(t, []string{"external_calls_stubbed"}, p.Caveats)
	wf := p.Pipeline["pipeline"].(map[string]any)["processors"].([]any)[0].(map[string]any)["workflow"].(map[string]any)
	br := wf["branches"].(map[string]any)["a"].(map[string]any)
	require.Equal(t, []any{map[string]any{"mapping": "root = this"}}, br["processors"])
}

func TestBuildIncrementalSnapshotCacheSwapped(t *testing.T) {
	cfg := pgToSnowflake()
	pg := cfg["input"].(map[string]any)["postgres_cdc"].(map[string]any)
	pg["incremental_snapshot"] = map[string]any{"enabled": true, "checkpoint_cache": "my_redis", "chunk_size": 1000}
	p := build(t, cfg)
	in := p.Pipeline["input"].(map[string]any)["postgres_cdc"].(map[string]any)
	inc := in["incremental_snapshot"].(map[string]any)
	require.Equal(t, "bench_cache", inc["checkpoint_cache"])
	require.Equal(t, 1000, inc["chunk_size"])
	require.Equal(t, []any{map[string]any{"label": "bench_cache", "memory": map[string]any{}}}, p.Pipeline["cache_resources"])
}

func TestBuildDedupeUsesBenchCache(t *testing.T) {
	cfg := pgToSnowflake()
	cfg["pipeline"] = map[string]any{"processors": []any{map[string]any{"dedupe": map[string]any{"cache": "cust_redis", "key": "${!id}"}}}}
	p := build(t, cfg)
	require.Empty(t, p.Caveats)
	d := p.Pipeline["pipeline"].(map[string]any)["processors"].([]any)[0].(map[string]any)["dedupe"].(map[string]any)
	require.Equal(t, "bench_cache", d["cache"])
	require.Contains(t, p.Pipeline, "cache_resources")
}

func TestBuildFingerprintOfOriginal(t *testing.T) {
	cfg := pgToSnowflake()
	want, err := catalog.Fingerprint(cfg)
	require.NoError(t, err)
	p := build(t, cfg)
	require.Equal(t, want, p.Fingerprint)

	other := pgToSnowflake()
	other["input"].(map[string]any)["postgres_cdc"].(map[string]any)["dsn"] = "postgres://other:pw@elsewhere/db"
	other["output"].(map[string]any)["snowflake_streaming"].(map[string]any)["private_key"] = "different"
	require.Equal(t, want, build(t, other).Fingerprint)
}

func TestBuildWrapperInputs(t *testing.T) {
	cfg := pgToSnowflake()
	cfg["input"] = map[string]any{"broker": map[string]any{"inputs": []any{
		map[string]any{"oracledb_cdc": map[string]any{"connection_string": "oracle-secret"}},
	}}}
	p := build(t, cfg)
	require.False(t, p.Runnable)
	require.Equal(t, "no bench stack for oracledb_cdc yet", p.Reason)
	b, _ := json.Marshal(p)
	require.NotContains(t, string(b), "oracle-secret")

	cfg["input"] = map[string]any{"read_until": map[string]any{"check": "false", "input": map[string]any{
		"mysql_cdc": map[string]any{"dsn": "u:pw@tcp(h)/d", "tables": []any{"x"}},
	}}}
	p = build(t, cfg)
	require.True(t, p.Runnable)
	require.Equal(t, SourceMySQL, p.Source)
	ru := p.Pipeline["input"].(map[string]any)["read_until"].(map[string]any)
	require.Equal(t, "${MYSQL_DSN}", ru["input"].(map[string]any)["mysql_cdc"].(map[string]any)["dsn"])

	cfg["input"] = map[string]any{"sequence": map[string]any{"inputs": []any{
		map[string]any{"kafka_franz": map[string]any{}},
		map[string]any{"http_server": map[string]any{}},
	}}}
	p = build(t, cfg)
	require.True(t, p.Runnable)
	require.Equal(t, SourceApproximated, p.Source)
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

func TestBuildStubsExternalReferencingPureProcessors(t *testing.T) {
	cases := map[string]any{
		"javascript fetch":    map[string]any{"javascript": map[string]any{"code": "const r = benthos.v0_fetch('http://x');"}},
		"javascript file":     map[string]any{"javascript": map[string]any{"file": "/etc/x.js"}},
		"javascript folders":  map[string]any{"javascript": map[string]any{"code": "1", "global_folders": []any{"/opt"}}},
		"awk system":          map[string]any{"awk": map[string]any{"program": "{ system(\"curl x\") }"}},
		"awk pipe":            map[string]any{"awk": map[string]any{"program": "{ print $1 | \"sh\" }"}},
		"awk getline":         map[string]any{"awk": map[string]any{"program": "{ getline x < \"/f\" }"}},
		"grok pattern_paths":  map[string]any{"grok": map[string]any{"expressions": []any{"%{X}"}, "pattern_paths": []any{"/p"}}},
		"avro schema_path":    map[string]any{"avro": map[string]any{"schema_path": "file:///s.avsc"}},
		"protobuf import":     map[string]any{"protobuf": map[string]any{"import_paths": []any{"/proto"}}},
		"workflow branch_res": map[string]any{"workflow": map[string]any{"branch_resources": []any{"r"}}},
	}
	for name, proc := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := pgToSnowflake()
			cfg["pipeline"] = map[string]any{"processors": []any{proc}}
			p := build(t, cfg)
			require.Equal(t, []string{"external_calls_stubbed"}, p.Caveats)
			require.Equal(t, []any{map[string]any{"mapping": "root = this"}}, p.Pipeline["pipeline"].(map[string]any)["processors"])
		})
	}
	cfg := pgToSnowflake()
	cfg["pipeline"] = map[string]any{"processors": []any{
		map[string]any{"javascript": map[string]any{"code": "return 1"}},
		map[string]any{"awk": map[string]any{"program": "{ print $1 }"}},
	}}
	require.Empty(t, build(t, cfg).Caveats)
}

func TestBuildOffsetTokenKeptKMSRemoved(t *testing.T) {
	cfg := pgToSnowflake()
	cfg["output"].(map[string]any)["snowflake_streaming"].(map[string]any)["offset_token"] = "${!@offset}"
	p := build(t, cfg)
	out := p.Pipeline["output"].(map[string]any)["snowflake_streaming"].(map[string]any)
	require.Equal(t, "${!@offset}", out["offset_token"])

	cfg = pgToSnowflake()
	cfg["output"] = map[string]any{"aws_s3": map[string]any{"kms_key_id": "arn:aws:kms:us-east-1:123456789012:key/x"}}
	b, _ := json.Marshal(build(t, cfg))
	require.NotContains(t, string(b), "123456789012")
	require.NotContains(t, string(b), "kms_key_id")
}

func TestBuildMultipleCDCChildrenNotRunnable(t *testing.T) {
	cfg := pgToSnowflake()
	pg := func() any { return map[string]any{"postgres_cdc": map[string]any{"dsn": "x", "tables": []any{"a"}}} }
	cfg["input"] = map[string]any{"broker": map[string]any{"inputs": []any{pg(), pg()}}}
	p := build(t, cfg)
	require.False(t, p.Runnable)
	require.Equal(t, "multiple CDC inputs in one pipeline not supported yet", p.Reason)
}

func TestBuildRetryAndTryCatchRecursed(t *testing.T) {
	ext := func() any { return map[string]any{"http": map[string]any{"url": "https://x"}} }
	keep := map[string]any{"mapping": "root.a = 1"}
	cfg := pgToSnowflake()
	cfg["pipeline"] = map[string]any{"processors": []any{
		map[string]any{"retry": map[string]any{"max_retries": 3, "processors": []any{ext(), keep}}},
		map[string]any{"try_catch": map[string]any{"processors": []any{ext(), keep}, "catch": []any{ext(), keep}}},
	}}
	p := build(t, cfg)
	require.Equal(t, []string{"external_calls_stubbed"}, p.Caveats)
	procs := p.Pipeline["pipeline"].(map[string]any)["processors"].([]any)
	stub := map[string]any{"mapping": "root = this"}
	require.Equal(t, []any{stub, keep}, procs[0].(map[string]any)["retry"].(map[string]any)["processors"])
	tc := procs[1].(map[string]any)["try_catch"].(map[string]any)
	require.Equal(t, []any{stub, keep}, tc["processors"])
	require.Equal(t, []any{stub, keep}, tc["catch"])
	require.Equal(t, 3, procs[0].(map[string]any)["retry"].(map[string]any)["max_retries"])
}
