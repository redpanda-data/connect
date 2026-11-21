// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSwapIdentityRedpandaToS3(t *testing.T) {
	cfg := map[string]any{
		"input": map[string]any{"redpanda": map[string]any{
			"seed_brokers": []any{"b:9092"}, "topics": []any{"t"}, "consumer_group": "g",
		}},
		"output": map[string]any{"aws_s3": map[string]any{
			"bucket": "b", "path": "x", "region": "eu-west-1", "endpoint": "http://minio", "batching": map[string]any{"count": 10},
		}},
	}
	out, caveats, err := SwapIdentity(cfg, SourceRedpanda, SinkS3)
	require.NoError(t, err)
	require.Empty(t, caveats)
	in := out["input"].(map[string]any)["redpanda"].(map[string]any)
	require.Equal(t, []any{"${BENCH_REDPANDA_BROKERS}"}, in["seed_brokers"])
	require.Equal(t, []any{"${BENCH_TOPIC}"}, in["topics"])
	require.Equal(t, "${BENCH_CONSUMER_GROUP}", in["consumer_group"])
	s3 := out["output"].(map[string]any)["aws_s3"].(map[string]any)
	require.Equal(t, "${BENCH_S3_BUCKET}", s3["bucket"])
	require.Equal(t, "${BENCH_S3_PREFIX}${!counter()}.json", s3["path"])
	require.NotContains(t, s3, "endpoint")
	require.Equal(t, 10, s3["batching"].(map[string]any)["count"])
}

func TestSwapIdentityIcebergTemplate(t *testing.T) {
	cfg := map[string]any{
		"input":  map[string]any{"redpanda": map[string]any{}},
		"output": map[string]any{"iceberg": map[string]any{"catalog": map[string]any{"url": "https://x", "auth": map[string]any{"bearer": "tok"}}, "namespace": "n", "table": "t"}},
	}
	out, _, err := SwapIdentity(cfg, SourceRedpanda, SinkIceberg)
	require.NoError(t, err)
	ice := out["output"].(map[string]any)["iceberg"].(map[string]any)
	cat := ice["catalog"].(map[string]any)
	require.Equal(t, "${BENCH_ICEBERG_CATALOG_URL}", cat["url"])
	require.Equal(t, "${BENCH_ICEBERG_WAREHOUSE}", cat["warehouse"])
	require.Equal(t, map[string]any{"aws_sigv4": map[string]any{"region": "${BENCH_AWS_REGION}", "service": "glue"}}, cat["auth"])
	require.Equal(t, "${BENCH_ICEBERG_NAMESPACE}", ice["namespace"])
	require.Equal(t, map[string]any{
		"enabled":        true,
		"table_location": "${BENCH_ICEBERG_TABLE_LOCATION}",
	}, ice["schema_evolution"], "no customer block: the default is added")
}

func swapIcebergEvo(t *testing.T, evo map[string]any) (map[string]any, []string) {
	t.Helper()
	cfg := map[string]any{
		"input":  map[string]any{"redpanda": map[string]any{}},
		"output": map[string]any{"iceberg": map[string]any{"namespace": "n", "table": "t", "schema_evolution": evo}},
	}
	out, caveats, err := SwapIdentity(cfg, SourceRedpanda, SinkIceberg)
	require.NoError(t, err)
	return out["output"].(map[string]any)["iceberg"].(map[string]any)["schema_evolution"].(map[string]any), caveats
}

func TestSwapIdentityIcebergKeepsCustomerSchemaEvolution(t *testing.T) {
	evo, caveats := swapIcebergEvo(t, map[string]any{
		"enabled":                 true,
		"partition_spec":          "(day(ts))",
		"new_column_type_mapping": "root = \"string\"",
		"table_location":          "s3://customer/",
	})
	require.Equal(t, "(day(ts))", evo["partition_spec"])
	require.Equal(t, "root = \"string\"", evo["new_column_type_mapping"])
	require.Equal(t, "${BENCH_ICEBERG_TABLE_LOCATION}", evo["table_location"])
	require.Equal(t, true, evo["enabled"])
	require.NotContains(t, caveats, CaveatSchemaEvoForced)
}

func TestSwapIdentityIcebergForcesSchemaEvolutionOn(t *testing.T) {
	evo, caveats := swapIcebergEvo(t, map[string]any{"enabled": false, "partition_spec": "(day(ts))"})
	require.Equal(t, true, evo["enabled"])
	require.Equal(t, "(day(ts))", evo["partition_spec"])
	require.Contains(t, caveats, CaveatSchemaEvoForced)
}

// The snowflake outputs read the key from the host-side file the bench
// script writes; the key itself is never a config value.
func TestSwapIdentitySnowflakeUsesPrivateKeyFile(t *testing.T) {
	for _, component := range []string{"snowflake_streaming", "snowflake_put"} {
		cfg := map[string]any{
			"input": map[string]any{"redpanda": map[string]any{}},
			"output": map[string]any{component: map[string]any{
				"account": "CUST", "user": "u", "role": "r", "database": "d", "schema": "s",
				"private_key": "-----BEGIN PRIVATE KEY-----MIIE-----END PRIVATE KEY-----", "private_key_pass": "pw",
			}},
		}
		out, _, err := SwapIdentity(cfg, SourceRedpanda, SinkSnowflake)
		require.NoError(t, err)
		sf := out["output"].(map[string]any)[component].(map[string]any)
		require.Equal(t, "${BENCH_SNOWFLAKE_PRIVATE_KEY_FILE}", sf["private_key_file"], component)
		require.NotContains(t, sf, "private_key", component)
		require.NotContains(t, sf, "private_key_pass", component)
		require.Equal(t, "${BENCH_SNOWFLAKE_ACCOUNT}", sf["account"], component)
		require.Equal(t, "${BENCH_SNOWFLAKE_SCHEMA}", sf["schema"], component)
	}
}

func swapSnowflakeEvo(t *testing.T, evo map[string]any) (map[string]any, []string) {
	t.Helper()
	sf := map[string]any{"table": "CUSTOMERS"}
	if evo != nil {
		sf["schema_evolution"] = evo
	}
	cfg := map[string]any{
		"input":  map[string]any{"redpanda": map[string]any{}},
		"output": map[string]any{"snowflake_streaming": sf},
	}
	out, caveats, err := SwapIdentity(cfg, SourceRedpanda, SinkSnowflake)
	require.NoError(t, err)
	return out["output"].(map[string]any)["snowflake_streaming"].(map[string]any)["schema_evolution"].(map[string]any), caveats
}

// The bench table comes from the output's schema evolution: it is forced on,
// with a caveat unless the customer already had it on, and the customer's
// other schema_evolution keys are kept.
func TestSwapIdentitySnowflakeForcesSchemaEvolution(t *testing.T) {
	evo, caveats := swapSnowflakeEvo(t, nil)
	require.Equal(t, map[string]any{"enabled": true}, evo)
	require.Contains(t, caveats, CaveatSchemaEvoForced, "absent")

	evo, caveats = swapSnowflakeEvo(t, map[string]any{"enabled": false, "ignore_nulls": false})
	require.Equal(t, true, evo["enabled"])
	require.Equal(t, false, evo["ignore_nulls"])
	require.Contains(t, caveats, CaveatSchemaEvoForced, "off")

	evo, caveats = swapSnowflakeEvo(t, map[string]any{"enabled": true, "new_column_type_mapping": "root = \"VARIANT\""})
	require.Equal(t, "root = \"VARIANT\"", evo["new_column_type_mapping"])
	require.NotContains(t, caveats, CaveatSchemaEvoForced, "already on")
}

func TestSwapIdentityMySQL(t *testing.T) {
	cfg := map[string]any{
		"input":  map[string]any{"mysql_cdc": map[string]any{"dsn": "u:p@tcp(h)/d", "tables": []any{"x"}, "checkpoint_cache": "redis_cache", "stream_snapshot": true}},
		"output": map[string]any{"drop": map[string]any{}},
	}
	out, caveats, err := SwapIdentity(cfg, SourceMySQL, SinkDrop)
	require.NoError(t, err)
	require.Equal(t, []string{"snapshot_not_measured"}, caveats)
	in := out["input"].(map[string]any)["mysql_cdc"].(map[string]any)
	require.Equal(t, "${MYSQL_DSN}", in["dsn"])
	require.Equal(t, "bench_cache", in["checkpoint_cache"])
	require.Equal(t, false, in["stream_snapshot"])
	require.Equal(t, []any{map[string]any{"label": "bench_cache", "memory": map[string]any{}}}, out["cache_resources"])
}

func TestSwapIdentityRedpandaCommonAndKafka(t *testing.T) {
	cfg := map[string]any{
		"input":  map[string]any{"kafka": map[string]any{"addresses": []any{"h:9092"}, "topics": []any{"t"}}},
		"output": map[string]any{"redpanda_common": map[string]any{"topic": "t"}},
	}
	out, _, err := SwapIdentity(cfg, SourceRedpanda, SinkRedpanda)
	require.NoError(t, err)
	k := out["input"].(map[string]any)["kafka"].(map[string]any)
	require.Equal(t, []any{"${BENCH_REDPANDA_BROKERS}"}, k["addresses"])
	require.Equal(t, map[string]any{"seed_brokers": []any{"${BENCH_REDPANDA_BROKERS}"}}, out["redpanda"])
	rc := out["output"].(map[string]any)["redpanda_common"].(map[string]any)
	require.Equal(t, "${BENCH_DST_TOPIC}", rc["topic"])
}

func TestSwapIdentityForcesEarliestReads(t *testing.T) {
	for _, tc := range []struct {
		component string
		customer  map[string]any
		want      map[string]any
		gone      []string
	}{
		{"redpanda", map[string]any{"start_offset": "latest", "start_from_oldest": false}, map[string]any{"start_offset": "earliest"}, []string{"start_from_oldest"}},
		{"redpanda_common", map[string]any{"start_offset": "committed"}, map[string]any{"start_offset": "earliest"}, []string{"start_from_oldest"}},
		{"kafka_franz", map[string]any{"start_offset": "latest", "start_from_oldest": false}, map[string]any{"start_offset": "earliest"}, []string{"start_from_oldest"}},
		{"kafka", map[string]any{"start_from_oldest": false}, map[string]any{"start_from_oldest": true}, []string{"start_offset"}},
	} {
		t.Run(tc.component, func(t *testing.T) {
			cfg := map[string]any{
				"input":  map[string]any{tc.component: tc.customer},
				"output": map[string]any{"drop": map[string]any{}},
			}
			out, _, err := SwapIdentity(cfg, SourceRedpanda, SinkDrop)
			require.NoError(t, err)
			in := out["input"].(map[string]any)[tc.component].(map[string]any)
			for k, v := range tc.want {
				require.Equal(t, v, in[k], k)
			}
			for _, k := range tc.gone {
				require.NotContains(t, in, k)
			}
		})
	}
}

func swapBigQuery(t *testing.T, bq map[string]any) (map[string]any, []string) {
	t.Helper()
	cfg := map[string]any{
		"input":  map[string]any{"generate": map[string]any{"mapping": `root = {}`}},
		"output": map[string]any{"gcp_bigquery": bq},
	}
	out, caveats, err := SwapIdentity(cfg, SourceApproximated, SinkBigQuery)
	require.NoError(t, err)
	return out["output"].(map[string]any)["gcp_bigquery"].(map[string]any), caveats
}

// The key never reaches the config (the runner host supplies it as
// application-default credentials), and CREATE_NEVER cannot stand: the run's
// dataset starts empty.
func TestSwapIdentityBigQueryNoCredentialsJSONAndForcesCreate(t *testing.T) {
	bq, caveats := swapBigQuery(t, map[string]any{
		"project": "cust", "dataset": "d", "table": "t",
		"credentials_json":   `{"private_key":"bq-private-key"}`,
		"create_disposition": "CREATE_NEVER",
	})
	require.NotContains(t, bq, "credentials_json")
	require.Equal(t, "${BENCH_BQ_PROJECT}", bq["project"])
	require.Equal(t, "${BENCH_BQ_DATASET}", bq["dataset"])
	require.Equal(t, "${BENCH_BQ_TABLE}", bq["table"])
	require.Equal(t, "CREATE_IF_NEEDED", bq["create_disposition"])
	require.Contains(t, caveats, CaveatSchemaEvoForced)

	bq, caveats = swapBigQuery(t, map[string]any{"project": "p", "dataset": "d", "table": "t"})
	require.Equal(t, "CREATE_IF_NEEDED", bq["create_disposition"])
	require.NotContains(t, caveats, CaveatSchemaEvoForced, "unset is the default: no caveat")

	_, caveats = swapBigQuery(t, map[string]any{"table": "t", "create_disposition": "CREATE_IF_NEEDED"})
	require.NotContains(t, caveats, CaveatSchemaEvoForced)
}
