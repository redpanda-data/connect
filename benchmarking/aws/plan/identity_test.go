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
	// Glue assigns no table location and the bench database starts empty,
	// so the output must create the table, under the provisioned prefix.
	require.Equal(t, map[string]any{
		"enabled":        true,
		"table_location": "${BENCH_ICEBERG_TABLE_LOCATION}",
	}, ice["schema_evolution"])
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
