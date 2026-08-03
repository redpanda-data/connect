// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func icebergArmScenario(t *testing.T) (*Scenario, map[string]string, Topology) {
	t.Helper()
	s, err := LoadScenario("testdata/valid-iceberg-arms.yaml")
	require.NoError(t, err)
	outs := map[string]string{
		"redpanda_broker_endpoints": "b1:9092",
		"glue_rest_uri":             "https://glue.example",
		"warehouse_account_id":      "1234",
		"aws_region":                "us-east-2",
		"s3_bucket":                 "wh-bucket",
		"warehouse_s3_uri":          "s3://wh-bucket/wh",
	}
	topo, err := topologyFor(s.Direction)
	require.NoError(t, err)
	return s, outs, topo
}

func readYAML(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, yaml.Unmarshal(raw, &m))
	return m
}

func TestRenderPointConfigs_SingleStreamArm(t *testing.T) {
	s, outs, topo := icebergArmScenario(t)
	names := newBenchNames("sess-x", "iceberg")
	plan := buildSweepPlan(s)

	got, err := renderPointConfigs(s, outs, topo, names, plan[1]) // a1-1pipe-gmp4
	require.NoError(t, err)
	require.Equal(t, "2-a1-1pipe-gmp4", got.Key)
	require.NotEmpty(t, got.Single)
	require.Empty(t, got.Root)
	require.Empty(t, got.Streams)

	cfg := readYAML(t, got.Single)
	// One config carries everything, exactly as the pre-arms renderer produced.
	for _, k := range []string{"http", "redpanda", "input", "output", "logger", "metrics", "buffer"} {
		require.Contains(t, cfg, k, "single-stream config must keep section %q", k)
	}
	ice := cfg["output"].(map[string]any)["iceberg"].(map[string]any)
	require.Equal(t, "bench_sess_x_iceberg_connect", ice["table"], "single-stream arm keeps the unsuffixed table")
	require.Equal(t, 16, ice["max_in_flight"], "arm a1 inherits the scenario's max_in_flight")
}

func TestRenderPointConfigs_StreamsModeSplitsRootAndStreams(t *testing.T) {
	s, outs, topo := icebergArmScenario(t)
	names := newBenchNames("sess-x", "iceberg")
	plan := buildSweepPlan(s)

	got, err := renderPointConfigs(s, outs, topo, names, plan[2]) // b-2pipe-gmp4
	require.NoError(t, err)
	require.Equal(t, "2-b-2pipe-gmp4", got.Key)
	require.Empty(t, got.Single)
	require.NotEmpty(t, got.Root)
	require.Len(t, got.Streams, 2)

	// The root config carries observability and service-wide fields ONLY —
	// benthos streams mode rejects input/output there.
	root := readYAML(t, got.Root)
	for _, k := range []string{"http", "logger", "metrics"} {
		require.Contains(t, root, k)
	}
	for _, k := range []string{"input", "output", "buffer"} {
		require.NotContains(t, root, k, "root config must not carry stream field %q", k)
	}

	// Each stream config carries only its own pipeline.
	for i, path := range got.Streams {
		sc := readYAML(t, path)
		for _, k := range []string{"input", "output", "buffer"} {
			require.Contains(t, sc, k, "stream %d missing %q", i, k)
		}
		for _, k := range []string{"http", "logger", "metrics"} {
			require.NotContains(t, sc, k, "stream %d must not carry root field %q", i, k)
		}
		ice := sc["output"].(map[string]any)["iceberg"].(map[string]any)
		require.Equal(t, 8, ice["max_in_flight"], "arm B's override must reach every stream")
		buf := sc["buffer"].(map[string]any)["memory"].(map[string]any)
		require.Equal(t, 262144000, buf["limit"], "arm B halves each stream's buffer")
	}

	// Distinct tables per stream, shared topic and consumer group.
	s0 := readYAML(t, got.Streams[0])
	s1 := readYAML(t, got.Streams[1])
	t0 := s0["output"].(map[string]any)["iceberg"].(map[string]any)["table"]
	t1 := s1["output"].(map[string]any)["iceberg"].(map[string]any)["table"]
	require.Equal(t, "bench_sess_x_iceberg_connect_s0", t0)
	require.Equal(t, "bench_sess_x_iceberg_connect_s1", t1)

	in0 := s0["input"].(map[string]any)["redpanda"].(map[string]any)
	in1 := s1["input"].(map[string]any)["redpanda"].(map[string]any)
	require.Equal(t, in0["consumer_group"], in1["consumer_group"],
		"both streams share the group so the 16 partitions split, not double")
	require.Equal(t, in0["topics"], in1["topics"])

	// Placeholders must be resolved, not left as ${...}.
	require.NotContains(t, t0, "${")
	require.Equal(t, "https://glue.example",
		s0["output"].(map[string]any)["iceberg"].(map[string]any)["catalog"].(map[string]any)["url"])
}

func TestRunnerConfigPaths_MapsKeysToRunnerHostPaths(t *testing.T) {
	sets := []renderedPointConfigs{
		{Key: "2-a0", Single: "/local/tmp/a"},
		{Key: "2-b", Root: "/local/tmp/root", Streams: []string{"/local/tmp/s0", "/local/tmp/s1"}},
	}
	got := runnerConfigPaths(sets)
	require.Equal(t, "/opt/bench/cfg/2-a0/config.yaml", got["2-a0"].Single)
	require.Empty(t, got["2-a0"].Root)
	require.Equal(t, "/opt/bench/cfg/2-b/root.yaml", got["2-b"].Root)
	require.Equal(t, "/opt/bench/cfg/2-b/streams", got["2-b"].Dir)
	require.Empty(t, got["2-b"].Single)
}
