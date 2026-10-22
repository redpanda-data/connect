// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func readYAML(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, yaml.Unmarshal(raw, &m))
	return m
}

func TestRenderPointConfigs_SingleStreamArm(t *testing.T) {
	s := &Scenario{
		Connector: "postgres_cdc",
		Pipeline: map[string]any{
			"input":  map[string]any{"postgres_cdc": map[string]any{"dsn": "${POSTGRES_DSN}"}},
			"buffer": map[string]any{"memory": map[string]any{"limit": 524288000}},
		},
	}
	names := newBenchNames("sess-x", "postgres_cdc")
	outs := map[string]string{}
	pt := sweepPoint{VCPU: 2, ArmID: "a1", GOMAXPROCS: 4, Streams: 1}

	got, err := renderPointConfigs(s, outs, sourceTopology{}, names, pt)
	require.NoError(t, err)
	require.Equal(t, "2-a1", got.Key)
	require.NotEmpty(t, got.Single)
	require.Empty(t, got.Root)
	require.Empty(t, got.Streams)

	cfg := readYAML(t, got.Single)
	// One config carries everything, exactly as the pre-arms renderer produced.
	for _, k := range []string{"http", "redpanda", "input", "output", "logger", "metrics", "buffer"} {
		require.Contains(t, cfg, k, "single-stream config must keep section %q", k)
	}
	rp := cfg["output"].(map[string]any)["redpanda"].(map[string]any)
	require.Equal(t, "bench_${BENCH_SESSION_ID}_postgres_cdc_connect", rp["topic"])
}

// TestRenderPointConfigs_StreamsModeSplitsRootAndStreams pins the streams-mode
// render mechanics (root/streams split, per-stream pipeline override) using a
// source scenario. Streams > 1 has no validated real-world use for source
// (see Scenario.Validate) — the per-stream Iceberg naming that made it
// meaningful returns with the iceberg-sink stack PR — but renderPointConfigs
// itself doesn't consult Validate, so the render mechanics stay covered here.
func TestRenderPointConfigs_StreamsModeSplitsRootAndStreams(t *testing.T) {
	s := &Scenario{
		Connector: "postgres_cdc",
		Pipeline: map[string]any{
			"input":  map[string]any{"postgres_cdc": map[string]any{"dsn": "${POSTGRES_DSN}"}},
			"buffer": map[string]any{"memory": map[string]any{"limit": 524288000}},
		},
	}
	names := newBenchNames("sess-x", "postgres_cdc")
	outs := map[string]string{"redpanda_broker_endpoints": "b1:9092"}
	pt := sweepPoint{VCPU: 2, ArmID: "b", GOMAXPROCS: 4, Streams: 2}

	got, err := renderPointConfigs(s, outs, sourceTopology{}, names, pt)
	require.NoError(t, err)
	require.Equal(t, "2-b", got.Key)
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
	// The root config goes through writeTempYAML too — its own placeholder
	// (${REDPANDA_BROKER_ENDPOINTS}) must resolve, not just the stream configs'.
	require.Equal(t, []any{"b1:9092"}, root["redpanda"].(map[string]any)["seed_brokers"])

	// Each stream config carries only its own pipeline.
	for i, path := range got.Streams {
		sc := readYAML(t, path)
		for _, k := range []string{"input", "output", "buffer"} {
			require.Contains(t, sc, k, "stream %d missing %q", i, k)
		}
		for _, k := range []string{"http", "logger", "metrics"} {
			require.NotContains(t, sc, k, "stream %d must not carry root field %q", i, k)
		}
	}
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

// TestBuildStagePlan_AgreesWithRunnerConfigPaths is finding #3 of the final
// whole-branch review: runnerConfigPaths (the launched path) and
// stageArtefacts' download commands (via buildStagePlan) used to rebuild the
// "/opt/bench/cfg/<key>" and "stage/cfg/<key>" literals independently, with
// nothing asserting they agreed. A drift there means the engine launches
// against a path that was never downloaded — which, combined with the
// early-abort guard now covering every arm point, must fail loud rather than
// report a plausible 0 MB/s. This pins the staged S3 key, the host download
// destination, and runnerConfigPaths' launch path to the SAME value for both
// a single-stream and a 2-stream point.
func TestBuildStagePlan_AgreesWithRunnerConfigPaths(t *testing.T) {
	sets := []renderedPointConfigs{
		{Key: "2-a0", Single: "/local/tmp/a"},
		{Key: "2-b", Root: "/local/tmp/root", Streams: []string{"/local/tmp/s0", "/local/tmp/s1"}},
	}
	const bucket = "results-bucket"
	cfgPaths := runnerConfigPaths(sets)
	items, dl := buildStagePlan(sets, bucket, false)
	dlJoined := strings.Join(dl, "\n")

	// Single-stream point: the download destination must be exactly the
	// launched path runnerConfigPaths computed, and the staged S3 key must
	// be what that download command actually copies from.
	single := cfgPaths["2-a0"].Single
	require.Equal(t, "/opt/bench/cfg/2-a0/config.yaml", single)
	require.Contains(t, dlJoined, fmt.Sprintf("aws s3 cp s3://%s/stage/cfg/2-a0/config.yaml %s", bucket, single))
	var sawSingleKey bool
	for _, it := range items {
		if it.key == "stage/cfg/2-a0/config.yaml" {
			sawSingleKey = true
		}
	}
	require.True(t, sawSingleKey, "staged S3 key must match the download source referenced above")

	// 2-stream point: root + both stream configs must agree across staged
	// key, download destination, and the launched Root/Dir.
	root := cfgPaths["2-b"].Root
	dir := cfgPaths["2-b"].Dir
	require.Equal(t, "/opt/bench/cfg/2-b/root.yaml", root)
	require.Equal(t, "/opt/bench/cfg/2-b/streams", dir)
	require.Contains(t, dlJoined, fmt.Sprintf("aws s3 cp s3://%s/stage/cfg/2-b/root.yaml %s", bucket, root))
	require.Contains(t, dlJoined, fmt.Sprintf("aws s3 cp s3://%s/stage/cfg/2-b/streams/stream-0.yaml %s/stream-0.yaml", bucket, dir))
	require.Contains(t, dlJoined, fmt.Sprintf("aws s3 cp s3://%s/stage/cfg/2-b/streams/stream-1.yaml %s/stream-1.yaml", bucket, dir))
}

// TestBuildStagePlan_ClearsStreamsDirBeforeDownload is finding #4: the
// streams/ directory must be wiped before download, not merely mkdir -p'd.
// /opt/bench/cfg/<key>/streams is session-independent (unlike runs/<sess>/),
// and downloads are per-file `aws s3 cp`, not `sync` — so a --keep re-run of
// the same scenario with a smaller stream count would otherwise leave a
// stale stream-N.yaml behind, launching an extra pipeline the sidecar never
// polls: a silent undercount with a plausible-looking value.
func TestBuildStagePlan_ClearsStreamsDirBeforeDownload(t *testing.T) {
	sets := []renderedPointConfigs{
		{Key: "2-b", Root: "/local/tmp/root", Streams: []string{"/local/tmp/s0", "/local/tmp/s1"}},
	}
	_, dl := buildStagePlan(sets, "b", false)
	dlJoined := strings.Join(dl, "\n")
	require.Contains(t, dlJoined, "rm -rf /opt/bench/cfg/2-b/streams && mkdir -p /opt/bench/cfg/2-b/streams",
		"a stale stream config from a previous --keep run with a different stream count must not survive a per-file aws s3 cp download")
}

// TestBuildStagePlan_LegacyKeepsHistoricalPath is the parity guard: arm-less
// scenarios must keep the exact stage/config.yaml -> /opt/bench/config.yaml
// path, byte-for-byte, regardless of the buildStagePlan refactor.
func TestBuildStagePlan_LegacyKeepsHistoricalPath(t *testing.T) {
	sets := []renderedPointConfigs{{Key: "1", Single: "/local/tmp/a"}}
	items, dl := buildStagePlan(sets, "b", true)
	require.Equal(t, []upload{{"stage/config.yaml", "/local/tmp/a"}}, items)
	require.Equal(t, []string{"aws s3 cp s3://b/stage/config.yaml /opt/bench/config.yaml"}, dl)
}
