// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/catalog"
)

func testEntry(t *testing.T, id, output string, size int) catalog.Entry {
	t.Helper()
	pipeline := map[string]any{
		"input":  map[string]any{"postgres_cdc": map[string]any{"schema": "public"}},
		"output": map[string]any{"processors": []any{map[string]any{"benchmark": map[string]any{}}}, output: map[string]any{}},
	}
	fp, err := catalog.Fingerprint(pipeline)
	require.NoError(t, err)
	return catalog.Entry{
		SchemaVersion:  catalog.SchemaVersion,
		ID:             id,
		Input:          "postgres_cdc",
		Processors:     []string{"benchmark"},
		Output:         output,
		Fingerprint:    fp,
		Workload:       catalog.Workload{MessageSizeBytes: size, Mode: "cdc"},
		ConnectVersion: "v4.50.0-3-gabc1234",
		BuildSHA:       "abc1234",
		Infra:          catalog.Infra{RunnerInstanceType: "c8g.4xlarge", SinkSize: "small"},
		SinkMeasured:   true,
		MemoryLimited:  true,
		RecordedAt:     time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		Points: []catalog.Point{
			{VCPU: 1, CUs: 10, CapacityMBps: 18, PeakRSSBytes: 1500000000},
			{VCPU: 2, CUs: 20, CapacityMBps: 35, PeakRSSBytes: 3000000000},
			{VCPU: 4, CUs: 40, CapacityMBps: 70, PeakRSSBytes: 6000000000},
			{VCPU: 8, CUs: 80, CapacityMBps: 120, PeakRSSBytes: 12000000000},
		},
	}
}

// seedStore writes entries into a fresh dir store and returns its --store arg.
func seedStore(t *testing.T, entries ...catalog.Entry) string {
	t.Helper()
	dir := t.TempDir()
	st := catalog.NewDirStore(dir)
	for _, e := range entries {
		require.NoError(t, st.Put(context.Background(), e))
	}
	return "dir:" + dir
}

func runCLI(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errb bytes.Buffer
	code = run(args, &out, &errb)
	return code, out.String(), errb.String()
}

type queryOut struct {
	Matches []struct {
		Kind        string            `json:"kind"`
		Score       float64           `json:"score"`
		Differences []string          `json:"differences"`
		Entry       catalog.Entry     `json:"entry"`
		Staleness   catalog.Staleness `json:"staleness"`
	} `json:"matches"`
}

func TestQueryJSON(t *testing.T) {
	store := seedStore(t, testEntry(t, "exact-1", "redpanda", 1200), testEntry(t, "other-1", "kafka", 1200))
	code, stdout, stderr := runCLI(t, "query", "--store", store, "--repo", t.TempDir(),
		"--input", "postgres_cdc", "--output", "redpanda", "--processors", "benchmark", "--message-size", "1200", "--mode", "cdc")
	require.Equal(t, 0, code, stderr)

	var got queryOut
	require.NoError(t, json.Unmarshal([]byte(stdout), &got), stdout)
	require.Len(t, got.Matches, 1)
	require.Equal(t, "exact", got.Matches[0].Kind)
	require.Equal(t, "exact-1", got.Matches[0].Entry.ID)
	// --repo is not a git repository: staleness degrades, the command does not fail.
	require.Equal(t, "unknown", got.Matches[0].Staleness.State)
	require.NotEmpty(t, got.Matches[0].Staleness.Reasons)
}

func TestQueryNoMatchesIsEmptyArray(t *testing.T) {
	code, stdout, stderr := runCLI(t, "query", "--store", seedStore(t), "--repo", t.TempDir(), "--input", "x", "--output", "y")
	require.Equal(t, 0, code, stderr)
	require.JSONEq(t, `{"matches":[]}`, stdout)
}

func TestQueryLimitsToFive(t *testing.T) {
	var es []catalog.Entry
	for _, id := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		es = append(es, testEntry(t, id, "redpanda", 1200))
	}
	code, stdout, stderr := runCLI(t, "query", "--store", seedStore(t, es...), "--repo", t.TempDir(), "--input", "postgres_cdc", "--output", "redpanda")
	require.Equal(t, 0, code, stderr)
	var got queryOut
	require.NoError(t, json.Unmarshal([]byte(stdout), &got))
	require.Len(t, got.Matches, 5)
}

func TestQueryWithConfig(t *testing.T) {
	store := seedStore(t, testEntry(t, "exact-1", "redpanda", 1200))
	cfg := filepath.Join(t.TempDir(), "cfg.yaml")
	// A different dsn must not change the fingerprint.
	require.NoError(t, os.WriteFile(cfg, []byte(`
input:
  postgres_cdc:
    dsn: postgres://other-host/db
    schema: public
output:
  processors:
    - benchmark: {}
  redpanda: {}
`), 0o644))
	code, stdout, stderr := runCLI(t, "query", "--store", store, "--repo", t.TempDir(), "--config", cfg, "--message-size", "1200")
	require.Equal(t, 0, code, stderr)
	var got queryOut
	require.NoError(t, json.Unmarshal([]byte(stdout), &got), stdout)
	require.Len(t, got.Matches, 1)
	require.Equal(t, "exact", got.Matches[0].Kind)
}

func TestRecommendJSON(t *testing.T) {
	store := seedStore(t, testEntry(t, "exact-1", "redpanda", 1200))
	code, stdout, stderr := runCLI(t, "recommend", "--store", store, "--repo", t.TempDir(), "--entry-id", "exact-1", "--target-mb-s", "40")
	require.Equal(t, 0, code, stderr)

	var got struct {
		EntryID        string                 `json:"entry_id"`
		ConnectVersion string                 `json:"connect_version"`
		BuildSHA       string                 `json:"build_sha"`
		InstanceType   string                 `json:"instance_type"`
		SinkSize       string                 `json:"sink_size"`
		Recommendation catalog.Recommendation `json:"recommendation"`
		Staleness      catalog.Staleness      `json:"staleness"`
		Points         []catalog.Point        `json:"points"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &got), stdout)
	require.Equal(t, "exact-1", got.EntryID)
	require.Equal(t, "v4.50.0-3-gabc1234", got.ConnectVersion)
	require.Equal(t, "abc1234", got.BuildSHA)
	require.Equal(t, "c8g.4xlarge", got.InstanceType)
	require.Equal(t, "small", got.SinkSize)
	require.Equal(t, 40, got.Recommendation.CUs)
	require.Len(t, got.Points, 4)
	require.Equal(t, "unknown", got.Staleness.State)
}

func TestRecommendSizingFile(t *testing.T) {
	store := seedStore(t, testEntry(t, "exact-1", "redpanda", 1200))
	sizing := filepath.Join(t.TempDir(), "sizing.json")
	require.NoError(t, os.WriteFile(sizing, []byte(`{"headroom":1.0,"cus_per_vcpu":10,"bytes_per_cu":400000000}`), 0o644))
	code, stdout, stderr := runCLI(t, "recommend", "--store", store, "--repo", t.TempDir(), "--entry-id", "exact-1", "--target-mb-s", "35", "--sizing", sizing)
	require.Equal(t, 0, code, stderr)
	var got struct {
		Recommendation catalog.Recommendation `json:"recommendation"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &got))
	require.Equal(t, 20, got.Recommendation.CUs)
}

func TestRecommendEntryNotFound(t *testing.T) {
	code, stdout, _ := runCLI(t, "recommend", "--store", seedStore(t), "--repo", t.TempDir(), "--entry-id", "nope", "--target-mb-s", "40")
	require.Equal(t, 1, code)
	require.JSONEq(t, `{"error":"entry not found"}`, stdout)
}

func TestBadFlagsExit2(t *testing.T) {
	for name, args := range map[string][]string{
		"no subcommand":       {},
		"unknown subcommand":  {"frobnicate"},
		"unknown flag":        {"query", "--nope"},
		"query no input":      {"query", "--store", "dir:/x"},
		"recommend no id":     {"recommend", "--target-mb-s", "1"},
		"recommend no target": {"recommend", "--entry-id", "x"},
		"import no roots":     {"import"},
		"bad store":           {"query", "--store", "ftp://x", "--input", "a", "--output", "b"},
	} {
		t.Run(name, func(t *testing.T) {
			code, _, _ := runCLI(t, args...)
			require.Equal(t, 2, code)
		})
	}
}
