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
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/catalog"
)

func catalogFixture(t *testing.T) (*Scenario, *Result) {
	t.Helper()
	s, err := LoadScenario("testdata/valid-orders-cdc.yaml")
	require.NoError(t, err)
	r := &Result{
		Scenario:       "postgres/orders-cdc",
		GitSHA:         "abc123",
		ConnectVersion: "v4.50.0-3-gabc123",
		Infra:          ResultInfra{RunnerInstanceType: "c7i.4xlarge", Region: "us-east-2"},
		Dataset:        ResultDataset{RowSizeBytes: 1200},
	}
	for i, v := range []int{1, 2, 4, 8} {
		r.Points = append(r.Points, PointResult{
			VCPU:    v,
			Engine:  "connect",
			Summary: Summary{MedianMBPerSec: float64(10 * (i + 1)), MedianMsgPerSec: float64(100 * (i + 1))},
			Prom:    []PromPoint{{RSSBytes: 100}, {RSSBytes: uint64(1000 * (i + 1))}, {RSSBytes: 500}},
		})
	}
	return s, r
}

func TestCatalogEntryFromResult(t *testing.T) {
	s, r := catalogFixture(t)
	meta := catalogMeta{SessionID: "bench-1", RunURL: "https://x/run/1", Now: time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)}
	e, err := catalogEntryFromResult(s, r, meta)
	require.NoError(t, err)
	require.NoError(t, e.Validate())
	require.Equal(t, "bench-1", e.ID)
	require.Equal(t, "postgres/orders-cdc", e.Scenario)
	require.Equal(t, "postgres_cdc", e.Input)
	require.Equal(t, "redpanda", e.Output)
	// The harness's benchmark processor and topic are not identity.
	require.Empty(t, e.Processors)
	wantFP, err := catalog.Fingerprint(map[string]any{
		"input":  s.Pipeline["input"],
		"output": map[string]any{"redpanda": map[string]any{"seed_brokers": []any{"b"}, "topic": "customer"}},
	})
	require.NoError(t, err)
	require.Equal(t, wantFP, e.Fingerprint)
	require.Equal(t, "v4.50.0-3-gabc123", e.ConnectVersion)
	require.Nil(t, e.AWSCostUSD)
	require.Equal(t, "cdc", e.Workload.Mode)
	require.Equal(t, 1200, e.Workload.MessageSizeBytes)
	require.Equal(t, 5000, e.Workload.OfferedRatePerSec)
	require.True(t, e.SinkMeasured)
	require.False(t, e.MemoryLimited)
	require.Len(t, e.Points, 4)
	wantCUs := []int{10, 20, 40, 80}
	for i, p := range e.Points {
		require.Equal(t, wantCUs[i], p.CUs)
		require.Equal(t, float64(10*(i+1)), p.CapacityMBps)
		require.Equal(t, float64(100*(i+1)), p.CapacityMsgps)
		require.Equal(t, uint64(1000*(i+1)), p.PeakRSSBytes)
	}
	require.Equal(t, "https://x/run/1", e.RunURL)
	require.Empty(t, e.ResultKey)
}

func TestCatalogEntryMemoryLimited(t *testing.T) {
	s, r := catalogFixture(t)
	s.Infra.MemoryPerVCPUBytes = 4_000_000_000
	e, err := catalogEntryFromResult(s, r, catalogMeta{SessionID: "bench-1"})
	require.NoError(t, err)
	require.True(t, e.MemoryLimited)
}

func TestCatalogEntryUnknownSHA(t *testing.T) {
	s, r := catalogFixture(t)
	r.GitSHA = "unknown"
	_, err := catalogEntryFromResult(s, r, catalogMeta{SessionID: "bench-1"})
	require.ErrorContains(t, err, "build sha unknown")
}

func TestCatalogEntryUnknownConnectVersion(t *testing.T) {
	s, r := catalogFixture(t)
	r.ConnectVersion = "unknown"
	_, err := catalogEntryFromResult(s, r, catalogMeta{SessionID: "bench-1"})
	require.ErrorContains(t, err, "connect version unknown")
}

func TestCatalogueSweepSkipsDirtyTree(t *testing.T) {
	prev := treeDirty
	defer func() { treeDirty = prev }()
	treeDirty = func(string) (bool, error) { return true, nil }

	s, r := catalogFixture(t)
	var warn bytes.Buffer
	// Returns before any AWS call: the bucket is never reached.
	require.NoError(t, catalogueSweep(t.Context(), benchOpts{repoRoot: t.TempDir()}, s, r, "bench-1", &warn))
	require.Equal(t, "warning: catalogue entry skipped: working tree dirty\n", warn.String())
}

func TestTreeDirty(t *testing.T) {
	dir := t.TempDir()
	git := func(args ...string) {
		t.Helper()
		out, err := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false"}, args...)...).CombinedOutput()
		require.NoError(t, err, string(out))
	}
	git("init", "-q")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o644))
	git("add", "f")
	git("commit", "-q", "-m", "init")

	dirty, err := treeDirty(dir)
	require.NoError(t, err)
	require.False(t, dirty)

	// Untracked files do not change the build.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "untracked"), []byte("x"), 0o644))
	dirty, err = treeDirty(dir)
	require.NoError(t, err)
	require.False(t, dirty)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "f"), []byte("y"), 0o644))
	dirty, err = treeDirty(dir)
	require.NoError(t, err)
	require.True(t, dirty)

	_, err = treeDirty(t.TempDir())
	require.Error(t, err)
}

func TestShouldCatalogue(t *testing.T) {
	s, r := catalogFixture(t)
	require.True(t, shouldCatalogue(s, r))

	s.Soak = true
	require.False(t, shouldCatalogue(s, r))
	s.Soak = false

	r.Points[1].Arm = "a"
	require.False(t, shouldCatalogue(s, r))
	r.Points[1].Arm = ""

	r.Points[2].Binary = "base"
	require.False(t, shouldCatalogue(s, r))
}

func TestRunURLFromEnv(t *testing.T) {
	t.Setenv("GITHUB_SERVER_URL", "https://github.com")
	t.Setenv("GITHUB_REPOSITORY", "o/r")
	t.Setenv("GITHUB_RUN_ID", "42")
	require.Equal(t, "https://github.com/o/r/actions/runs/42", runURLFromEnv())
	t.Setenv("GITHUB_RUN_ID", "")
	require.Empty(t, runURLFromEnv())
}

func TestCatalogEntryLimitedByProducer(t *testing.T) {
	s, r := catalogFixture(t)
	r.Points[3].Anomalies = []Anomaly{{Kind: anomalyProducerBound, Note: "producer_bound: x"}}
	r.Points[2].Anomalies = []Anomaly{{StartT: 1, DurationSec: 90, MinRatio: 0.5}}
	e, err := catalogEntryFromResult(s, r, catalogMeta{SessionID: "bench-1"})
	require.NoError(t, err)
	for i, want := range []string{"", "", "", "producer"} {
		require.Equal(t, want, e.Points[i].LimitedBy, "point %d", i)
	}
}
