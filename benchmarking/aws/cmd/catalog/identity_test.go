// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/catalog"
)

// TestBenchIdentityMatchesCustomerConfig proves the runner's entry, the
// importer's entry and a plain customer config share one identity, so a
// customer asking about postgres_cdc -> redpanda gets an exact match. The
// runner is package main and cannot be imported; its entry is built with the
// same RunInput construction as runner/catalogentry.go.
func TestBenchIdentityMatchesCustomerConfig(t *testing.T) {
	scenRaw, err := os.ReadFile(filepath.Join("testdata", "scenarios", "postgres", "orders-cdc.yaml"))
	require.NoError(t, err)
	var scen scenarioFile
	require.NoError(t, yaml.Unmarshal(scenRaw, &scen))

	runner, err := catalog.EntryFromRun(catalog.RunInput{
		ID: "bench-1", Scenario: "postgres/orders-cdc", GitSHA: "abcdef1", ConnectVersion: "v4.1.0",
		InstanceType: "c8g.4xlarge", Pipeline: catalog.BenchSourcePipeline(scen.Pipeline),
		MessageSizeBytes: 1200, SinkMeasured: true, Now: time.Now(),
		Points: []catalog.RunPoint{{VCPU: 1, MedianMBps: 18}},
	})
	require.NoError(t, err)

	dir := t.TempDir()
	f := filepath.Join(dir, "postgres", "orders-cdc", "2026-06-01T10-00-00Z.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(f), 0o755))
	b, err := os.ReadFile(filepath.Join("testdata", "results", "postgres", "orders-cdc", "2026-06-01T10-00-00Z.json"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(f, []byte(strings.ReplaceAll(string(b), "@SHA@", "abcdef1")), 0o644))
	imported, reason := entryFromResultFile(f, dir,
		func(_, _ string) ([]byte, error) { return scenRaw, nil },
		func(string) (string, error) { return "v4.1.0", nil })
	require.Empty(t, reason)

	customer := map[string]any{
		"input": map[string]any{"postgres_cdc": map[string]any{"dsn": "x", "tables": []any{"t"}}},
		"output": map[string]any{"redpanda": map[string]any{
			"seed_brokers": []any{"b"}, "topic": "t",
		}},
	}
	in, procs, out, err := catalog.Components(customer)
	require.NoError(t, err)
	fp, err := catalog.Fingerprint(customer)
	require.NoError(t, err)

	for name, e := range map[string]catalog.Entry{"runner": runner, "importer": imported} {
		require.Equal(t, in, e.Input, name)
		require.Equal(t, out, e.Output, name)
		require.Equal(t, procs, e.Processors, name)
		require.Equal(t, fp, e.Fingerprint, name)
	}

	q := catalog.Query{Input: in, Output: out, Processors: procs, Fingerprint: fp, MessageSizeBytes: 1200, Mode: catalog.ModeFor(in)}
	matches := catalog.FindMatches([]catalog.Entry{runner, imported}, q)
	require.Len(t, matches, 2)
	for _, m := range matches {
		require.Equal(t, catalog.MatchExact, m.Kind, m.Entry.ID)
	}
}
