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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/benchmarking/aws/plan"
)

func writePlan(t *testing.T, p plan.Plan) string {
	t.Helper()
	b, err := json.Marshal(p)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "plan.json")
	require.NoError(t, os.WriteFile(path, b, 0o600))
	return path
}

func runnablePlan(t *testing.T) plan.Plan {
	t.Helper()
	p, err := plan.Build(map[string]any{
		"input":  map[string]any{"http_server": map[string]any{"path": "/post"}},
		"output": map[string]any{"drop": map[string]any{}},
	}, plan.Workload{MessageSizeBytes: 500}, plan.SlackRef{}, time.Now())
	require.NoError(t, err)
	return p
}

func TestBenchplanRender(t *testing.T) {
	p := runnablePlan(t)
	out := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"render", "--plan", writePlan(t, p), "--out", out}, &stdout, &stderr)
	require.Equal(t, 0, code, stderr.String())
	want := filepath.Join(out, "bot-"+p.ID+".yaml")
	require.Equal(t, want+"\n", stdout.String())
	_, err := os.Stat(want)
	require.NoError(t, err)
}

func TestBenchplanShow(t *testing.T) {
	p := runnablePlan(t)
	var stdout, stderr bytes.Buffer
	require.Equal(t, 0, run([]string{"show", "--plan", writePlan(t, p)}, &stdout, &stderr), stderr.String())
	var got plan.Plan
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &got))
	require.Equal(t, p.ID, got.ID)
}

func TestBenchplanExitCodes(t *testing.T) {
	var so, se bytes.Buffer
	require.Equal(t, 2, run(nil, &so, &se))
	require.Equal(t, 2, run([]string{"bogus"}, &so, &se))
	require.Equal(t, 2, run([]string{"render", "--plan", "x"}, &so, &se), "missing --out")
	require.Equal(t, 2, run([]string{"show"}, &so, &se), "missing --plan")
	require.Equal(t, 1, run([]string{"show", "--plan", "/nonexistent/plan.json"}, &so, &se))
	require.Equal(t, 1, run([]string{"show", "--plan", "s3://bucket/plan.json"}, &so, &se), "s3 not supported yet")

	unrunnable := writePlan(t, plan.Plan{ID: "z", Reason: "no bench stack for x yet"})
	require.Equal(t, 1, run([]string{"render", "--plan", unrunnable, "--out", t.TempDir()}, &so, &se))
}
