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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/plan"
)

// snowflakePlan has customer logic in its pipeline that a public CI log
// must never show.
func snowflakePlan(t *testing.T) plan.Plan {
	t.Helper()
	p, err := plan.Build(map[string]any{
		"input": map[string]any{"postgres_cdc": map[string]any{"dsn": "postgres://u:p@h/db", "tables": []any{"t"}}},
		"pipeline": map[string]any{"processors": []any{
			map[string]any{"mapping": `root.customer_secret_logic = this.ssn.hash("sha256")`},
		}},
		"output": map[string]any{"snowflake_streaming": map[string]any{"table": "T"}},
	}, plan.Workload{MessageSizeBytes: 1024, TargetMBps: 10}, plan.SlackRef{Channel: "C1", ThreadTS: "1.2"}, time.Now())
	require.NoError(t, err)
	require.True(t, p.Runnable)
	return p
}

func TestSummaryPrintsNoPipeline(t *testing.T) {
	p := snowflakePlan(t)
	var so, se bytes.Buffer
	require.Equal(t, 0, run([]string{"summary", "--plan", writePlan(t, p)}, &so, &se), se.String())
	require.NotContains(t, so.String(), "customer_secret_logic")
	require.NotContains(t, so.String(), "pipeline")
	require.NotContains(t, so.String(), "C1", "no Slack thread either")

	var got map[string]any
	require.NoError(t, json.Unmarshal(so.Bytes(), &got))
	keys := make([]string, 0, len(got))
	for k := range got {
		keys = append(keys, k)
	}
	require.ElementsMatch(t, []string{"plan_id", "input", "processors", "output", "caveats", "sink", "sink_size", "estimate"}, keys)
	require.Equal(t, p.ID, got["plan_id"])
	require.Equal(t, "postgres_cdc", got["input"])
	require.Equal(t, []any{"mapping"}, got["processors"])
	require.Equal(t, "snowflake_streaming", got["output"])
	require.Equal(t, "snowflake", got["sink"])
	require.Equal(t, "XSMALL", got["sink_size"])
	require.NotNil(t, got["estimate"])
}

func TestRenderWritesLintableConnectConfig(t *testing.T) {
	p := snowflakePlan(t)
	out := t.TempDir()
	var so, se bytes.Buffer
	require.Equal(t, 0, run([]string{"render", "--plan", writePlan(t, p), "--out", out}, &so, &se), se.String())
	require.Equal(t, filepath.Join(out, "bot-"+p.ID+".yaml")+"\n", so.String(), "stdout is the scenario path only")

	raw, err := os.ReadFile(filepath.Join(out, "connect-"+p.ID+".yaml"))
	require.NoError(t, err)
	require.NotContains(t, string(raw), "${BENCH_")
	require.NotContains(t, string(raw), "${POSTGRES_DSN}")
	var cfg map[string]any
	require.NoError(t, yaml.Unmarshal(raw, &cfg))
	require.Contains(t, cfg, "input")
	require.Contains(t, cfg, "output")
	require.True(t, strings.Contains(string(raw), "customer_secret_logic"), "the pipeline logic is linted as written")
}

func TestClaim(t *testing.T) {
	root := t.TempDir()
	seedStore(t, root, "0123456789ab", -1)
	useFakes(t, root, &fakeEC2{})
	loc := "s3://b/plans/0123456789ab.json"

	var so, se bytes.Buffer
	require.Equal(t, 0, run([]string{"claim", "--plan", loc, "--approver", "prakhar"}, &so, &se), se.String())
	var got struct {
		PlanID   string `json:"plan_id"`
		Won      bool   `json:"won"`
		Approver string `json:"approver"`
	}
	require.NoError(t, json.Unmarshal(so.Bytes(), &got))
	require.Equal(t, "0123456789ab", got.PlanID)
	require.True(t, got.Won)

	st, code, _ := status(t, "--plan", loc)
	require.Equal(t, 0, code)
	require.True(t, st.Claimed)
	require.Equal(t, "prakhar", st.Approver)

	so.Reset()
	require.Equal(t, 0, run([]string{"claim", "--plan", loc, "--approver", "someone-else"}, &so, &se), se.String())
	require.NoError(t, json.Unmarshal(so.Bytes(), &got))
	require.False(t, got.Won, "the second claim loses")
	require.Equal(t, "prakhar", got.Approver, "the winner stays the approver")
}

func TestClaimErrors(t *testing.T) {
	root := t.TempDir()
	useFakes(t, root, &fakeEC2{})
	var so, se bytes.Buffer
	require.Equal(t, 2, run([]string{"claim", "--plan", "s3://b/plans/0123456789ab.json"}, &so, &se), "--approver required")
	require.Equal(t, 2, run([]string{"claim", "--approver", "x"}, &so, &se), "--plan required")
	require.Equal(t, 1, run([]string{"claim", "--plan", "s3://b/plans/0123456789ab.json", "--approver", "x"}, &so, &se), "no such plan")
}
