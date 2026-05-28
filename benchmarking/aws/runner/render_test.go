// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func sampleResult() *Result {
	return &Result{
		Scenario:     "postgres/orders-cdc",
		ScenarioHash: "sha256:abc123",
		GitSHA:       "e491c80fc",
		StartedAt:    time.Date(2026, 5, 19, 14, 2, 11, 0, time.UTC),
		FinishedAt:   time.Date(2026, 5, 19, 15, 33, 48, 0, time.UTC),
		Infra: ResultInfra{
			RunnerInstanceType:  "c7i.4xlarge",
			SourceInstanceClass: "db.r6g.2xlarge",
			SourceStorageGB:     400,
			Region:              "us-east-2",
		},
		Dataset: ResultDataset{Rows: 75_000_000, RowSizeBytes: 1200, TotalBytes: 90_000_000_000},
		Points: []PointResult{
			{
				VCPU:    1,
				Engine:  "connect",
				Samples: []Sample{{T: 0, MBPerSec: 153, MsgPerSec: 127344}},
				Summary: Summary{MedianMBPerSec: 153, P5MBPerSec: 144, P95MBPerSec: 161, PeakMBPerSec: 167,
					MedianMsgPerSec: 127344, P5MsgPerSec: 119800, P95MsgPerSec: 134000, PeakMsgPerSec: 138200},
			},
		},
	}
}

func TestWriteResultJSON(t *testing.T) {
	dir := t.TempDir()
	r := sampleResult()
	path, err := WriteResultJSON(dir, r)
	require.NoError(t, err)
	require.True(t, filepath.IsAbs(path) || filepath.Dir(path) != ".")

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var got Result
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Equal(t, r.Scenario, got.Scenario)
	require.Equal(t, r.Points[0].Summary.MedianMBPerSec, got.Points[0].Summary.MedianMBPerSec)
}

func TestAppendMarkdown(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "postgres.md")
	require.NoError(t, os.WriteFile(target, []byte("# Postgres existing content\n"), 0o644))

	r := sampleResult()
	require.NoError(t, AppendMarkdown(target, r, "Stream changes from a 75M-row orders table at 5K writes/sec sustained."))

	out, err := os.ReadFile(target)
	require.NoError(t, err)
	s := string(out)
	require.Contains(t, s, "# Postgres existing content")
	require.Contains(t, s, "## AWS — orders-cdc — 2026-05-19")
	require.Contains(t, s, "c7i.4xlarge")
	require.Contains(t, s, "db.r6g.2xlarge")
	require.Contains(t, s, "| 1          |          153 |         144 |          161 |       127,344 |")
}
