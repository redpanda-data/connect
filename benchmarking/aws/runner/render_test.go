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

func sampleDualEngineResult() *Result {
	r := sampleResult()
	r.Points = []PointResult{
		{
			VCPU:   1,
			Engine: "connect",
			Summary: Summary{MedianMBPerSec: 100, P5MBPerSec: 90, P95MBPerSec: 110, PeakMBPerSec: 115,
				MedianMsgPerSec: 80000},
		},
		{
			VCPU:    1,
			Engine:  "kafka_connect",
			Summary: Summary{MedianMBPerSec: 72, P5MBPerSec: 65, P95MBPerSec: 78, PeakMBPerSec: 80},
			BrokerSeries: []TopicPoint{
				{T: 10, MBPerSec: 70}, {T: 20, MBPerSec: 72}, {T: 30, MBPerSec: 74},
			},
		},
	}
	return r
}

func TestAppendMarkdown_DualEngineWithDelta(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "postgres.md")
	require.NoError(t, os.WriteFile(target, []byte("# Postgres\n"), 0o644))

	r := sampleDualEngineResult()
	require.NoError(t, AppendMarkdown(target, r, "desc"))

	out, err := os.ReadFile(target)
	require.NoError(t, err)
	s := string(out)
	// Both engines appear:
	require.Contains(t, s, "connect")
	require.Contains(t, s, "kafka_connect")
	// Delta column header:
	require.Contains(t, s, "Δ vs Connect")
	// KC's delta: (72 - 100) / 100 = -28%
	require.Contains(t, s, "-28%", "expected KC -28%% delta vs Connect; full markdown:\n"+s)
	// Connect row's delta column is blank — no "+0%" / "-0%" anywhere.
	require.NotContains(t, s, "+0%", "connect row should not have a delta value")
}

func TestAppendMarkdown_PointOrderingIsRobust(t *testing.T) {
	// Build the same dual-engine result two ways: interleaved
	// [connect, kc] versus reverse-engine order [kc, connect] within
	// the same vCPU. The grouping logic should produce identical
	// markdown output.
	r1 := sampleDualEngineResult()
	r2 := sampleDualEngineResult()
	// Swap order in r2: [kc, connect] instead of [connect, kc].
	r2.Points = []PointResult{r2.Points[1], r2.Points[0]}

	dir := t.TempDir()
	target1 := filepath.Join(dir, "r1.md")
	target2 := filepath.Join(dir, "r2.md")
	require.NoError(t, os.WriteFile(target1, []byte{}, 0o644))
	require.NoError(t, os.WriteFile(target2, []byte{}, 0o644))
	require.NoError(t, AppendMarkdown(target1, r1, "desc"))
	require.NoError(t, AppendMarkdown(target2, r2, "desc"))

	b1, err := os.ReadFile(target1)
	require.NoError(t, err)
	b2, err := os.ReadFile(target2)
	require.NoError(t, err)
	require.Equal(t, string(b1), string(b2),
		"markdown output should be invariant to point ordering within a vCPU group")
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
	// New header columns:
	require.Contains(t, s, "engine")
	require.Contains(t, s, "broker MB/s")
	// The engine label appears in the row:
	require.Contains(t, s, "connect")
	// The vCPU row still renders the median:
	require.Contains(t, s, "153")
	// msg/sec (p50) — formatted with thousands separator:
	require.Contains(t, s, "127,344")
}
