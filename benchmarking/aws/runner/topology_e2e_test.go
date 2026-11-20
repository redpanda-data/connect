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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func e2eScenario(output map[string]any) *Scenario {
	pl := map[string]any{
		"input":  map[string]any{"generate": map[string]any{"count": 1}},
		"output": output,
	}
	return &Scenario{
		Connector: "http_server", Direction: DirectionE2E, Stack: stackNone,
		Pipeline: pl,
	}
}

var benchProc = map[string]any{"benchmark": map[string]any{"interval": "1s", "count_bytes": true}}

func TestE2EPipelineAppendsBenchmarkLast(t *testing.T) {
	s := e2eScenario(map[string]any{
		"drop":       map[string]any{},
		"processors": []any{map[string]any{"mapping": "x"}},
	})
	in, out, err := e2eTopology{}.Pipeline(s, BenchNames{})
	require.NoError(t, err)
	require.Equal(t, s.Pipeline["input"], in)
	require.Equal(t, []any{map[string]any{"mapping": "x"}, benchProc}, out["processors"])
	require.Contains(t, out, "drop")

	// Safe to call per point: the scenario is untouched and results are stable.
	require.Len(t, s.Pipeline["output"].(map[string]any)["processors"], 1)
	_, out2, err := e2eTopology{}.Pipeline(s, BenchNames{})
	require.NoError(t, err)
	require.Len(t, out2["processors"], 2)
}

func TestE2EPipelineCreatesProcessorsList(t *testing.T) {
	s := e2eScenario(map[string]any{"drop": map[string]any{}})
	_, out, err := e2eTopology{}.Pipeline(s, BenchNames{})
	require.NoError(t, err)
	require.Equal(t, []any{benchProc}, out["processors"])
	require.NotContains(t, s.Pipeline["output"], "processors")
}

func TestE2ERenderedConfigCarriesPipelineSection(t *testing.T) {
	s := e2eScenario(map[string]any{"drop": map[string]any{}})
	s.Pipeline["pipeline"] = map[string]any{"processors": []any{map[string]any{"mapping": "root = this"}}}
	path, err := renderPipelineConfig(s, map[string]string{}, e2eTopology{}, BenchNames{})
	require.NoError(t, err)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var cfg map[string]any
	require.NoError(t, yaml.Unmarshal(raw, &cfg))
	require.Equal(t, map[string]any{"processors": []any{map[string]any{"mapping": "root = this"}}}, cfg["pipeline"])
	require.Equal(t, []any{map[string]any{"benchmark": map[string]any{"interval": "1s", "count_bytes": true}}},
		cfg["output"].(map[string]any)["processors"])
	require.Contains(t, cfg, "input")
}

func TestE2EStackNoneScripts(t *testing.T) {
	// Seed and workload scripts for stack none are covered in producer_test.go.
	s := e2eScenario(map[string]any{"drop": map[string]any{}})
	got, err := e2eTopology{}.ResetScript(s, nil, BenchNames{})
	require.NoError(t, err)
	require.Empty(t, got)
	pts, err := e2eTopology{}.EngineSeries(MetricInputs{})
	require.NoError(t, err)
	require.Nil(t, pts)
	require.Equal(t, MetricSidecar{}, e2eTopology{}.MetricSidecar(MetricSidecarArgs{Outs: map[string]string{"redpanda_metrics_endpoint": "x"}}))
}

func TestSourceTopologyUnchangedSummarySource(t *testing.T) {
	require.False(t, sourceTopology{}.SummaryFromSamples())
	require.True(t, e2eTopology{}.SummaryFromSamples())
}

func TestMatrixSummaryFromSamplesForE2E(t *testing.T) {
	const sessionID = "bench-e2e"
	fetcher := &FakeLogFetcher{Contents: map[string]string{
		fmt.Sprintf("runs/%s/sweep-1.log", sessionID): makeLog(180, 7),
	}}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": {"bench point complete"}}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM: ssm, LogFetcher: fetcher, RunnerInstance: "i-runner", Bucket: "b",
		SessionID: sessionID, Topology: e2eTopology{}, Names: newBenchNames(sessionID, "http_server"),
	}
	points, err := mr.Run(t.Context(), []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}}, 1, 60*time.Second, 120*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 1)
	p := points[0]
	require.Len(t, p.Samples, 120)
	require.Equal(t, Summarise(p.Samples), p.Summary)
	require.InDelta(t, 7, p.Summary.MedianMBPerSec, 1e-9)
	require.Empty(t, p.BrokerSeries)
	require.Empty(t, p.Backlog)
}

func TestE2EMetricArtifactEmpty(t *testing.T) {
	require.Empty(t, e2eTopology{}.MetricArtifact("2"))
	plan := buildSoakArchivePlan("sess", "scn", "2", e2eTopology{}.MetricArtifact("2"))
	for _, k := range plan.RawKeys {
		require.NotContains(t, k, "redpanda-")
	}
}

func TestMatrixE2ESoakGuards(t *testing.T) {
	const sessionID = "bench-e2e-soak"
	prom := "###timestamp=1000\nprocess_resident_memory_bytes 2e+08\n###timestamp=1010\nprocess_resident_memory_bytes 2.1e+08\n"
	run := func(withProm bool) error {
		contents := map[string]string{fmt.Sprintf("runs/%s/sweep-1.log", sessionID): makeLog(180, 7)}
		if withProm {
			contents[fmt.Sprintf("runs/%s/prom-1.txt", sessionID)] = prom
		}
		prev := stdout
		stdout = &bytes.Buffer{}
		defer func() { stdout = prev }()
		mr := &MatrixRunner{
			SSM:        &FakeSSM{Transcripts: map[string][]string{"i-runner": {"bench point complete"}}},
			LogFetcher: &FakeLogFetcher{Contents: contents}, RunnerInstance: "i-runner", Bucket: "b",
			SessionID: sessionID, Topology: e2eTopology{}, Names: newBenchNames(sessionID, "http_server"),
			ExpectedRecordsPerSec: 1000,
		}
		pts, err := mr.Run(t.Context(), []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}}, 1, 60*time.Second, 120*time.Second, "", "")
		if err == nil {
			require.InDelta(t, 7, pts[0].Summary.MedianMBPerSec, 1e-9)
			require.Empty(t, pts[0].BrokerSeries)
		}
		return err
	}
	require.NoError(t, run(true), "missing broker artifact must not fail an e2e soak")
	err := run(false)
	require.ErrorContains(t, err, "prom artifact missing or empty")
}
