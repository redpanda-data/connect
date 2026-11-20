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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const testBrokers = "10.42.1.10:9092,10.42.1.11:9092,10.42.1.12:9092"

func stackNoneScenario() *Scenario {
	return &Scenario{
		Name: "bot-x", Connector: "http_server", Direction: DirectionE2E, Stack: stackNone,
		Dataset:  DatasetSpec{RowSizeBytes: 1000},
		Workload: &WorkloadSpec{WriteRatePerSec: 450_000, Warmup: 2 * time.Minute, Duration: 15 * time.Minute},
		Pipeline: map[string]any{
			"input": map[string]any{"redpanda": map[string]any{
				"seed_brokers":   []any{"${BENCH_REDPANDA_BROKERS}"},
				"topics":         []any{"${BENCH_TOPIC}"},
				"consumer_group": "${BENCH_CONSUMER_GROUP}",
			}},
			"output": map[string]any{"drop": map[string]any{}},
		},
		Matrix: MatrixSpec{CPUPoints: []int{1, 2, 4, 8}},
	}
}

// producerConfig extracts the Connect config the producer script writes
// through its quoted heredoc.
func producerConfig(t *testing.T, script string) map[string]any {
	t.Helper()
	start := strings.Index(script, "<<'"+producerHeredoc+"'\n")
	require.GreaterOrEqual(t, start, 0, "script must write the config through a quoted heredoc")
	body := script[start+len("<<'"+producerHeredoc+"'\n"):]
	end := strings.Index(body, "\n"+producerHeredoc+"\n")
	require.GreaterOrEqual(t, end, 0, "heredoc must be terminated")
	var cfg map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(body[:end]), &cfg))
	return cfg
}

func TestRenderProducerScript(t *testing.T) {
	script := renderProducerScript(ProducerArgs{
		Brokers: testBrokers, Topic: "bench_s1_src", RatePerSec: 450_000, RowSizeBytes: 1000, TotalSec: 1020,
	})
	require.NotContains(t, script, "localhost")
	require.NotContains(t, script, "127.0.0.1")
	require.Contains(t, script, "timeout --signal=TERM 1020s")
	require.Contains(t, script, "taskset -c 10-15")

	cfg := producerConfig(t, script)
	gen := cfg["input"].(map[string]any)["generate"].(map[string]any)
	require.Empty(t, gen["interval"], "interval must be empty: the rate limit, not a timer, sets the pace")
	require.Equal(t, 0, gen["count"], "unlimited count")

	rl := cfg["rate_limit_resources"].([]any)[0].(map[string]any)
	local := rl["local"].(map[string]any)
	require.Equal(t, 45_000, local["count"], "ceil(450000/10) per 100ms")
	require.Equal(t, "100ms", local["interval"])
	procs := cfg["pipeline"].(map[string]any)["processors"].([]any)
	require.Equal(t, map[string]any{"rate_limit": map[string]any{"resource": rl["label"]}}, procs[0])

	out := cfg["output"].(map[string]any)["redpanda"].(map[string]any)
	require.Equal(t, []any{testBrokers}, out["seed_brokers"])
	require.Equal(t, "bench_s1_src", out["topic"])
	require.Equal(t, 64, out["max_in_flight"])
	require.Contains(t, out, "batching")

	// The mapping renders a JSON object of exactly row_size_bytes: a fixed
	// 28-byte frame around a pad literal.
	mapping := gen["mapping"].(string)
	require.Contains(t, mapping, `"%12d".format(counter())`)
	require.Contains(t, mapping, strings.Repeat("x", 1000-producerFrameBytes))
	require.NotContains(t, mapping, strings.Repeat("x", 1000-producerFrameBytes+1))
	require.Equal(t, 28, producerFrameBytes)

	// Port 4195 belongs to the Connect under test on the same host.
	require.Equal(t, map[string]any{"enabled": false}, cfg["http"])
}

func TestRenderProducerScriptRateCeil(t *testing.T) {
	cfg := producerConfig(t, renderProducerScript(ProducerArgs{
		Brokers: testBrokers, Topic: "t", RatePerSec: 450_001, RowSizeBytes: 10, TotalSec: 60,
	}))
	local := cfg["rate_limit_resources"].([]any)[0].(map[string]any)["local"].(map[string]any)
	require.Equal(t, 45_001, local["count"])
	// A row smaller than the frame gets no padding rather than a negative repeat.
	mapping := cfg["input"].(map[string]any)["generate"].(map[string]any)["mapping"].(string)
	require.NotContains(t, mapping, "x\"")
}

func TestE2EStackNoneWorkloadScriptIsProducer(t *testing.T) {
	s := stackNoneScenario()
	names := newBenchNames("bench-20261120-150405", s.Connector)
	outs := map[string]string{"redpanda_broker_endpoints": testBrokers}
	script, err := e2eTopology{}.WorkloadScript(s, outs, names)
	require.NoError(t, err)
	cfg := producerConfig(t, script)
	out := cfg["output"].(map[string]any)["redpanda"].(map[string]any)
	require.Equal(t, "bench_bench_20261120_150405_src", out["topic"])
	require.Equal(t, []any{testBrokers}, out["seed_brokers"])
	// Same total as renderWorkloadScript: warmup + duration.
	require.Contains(t, script, "timeout --signal=TERM 1020s")

	_, err = e2eTopology{}.WorkloadScript(s, map[string]string{}, names)
	require.ErrorContains(t, err, "redpanda_broker_endpoints")
}

func TestE2EStackNoneSeedCreatesSourceTopic(t *testing.T) {
	s := stackNoneScenario()
	names := newBenchNames("bench-20261120-150405", s.Connector)
	script, err := e2eTopology{}.SeedScript(s, map[string]string{"redpanda_broker_endpoints": testBrokers}, names)
	require.NoError(t, err)
	require.Contains(t, script, "set -euo pipefail")
	require.Contains(t, script, "rpk topic create")
	require.Contains(t, script, "bench_bench_20261120_150405_src")
	require.Contains(t, script, "-p 12")
	require.Contains(t, script, "-r 3")
	require.Contains(t, script, "retention.bytes=4294967296")
	require.Contains(t, script, "retention.ms=1800000")
	require.Contains(t, script, `-X brokers="`+testBrokers+`"`)
	require.NotContains(t, script, "localhost")

	_, err = e2eTopology{}.SeedScript(s, map[string]string{}, names)
	require.ErrorContains(t, err, "redpanda_broker_endpoints")
}

func TestE2EStackNoneResetEmpty(t *testing.T) {
	s := stackNoneScenario()
	got, err := e2eTopology{}.ResetScript(s, map[string]string{"redpanda_broker_endpoints": testBrokers}, BenchNames{})
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestStackNonePerPointConsumerGroups(t *testing.T) {
	s := stackNoneScenario()
	names := newBenchNames("bench-20261120-150405", s.Connector)
	outs := map[string]string{"redpanda_broker_endpoints": testBrokers, "bench_session_id": names.SessionID}
	require.True(t, perPointConfigs(s), "stack none must render one config per point")

	groups := map[string]bool{}
	for _, p := range buildSweepPlan(s) {
		set, err := renderPointConfigs(s, outs, e2eTopology{}, names, p)
		require.NoError(t, err)
		raw, err := os.ReadFile(set.Single)
		require.NoError(t, err)
		require.NotContains(t, string(raw), "${BENCH_")
		cfg := readYAML(t, set.Single)
		in := cfg["input"].(map[string]any)["redpanda"].(map[string]any)
		require.Equal(t, []any{testBrokers}, in["seed_brokers"])
		require.Equal(t, []any{"bench_bench_20261120_150405_src"}, in["topics"])
		g := in["consumer_group"].(string)
		require.Equal(t, "bench_bench_20261120_150405_p"+p.Key(), g)
		groups[g] = true
	}
	require.Len(t, groups, 4, "every point starts from a fresh consumer group")

	// The scenario itself still carries the placeholders: rendering is per point.
	in := s.Pipeline["input"].(map[string]any)["redpanda"].(map[string]any)
	require.Equal(t, "${BENCH_CONSUMER_GROUP}", in["consumer_group"])
}

func TestPerPointConfigsOnlyForStackNoneOrArms(t *testing.T) {
	s := stackNoneScenario()
	s.Stack = "postgres"
	require.False(t, perPointConfigs(s))
	s.Matrix.Arms = []Arm{{ID: "a"}}
	require.True(t, perPointConfigs(s))
}

func TestProducerBoundAnomaly(t *testing.T) {
	const rate = 450_000.0
	a, ok := producerBoundAnomaly(Summary{MedianMsgPerSec: 0.96 * rate}, rate)
	require.True(t, ok)
	require.True(t, strings.HasPrefix(a.Note, "producer_bound"), a.Note)

	_, ok = producerBoundAnomaly(Summary{MedianMsgPerSec: 0.90 * rate}, rate)
	require.False(t, ok)
	_, ok = producerBoundAnomaly(Summary{MedianMsgPerSec: 0.96 * rate}, 0)
	require.False(t, ok, "no producer rate, no verdict")
}

func TestMatrixAddsProducerBoundAnomaly(t *testing.T) {
	const sessionID = "bench-e2e-pb"
	run := func(rate float64) []Anomaly {
		prev := stdout
		stdout = &strings.Builder{}
		defer func() { stdout = prev }()
		mr := &MatrixRunner{
			SSM: &FakeSSM{Transcripts: map[string][]string{"i-runner": {"bench point complete"}}},
			LogFetcher: &FakeLogFetcher{Contents: map[string]string{
				"runs/" + sessionID + "/sweep-1.log": makeLog(180, 7),
			}},
			RunnerInstance: "i-runner", Bucket: "b", SessionID: sessionID,
			Topology: e2eTopology{}, Names: newBenchNames(sessionID, "http_server"),
			ProducerRatePerSec: rate,
		}
		pts, err := mr.Run(t.Context(), []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}}, 1, 60*time.Second, 120*time.Second, "", "")
		require.NoError(t, err)
		return pts[0].Anomalies
	}
	// makeLog logs a flat 1000 msg/s, so that is the measured median.
	const median = 1000.0
	got := run(median / 0.96)
	require.Len(t, got, 1)
	require.True(t, strings.HasPrefix(got[0].Note, "producer_bound"))
	require.Empty(t, run(median/0.90))
}

func TestWorkloadInstanceStackNoneIsRunner(t *testing.T) {
	outs := map[string]string{"runner_instance_id": "i-runner", "load_gen_instance_id": "i-load"}
	s := stackNoneScenario()
	require.Equal(t, "i-runner", workloadInstance(s, outs))
	s.Stack = "postgres"
	require.Equal(t, "i-load", workloadInstance(s, outs))
}

func TestStackTerraformSkippedForStackNone(t *testing.T) {
	s := stackNoneScenario()
	tf := stackTerraform(s, "/repo", "/repo/backend.hcl")
	require.IsType(t, noStackTerraform{}, tf)
	require.NoError(t, tf.Init())
	require.NoError(t, tf.Apply(map[string]string{"x": "y"}))
	require.NoError(t, tf.Destroy(map[string]string{"x": "y"}))
	outs, err := tf.Outputs()
	require.NoError(t, err)
	require.Empty(t, outs)

	s.Stack = "postgres"
	real, ok := stackTerraform(s, "/repo", "/repo/backend.hcl").(*Terraform)
	require.True(t, ok)
	require.Equal(t, StackDir("/repo", "postgres"), real.Dir)
	require.Equal(t, "postgres", real.StateKey)
	require.Equal(t, "/repo/backend.hcl", real.BackendFile)
}

func TestValidateBinaryFlagsRejectsStackNone(t *testing.T) {
	s := stackNoneScenario()
	s.Matrix.Arms = []Arm{{ID: "a", Binary: "base"}}
	require.ErrorContains(t, validateBinaryFlags(s, map[string]string{"base": "/tmp/x"}), "does not support --binary")
	s.Matrix.Arms = nil
	require.NoError(t, validateBinaryFlags(s, nil))
}

// TestWriteProducerFixture writes the producer config for a manual
// `redpanda-connect lint` when BENCH_PRODUCER_FIXTURE names a path.
func TestWriteProducerFixture(t *testing.T) {
	path := os.Getenv("BENCH_PRODUCER_FIXTURE")
	if path == "" {
		t.Skip("BENCH_PRODUCER_FIXTURE not set")
	}
	script := renderProducerScript(ProducerArgs{Brokers: testBrokers, Topic: "t", RatePerSec: 450_000, RowSizeBytes: 1000, TotalSec: 60})
	cfg := producerConfig(t, script)
	raw, err := yaml.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o644))
}
