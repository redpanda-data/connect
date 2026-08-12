// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// iceberg7TableScenario loads the 7-topic streams7-vs-fanin fixture, mirroring
// icebergArmScenario (configrender_test.go) for the 2-stream arms fixture.
func iceberg7TableScenario(t *testing.T) (*Scenario, map[string]string, Topology) {
	t.Helper()
	s, err := LoadScenario("testdata/valid-iceberg-7table.yaml")
	require.NoError(t, err)
	outs := map[string]string{
		"redpanda_broker_endpoints": "b1:9092",
		"glue_rest_uri":             "https://glue.example",
		"warehouse_account_id":      "1234",
		"aws_region":                "us-east-2",
		"s3_bucket":                 "wh-bucket",
		"warehouse_s3_uri":          "s3://wh-bucket/wh",
	}
	topo, err := topologyFor(s.Direction)
	require.NoError(t, err)
	return s, outs, topo
}

// planPoint finds the sweep point for the given arm ID, so tests don't rely
// on the plan's numeric order.
func planPoint(t *testing.T, plan []sweepPoint, armID string) sweepPoint {
	t.Helper()
	for _, p := range plan {
		if p.ArmID == armID {
			return p
		}
	}
	t.Fatalf("no sweep point for arm %q in plan", armID)
	return sweepPoint{}
}

func TestRenderPointConfigs_FanIn(t *testing.T) {
	s, outs, topo := iceberg7TableScenario(t)
	names := newBenchNames("sess-x", "iceberg")
	plan := buildSweepPlan(s)
	point := planPoint(t, plan, "fanin")
	require.True(t, point.FanIn)

	got, err := renderPointConfigs(s, outs, topo, names, point)
	require.NoError(t, err)
	require.NotEmpty(t, got.Single, "fan-in renders one config, like the single-stream path")
	require.Empty(t, got.Root)
	require.Empty(t, got.Streams)

	cfg := readYAML(t, got.Single)
	in := cfg["input"].(map[string]any)["redpanda"].(map[string]any)

	// All 7 source topics subscribed under one input.
	wantTopics := make([]any, 7)
	scoped := names.WithTopics(7)
	for i := 0; i < 7; i++ {
		wantTopics[i] = scoped.WithTopic(i).SourceTopic()
	}
	require.Equal(t, wantTopics, in["topics"])

	// The unsuffixed consumer group — one group, seven subscriptions — not
	// any of the per-topic _t<i> groups streams mode uses.
	require.Equal(t, names.ConsumerGroup("connect"), in["consumer_group"])
	require.NotContains(t, in["consumer_group"], "_t", "fan-in group must be unsuffixed")

	// The iceberg output's table is the interpolated expression, not a
	// literal table name.
	out := cfg["output"].(map[string]any)["iceberg"].(map[string]any)
	require.Equal(t, fanInTableExpr, out["table"])

	// Arm B's own pipeline override (batching count 70000) reached the config.
	require.Equal(t, 70000, out["batching"].(map[string]any)["count"])

	// Placeholders resolved as usual.
	require.Equal(t, "https://glue.example", out["catalog"].(map[string]any)["url"])
}

// TestFanInTableExpr_MatchesTopicDerivedTableNames is the equivalence check
// the design doc requires: fan-in and streams7 MUST write the identical N
// tables, or the whole A/B comparison is invalid. This computes what the
// fan-in interpolation expression would produce for each of the 7 source
// topics (by applying the same two replace_all steps the Bloblang expression
// performs) and checks it against BenchNames.IcebergTablesForTopics("connect")
// element-for-element, so the test survives a naming change on either side.
func TestFanInTableExpr_MatchesTopicDerivedTableNames(t *testing.T) {
	names := newBenchNames("sess-x", "iceberg")
	scoped := names.WithTopics(7)

	gotFromTopics := make([]string, 7)
	for i := 0; i < 7; i++ {
		topic := scoped.WithTopic(i).SourceTopic()
		// Mirror fanInTableExpr's two replace_all steps exactly.
		derived := strings.ReplaceAll(topic, "-", "_")
		derived = strings.ReplaceAll(derived, "_src_t", "_connect_t")
		gotFromTopics[i] = derived
	}

	want := scoped.IcebergTablesForTopics("connect")
	require.Len(t, want, 7)
	require.Equal(t, want, gotFromTopics,
		"the fan-in interpolation must derive the exact same 7 tables streams7 writes, or the A/B comparison is invalid")
}

func TestRenderPointConfigs_Streams7TopicMapping(t *testing.T) {
	s, outs, topo := iceberg7TableScenario(t)
	names := newBenchNames("sess-x", "iceberg")
	plan := buildSweepPlan(s)
	point := planPoint(t, plan, "streams7")
	require.False(t, point.FanIn)
	require.Equal(t, 7, point.Streams)

	got, err := renderPointConfigs(s, outs, topo, names, point)
	require.NoError(t, err)
	require.Empty(t, got.Single)
	require.NotEmpty(t, got.Root)
	require.Len(t, got.Streams, 7)

	tables := make(map[string]bool, 7)
	topics := make(map[string]bool, 7)
	groups := make(map[string]bool, 7)
	for i, path := range got.Streams {
		sc := readYAML(t, path)
		in := sc["input"].(map[string]any)["redpanda"].(map[string]any)
		out := sc["output"].(map[string]any)["iceberg"].(map[string]any)

		wantTopic := names.WithTopics(7).WithTopic(i).SourceTopic()
		wantGroup := names.WithTopics(7).WithTopic(i).ConsumerGroup("connect")
		wantTable := names.WithTopics(7).WithTopic(i).IcebergTable("connect")

		require.Equal(t, []any{wantTopic}, in["topics"], "stream %d topic", i)
		require.Equal(t, wantGroup, in["consumer_group"], "stream %d group", i)
		require.Equal(t, wantTable, out["table"], "stream %d table", i)

		tables[out["table"].(string)] = true
		topics[in["topics"].([]any)[0].(string)] = true
		groups[in["consumer_group"].(string)] = true
	}
	require.Len(t, tables, 7, "all 7 stream tables must be pairwise distinct")
	require.Len(t, topics, 7, "all 7 stream topics must be pairwise distinct")
	require.Len(t, groups, 7, "all 7 stream consumer groups must be pairwise distinct")
}

func TestRenderPointConfigs_RejectsStreamsTopicsMismatch(t *testing.T) {
	s, outs, topo := iceberg7TableScenario(t)
	names := newBenchNames("sess-x", "iceberg")
	// A hand-built point simulating a scenario misconfiguration: streams
	// count that doesn't equal dataset.topics for a multi-topic scenario.
	point := sweepPoint{VCPU: 2, ArmID: "bad", GOMAXPROCS: 2, Streams: 3}

	_, err := renderPointConfigs(s, outs, topo, names, point)
	require.Error(t, err)
	require.Contains(t, err.Error(), "streams")
	require.Contains(t, err.Error(), "topics")
	require.Contains(t, err.Error(), "3")
	require.Contains(t, err.Error(), "7")
}
