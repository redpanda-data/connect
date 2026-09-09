// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The parity guard: no arms means one point per cpu_point, GOMAXPROCS == VCPU,
// one stream, and a bare-integer Key so every artifact name is unchanged.
func TestBuildSweepPlan_NoArmsIsIdentityOverCPUPoints(t *testing.T) {
	s := &Scenario{Matrix: MatrixSpec{CPUPoints: []int{1, 2, 4, 8}}}
	plan := buildSweepPlan(s)
	require.Len(t, plan, 4)
	for i, want := range []int{1, 2, 4, 8} {
		require.Equal(t, want, plan[i].VCPU)
		require.Equal(t, want, plan[i].GOMAXPROCS, "GOMAXPROCS must default to VCPU")
		require.Equal(t, 1, plan[i].Streams)
		require.Empty(t, plan[i].ArmID)
		require.Nil(t, plan[i].Pipeline, "no arms means no merged pipeline")
	}
	require.Equal(t, []string{"1", "2", "4", "8"}, []string{
		plan[0].Key(), plan[1].Key(), plan[2].Key(), plan[3].Key(),
	})
}

func TestBuildSweepPlan_ArmsExpandAtSingleCPUPoint(t *testing.T) {
	s := &Scenario{
		Pipeline: map[string]any{
			"buffer": map[string]any{"memory": map[string]any{"limit": 524288000}},
			"output": map[string]any{"iceberg": map[string]any{"max_in_flight": 16, "batching": map[string]any{"count": 10000}}},
		},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms: []Arm{
				{ID: "a0-1pipe-gmp2", GOMAXPROCS: 2, Streams: 1},
				{ID: "a1-1pipe-gmp4", GOMAXPROCS: 4, Streams: 1},
				{ID: "b-2pipe-gmp4", GOMAXPROCS: 4, Streams: 2, Pipeline: map[string]any{
					"buffer": map[string]any{"memory": map[string]any{"limit": 262144000}},
					"output": map[string]any{"iceberg": map[string]any{"max_in_flight": 8}},
				}},
			},
		},
	}
	plan := buildSweepPlan(s)
	require.Len(t, plan, 3)
	require.Equal(t, "2-a0-1pipe-gmp2", plan[0].Key())
	require.Equal(t, "2-b-2pipe-gmp4", plan[2].Key())
	require.Equal(t, 2, plan[0].GOMAXPROCS)
	require.Equal(t, 4, plan[2].GOMAXPROCS)
	require.Equal(t, 2, plan[2].Streams)
	require.Equal(t, 2, plan[0].VCPU, "every arm is measured at the same vCPU pin")

	// Arm 0 inherits the scenario pipeline verbatim.
	buf0 := plan[0].Pipeline["buffer"].(map[string]any)["memory"].(map[string]any)
	require.Equal(t, 524288000, buf0["limit"])

	// Arm 2 overrides the two fields it names and inherits the rest.
	buf2 := plan[2].Pipeline["buffer"].(map[string]any)["memory"].(map[string]any)
	require.Equal(t, 262144000, buf2["limit"])
	ice2 := plan[2].Pipeline["output"].(map[string]any)["iceberg"].(map[string]any)
	require.Equal(t, 8, ice2["max_in_flight"])
	require.Equal(t, map[string]any{"count": 10000}, ice2["batching"], "unnamed sibling keys survive the merge")
}

// The merge must not mutate the scenario or leak shared sub-maps between arms:
// two arms merged from one base must be independently editable.
func TestMergePipeline_DoesNotAliasBase(t *testing.T) {
	base := map[string]any{"output": map[string]any{"iceberg": map[string]any{"max_in_flight": 16}}}
	got := mergePipeline(base, map[string]any{"output": map[string]any{"iceberg": map[string]any{"max_in_flight": 8}}})

	require.Equal(t, 8, got["output"].(map[string]any)["iceberg"].(map[string]any)["max_in_flight"])
	require.Equal(t, 16, base["output"].(map[string]any)["iceberg"].(map[string]any)["max_in_flight"],
		"base must be untouched")

	got["output"].(map[string]any)["iceberg"].(map[string]any)["new"] = true
	_, leaked := base["output"].(map[string]any)["iceberg"].(map[string]any)["new"]
	require.False(t, leaked, "merged result must not share sub-maps with base")
}

func TestMergePipeline_DeepCopiesSlices(t *testing.T) {
	base := map[string]any{"input": map[string]any{"redpanda": map[string]any{"topics": []any{"a"}}}}
	got := mergePipeline(base, nil)
	got["input"].(map[string]any)["redpanda"].(map[string]any)["topics"].([]any)[0] = "mutated"
	require.Equal(t, "a", base["input"].(map[string]any)["redpanda"].(map[string]any)["topics"].([]any)[0])
}

func TestBuildSweepPlan_ArmDefaultsGOMAXPROCSToVCPUAndStreamsToOne(t *testing.T) {
	s := &Scenario{Matrix: MatrixSpec{CPUPoints: []int{2}, Arms: []Arm{{ID: "bare"}}}}
	plan := buildSweepPlan(s)
	require.Len(t, plan, 1)
	require.Equal(t, 2, plan[0].GOMAXPROCS)
	require.Equal(t, 1, plan[0].Streams)
}

func TestBuildSweepPlan_ArmsExpandAcrossMultipleCPUPoints(t *testing.T) {
	// The single-cpu_points restriction on arms is lifted (Scenario.Validate),
	// so buildSweepPlan must expand the full cpu_points x arms product, in
	// cpu_points-major order (outer loop over CPUPoints, inner over Arms) —
	// this pins that shape rather than assuming it.
	s := &Scenario{
		Matrix: MatrixSpec{
			CPUPoints: []int{2, 4},
			Arms: []Arm{
				{ID: "streams7", Streams: 7},
				{ID: "fanin", Streams: 1},
			},
		},
	}
	plan := buildSweepPlan(s)
	require.Len(t, plan, 4, "2 cpu_points x 2 arms = 4 points")
	wantKeys := []string{"2-streams7", "2-fanin", "4-streams7", "4-fanin"}
	gotKeys := make([]string, len(plan))
	for i, p := range plan {
		gotKeys[i] = p.Key()
	}
	require.Equal(t, wantKeys, gotKeys)
	for _, p := range plan {
		if p.ArmID == "streams7" {
			require.Equal(t, 7, p.Streams)
		} else {
			require.Equal(t, 1, p.Streams)
		}
	}
	require.Equal(t, 2, plan[0].VCPU)
	require.Equal(t, 2, plan[1].VCPU)
	require.Equal(t, 4, plan[2].VCPU)
	require.Equal(t, 4, plan[3].VCPU)
}

// TestBuildSweepPlan_ArmsCarryBinary pins Arm.Binary reaching the sweep
// point unchanged (see sweepPoint.Binary), and confirms an arm that leaves
// Binary unset still defaults to "" — the scenario's single default staged
// binary, unchanged from before this field existed.
func TestBuildSweepPlan_ArmsCarryBinary(t *testing.T) {
	s := &Scenario{
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms: []Arm{
				{ID: "base", Binary: "base"},
				{ID: "pr", Binary: "pr"},
				{ID: "bare"},
			},
		},
	}
	plan := buildSweepPlan(s)
	require.Len(t, plan, 3)
	require.Equal(t, "base", plan[0].Binary)
	require.Equal(t, "pr", plan[1].Binary)
	require.Empty(t, plan[2].Binary, "an arm that doesn't set binary defaults to the scenario's single staged binary")
}

func TestPlanMaxStreams(t *testing.T) {
	require.Equal(t, 1, planMaxStreams([]sweepPoint{{Streams: 1}, {Streams: 1}}))
	require.Equal(t, 2, planMaxStreams([]sweepPoint{{Streams: 1}, {Streams: 2}}))
	require.Equal(t, 1, planMaxStreams(nil), "empty plan still yields a usable single-table reset")
}
