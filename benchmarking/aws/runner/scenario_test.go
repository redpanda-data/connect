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
)

func TestLoadScenario_Valid(t *testing.T) {
	s, err := LoadScenario("testdata/valid-orders-cdc.yaml")
	require.NoError(t, err)
	require.Equal(t, "postgres-orders-cdc", s.Name)
	require.Equal(t, "postgres_cdc", s.Connector)
	require.Equal(t, "postgres", s.Stack)
	require.Equal(t, "c7i.4xlarge", s.Infra.Runner.InstanceType)
	require.Equal(t, 15*time.Minute, s.Workload.Duration)
	require.Equal(t, 2*time.Minute, s.Workload.Warmup)
	require.Equal(t, []int{1, 2, 4, 8}, s.Matrix.CPUPoints)
}

func TestLoadScenario_RejectsShortDuration(t *testing.T) {
	_, err := LoadScenario("testdata/invalid-short-duration.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "workload.duration")
	require.Contains(t, err.Error(), "15m")
}

func TestLoadScenario_RejectsRunnerTooSmall(t *testing.T) {
	_, err := LoadScenario("testdata/invalid-runner-too-small.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "infra.runner.instance_type")
	require.Contains(t, err.Error(), "vCPU")
}

func TestVCPUForInstanceType_Known(t *testing.T) {
	require.Equal(t, 16, vcpuForInstanceType("c7i.4xlarge"))
	require.Equal(t, 2, vcpuForInstanceType("c7i.large"))
	require.Equal(t, 0, vcpuForInstanceType("not-a-real-type"))
}

func TestLoadScenario_BoundedRejectsMissingExpectedPeak(t *testing.T) {
	_, err := LoadScenario("testdata/bounded-missing-expected-peak.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected_peak_mb_s")
}

func TestLoadScenario_BoundedAcceptsValid(t *testing.T) {
	s, err := LoadScenario("testdata/bounded-valid.yaml")
	require.NoError(t, err)
	require.Equal(t, "postgres-snapshot-large", s.Name)
	require.Nil(t, s.Workload)
	require.Equal(t, 100, s.Dataset.ExpectedPeakMBSec)
}

func TestLoadScenario_BoundedRejectsTooSmallDataset(t *testing.T) {
	_, err := LoadScenario("testdata/bounded-too-small.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "below minimum 15m")
}

func TestLoadScenario_RejectsNonAscendingCPUPoints(t *testing.T) {
	_, err := LoadScenario("testdata/invalid-non-ascending-cpu.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "strictly ascending")
}

func TestLoadScenario_RejectsNonPositiveCPUPoints(t *testing.T) {
	_, err := LoadScenario("testdata/invalid-non-positive-cpu.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "must all be positive")
}

func TestEngineSpecFor_Postgres(t *testing.T) {
	es, ok := engineSpecFor("postgres_cdc")
	if !ok {
		t.Fatalf("postgres_cdc should be registered")
	}
	if es.DSNOutputKey != "postgres_dsn" {
		t.Errorf("DSNOutputKey = %q, want postgres_dsn", es.DSNOutputKey)
	}
	if es.DSNEnvVar != "POSTGRES_DSN" {
		t.Errorf("DSNEnvVar = %q, want POSTGRES_DSN", es.DSNEnvVar)
	}
	if es.ResetHostOutputKey != "" {
		t.Errorf("postgres should use DSN-style reset, not host/port; got ResetHostOutputKey=%q", es.ResetHostOutputKey)
	}
}

func TestEngineSpecFor_Unknown(t *testing.T) {
	if _, ok := engineSpecFor("kafka_franz_in_disguise"); ok {
		t.Error("unknown connector should not resolve")
	}
	// mysql_cdc, oracledb_cdc, microsoft_sql_server_cdc, mongodb_cdc, and
	// aws_dynamodb_cdc were trimmed from the registry in this scope-reduced
	// (postgres_cdc-only) tree; each returns with its own stack PR.
	for _, trimmed := range []string{"mysql_cdc", "oracledb_cdc", "microsoft_sql_server_cdc", "mongodb_cdc", "aws_dynamodb_cdc"} {
		if _, ok := engineSpecFor(trimmed); ok {
			t.Errorf("%s should not be registered in this scope-reduced tree", trimmed)
		}
	}
}

func TestValidate_RejectsUnknownConnector(t *testing.T) {
	s := &Scenario{
		Name: "bad", Connector: "kafka_franz_in_disguise", Stack: "kafka",
		Infra:    InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Matrix:   MatrixSpec{CPUPoints: []int{1, 2}},
		Workload: &WorkloadSpec{Warmup: 2 * time.Minute, Duration: 15 * time.Minute, WriteRatePerSec: 1000},
	}
	err := s.Validate()
	if err == nil {
		t.Fatal("expected unknown-connector error")
	}
	if !strings.Contains(err.Error(), "kafka_franz_in_disguise") {
		t.Errorf("error should name the unknown connector; got: %v", err)
	}
}

func TestLoadScenario_DirectionDefaultsToSource(t *testing.T) {
	s := &Scenario{Direction: ""}
	s.applyDirectionDefault()
	if s.Direction != DirectionSource {
		t.Errorf("empty direction must default to source, got %q", s.Direction)
	}
}

func TestValidate_RejectsUnknownDirection(t *testing.T) {
	s := &Scenario{
		Name:      "x",
		Connector: "postgres_cdc",
		Stack:     "postgres",
		Direction: "sideways",
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Matrix:    MatrixSpec{CPUPoints: []int{1}},
		Workload:  &WorkloadSpec{Warmup: 2 * time.Minute, Duration: 15 * time.Minute},
	}
	err := s.Validate()
	if err == nil || !strings.Contains(err.Error(), "direction") {
		t.Fatalf("expected a direction error, got %v", err)
	}
}

func TestRenderPipelineConfig_PassesCacheResourcesThrough(t *testing.T) {
	s := &Scenario{
		Pipeline: map[string]any{
			"input": map[string]any{
				"mysql_cdc": map[string]any{"dsn": "${MYSQL_DSN}"},
			},
			"cache_resources": []any{
				map[string]any{"label": "bench_checkpoint", "memory": map[string]any{}},
			},
		},
	}
	outs := map[string]string{"mysql_dsn": "u:p@tcp(h:3306)/db"}
	path, err := renderPipelineConfig(s, outs, sourceTopology{}, BenchNames{})
	if err != nil {
		t.Fatalf("renderPipelineConfig: %v", err)
	}
	defer os.Remove(path)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := string(body)
	if !strings.Contains(got, "cache_resources:") {
		t.Errorf("rendered config missing cache_resources block; got:\n%s", got)
	}
	if !strings.Contains(got, "bench_checkpoint") {
		t.Errorf("cache_resources label not threaded through; got:\n%s", got)
	}
	if !strings.Contains(got, "u:p@tcp(h:3306)/db") {
		t.Errorf("MYSQL_DSN placeholder not substituted; got:\n%s", got)
	}
}

func TestRenderPipelineConfig_OmitsCacheResourcesWhenAbsent(t *testing.T) {
	s := &Scenario{
		Pipeline: map[string]any{
			"input": map[string]any{
				"postgres_cdc": map[string]any{"dsn": "${POSTGRES_DSN}"},
			},
		},
	}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@h:5432/db"}
	path, err := renderPipelineConfig(s, outs, sourceTopology{}, BenchNames{})
	if err != nil {
		t.Fatalf("renderPipelineConfig: %v", err)
	}
	defer os.Remove(path)
	body, _ := os.ReadFile(path)
	if strings.Contains(string(body), "cache_resources:") {
		t.Errorf("postgres scenario without cache_resources should not have a cache_resources key in rendered config; got:\n%s", body)
	}
}

func TestLoadScenario_ParsesArms(t *testing.T) {
	s, err := LoadScenario("testdata/valid-arms-multi-cpu.yaml")
	require.NoError(t, err)
	require.Len(t, s.Matrix.Arms, 3)
	require.Equal(t, "a0-gmp2", s.Matrix.Arms[0].ID)
	require.Equal(t, 2, s.Matrix.Arms[0].GOMAXPROCS)
	require.Equal(t, "b-gmp8", s.Matrix.Arms[2].ID)
	require.Equal(t, 8, s.Matrix.Arms[2].GOMAXPROCS)
	// Per-arm pipeline override is parsed as a nested map, not flattened.
	in, ok := s.Matrix.Arms[2].Pipeline["input"].(map[string]any)
	require.True(t, ok, "arm pipeline override must parse as map[string]any")
	pg, ok := in["postgres_cdc"].(map[string]any)
	require.True(t, ok)
	batching, ok := pg["batching"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, 10000, batching["count"])
}

// TestLoadScenario_RejectsMultiStreamArmsOnSource pins the narrowed rule: arms
// themselves are legal on a source scenario (single-pipeline arms render
// through the topology-agnostic renderPipelineConfig, which is what the
// /soak base-vs-pr comparison relies on). streams > 1 remains rejected: its
// renderer derives per-stream Iceberg table names, which returns with the
// iceberg-sink stack PR.
func TestLoadScenario_RejectsMultiStreamArmsOnSource(t *testing.T) {
	_, err := LoadScenario("testdata/invalid-arms-source.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "streams")
	require.Contains(t, err.Error(), "iceberg-sink stack PR")
}

// TestLoadScenario_AcceptsSinglePipelineArmsOnSource is the positive half of
// the rule above.
func TestLoadScenario_AcceptsSinglePipelineArmsOnSource(t *testing.T) {
	s, err := LoadScenario("../scenarios/postgres/orders-soak-pr.yaml")
	require.NoError(t, err)
	require.Equal(t, DirectionSource, s.Direction)
	require.Len(t, s.Matrix.Arms, 2)
	for _, a := range s.Matrix.Arms {
		require.LessOrEqual(t, a.Streams, 1, "arm %s", a.ID)
	}
}

func TestLoadScenario_RejectsArmsSweepProductTooLarge(t *testing.T) {
	// matrix.arms used to require exactly one cpu_points entry; that
	// restriction is lifted, replaced by a product ceiling. This fixture is
	// 3 cpu_points x 3 arms = 9, over the 8-point guard.
	_, err := LoadScenario("testdata/invalid-arms-multi-cpu.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "matrix.arms")
	require.Contains(t, err.Error(), "cpu_points")
	require.Contains(t, err.Error(), "9")
}

func TestLoadScenario_AcceptsArmsWithMultipleCPUPointsUnderGuard(t *testing.T) {
	// The single-cpu_points restriction is gone: 2 cpu_points x 3 arms = 6,
	// within the 8-point guard, must now validate.
	s, err := LoadScenario("testdata/valid-arms-multi-cpu.yaml")
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, s.Matrix.CPUPoints)
	require.Len(t, s.Matrix.Arms, 3)
}

// armTestScenario builds a minimal valid postgres_cdc (source) scenario for
// testing matrix.arms validation in isolation. c8g.16xlarge (64 vCPU) covers
// every cpu_points value these tests use.
func armTestScenario(cpuPoints []int, arms []Arm) *Scenario {
	return &Scenario{
		Name:      "postgres-x",
		Connector: "postgres_cdc",
		Stack:     "postgres",
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.16xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 110000000, RowSizeBytes: 1200, Seeder: "cdc-rows-postgres", ExpectedPeakMBSec: 133},
		Matrix:    MatrixSpec{CPUPoints: cpuPoints, Arms: arms},
	}
}

func TestScenarioValidate_RejectsBadArmID(t *testing.T) {
	s := armTestScenario([]int{2}, []Arm{{ID: "Bad_ID", GOMAXPROCS: 4}})
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "matrix.arms[0].id")
}

func TestScenarioValidate_RejectsDuplicateArmIDs(t *testing.T) {
	s := armTestScenario([]int{2}, []Arm{{ID: "dup"}, {ID: "dup"}})
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
}

func TestScenarioValidate_RejectsNegativeGOMAXPROCS(t *testing.T) {
	s := armTestScenario([]int{2}, []Arm{{ID: "a0", GOMAXPROCS: -1}})
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "matrix.arms[0].gomaxprocs")
}

func TestScenarioValidate_RejectsNegativeStreams(t *testing.T) {
	s := armTestScenario([]int{2}, []Arm{{ID: "a0", GOMAXPROCS: 2, Streams: -1}})
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "matrix.arms[0].streams")
}

func TestScenarioValidate_RejectsGOMAXPROCSAboveMax(t *testing.T) {
	// Finding #6: an unbounded gomaxprocs (e.g. a typo like 1000) validates
	// fine today and would silently oversubscribe far past any real
	// instance's vCPU count, with no error until the AWS run is already
	// paying for it.
	s := armTestScenario([]int{2}, []Arm{{ID: "a0", GOMAXPROCS: 1000}})
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "matrix.arms[0].gomaxprocs")
	require.Contains(t, err.Error(), "must be <= 64")
}

func TestScenarioValidate_AcceptsGOMAXPROCSAtMax(t *testing.T) {
	s := armTestScenario([]int{2}, []Arm{{ID: "a0", GOMAXPROCS: 64}})
	require.NoError(t, s.Validate(), "the boundary value itself must validate")
}

func TestScenarioValidate_AcceptsZeroGOMAXPROCSAndStreams(t *testing.T) {
	s := armTestScenario([]int{2}, []Arm{{ID: "a0"}})
	require.NoError(t, s.Validate())
}

func TestScenarioValidate_AcceptsArmSweepProductAtMax(t *testing.T) {
	s := armTestScenario([]int{1, 2, 4, 8}, []Arm{{ID: "a0"}, {ID: "a1"}})
	require.NoError(t, s.Validate(), "4 cpu_points x 2 arms = 8, exactly at the guard")
}

func TestScenarioValidate_RejectsArmSweepProductOverMax(t *testing.T) {
	s := armTestScenario([]int{1, 2, 4, 8}, []Arm{{ID: "a0"}, {ID: "a1"}, {ID: "a2"}})
	err := s.Validate()
	require.Error(t, err, "4 cpu_points x 3 arms = 12, over the 8-point guard")
	require.Contains(t, err.Error(), "matrix.arms")
	require.Contains(t, err.Error(), "cpu_points")
}

// TestValidate_RejectsSinkDirection pins the seam: direction: sink has no
// implementation in this scope-reduced tree (Iceberg sink, per-topic/
// per-stream naming, and the kafka_connect comparison were all cut). The
// error must name the future PR that restores it.
func TestValidate_RejectsSinkDirection(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
	}
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "sink")
	require.Contains(t, err.Error(), "iceberg-sink stack PR")
}

const ordersSoakScenario = "../scenarios/postgres/orders-soak.yaml"

// TestLoadScenario_OrdersSoak is the validity gate for the CON-179 R6 soak
// profile: the scenario shipped in scenarios/postgres/orders-soak.yaml must
// load and validate as a soak scenario, not merely parse.
func TestLoadScenario_OrdersSoak(t *testing.T) {
	s, err := LoadScenario(ordersSoakScenario)
	require.NoError(t, err)
	require.True(t, s.Soak)
	require.Equal(t, []int{2}, s.Matrix.CPUPoints)
	require.Empty(t, s.Matrix.Arms)
	require.Equal(t, "c8g.xlarge", s.Infra.Runner.InstanceType)
	require.Equal(t, 4, vcpuForInstanceType(s.Infra.Runner.InstanceType))
	require.NotNil(t, s.Workload)
	require.Equal(t, 90*time.Minute, s.Workload.Duration)
	require.Equal(t, 5*time.Minute, s.Workload.Warmup)
	require.Equal(t, 10000, s.Workload.WriteRatePerSec)
	require.NoError(t, s.Validate())
}

func TestScenarioValidate_RejectsSoakWithMultipleCPUPoints(t *testing.T) {
	s := &Scenario{
		Name: "soak-x", Connector: "postgres_cdc", Stack: "postgres", Soak: true,
		Infra:    InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.xlarge"}},
		Dataset:  DatasetSpec{Tables: []string{"orders"}},
		Pipeline: map[string]any{"input": map[string]any{"postgres_cdc": map[string]any{}}},
		Workload: &WorkloadSpec{Warmup: minWarmup, Duration: minDuration},
		Matrix:   MatrixSpec{CPUPoints: []int{1, 2}},
	}
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "soak scenarios must set exactly one matrix.cpu_points entry")
}

func TestScenarioValidate_RejectsSoakWithArms(t *testing.T) {
	s := &Scenario{
		Name: "soak-x", Connector: "postgres_cdc", Stack: "postgres", Soak: true,
		Infra:    InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.xlarge"}},
		Dataset:  DatasetSpec{Tables: []string{"orders"}},
		Pipeline: map[string]any{"input": map[string]any{"postgres_cdc": map[string]any{}}},
		Workload: &WorkloadSpec{Warmup: minWarmup, Duration: minDuration},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms:      []Arm{{ID: "a0", GOMAXPROCS: 2, Streams: 1}},
		},
	}
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "soak scenarios must not set matrix.arms")
}

func TestScenarioValidate_AcceptsSoakSingleCPUPointNoArms(t *testing.T) {
	s := &Scenario{
		Name: "soak-x", Connector: "postgres_cdc", Stack: "postgres", Soak: true,
		Infra:    InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.xlarge"}},
		Dataset:  DatasetSpec{Tables: []string{"orders"}},
		Pipeline: map[string]any{"input": map[string]any{"postgres_cdc": map[string]any{}}},
		Workload: &WorkloadSpec{Warmup: minWarmup, Duration: 90 * time.Minute},
		Matrix:   MatrixSpec{CPUPoints: []int{2}},
	}
	require.NoError(t, s.Validate())
}

// soakScenarioWithArms builds a minimal valid soak scenario except for its
// matrix.arms, which the caller supplies — shared setup for the CON-179 R6
// increment 5 binary-arm validation tests below.
func soakScenarioWithArms(arms []Arm) *Scenario {
	return &Scenario{
		Name: "soak-x", Connector: "postgres_cdc", Stack: "postgres", Soak: true,
		Infra:    InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.xlarge"}},
		Dataset:  DatasetSpec{Tables: []string{"orders"}},
		Pipeline: map[string]any{"input": map[string]any{"postgres_cdc": map[string]any{}}},
		Workload: &WorkloadSpec{Warmup: minWarmup, Duration: 90 * time.Minute},
		Matrix:   MatrixSpec{CPUPoints: []int{2}, Arms: arms},
	}
}

// TestScenarioValidate_AcceptsSoakBinaryArms is the positive case CON-179 R6
// increment 5 adds: a soak scenario MAY set matrix.arms when every arm sets
// a non-empty, unique Binary and overrides nothing else.
func TestScenarioValidate_AcceptsSoakBinaryArms(t *testing.T) {
	s := soakScenarioWithArms([]Arm{{ID: "base", Binary: "base"}, {ID: "pr", Binary: "pr"}})
	require.NoError(t, s.Validate())
	require.True(t, s.IsBinaryArmScenario())
}

// TestScenarioValidate_RejectsSoakBinaryArmWithOverride pins the "hold
// everything constant except the build" rule: a binary arm that ALSO
// overrides gomaxprocs/streams/pipeline is rejected, since the override —
// not the build — could then explain any measured delta.
func TestScenarioValidate_RejectsSoakBinaryArmWithOverride(t *testing.T) {
	tests := []struct {
		name string
		arms []Arm
	}{
		{"gomaxprocs", []Arm{{ID: "base", Binary: "base"}, {ID: "pr", Binary: "pr", GOMAXPROCS: 4}}},
		{"streams", []Arm{{ID: "base", Binary: "base"}, {ID: "pr", Binary: "pr", Streams: 2}}},
		{"pipeline", []Arm{{ID: "base", Binary: "base"}, {ID: "pr", Binary: "pr", Pipeline: map[string]any{"x": 1}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := soakScenarioWithArms(tt.arms).Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), "must not override")
			require.Contains(t, err.Error(), "hold everything constant except the build")
		})
	}
}

// TestScenarioValidate_RejectsSoakArmsMissingBinary covers a mix of binary
// and non-binary arms (or every arm bare) — the all-or-nothing rule.
func TestScenarioValidate_RejectsSoakArmsMissingBinary(t *testing.T) {
	s := soakScenarioWithArms([]Arm{{ID: "base", Binary: "base"}, {ID: "bare"}})
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "soak scenarios must not set matrix.arms")
	require.Contains(t, err.Error(), "non-empty binary")
}

// TestScenarioValidate_RejectsSoakSingleBinaryArm pins the >= 2 arms rule: a
// build comparison needs two builds.
func TestScenarioValidate_RejectsSoakSingleBinaryArm(t *testing.T) {
	s := soakScenarioWithArms([]Arm{{ID: "base", Binary: "base"}})
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "at least 2 arms")
}

// TestScenarioValidate_RejectsSoakDuplicateBinary pins uniqueness of the
// Binary values themselves, independent of the (already-checked) arm ID
// uniqueness.
func TestScenarioValidate_RejectsSoakDuplicateBinary(t *testing.T) {
	s := soakScenarioWithArms([]Arm{{ID: "a0", Binary: "base"}, {ID: "a1", Binary: "base"}})
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
	require.Contains(t, err.Error(), `binary "base"`)
}

func TestIsBinaryArmScenario(t *testing.T) {
	require.False(t, (&Scenario{}).IsBinaryArmScenario(), "no arms at all")
	require.False(t, (&Scenario{Matrix: MatrixSpec{Arms: []Arm{{ID: "a0"}}}}).IsBinaryArmScenario(), "arm with no binary")
	require.False(t, (&Scenario{Matrix: MatrixSpec{Arms: []Arm{{ID: "a0", Binary: "base"}, {ID: "a1"}}}}).IsBinaryArmScenario(), "mixed")
	require.True(t, (&Scenario{Matrix: MatrixSpec{Arms: []Arm{{ID: "a0", Binary: "base"}, {ID: "a1", Binary: "pr"}}}}).IsBinaryArmScenario())
}
