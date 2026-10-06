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

func gogcScript(gogc int) string {
	return renderBenchScript(benchScriptArgs{
		VCPU: 8, GOGC: gogc, MemLimitGiB: 24, DurationSec: 900,
		ConfigPath: "/opt/bench/config.yaml", BinaryPath: "/opt/bench/redpanda-connect",
		Bucket: "b", SessionID: "s",
	})
}

func TestRenderBenchScript_GOGC(t *testing.T) {
	const base = "taskset -c 2-9 env GOMAXPROCS=8 GOMEMLIMIT=24GiB REDPANDA_LICENSE_FILEPATH=/opt/bench/license.jwt "
	tests := []struct {
		name string
		gogc int
		want string
	}{
		{"unset is byte-identical to the pre-GOGC line", 0, base},
		{"positive", 400, "taskset -c 2-9 env GOMAXPROCS=8 GOMEMLIMIT=24GiB GOGC=400 REDPANDA_LICENSE_FILEPATH=/opt/bench/license.jwt "},
		{"off", -1, "taskset -c 2-9 env GOMAXPROCS=8 GOMEMLIMIT=24GiB GOGC=off REDPANDA_LICENSE_FILEPATH=/opt/bench/license.jwt "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Contains(t, gogcScript(tc.gogc), tc.want)
		})
	}
	require.NotContains(t, gogcScript(0), "GOGC")
}

func TestBuildSweepPlan_CarriesGOGCAndFanout(t *testing.T) {
	s := &Scenario{Matrix: MatrixSpec{CPUPoints: []int{8}, Arms: []Arm{
		{ID: "a0"}, {ID: "g", GOGC: -1, OutputFanout: 4},
	}}}
	plan := buildSweepPlan(s)
	require.Equal(t, 0, plan[0].GOGC)
	require.Equal(t, -1, plan[1].GOGC)
	require.Equal(t, 4, plan[1].OutputFanout)
}

func s3ArmValidateScenario(dir Direction, arm Arm) *Scenario {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg", Direction: dir,
		Infra:    InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:  DatasetSpec{InitialRows: 110000000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 133},
		Pipeline: map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix:   MatrixSpec{CPUPoints: []int{8}, Arms: []Arm{arm}},
	}
	return s
}

func TestScenarioValidate_ArmGOGCAndFanout(t *testing.T) {
	tests := []struct {
		name    string
		dir     Direction
		arm     Arm
		wantErr string
	}{
		{"gogc below -1", DirectionSink, Arm{ID: "a", GOGC: -2}, "matrix.arms[0].gogc"},
		{"gogc off ok", DirectionSink, Arm{ID: "a", GOGC: -1}, ""},
		{"gogc 400 ok", DirectionSink, Arm{ID: "a", GOGC: 400}, ""},
		{"fanout negative", DirectionSink, Arm{ID: "a", OutputFanout: -1}, "matrix.arms[0].output_fanout"},
		{"fanout too big", DirectionSink, Arm{ID: "a", OutputFanout: 17}, "output_fanout must be <= 16"},
		{"fanout 16 ok", DirectionSink, Arm{ID: "a", OutputFanout: 16}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := s3ArmValidateScenario(tc.dir, tc.arm).Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestRenderPointConfigs_OutputFanoutS3(t *testing.T) {
	batching := func() map[string]any {
		return map[string]any{
			"count": 100000,
			"processors": []any{map[string]any{"parquet_encode": map[string]any{
				"default_compression": "zstd",
			}}},
		}
	}
	s := &Scenario{
		Name: "s3-x", Connector: "s3", Stack: "s3", Direction: DirectionSink,
		Pipeline: map[string]any{"output": map[string]any{"aws_s3": map[string]any{
			"path":     "${!uuid_v4()}.parquet",
			"batching": batching(),
		}}},
		Matrix: MatrixSpec{CPUPoints: []int{8}, Arms: []Arm{{ID: "a0"}, {ID: "fan4", OutputFanout: 4}}},
	}
	outs := map[string]string{"redpanda_broker_endpoints": "b1:9092"}
	topo, err := topologyFor(s.Direction)
	require.NoError(t, err)
	names := newBenchNames("sess-x", "s3")
	plan := buildSweepPlan(s)

	// Arm without fanout keeps the plain aws_s3 output.
	base, err := renderPointConfigs(s, outs, topo, names, plan[0])
	require.NoError(t, err)
	baseOut := readYAML(t, base.Single)["output"].(map[string]any)
	require.Contains(t, baseOut, "aws_s3")
	require.NotContains(t, baseOut, "broker")

	got, err := renderPointConfigs(s, outs, topo, names, plan[1])
	require.NoError(t, err)
	out := readYAML(t, got.Single)["output"].(map[string]any)
	require.Len(t, out, 1)
	broker := out["broker"].(map[string]any)
	require.Equal(t, "round_robin", broker["pattern"])
	copies := broker["outputs"].([]any)
	require.Len(t, copies, 4)
	prefix := s3Prefix(names, "connect")
	for i, c := range copies {
		s3 := c.(map[string]any)["aws_s3"].(map[string]any)
		require.Equal(t, prefix+"${!uuid_v4()}.parquet", s3["path"], "copy %d", i)
		require.True(t, strings.HasPrefix(s3["path"].(string), prefix))
		b := s3["batching"].(map[string]any)
		require.NotEmpty(t, b["processors"], "copy %d needs its own batching.processors", i)
	}
}

func TestLoadScenario_RejectsOutputFanoutOnSource(t *testing.T) {
	_, err := LoadScenario("testdata/invalid-arms-source-fanout.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "output_fanout is only supported for direction: sink")
}
