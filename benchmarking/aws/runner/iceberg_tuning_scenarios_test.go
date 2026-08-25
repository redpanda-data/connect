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

// icebergOuts mirrors icebergArmScenario's TF outputs for tests that load the
// real committed scenarios rather than testdata.
func icebergOuts() map[string]string {
	return map[string]string{
		"redpanda_broker_endpoints": "b1:9092",
		"glue_rest_uri":             "https://glue.example",
		"warehouse_account_id":      "1234",
		"aws_region":                "us-east-2",
		"s3_bucket":                 "wh-bucket",
		"warehouse_s3_uri":          "s3://wh-bucket/wh",
	}
}

// renderArm loads a committed scenario, finds the sweep point whose arm id
// matches, and returns the rendered single-stream config.
func renderArm(t *testing.T, scenarioPath, armID string) map[string]any {
	t.Helper()
	s, err := LoadScenario(scenarioPath)
	require.NoError(t, err)
	require.NoError(t, s.Validate())
	topo, err := topologyFor(s.Direction)
	require.NoError(t, err)
	names := newBenchNames("sess-x", s.Connector)
	for _, pt := range buildSweepPlan(s) {
		if pt.ArmID != armID {
			continue
		}
		got, err := renderPointConfigs(s, icebergOuts(), topo, names, pt)
		require.NoError(t, err)
		require.NotEmpty(t, got.Single, "arm %s must render a single-stream config", armID)
		return readYAML(t, got.Single)
	}
	t.Fatalf("scenario %s has no arm %q", scenarioPath, armID)
	return nil
}

func icebergOutputOf(t *testing.T, cfg map[string]any) map[string]any {
	t.Helper()
	out, ok := cfg["output"].(map[string]any)
	require.True(t, ok, "config must carry an output section")
	ice, ok := out["iceberg"].(map[string]any)
	require.True(t, ok, "output must be iceberg")
	return ice
}

func TestOutputTuningScenario_MifArmsOverrideOnlyMaxInFlight(t *testing.T) {
	const path = "../scenarios/iceberg/orders-sink-output-tuning.yaml"

	base := icebergOutputOf(t, renderArm(t, path, "a0-mif16"))
	require.Equal(t, 16, base["max_in_flight"], "baseline arm must keep Recipe A's max_in_flight")

	ice := icebergOutputOf(t, renderArm(t, path, "mif32"))
	require.Equal(t, 32, ice["max_in_flight"])
	// Deep-merge must preserve untouched siblings.
	batching, ok := ice["batching"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, 10000, batching["count"], "mif arm must not disturb batching")
	require.Equal(t, "10s", batching["period"])
}

func TestOutputTuningScenario_BatchArmsMoveBufferAndOutputTogether(t *testing.T) {
	const path = "../scenarios/iceberg/orders-sink-output-tuning.yaml"

	cfg := renderArm(t, path, "b50k-mif32")
	ice := icebergOutputOf(t, cfg)
	require.Equal(t, 32, ice["max_in_flight"])
	batching := ice["batching"].(map[string]any)
	require.Equal(t, 50000, batching["count"])
	require.Equal(t, "10s", batching["period"], "unoverridden period must survive the merge")

	// The batch size the committer sees is decided by the buffer's
	// batch_policy — a batch arm that only moved output batching would
	// measure nothing.
	buf := cfg["buffer"].(map[string]any)["memory"].(map[string]any)
	policy := buf["batch_policy"].(map[string]any)
	require.Equal(t, 50000, policy["count"])
	require.Equal(t, 524288000, buf["limit"], "buffer limit must survive the merge")
}

func TestUpsertScenario_ArmsRenderRowOperationAndMergeStrategy(t *testing.T) {
	const path = "../scenarios/iceberg/orders-upsert.yaml"

	insert := icebergOutputOf(t, renderArm(t, path, "a0-insert"))
	require.NotContains(t, insert, "row_operation", "baseline arm must stay append-only")
	require.NotContains(t, insert, "merge_strategy")

	cow := icebergOutputOf(t, renderArm(t, path, "upsert-cow"))
	require.Equal(t, "upsert", cow["row_operation"])
	require.Equal(t, "copy-on-write", cow["merge_strategy"])
	require.Equal(t, []any{"id"}, cow["identifier_fields"])

	mor := icebergOutputOf(t, renderArm(t, path, "upsert-mor"))
	require.Equal(t, "merge-on-read", mor["merge_strategy"])

	// The output LINTS OUT upsert/delete with max_in_flight > 1 (out-of-order
	// commits corrupt last-writer-wins); an arm violating this fails at
	// startup and benches as silent zeros — hit live 2026-08-21.
	for _, ice := range []map[string]any{cow, mor} {
		require.Equal(t, 1, ice["max_in_flight"], "keyed arms must run max_in_flight: 1")
	}

	// The bench-managed fields must still be decorated on every arm.
	for _, ice := range []map[string]any{insert, cow, mor} {
		require.Equal(t, "bench_sess_x_iceberg_connect", ice["table"])
		require.Contains(t, ice, "catalog")
	}
}

func TestUpsertScenario_SeedScriptCarriesKeySpace(t *testing.T) {
	s, err := LoadScenario("../scenarios/iceberg/orders-upsert.yaml")
	require.NoError(t, err)
	got, err := (sinkTopology{}).SeedScript(s, map[string]string{
		"results_bucket":            "bucket",
		"redpanda_broker_endpoints": "b1:9092",
	}, newBenchNames("sess-x", "iceberg"))
	require.NoError(t, err)
	require.Contains(t, got, " --key-space=12000000",
		"upsert seed must cap the id space so keys actually collide")
	require.Equal(t, 1, strings.Count(got, "--key-space"),
		"the flag must appear exactly once for a single-topic dataset")
}

func TestValidate_KeySpaceBounds(t *testing.T) {
	load := func() *Scenario {
		s, err := LoadScenario("../scenarios/iceberg/orders-upsert.yaml")
		require.NoError(t, err)
		return s
	}

	require.NoError(t, load().Validate(), "committed upsert scenario must validate")

	s := load()
	s.Dataset.KeySpace = -1
	require.ErrorContains(t, s.Validate(), "key_space must be >= 0")

	s = load()
	s.Dataset.KeySpace = s.Dataset.InitialRows
	require.ErrorContains(t, s.Validate(), "never produces a key collision")
}

func TestOutputTuningScenario_Validates(t *testing.T) {
	s, err := LoadScenario("../scenarios/iceberg/orders-sink-output-tuning.yaml")
	require.NoError(t, err)
	require.NoError(t, s.Validate())
	require.Len(t, s.Matrix.Arms, 7)
	require.Len(t, s.Matrix.CPUPoints, 1, "arms require a single cpu point")
}

func TestUpsertCowScenario_ArmsRenderStrategyAndPartitionSpec(t *testing.T) {
	const path = "../scenarios/iceberg/orders-upsert-cow.yaml"

	// Every keyed arm must carry the lint-mandated max_in_flight: 1.
	for _, arm := range []string{"mor-50k", "cow-10k", "cow-50k", "cow-50k-bucket16"} {
		ice := icebergOutputOf(t, renderArm(t, path, arm))
		require.Equal(t, 1, ice["max_in_flight"], "arm %s", arm)
		require.Equal(t, "upsert", ice["row_operation"], "arm %s", arm)
	}

	// The bucket arm's partition_spec must SURVIVE icebergDecorateOutput,
	// which used to replace the schema_evolution block wholesale.
	bucket := icebergOutputOf(t, renderArm(t, path, "cow-50k-bucket16"))
	se, ok := bucket["schema_evolution"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, `"(bucket(16, id))"`, se["partition_spec"])
	require.Equal(t, true, se["enabled"], "bench-managed fields must still be decorated")
	require.Contains(t, se, "table_location")

	// The 10k arm shrinks BOTH the buffer policy and the output batching —
	// the buffer decides what the committer is fed.
	cfg10 := renderArm(t, path, "cow-10k")
	require.Equal(t, 10000, icebergOutputOf(t, cfg10)["batching"].(map[string]any)["count"])
	buf := cfg10["buffer"].(map[string]any)["memory"].(map[string]any)
	require.Equal(t, 10000, buf["batch_policy"].(map[string]any)["count"])

	// Non-bucket arms keep the decorated schema_evolution without a spec.
	plain := icebergOutputOf(t, renderArm(t, path, "cow-50k"))
	seP, ok := plain["schema_evolution"].(map[string]any)
	require.True(t, ok)
	require.NotContains(t, seP, "partition_spec")
}

func TestUpsertCowScenario_SeedScriptCarriesKeyOrder(t *testing.T) {
	s, err := LoadScenario("../scenarios/iceberg/orders-upsert-cow.yaml")
	require.NoError(t, err)
	got, err := (sinkTopology{}).SeedScript(s, map[string]string{
		"results_bucket":            "bucket",
		"redpanda_broker_endpoints": "b1:9092",
	}, newBenchNames("sess-x", "iceberg"))
	require.NoError(t, err)
	require.Contains(t, got, " --key-space=250000 --key-order=scattered")
}

func TestUpsertCowScenario_SkipsConnectPrecreateOnly(t *testing.T) {
	s, err := LoadScenario("../scenarios/iceberg/orders-upsert-cow.yaml")
	require.NoError(t, err)
	require.True(t, s.SkipConnectTablePrecreate)
	names := newBenchNames("sess-x", "iceberg")
	script := icebergResetScript(s, icebergOuts(), names)

	// Connect: table still DROPPED (fresh start per arm) but NOT pre-created,
	// so the output's own creation applies partition_spec.
	require.Contains(t, script, names.IcebergTable("connect"))
	require.NotContains(t, script, "--table="+names.IcebergTable("connect"),
		"connect table must not be pre-created by iceberg-tablegen")
	// KC keeps its pre-create unconditionally (cannot supply a location).
	require.Contains(t, script, "--table="+names.IcebergTable("kafka_connect"))
}

func TestValidate_KeyOrderBounds(t *testing.T) {
	load := func() *Scenario {
		s, err := LoadScenario("../scenarios/iceberg/orders-upsert-cow.yaml")
		require.NoError(t, err)
		return s
	}
	require.NoError(t, load().Validate())

	s := load()
	s.Dataset.KeyOrder = "random"
	require.ErrorContains(t, s.Validate(), `key_order must be "sequential" or "scattered"`)

	s = load()
	s.Dataset.KeySpace = 0
	require.ErrorContains(t, s.Validate(), "key_order requires dataset.key_space")
}

func TestUpsertCowScenario_PipelineCoercesIDToInt(t *testing.T) {
	// JSON numbers decode as float64; with the tablegen pre-create skipped
	// the output infers the table schema from the first record, and a double
	// id is rejected as an identifier field. The scenario must therefore
	// coerce id before the output, for every arm — and the processors must
	// land in the standard benthos pipeline.processors section, NOT inside
	// the input component map, where benthos rejects the field ("field
	// processors not recognised", hit live 2026-08-24).
	cfg := renderArm(t, "../scenarios/iceberg/orders-upsert-cow.yaml", "mor-50k")
	in, ok := cfg["input"].(map[string]any)["redpanda"].(map[string]any)
	require.True(t, ok)
	require.NotContains(t, in, "processors", "processors inside the input component map fail the config lint")
	pl, ok := cfg["pipeline"].(map[string]any)
	require.True(t, ok, "config must carry a pipeline section")
	procs, ok := pl["processors"].([]any)
	require.True(t, ok, "pipeline must carry the id-coercion processors")
	require.Len(t, procs, 1)
	m, ok := procs[0].(map[string]any)["mapping"].(string)
	require.True(t, ok)
	require.Contains(t, m, "root.id = this.id.int64()")
}

func TestUpsertCowClusteredScenario_Validates(t *testing.T) {
	const path = "../scenarios/iceberg/orders-upsert-cow-clustered.yaml"
	s, err := LoadScenario(path)
	require.NoError(t, err)
	require.NoError(t, s.Validate())
	require.Equal(t, "sequential", s.Dataset.KeyOrder)

	got, err := (sinkTopology{}).SeedScript(s, map[string]string{
		"results_bucket":            "bucket",
		"redpanda_broker_endpoints": "b1:9092",
	}, newBenchNames("sess-x", "iceberg"))
	require.NoError(t, err)
	require.Contains(t, got, " --key-space=250000 --key-order=sequential")

	// Keyed arms carry mif 1 and the pipeline-level coercion, same as the
	// scattered scenario.
	cow := icebergOutputOf(t, renderArm(t, path, "cow-50k"))
	require.Equal(t, 1, cow["max_in_flight"])
	require.Equal(t, "copy-on-write", cow["merge_strategy"])
	cfg := renderArm(t, path, "a0-mor-50k")
	pl, ok := cfg["pipeline"].(map[string]any)
	require.True(t, ok)
	require.Len(t, pl["processors"], 1)
}
