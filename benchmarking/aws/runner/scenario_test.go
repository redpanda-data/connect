// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

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

func TestEngineSpecFor_MySQL(t *testing.T) {
	es, ok := engineSpecFor("mysql_cdc")
	if !ok {
		t.Fatalf("mysql_cdc should be registered")
	}
	if es.DSNOutputKey != "mysql_dsn" {
		t.Errorf("DSNOutputKey = %q, want mysql_dsn", es.DSNOutputKey)
	}
	if es.DSNEnvVar != "MYSQL_DSN" {
		t.Errorf("DSNEnvVar = %q, want MYSQL_DSN", es.DSNEnvVar)
	}
	if es.ResetHostOutputKey != "mysql_host" {
		t.Errorf("ResetHostOutputKey = %q, want mysql_host", es.ResetHostOutputKey)
	}
	if es.ResetPortOutputKey != "mysql_port" || es.ResetUserOutputKey != "mysql_user" ||
		es.ResetPassOutputKey != "mysql_password" || es.ResetDBOutputKey != "mysql_db" {
		t.Errorf("mysql reset output keys incomplete: %+v", es)
	}
}

func TestEngineSpecFor_MSSQL(t *testing.T) {
	es, ok := engineSpecFor("microsoft_sql_server_cdc")
	if !ok {
		t.Fatalf("microsoft_sql_server_cdc should be registered")
	}
	if es.DSNOutputKey != "mssql_dsn" {
		t.Errorf("DSNOutputKey = %q, want mssql_dsn", es.DSNOutputKey)
	}
	if es.DSNEnvVar != "MSSQL_DSN" {
		t.Errorf("DSNEnvVar = %q, want MSSQL_DSN", es.DSNEnvVar)
	}
	if es.NoDSN {
		t.Errorf("microsoft_sql_server_cdc must not set NoDSN (it uses MSSQL_DSN); got %+v", es)
	}
	if es.ResetHostOutputKey != "mssql_host" || es.ResetPortOutputKey != "mssql_port" ||
		es.ResetUserOutputKey != "mssql_user" || es.ResetPassOutputKey != "mssql_password" ||
		es.ResetDBOutputKey != "mssql_db" {
		t.Errorf("mssql reset output keys incomplete: %+v", es)
	}
	// RDS rejects db_name for every sqlserver engine, so the seeder has to
	// CREATE DATABASE against master before MSSQL_DSN is connectable at all.
	// Losing this ExtraEnvVars entry makes the seed phase fail on a fresh stack
	// with a bare login error, which is a long way from the actual cause.
	if got := es.ExtraEnvVars["MSSQL_MASTER_DSN"]; got != "mssql_master_dsn" {
		t.Errorf("ExtraEnvVars[MSSQL_MASTER_DSN] = %q, want mssql_master_dsn", got)
	}
}

// TestEnvVarPrefix_MSSQLCarriesMasterDSN pins the rendered env prefix: both the
// bench DSN and the master DSN must reach the seeder, since the seeder needs
// master to create the database and enable database-level CDC.
func TestEnvVarPrefix_MSSQLCarriesMasterDSN(t *testing.T) {
	es, ok := engineSpecFor("microsoft_sql_server_cdc")
	if !ok {
		t.Fatal("microsoft_sql_server_cdc should be registered")
	}
	got := envVarPrefix(es, map[string]string{
		"mssql_dsn":        "sqlserver://bench:pw@host:1433?database=benchdb",
		"mssql_master_dsn": "sqlserver://bench:pw@host:1433?database=master",
	})
	if !strings.Contains(got, `MSSQL_DSN="sqlserver://bench:pw@host:1433?database=benchdb"`) {
		t.Errorf("env prefix missing MSSQL_DSN; got %q", got)
	}
	if !strings.Contains(got, `MSSQL_MASTER_DSN="sqlserver://bench:pw@host:1433?database=master"`) {
		t.Errorf("env prefix missing MSSQL_MASTER_DSN; got %q", got)
	}
}

func TestEngineSpecFor_MongoDB(t *testing.T) {
	es, ok := engineSpecFor("mongodb_cdc")
	if !ok {
		t.Fatalf("mongodb_cdc should be registered")
	}
	if es.DSNOutputKey != "mongodb_dsn" {
		t.Errorf("DSNOutputKey = %q, want mongodb_dsn", es.DSNOutputKey)
	}
	if es.DSNEnvVar != "MONGODB_DSN" {
		t.Errorf("DSNEnvVar = %q, want MONGODB_DSN", es.DSNEnvVar)
	}
	// Discrete reset keys drive buildKCRenderInputs' Host/Port (no mongosh on the
	// runner; reset is a bash: step, so these feed the KC render, not a psql/mysql
	// CLI). NoDSN must stay false — mongodb_cdc uses a DSN (MONGODB_DSN).
	if es.NoDSN {
		t.Errorf("mongodb_cdc must not set NoDSN (it uses MONGODB_DSN); got %+v", es)
	}
	if es.ResetHostOutputKey != "mongodb_host" || es.ResetPortOutputKey != "mongodb_port" ||
		es.ResetUserOutputKey != "mongodb_user" || es.ResetPassOutputKey != "mongodb_password" ||
		es.ResetDBOutputKey != "mongodb_db" {
		t.Errorf("mongodb reset output keys incomplete: %+v", es)
	}
}

func TestEngineSpecFor_Unknown(t *testing.T) {
	if _, ok := engineSpecFor("kafka_franz_in_disguise"); ok {
		t.Error("unknown connector should not resolve")
	}
}

func TestEngineSpecFor_DynamoDB(t *testing.T) {
	es, ok := engineSpecFor("aws_dynamodb_cdc")
	if !ok {
		t.Fatalf("aws_dynamodb_cdc should be registered")
	}
	if !es.NoDSN {
		t.Errorf("aws_dynamodb_cdc must set NoDSN=true (IAM auth, no DSN); got %+v", es)
	}
	if es.DSNOutputKey != "" || es.DSNEnvVar != "" {
		t.Errorf("aws_dynamodb_cdc must not declare DSN fields; got DSNOutputKey=%q DSNEnvVar=%q", es.DSNOutputKey, es.DSNEnvVar)
	}
	// The seeder and the bash reset block both read AWS_REGION + DDB_TABLE
	// from the env; the engineSpec maps those to the dynamodb stack's TF
	// output keys. If either link breaks, the rendered scripts will reference
	// empty strings and fail at runtime — covered here so the regression
	// surfaces in unit tests, not in an AWS smoke.
	if got, want := es.ExtraEnvVars["AWS_REGION"], "aws_region"; got != want {
		t.Errorf("ExtraEnvVars[AWS_REGION] = %q, want %q", got, want)
	}
	if got, want := es.ExtraEnvVars["DDB_TABLE"], "dynamodb_table_name"; got != want {
		t.Errorf("ExtraEnvVars[DDB_TABLE] = %q, want %q", got, want)
	}
	// READ_CAPACITY / WRITE_CAPACITY are referenced by the scenario's reset
	// bash (drop+recreate between sweep points). If these mappings break,
	// the recreate falls back to empty WCU/RCU args and the table gets
	// created with the wrong provisioned capacity mid-sweep.
	if got, want := es.ExtraEnvVars["READ_CAPACITY"], "read_capacity"; got != want {
		t.Errorf("ExtraEnvVars[READ_CAPACITY] = %q, want %q", got, want)
	}
	if got, want := es.ExtraEnvVars["WRITE_CAPACITY"], "write_capacity"; got != want {
		t.Errorf("ExtraEnvVars[WRITE_CAPACITY] = %q, want %q", got, want)
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

func TestLoadScenario_KafkaConnectOverride(t *testing.T) {
	const yamlBody = `
name: test
connector: postgres_cdc
stack: postgres
infra:
  source: {}
  runner:
    instance_type: c8g.4xlarge
dataset:
  initial_rows: 0
  row_size_bytes: 1200
  tables: [orders]
  seeder: cdc-rows-postgres
workload:
  write_rate_per_sec: 150000
  duration: 15m
  warmup: 2m
pipeline:
  input:
    postgres_cdc:
      dsn: ${POSTGRES_DSN}
matrix:
  cpu_points: [1]
reset: []
kafka_connect:
  config:
    snapshot.mode: never
    decimal.handling.mode: string
`
	tmp, _ := os.CreateTemp("", "scen-*.yaml")
	t.Cleanup(func() { os.Remove(tmp.Name()) })
	tmp.WriteString(yamlBody)
	tmp.Close()

	s, err := LoadScenario(tmp.Name())
	if err != nil {
		t.Fatalf("LoadScenario: %v", err)
	}
	if s.KafkaConnect == nil {
		t.Fatalf("KafkaConnect field should be populated")
	}
	cfg, ok := s.KafkaConnect["config"].(map[string]any)
	if !ok {
		t.Fatalf("expected kafka_connect.config map; got %T", s.KafkaConnect["config"])
	}
	if cfg["snapshot.mode"] != "never" {
		t.Errorf("snapshot.mode = %v, want never", cfg["snapshot.mode"])
	}
}

func TestLoadScenario_ParsesArms(t *testing.T) {
	s, err := LoadScenario("testdata/valid-iceberg-arms.yaml")
	require.NoError(t, err)
	require.Len(t, s.Matrix.Arms, 3)
	require.Equal(t, "a0-1pipe-gmp2", s.Matrix.Arms[0].ID)
	require.Equal(t, 2, s.Matrix.Arms[0].GOMAXPROCS)
	require.Equal(t, 1, s.Matrix.Arms[0].Streams)
	require.Equal(t, "b-2pipe-gmp4", s.Matrix.Arms[2].ID)
	require.Equal(t, 4, s.Matrix.Arms[2].GOMAXPROCS)
	require.Equal(t, 2, s.Matrix.Arms[2].Streams)
	// Per-arm pipeline override is parsed as a nested map, not flattened.
	out, ok := s.Matrix.Arms[2].Pipeline["output"].(map[string]any)
	require.True(t, ok, "arm pipeline override must parse as map[string]any")
	ice, ok := out["iceberg"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, 8, ice["max_in_flight"])
}

// TestLoadScenario_RejectsMultiStreamArmsOnSource pins the narrowed rule: arms
// themselves are legal on a source scenario (single-pipeline arms render
// through the topology-agnostic renderPipelineConfig, which is what
// scenarios/oracle/orders-5table-split.yaml relies on to compare one input
// mining 1 table vs. 5 on the same instance under the same load). Only the
// sink-shaped arm features — streams > 1 and fan_in — remain rejected, because
// their renderers derive per-topic names and sink tables.
func TestLoadScenario_RejectsMultiStreamArmsOnSource(t *testing.T) {
	_, err := LoadScenario("testdata/invalid-arms-source.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "streams")
	require.Contains(t, err.Error(), "source")
}

// TestLoadScenario_AcceptsSinglePipelineArmsOnSource is the positive half of
// the rule above.
func TestLoadScenario_AcceptsSinglePipelineArmsOnSource(t *testing.T) {
	s, err := LoadScenario("../scenarios/oracle/orders-5table-split.yaml")
	require.NoError(t, err)
	require.Equal(t, DirectionSource, s.Direction)
	require.Len(t, s.Matrix.Arms, 2)
	for _, a := range s.Matrix.Arms {
		require.LessOrEqual(t, a.Streams, 1, "arm %s", a.ID)
		require.False(t, a.FanIn, "arm %s", a.ID)
	}
}

func TestLoadScenario_RejectsArmsSweepProductTooLarge(t *testing.T) {
	// matrix.arms used to require exactly one cpu_points entry; that
	// restriction is lifted (topology x core count is the point of the
	// 7-table consolidation test), replaced by a product ceiling. This
	// fixture is 3 cpu_points x 3 arms = 9, over the 8-point guard.
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

func TestScenarioValidate_RejectsBadArmID(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 1000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 200},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms:      []Arm{{ID: "Bad_ID", GOMAXPROCS: 4, Streams: 1}},
		},
	}
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "matrix.arms[0].id")
}

func TestScenarioValidate_RejectsDuplicateArmIDs(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 1000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 200},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms:      []Arm{{ID: "dup", Streams: 1}, {ID: "dup", Streams: 2}},
		},
	}
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
}

func TestScenarioValidate_RejectsNegativeGOMAXPROCS(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 1000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 200},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms:      []Arm{{ID: "a0", GOMAXPROCS: -1, Streams: 1}},
		},
	}
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "matrix.arms[0].gomaxprocs")
}

func TestScenarioValidate_RejectsNegativeStreams(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 1000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 200},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms:      []Arm{{ID: "a0", GOMAXPROCS: 2, Streams: -1}},
		},
	}
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "matrix.arms[0].streams")
}

func TestScenarioValidate_RejectsGOMAXPROCSAboveMax(t *testing.T) {
	// Finding #6: an unbounded gomaxprocs (e.g. a typo like 1000) validates
	// fine today and would silently oversubscribe far past any real
	// instance's vCPU count, with no error until the AWS run is already
	// paying for it.
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 1000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 200},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms:      []Arm{{ID: "a0", GOMAXPROCS: 1000, Streams: 1}},
		},
	}
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "matrix.arms[0].gomaxprocs")
	require.Contains(t, err.Error(), "must be <= 64")
}

func TestScenarioValidate_AcceptsGOMAXPROCSAtMax(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 110000000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 133},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms:      []Arm{{ID: "a0", GOMAXPROCS: 64, Streams: 1}},
		},
	}
	require.NoError(t, s.Validate(), "the boundary value itself must validate")
}

func TestScenarioValidate_RejectsStreamsAboveMax(t *testing.T) {
	// Finding #6: streams: 1000 would render ~1001 tables in the reset
	// union and ~2002 tablegen invocations per reset, all against real AWS
	// spend.
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 1000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 200},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms:      []Arm{{ID: "a0", GOMAXPROCS: 2, Streams: 1000}},
		},
	}
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "matrix.arms[0].streams")
	require.Contains(t, err.Error(), "must be <= 8")
}

func TestScenarioValidate_AcceptsStreamsAtMax(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 110000000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 133},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms:      []Arm{{ID: "a0", GOMAXPROCS: 2, Streams: 8}},
		},
	}
	require.NoError(t, s.Validate(), "the boundary value itself must validate")
}

func TestScenarioValidate_AcceptsZeroGOMAXPROCSAndStreams(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 110000000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 133},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms:      []Arm{{ID: "a0"}},
		},
	}
	require.NoError(t, s.Validate())
}

func TestScenarioValidate_AcceptsArmSweepProductAtMax(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 110000000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 133},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix: MatrixSpec{
			CPUPoints: []int{1, 2, 4, 8},
			Arms:      []Arm{{ID: "a0"}, {ID: "a1"}},
		},
	}
	require.NoError(t, s.Validate(), "4 cpu_points x 2 arms = 8, exactly at the guard")
}

func TestScenarioValidate_RejectsArmSweepProductOverMax(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 110000000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 133},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix: MatrixSpec{
			CPUPoints: []int{1, 2, 4, 8},
			Arms:      []Arm{{ID: "a0"}, {ID: "a1"}, {ID: "a2"}},
		},
	}
	err := s.Validate()
	require.Error(t, err, "4 cpu_points x 3 arms = 12, over the 8-point guard")
	require.Contains(t, err.Error(), "matrix.arms")
	require.Contains(t, err.Error(), "cpu_points")
}

func TestScenarioValidate_RejectsTopicsOnSource(t *testing.T) {
	s := &Scenario{
		Name: "postgres-x", Connector: "postgres_cdc", Stack: "postgres",
		Direction: DirectionSource,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c7i.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 1000, RowSizeBytes: 256, Tables: []string{"orders"}, Seeder: "cdc-rows", Topics: 7},
		Pipeline:  map[string]any{"input": map[string]any{"postgres_cdc": map[string]any{}}},
		Workload:  &WorkloadSpec{Warmup: 2 * time.Minute, Duration: 15 * time.Minute},
		Matrix:    MatrixSpec{CPUPoints: []int{2}},
	}
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "dataset.topics")
	require.Contains(t, err.Error(), "sink")
}

func TestScenarioValidate_RejectsTopicsAboveMax(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 170000000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 145, Topics: 17},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix:    MatrixSpec{CPUPoints: []int{2}},
	}
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "dataset.topics")
	require.Contains(t, err.Error(), "16")
}

func TestScenarioValidate_RejectsTopicsNotDividingInitialRows(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 119000001, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 145, Topics: 7},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix:    MatrixSpec{CPUPoints: []int{2}},
	}
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "dataset.initial_rows")
	require.Contains(t, err.Error(), "dataset.topics")
}

func TestScenarioValidate_AcceptsValidTopics(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 119000000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 145, Topics: 7},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix:    MatrixSpec{CPUPoints: []int{2, 4}},
	}
	require.NoError(t, s.Validate())
}

func TestScenarioValidate_AcceptsAbsentTopics(t *testing.T) {
	// The zero value (field omitted from YAML) must validate exactly like
	// Topics: 1 — the parity guard for every existing scenario.
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 110000000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 133},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix:    MatrixSpec{CPUPoints: []int{2}},
	}
	require.NoError(t, s.Validate())
}

func TestLoadScenario_ParsesFanInArm(t *testing.T) {
	s, err := LoadScenario("testdata/valid-iceberg-7table.yaml")
	require.NoError(t, err)
	require.Equal(t, 7, s.Dataset.Topics)
	require.Len(t, s.Matrix.Arms, 2)
	require.Equal(t, "streams7", s.Matrix.Arms[0].ID)
	require.Equal(t, 7, s.Matrix.Arms[0].Streams)
	require.False(t, s.Matrix.Arms[0].FanIn, "streams7 arm must not set fan_in")
	require.Equal(t, "fanin", s.Matrix.Arms[1].ID)
	require.True(t, s.Matrix.Arms[1].FanIn)
	require.Equal(t, 0, s.Matrix.Arms[1].Streams, "fanin arm must not also set streams")
}

func TestScenarioValidate_RejectsFanInWithSingleTopic(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 1000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 200},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms:      []Arm{{ID: "fanin", FanIn: true}},
		},
	}
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "matrix.arms[0].fan_in")
	require.Contains(t, err.Error(), "dataset.topics")
}

func TestScenarioValidate_RejectsFanInWithStreamsAboveOne(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 119000000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 145, Topics: 7},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms:      []Arm{{ID: "fanin", FanIn: true, Streams: 2}},
		},
	}
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "matrix.arms[0].fan_in")
	require.Contains(t, err.Error(), "streams")
}

func TestScenarioValidate_AcceptsFanInWithMultiTopic(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 119000000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 145, Topics: 7},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms:      []Arm{{ID: "fanin", FanIn: true}},
		},
	}
	require.NoError(t, s.Validate())
}

func TestDatasetSpec_PartitionsPerTopicDefaultsTo4(t *testing.T) {
	require.Equal(t, 4, DatasetSpec{Topics: 7}.partitionsPerTopic())
	require.Equal(t, 8, DatasetSpec{Topics: 7, PartitionsPerTopic: 8}.partitionsPerTopic())
}

func TestLoadScenario_KafkaConnectOptional(t *testing.T) {
	const yamlBody = `
name: test
connector: postgres_cdc
stack: postgres
infra:
  source: {}
  runner:
    instance_type: c8g.4xlarge
dataset:
  initial_rows: 0
  row_size_bytes: 1200
  tables: [orders]
  seeder: cdc-rows-postgres
workload:
  write_rate_per_sec: 150000
  duration: 15m
  warmup: 2m
pipeline:
  input:
    postgres_cdc:
      dsn: ${POSTGRES_DSN}
matrix:
  cpu_points: [1]
reset: []
`
	tmp, _ := os.CreateTemp("", "scen-*.yaml")
	t.Cleanup(func() { os.Remove(tmp.Name()) })
	tmp.WriteString(yamlBody)
	tmp.Close()

	s, err := LoadScenario(tmp.Name())
	if err != nil {
		t.Fatalf("LoadScenario: %v", err)
	}
	if s.KafkaConnect != nil {
		t.Errorf("KafkaConnect should be nil when omitted; got %v", s.KafkaConnect)
	}
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
