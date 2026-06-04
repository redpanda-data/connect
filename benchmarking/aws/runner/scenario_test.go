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
  seeder: cdc-rows
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
  seeder: cdc-rows
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
