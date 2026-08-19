// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"strings"
	"testing"
	"time"
)

// --- renderSeedScript ---

func TestRenderSeedScript_Postgres(t *testing.T) {
	s := &Scenario{
		Connector: "postgres_cdc",
		Dataset:   DatasetSpec{Tables: []string{"orders"}, RowSizeBytes: 1200, Seeder: "cdc-rows-postgres", InitialRows: 1000},
	}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@host:5432/db", "results_bucket": "bucket"}
	script, err := renderSeedScript(s, outs, "stage/cdc-rows-postgres")
	if err != nil {
		t.Fatalf("renderSeedScript: %v", err)
	}
	if !strings.Contains(script, "POSTGRES_DSN=") {
		t.Errorf("postgres seed script must set POSTGRES_DSN; got:\n%s", script)
	}
	if !strings.Contains(script, "/opt/bench/cdc-rows-postgres seed") {
		t.Errorf("postgres seed script must invoke /opt/bench/cdc-rows-postgres seed; got:\n%s", script)
	}
}

func TestRenderSeedScript_NoDSN_WithExtraEnvVars(t *testing.T) {
	// Register a test-only NoDSN engine and clean up after.
	engineSpecs["aws_dynamodb_cdc_test"] = engineSpec{
		NoDSN: true,
		ExtraEnvVars: map[string]string{
			"DDB_TABLE":  "dynamodb_table_name",
			"AWS_REGION": "aws_region",
		},
	}
	t.Cleanup(func() { delete(engineSpecs, "aws_dynamodb_cdc_test") })

	s := &Scenario{
		Connector: "aws_dynamodb_cdc_test",
		Dataset:   DatasetSpec{Tables: []string{"orders"}, RowSizeBytes: 2048, Seeder: "cdc-ddb", InitialRows: 0},
	}
	outs := map[string]string{
		"aws_region":          "us-east-2",
		"dynamodb_table_name": "bench_orders",
		"results_bucket":      "bucket",
	}
	script, err := renderSeedScript(s, outs, "stage/cdc-ddb")
	if err != nil {
		t.Fatalf("renderSeedScript: %v", err)
	}
	// ExtraEnvVars must appear sorted by key, BEFORE the seeder command. No DSN.
	if !strings.Contains(script, `AWS_REGION="us-east-2" DDB_TABLE="bench_orders" /opt/bench/cdc-ddb seed`) {
		t.Errorf("expected sorted ExtraEnvVars then seeder invocation; got:\n%s", script)
	}
	if strings.Contains(script, "_DSN=") {
		t.Errorf("NoDSN engine must not emit any *_DSN= prefix; got:\n%s", script)
	}
}

func TestRenderSeedScript_UnknownConnector(t *testing.T) {
	s := &Scenario{Connector: "unknown_connector", Dataset: DatasetSpec{Seeder: "x"}}
	_, err := renderSeedScript(s, map[string]string{}, "stage/x")
	if err == nil {
		t.Fatal("expected error for unknown connector")
	}
}

// --- combineReset ---

func TestCombineReset_Postgres_DSNForm(t *testing.T) {
	steps := []ResetStep{{SQL: "SELECT 1"}}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@host:5432/db"}
	got, err := combineReset("postgres_cdc", steps, outs)
	if err != nil {
		t.Fatalf("combineReset: %v", err)
	}
	if !strings.Contains(got, "psql ") {
		t.Errorf("postgres reset must use psql; got:\n%s", got)
	}
	if !strings.Contains(got, "SELECT 1") {
		t.Errorf("reset must include SQL; got:\n%s", got)
	}
}

func TestCombineReset_EmptySteps(t *testing.T) {
	got, err := combineReset("postgres_cdc", nil, map[string]string{})
	if err != nil {
		t.Fatalf("combineReset: %v", err)
	}
	if got != "" {
		t.Errorf("empty reset should produce empty string, got %q", got)
	}
}

func TestCombineReset_BashStepPasses(t *testing.T) {
	steps := []ResetStep{{Bash: "echo ${POSTGRES_DSN}"}}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@host:5432/db"}
	got, err := combineReset("postgres_cdc", steps, outs)
	if err != nil {
		t.Fatalf("combineReset: %v", err)
	}
	if !strings.Contains(got, "postgres://u:p@host:5432/db") {
		t.Errorf("bash step must have placeholders substituted; got:\n%s", got)
	}
}

func TestCombineReset_AppendsTopicCleanup_Postgres(t *testing.T) {
	outs := map[string]string{
		"postgres_dsn":              "postgres://user:pw@host/db",
		"bench_session_id":          "sess-abc",
		"redpanda_broker_endpoints": "10.42.10.10:9092",
	}
	steps := []ResetStep{
		{SQL: "SELECT 1"},
	}
	out, err := combineReset("postgres_cdc", steps, outs)
	if err != nil {
		t.Fatalf("combineReset: %v", err)
	}
	// Existing SQL still present.
	if !strings.Contains(out, "SELECT 1") {
		t.Errorf("expected original SQL to remain; got:\n%s", out)
	}
	// Connect's per-session output topic is torn down between points.
	if !strings.Contains(out, "kafka-topics.sh") {
		t.Errorf("expected kafka-topics.sh delete; got:\n%s", out)
	}
	if !strings.Contains(out, "bench_sess-abc_postgres_cdc_connect") {
		t.Errorf("expected Connect topic delete; got:\n%s", out)
	}
}

func TestCombineReset_NoOpWhenSessionIDMissing(t *testing.T) {
	// If bench_session_id is somehow unset, the reset should skip the
	// topic/connector cleanup steps rather than emit malformed commands.
	outs := map[string]string{
		"postgres_dsn":              "postgres://user:pw@host/db",
		"redpanda_broker_endpoints": "10.42.10.10:9092",
	}
	steps := []ResetStep{{SQL: "SELECT 1"}}
	out, err := combineReset("postgres_cdc", steps, outs)
	if err != nil {
		t.Fatalf("combineReset: %v", err)
	}
	if strings.Contains(out, "kafka-topics.sh") {
		t.Errorf("topic delete should be skipped when session id is empty; got:\n%s", out)
	}
}

// --- renderWorkloadScript ---

func TestRenderWorkloadScript_Postgres(t *testing.T) {
	s := &Scenario{
		Connector: "postgres_cdc",
		Dataset:   DatasetSpec{Tables: []string{"orders"}, RowSizeBytes: 1200, Seeder: "cdc-rows-postgres"},
		Workload:  &WorkloadSpec{Warmup: 2 * time.Minute, Duration: 15 * time.Minute, WriteRatePerSec: 80000},
	}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@host:5432/db"}
	got, err := renderWorkloadScript(s, outs)
	if err != nil {
		t.Fatalf("renderWorkloadScript: %v", err)
	}
	if !strings.Contains(got, "POSTGRES_DSN=") {
		t.Errorf("postgres workload must set POSTGRES_DSN; got:\n%s", got)
	}
	if !strings.Contains(got, "/opt/bench/cdc-rows-postgres workload") {
		t.Errorf("postgres workload must invoke cdc-rows-postgres; got:\n%s", got)
	}
}

func TestRenderWorkloadScript_NoDSN_WithExtraEnvVars(t *testing.T) {
	engineSpecs["aws_dynamodb_cdc_test"] = engineSpec{
		NoDSN: true,
		ExtraEnvVars: map[string]string{
			"AWS_REGION": "aws_region",
			"DDB_TABLE":  "dynamodb_table_name",
		},
	}
	t.Cleanup(func() { delete(engineSpecs, "aws_dynamodb_cdc_test") })

	s := &Scenario{
		Connector: "aws_dynamodb_cdc_test",
		Dataset:   DatasetSpec{Tables: []string{"orders"}, RowSizeBytes: 2048, Seeder: "cdc-ddb"},
		Workload:  &WorkloadSpec{Warmup: 2 * time.Minute, Duration: 15 * time.Minute, WriteRatePerSec: 5000},
	}
	outs := map[string]string{
		"aws_region":          "us-east-2",
		"dynamodb_table_name": "bench_orders",
	}
	got, err := renderWorkloadScript(s, outs)
	if err != nil {
		t.Fatalf("renderWorkloadScript: %v", err)
	}
	if !strings.Contains(got, `AWS_REGION="us-east-2" DDB_TABLE="bench_orders" /opt/bench/cdc-ddb workload`) {
		t.Errorf("expected sorted ExtraEnvVars then workload invocation; got:\n%s", got)
	}
	if strings.Contains(got, "_DSN=") {
		t.Errorf("NoDSN engine must not emit any *_DSN= prefix; got:\n%s", got)
	}
}

func TestCombineReset_NoDSN_RejectsSQL(t *testing.T) {
	engineSpecs["aws_dynamodb_cdc_test"] = engineSpec{NoDSN: true}
	t.Cleanup(func() { delete(engineSpecs, "aws_dynamodb_cdc_test") })

	_, err := combineReset("aws_dynamodb_cdc_test", []ResetStep{{SQL: "TRUNCATE TABLE orders"}}, map[string]string{})
	if err == nil {
		t.Fatal("expected error when NoDSN engine has a sql: reset step")
	}
	if !strings.Contains(err.Error(), "NoDSN") {
		t.Errorf("error should mention NoDSN; got: %v", err)
	}
}

func TestCombineReset_NoDSN_AllowsBash(t *testing.T) {
	engineSpecs["aws_dynamodb_cdc_test"] = engineSpec{NoDSN: true}
	t.Cleanup(func() { delete(engineSpecs, "aws_dynamodb_cdc_test") })

	steps := []ResetStep{{Bash: "aws dynamodb delete-table --table-name ${DYNAMODB_TABLE_NAME} || true"}}
	outs := map[string]string{"dynamodb_table_name": "bench_orders"}
	got, err := combineReset("aws_dynamodb_cdc_test", steps, outs)
	if err != nil {
		t.Fatalf("combineReset: %v", err)
	}
	if !strings.Contains(got, "aws dynamodb delete-table --table-name bench_orders") {
		t.Errorf("bash placeholder substitution should fire; got:\n%s", got)
	}
}

func TestRenderWorkloadScript_NilWorkload(t *testing.T) {
	s := &Scenario{Connector: "postgres_cdc", Workload: nil}
	got, err := renderWorkloadScript(s, map[string]string{})
	if err != nil {
		t.Fatalf("renderWorkloadScript: %v", err)
	}
	if got != "" {
		t.Errorf("nil workload should produce empty string, got %q", got)
	}
}
