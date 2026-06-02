// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

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
		Dataset:   DatasetSpec{Tables: []string{"orders"}, RowSizeBytes: 1200, Seeder: "cdc-rows", InitialRows: 1000},
	}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@host:5432/db", "results_bucket": "bucket"}
	script, err := renderSeedScript(s, outs, "stage/cdc-rows")
	if err != nil {
		t.Fatalf("renderSeedScript: %v", err)
	}
	if !strings.Contains(script, "POSTGRES_DSN=") {
		t.Errorf("postgres seed script must set POSTGRES_DSN; got:\n%s", script)
	}
	if !strings.Contains(script, "/opt/bench/cdc-rows seed") {
		t.Errorf("postgres seed script must invoke /opt/bench/cdc-rows seed; got:\n%s", script)
	}
}

func TestRenderSeedScript_MySQL(t *testing.T) {
	s := &Scenario{
		Connector: "mysql_cdc",
		Dataset:   DatasetSpec{Tables: []string{"orders"}, RowSizeBytes: 1200, Seeder: "cdc-rows-mysql", InitialRows: 0},
	}
	outs := map[string]string{"mysql_dsn": "u:p@tcp(h:3306)/db", "results_bucket": "bucket"}
	script, err := renderSeedScript(s, outs, "stage/cdc-rows-mysql")
	if err != nil {
		t.Fatalf("renderSeedScript: %v", err)
	}
	if !strings.Contains(script, "MYSQL_DSN=") {
		t.Errorf("mysql seed script must set MYSQL_DSN; got:\n%s", script)
	}
	if strings.Contains(script, "POSTGRES_DSN=") {
		t.Errorf("mysql seed script must not leak POSTGRES_DSN; got:\n%s", script)
	}
	if !strings.Contains(script, "/opt/bench/cdc-rows-mysql seed") {
		t.Errorf("mysql seed script must invoke /opt/bench/cdc-rows-mysql seed; got:\n%s", script)
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

func TestCombineReset_MySQL_HostPortForm(t *testing.T) {
	steps := []ResetStep{{SQL: "TRUNCATE TABLE orders"}}
	outs := map[string]string{
		"mysql_host":     "rpcn-bench-my.xyz.rds.amazonaws.com",
		"mysql_port":     "3306",
		"mysql_user":     "bench",
		"mysql_password": "s3cret",
		"mysql_db":       "benchdb",
	}
	got, err := combineReset("mysql_cdc", steps, outs)
	if err != nil {
		t.Fatalf("combineReset: %v", err)
	}
	if !strings.Contains(got, "mysql ") {
		t.Errorf("mysql reset must invoke mysql CLI; got:\n%s", got)
	}
	if !strings.Contains(got, "-h \"rpcn-bench-my.xyz.rds.amazonaws.com\"") {
		t.Errorf("mysql reset must pass -h flag; got:\n%s", got)
	}
	if !strings.Contains(got, "TRUNCATE TABLE orders") {
		t.Errorf("reset must include SQL; got:\n%s", got)
	}
	if strings.Contains(got, "psql") {
		t.Errorf("mysql reset must not contain psql; got:\n%s", got)
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

func TestCombineReset_AppendsKCAndTopicCleanup_Postgres(t *testing.T) {
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
	// KC connector idempotent delete.
	if !strings.Contains(out, "curl") || !strings.Contains(out, "X DELETE") {
		t.Errorf("expected idempotent KC connector DELETE; got:\n%s", out)
	}
	// Topic deletes for both engines via kafka-topics.sh.
	if !strings.Contains(out, "kafka-topics.sh") {
		t.Errorf("expected kafka-topics.sh delete; got:\n%s", out)
	}
	if !strings.Contains(out, "bench_sess-abc_postgres_cdc_connect") {
		t.Errorf("expected Connect topic delete; got:\n%s", out)
	}
	if !strings.Contains(out, "bench_sess-abc_postgres_cdc_kc") {
		t.Errorf("expected KC topic delete; got:\n%s", out)
	}
	// KC delete should enumerate via --list | grep | xargs because Debezium
	// emits <prefix>.<schema>.<table> topics, not a single bare-prefix topic.
	if !strings.Contains(out, "--list") || !strings.Contains(out, "xargs") {
		t.Errorf("KC topic delete should enumerate matching topics with --list | xargs; got:\n%s", out)
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
		Dataset:   DatasetSpec{Tables: []string{"orders"}, RowSizeBytes: 1200, Seeder: "cdc-rows"},
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
	if !strings.Contains(got, "/opt/bench/cdc-rows workload") {
		t.Errorf("postgres workload must invoke cdc-rows; got:\n%s", got)
	}
}

func TestRenderWorkloadScript_MySQL(t *testing.T) {
	s := &Scenario{
		Connector: "mysql_cdc",
		Dataset:   DatasetSpec{Tables: []string{"orders"}, RowSizeBytes: 1200, Seeder: "cdc-rows-mysql"},
		Workload:  &WorkloadSpec{Warmup: 2 * time.Minute, Duration: 15 * time.Minute, WriteRatePerSec: 80000},
	}
	outs := map[string]string{"mysql_dsn": "u:p@tcp(h:3306)/db"}
	got, err := renderWorkloadScript(s, outs)
	if err != nil {
		t.Fatalf("renderWorkloadScript: %v", err)
	}
	if !strings.Contains(got, "MYSQL_DSN=") {
		t.Errorf("mysql workload must set MYSQL_DSN; got:\n%s", got)
	}
	if !strings.Contains(got, "/opt/bench/cdc-rows-mysql workload") {
		t.Errorf("mysql workload must invoke cdc-rows-mysql, not the hardcoded cdc-rows; got:\n%s", got)
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
