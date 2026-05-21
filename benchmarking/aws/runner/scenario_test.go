// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
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
