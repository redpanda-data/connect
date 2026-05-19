// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
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
