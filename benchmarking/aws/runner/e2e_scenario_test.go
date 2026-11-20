// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
package main

import (
	"io/fs"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadE2EScenarioGolden(t *testing.T) {
	for _, name := range []string{"bot-aaaaaaaaaaaa.yaml", "bot-bbbbbbbbbbbb.yaml", "bot-cccccccccccc.yaml"} {
		t.Run(name, func(t *testing.T) {
			s, err := LoadScenario(filepath.Join("..", "plan", "testdata", name))
			require.NoError(t, err)
			require.Equal(t, DirectionE2E, s.Direction)
			require.NotNil(t, s.Sink)
			require.Equal(t, []int{1, 2, 4, 8}, s.Matrix.CPUPoints)
			require.True(t, s.MemoryLimited())
		})
	}
}

func TestE2EScenarioNoneStackWorkload(t *testing.T) {
	s, err := LoadScenario(filepath.Join("..", "plan", "testdata", "bot-bbbbbbbbbbbb.yaml"))
	require.NoError(t, err)
	require.Equal(t, stackNone, s.Stack)
	require.NotNil(t, s.Workload)
	require.Equal(t, 450_000, s.Workload.WriteRatePerSec)
	w, d := sweepWarmupDuration(s)
	require.Equal(t, 2*time.Minute, w)
	require.Equal(t, 15*time.Minute, d)
}

func TestE2ETopologyFor(t *testing.T) {
	topo, err := topologyFor(DirectionE2E)
	require.NoError(t, err)
	require.IsType(t, e2eTopology{}, topo)
}

func TestE2EScenarioValidation(t *testing.T) {
	base := func() Scenario {
		return Scenario{
			Name: "bot-x", Connector: "http_server", Direction: DirectionE2E, Stack: stackNone,
			Infra:    InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
			Workload: &WorkloadSpec{WriteRatePerSec: 1000, Warmup: minWarmup, Duration: minDuration},
			Sink:     &SinkSpec{Kind: "drop"},
			Matrix:   MatrixSpec{CPUPoints: []int{1, 2, 4, 8}},
		}
	}
	good := base()
	require.NoError(t, good.Validate())

	for name, tc := range map[string]struct {
		mut  func(*Scenario)
		want string
	}{
		"e2e without sink":           {func(s *Scenario) { s.Sink = nil }, "requires sink"},
		"unknown sink kind":          {func(s *Scenario) { s.Sink.Kind = "kafka" }, "sink.kind"},
		"size on non-snowflake":      {func(s *Scenario) { s.Sink.Size = "SMALL" }, "only valid for snowflake"},
		"bad snowflake size":         {func(s *Scenario) { s.Sink = &SinkSpec{Kind: "snowflake", Size: "HUGE"} }, "sink.size"},
		"none stack on source":       {func(s *Scenario) { s.Direction, s.Connector = DirectionSource, "postgres_cdc" }, "only allowed for direction"},
		"none stack without load":    {func(s *Scenario) { s.Workload = nil }, "write_rate_per_sec"},
		"none stack zero rate":       {func(s *Scenario) { s.Workload.WriteRatePerSec = 0 }, "write_rate_per_sec"},
		"none stack above 8 vCPU":    {func(s *Scenario) { s.Matrix.CPUPoints = []int{1, 2, 4, 8, 12} }, "cores 10-15"},
		"postgres e2e unknown conn.": {func(s *Scenario) { s.Stack, s.Connector = "postgres", "oracledb_cdc" }, "no engineSpec"},
	} {
		t.Run(name, func(t *testing.T) {
			s := base()
			tc.mut(&s)
			require.ErrorContains(t, s.Validate(), tc.want)
		})
	}

	t.Run("snowflake size defaults to XSMALL", func(t *testing.T) {
		s := base()
		s.Sink = &SinkSpec{Kind: "snowflake"}
		require.NoError(t, s.Validate())
		require.Equal(t, "XSMALL", s.Sink.Size)
	})
}

func TestAllCheckedInScenariosLoad(t *testing.T) {
	root := filepath.Join("..", "scenarios")
	n := 0
	require.NoError(t, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".yaml" {
			return err
		}
		n++
		t.Run(path, func(t *testing.T) {
			_, err := LoadScenario(path)
			require.NoError(t, err)
		})
		return nil
	}))
	require.NotZero(t, n)
}
