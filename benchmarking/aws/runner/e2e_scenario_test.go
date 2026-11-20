// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadE2EScenarioGolden(t *testing.T) {
	for _, name := range []string{"bot-aaaaaaaaaaaa.yaml", "bot-bbbbbbbbbbbb.yaml"} {
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

func TestE2EScenarioNoneStackWarmupDuration(t *testing.T) {
	s, err := LoadScenario(filepath.Join("..", "plan", "testdata", "bot-bbbbbbbbbbbb.yaml"))
	require.NoError(t, err)
	require.Equal(t, stackNone, s.Stack)
	require.Nil(t, s.Workload)
	w, d := sweepWarmupDuration(s)
	require.Equal(t, 2*time.Minute, w)
	require.Equal(t, 15*time.Minute, d)
}

func TestE2EScenarioValidation(t *testing.T) {
	base := func() Scenario {
		return Scenario{
			Name: "bot-x", Connector: "http_server", Direction: DirectionE2E, Stack: stackNone,
			Infra:   InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
			Dataset: DatasetSpec{PreloadMessages: 1000},
			Sink:    &SinkSpec{Kind: "drop"},
			Matrix:  MatrixSpec{CPUPoints: []int{1, 2, 4, 8}},
		}
	}
	good := base()
	require.NoError(t, good.Validate())

	for name, tc := range map[string]struct {
		mut  func(*Scenario)
		want string
	}{
		"e2e without sink":             {func(s *Scenario) { s.Sink = nil }, "requires sink"},
		"none stack on source":         {func(s *Scenario) { s.Direction, s.Connector = DirectionSource, "postgres_cdc" }, "only allowed for direction"},
		"none stack without preload":   {func(s *Scenario) { s.Dataset.PreloadMessages = 0 }, "preload_messages"},
		"short preload warmup":         {func(s *Scenario) { s.Dataset.PreloadWarmup = time.Minute }, "preload_warmup"},
		"e2e topology not implemented": {func(s *Scenario) {}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			if tc.want == "" {
				_, err := topologyFor(DirectionE2E)
				require.ErrorContains(t, err, "e2e topology not implemented yet")
				return
			}
			s := base()
			tc.mut(&s)
			require.ErrorContains(t, s.Validate(), tc.want)
		})
	}
}
