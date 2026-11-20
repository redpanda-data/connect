// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func validRunInput() RunInput {
	return RunInput{
		ID:                "bench-1",
		Scenario:          "postgres/orders-cdc",
		GitSHA:            "abc123",
		ConnectVersion:    "v4.50.0-3-gabc123",
		InstanceType:      "c7i.4xlarge",
		Region:            "us-east-2",
		RunURL:            "https://example.com/run",
		MessageSizeBytes:  1200,
		OfferedRatePerSec: 5000,
		Pipeline: map[string]any{
			"input": map[string]any{"postgres_cdc": map[string]any{"schema": "public"}},
			"output": map[string]any{
				"processors": []any{map[string]any{"benchmark": map[string]any{}}},
				"redpanda":   map[string]any{"topic": "t"},
			},
		},
		Points: []RunPoint{
			{VCPU: 1, Engine: "connect", MedianMBps: 10, MedianMsgps: 100, RSSBytes: []uint64{5, 9, 7}},
			{VCPU: 2, MedianMBps: 20, MedianMsgps: 200},
			{VCPU: 4, Engine: "kafka-connect", MedianMBps: 99},
		},
		Now: time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC),
	}
}

func TestEntryFromRun(t *testing.T) {
	e, err := EntryFromRun(validRunInput())
	require.NoError(t, err)
	require.NoError(t, e.Validate())
	require.Equal(t, "postgres_cdc", e.Input)
	require.Equal(t, "redpanda", e.Output)
	require.Equal(t, []string{"benchmark"}, e.Processors)
	require.Equal(t, "cdc", e.Workload.Mode)
	require.Equal(t, 5000, e.Workload.OfferedRatePerSec)
	require.True(t, e.SinkMeasured)
	require.False(t, e.ExternalStubbed)
	require.Equal(t, "c7i.4xlarge", e.Infra.RunnerInstanceType)
	require.Equal(t, []Point{
		{VCPU: 1, CUs: 10, CapacityMBps: 10, CapacityMsgps: 100, PeakRSSBytes: 9},
		{VCPU: 2, CUs: 20, CapacityMBps: 20, CapacityMsgps: 200},
	}, e.Points)
	require.Equal(t, validRunInput().Now, e.RecordedAt)
}

func TestEntryFromRunErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*RunInput)
		want   string
	}{
		"unknown sha": {func(r *RunInput) { r.GitSHA = "unknown" }, "build sha unknown"},
		"empty sha":   {func(r *RunInput) { r.GitSHA = "" }, "build sha unknown"},
		"soak":        {func(r *RunInput) { r.Soak = true }, "soak result"},
		"arm":         {func(r *RunInput) { r.Points[0].Arm = "a" }, "arm/binary result"},
		"binary":      {func(r *RunInput) { r.Points[1].Binary = "base" }, "arm/binary result"},
		"no instance": {func(r *RunInput) { r.InstanceType = "" }, "runner instance type missing"},
	} {
		t.Run(name, func(t *testing.T) {
			r := validRunInput()
			tc.mutate(&r)
			_, err := EntryFromRun(r)
			require.ErrorContains(t, err, tc.want)
		})
	}
}
