// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func validEntry() Entry {
	return Entry{
		SchemaVersion:  SchemaVersion,
		ID:             "sess-1",
		Scenario:       "postgres/orders-cdc",
		Input:          "postgres_cdc",
		Processors:     []string{"mapping", "mutation"},
		Output:         "kafka_franz",
		Fingerprint:    "abc123",
		Workload:       Workload{MessageSizeBytes: 1024, Mode: "cdc", OfferedRatePerSec: 5000},
		ConnectVersion: "v4.118.0-3-gabc1234",
		BuildSHA:       "abc1234",
		Infra:          Infra{RunnerInstanceType: "c6i.4xlarge", Region: "us-east-1", SinkSize: "small"},
		SinkMeasured:   true,
		MemoryLimited:  true,
		Points: []Point{
			{VCPU: 0.5, CUs: 5, CapacityMBps: 1.5, CapacityMsgps: 1500, PeakRSSBytes: 1 << 28},
			{VCPU: 1, CUs: 10, CapacityMBps: 3, CapacityMsgps: 3000},
		},
		AWSCostUSD:  new(1.25),
		SaaSCostUSD: new(0.5),
		RunURL:      "https://example.com/run/1",
		ResultKey:   "results/sess-1.json",
		RecordedAt:  time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC),
	}
}

func TestEntryJSONRoundTrip(t *testing.T) {
	in := validEntry()
	b, err := json.Marshal(in)
	require.NoError(t, err)

	var out Entry
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestEntryValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(e *Entry)
		wantErr string
	}{
		{name: "valid", mutate: func(*Entry) {}},
		{name: "bad schema version", mutate: func(e *Entry) { e.SchemaVersion = 2 }, wantErr: "schema_version"},
		{name: "empty id", mutate: func(e *Entry) { e.ID = "" }, wantErr: "id: must"},
		{name: "empty input", mutate: func(e *Entry) { e.Input = "" }, wantErr: "input: must"},
		{name: "empty output", mutate: func(e *Entry) { e.Output = "" }, wantErr: "output: must"},
		{name: "empty fingerprint", mutate: func(e *Entry) { e.Fingerprint = "" }, wantErr: "fingerprint"},
		{name: "empty build sha", mutate: func(e *Entry) { e.BuildSHA = "" }, wantErr: "build_sha"},
		{name: "bad mode", mutate: func(e *Entry) { e.Workload.Mode = "stream" }, wantErr: "workload.mode"},
		{name: "no points", mutate: func(e *Entry) { e.Points = nil }, wantErr: "points"},
		{name: "cus mismatch", mutate: func(e *Entry) { e.Points[1].CUs = 11 }, wantErr: "cus"},
		{name: "duplicate vcpu", mutate: func(e *Entry) { e.Points[1] = e.Points[0] }, wantErr: "vcpu"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := validEntry()
			tc.mutate(&e)
			err := e.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestModeFor(t *testing.T) {
	for input, want := range map[string]string{
		"postgres_cdc":     "cdc",
		"mongodb_cdc":      "cdc",
		"aws_dynamodb_cdc": "cdc",
		"kafka_franz":      "bulk",
		"generate":         "bulk",
	} {
		require.Equal(t, want, ModeFor(input), input)
	}
}

func TestEntryUnreleased(t *testing.T) {
	for v, want := range map[string]bool{
		"v4.50.0":                 false,
		"v4.50.0-rc1":             false,
		"v4.50.0-3-gabc123":       true,
		"v4.50.0-120-g0123456789": true,
		"v4.50.0-3-gABC123":       false,
		"v4.50.0-3-gabc123-dirty": false,
		"":                        false,
	} {
		require.Equal(t, want, Entry{ConnectVersion: v}.Unreleased(), v)
	}
}

func TestEntryPlanFieldsOmittedWhenEmpty(t *testing.T) {
	b, err := json.Marshal(Entry{})
	require.NoError(t, err)
	require.NotContains(t, string(b), "source_approximated")
	require.NotContains(t, string(b), "caveats")

	b, err = json.Marshal(Entry{SourceApproximated: true, Caveats: []string{"source_approximated"}})
	require.NoError(t, err)
	require.Contains(t, string(b), `"source_approximated":true`)
	require.Contains(t, string(b), `"caveats":["source_approximated"]`)
}

func TestEntryBaseRelease(t *testing.T) {
	for v, want := range map[string]string{
		"v4.50.0":                 "v4.50.0",
		"v4.50.0-3-gabc123":       "v4.50.0",
		"v4.50.0-120-g0123456789": "v4.50.0",
		"v4.50.0-rc1":             "v4.50.0-rc1",
		"v4.50.0-rc1-2-gabc1234":  "v4.50.0-rc1",
		"":                        "",
	} {
		require.Equal(t, want, Entry{ConnectVersion: v}.BaseRelease(), v)
	}
}
