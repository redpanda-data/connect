// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/catalog"
)

// e2eCapacityResult is a one-point result of a 1 KB-message pipeline that
// shrinks each message to 200 B before the output.
func e2eCapacityResult(t *testing.T, name string, anomalies ...Anomaly) *Result {
	t.Helper()
	r := e2eResult(t, name)
	r.Dataset.RowSizeBytes = 1024
	r.Points = []PointResult{{
		VCPU:      1,
		Engine:    "connect",
		Summary:   Summary{MedianMsgPerSec: 10_000, MedianMBPerSec: 2.0},
		Anomalies: anomalies,
	}}
	return r
}

// An e2e entry's capacity is input-equivalent: msg/s at the output times
// the workload's message size, whatever the pipeline did to the bytes.
func TestCatalogEntryE2ECapacityIsInputEquivalent(t *testing.T) {
	_, s := renderedScenario(t, pgToSnowflakeConfig())
	e, err := catalogEntryFromResult(s, e2eCapacityResult(t, s.Name), catalogMeta{SessionID: "bench-1"})
	require.NoError(t, err)
	require.Equal(t, catalog.CapacityBasisInputEquivalent, e.CapacityBasis)
	require.InDelta(t, 10.24, e.Points[0].CapacityMBps, 1e-9, "10000 msg/s x 1024 B")
	require.InDelta(t, 2.0, e.Points[0].OutputMBps, 1e-9)
	require.InDelta(t, 10_000, e.Points[0].CapacityMsgps, 0)

	b, err := json.Marshal(e)
	require.NoError(t, err)
	require.Contains(t, string(b), `"capacity_basis":"input_equivalent"`)
	require.Contains(t, string(b), `"output_mb_s":2`)

	// Recommend sizes from the input-equivalent capacity.
	cfg, err := catalog.DefaultSizingConfig()
	require.NoError(t, err)
	rec, err := catalog.Recommend(e, 10.24*cfg.Headroom, cfg)
	require.NoError(t, err)
	require.True(t, rec.BelowFloor || rec.CUs == 10, "the 1 vCPU point covers 10.24 MB/s at headroom")
}

func TestCatalogEntrySourceKeepsOutputBasis(t *testing.T) {
	s, r := catalogFixture(t)
	e, err := catalogEntryFromResult(s, r, catalogMeta{SessionID: "bench-1"})
	require.NoError(t, err)
	require.Empty(t, e.CapacityBasis, "absent means the legacy output basis")
	for _, p := range e.Points {
		require.Zero(t, p.OutputMBps)
	}
	b, err := json.Marshal(e)
	require.NoError(t, err)
	require.NotContains(t, string(b), "capacity_basis")
	require.NotContains(t, string(b), "output_mb_s")
}

func TestCatalogEntryE2EWorkloadBound(t *testing.T) {
	_, s := renderedScenario(t, pgToSnowflakeConfig())
	r := e2eCapacityResult(t, s.Name, Anomaly{Kind: anomalyWorkloadBound, Note: "x"})
	e, err := catalogEntryFromResult(s, r, catalogMeta{SessionID: "bench-1"})
	require.NoError(t, err)
	require.Equal(t, catalog.LimitedByWorkload, e.Points[0].LimitedBy)
	cfg, err := catalog.DefaultSizingConfig()
	require.NoError(t, err)
	rec, err := catalog.Recommend(e, 1, cfg)
	require.NoError(t, err)
	require.Contains(t, rec.Caveats, catalog.CaveatSourceRateLimited)
}

func TestCatalogEntryE2ESinkUnverifiedCaveat(t *testing.T) {
	_, s := renderedScenario(t, pgToSnowflakeConfig())
	e, err := catalogEntryFromResult(s, e2eCapacityResult(t, s.Name), catalogMeta{SessionID: "bench-1"})
	require.NoError(t, err)
	require.NotContains(t, e.Caveats, catalog.CaveatSinkUnverified)

	r := e2eCapacityResult(t, s.Name, Anomaly{Kind: anomalySinkUnverified, Note: "x"})
	e, err = catalogEntryFromResult(s, r, catalogMeta{SessionID: "bench-1"})
	require.NoError(t, err)
	require.Contains(t, e.Caveats, catalog.CaveatSinkUnverified)
	require.Contains(t, catalog.KnownCaveats, catalog.CaveatSinkUnverified)
	cfg, err := catalog.DefaultSizingConfig()
	require.NoError(t, err)
	rec, err := catalog.Recommend(e, 1, cfg)
	require.NoError(t, err)
	require.Contains(t, rec.Caveats, catalog.CaveatSinkUnverified, "sales see it")
}

func TestCatalogEntryE2ESinkRetriedCaveat(t *testing.T) {
	_, s := renderedScenario(t, pgToSnowflakeConfig())
	e, err := catalogEntryFromResult(s, e2eCapacityResult(t, s.Name), catalogMeta{SessionID: "bench-1"})
	require.NoError(t, err)
	require.NotContains(t, e.Caveats, catalog.CaveatSinkRetried)

	r := e2eCapacityResult(t, s.Name, Anomaly{Kind: anomalySinkRetried, Note: "x"})
	e, err = catalogEntryFromResult(s, r, catalogMeta{SessionID: "bench-1"})
	require.NoError(t, err)
	require.Contains(t, e.Caveats, catalog.CaveatSinkRetried)
	cfg, err := catalog.DefaultSizingConfig()
	require.NoError(t, err)
	rec, err := catalog.Recommend(e, 1, cfg)
	require.NoError(t, err)
	require.Contains(t, rec.Caveats, catalog.CaveatSinkRetried, "sales see it")
}

func TestCatalogEntryE2ERecordsSinkSize(t *testing.T) {
	_, s := renderedScenario(t, pgToSnowflakeConfig())
	require.Equal(t, "XSMALL", s.Sink.Size)
	e, err := catalogEntryFromResult(s, e2eCapacityResult(t, s.Name), catalogMeta{SessionID: "bench-1"})
	require.NoError(t, err)
	require.Equal(t, "XSMALL", e.Infra.SinkSize)
}
