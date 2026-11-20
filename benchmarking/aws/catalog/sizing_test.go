// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func sizingCfg() SizingConfig {
	return SizingConfig{Headroom: 0.70, CUsPerVCPU: 10, BytesPerCU: 400000000}
}

func fixtureEntry() Entry {
	const gb = uint64(1_000_000_000)
	return Entry{
		MemoryLimited: true,
		SinkMeasured:  true,
		Points: []Point{
			{VCPU: 1, CUs: 10, CapacityMBps: 18, PeakRSSBytes: 1500000000},
			{VCPU: 2, CUs: 20, CapacityMBps: 35, PeakRSSBytes: 3 * gb},
			{VCPU: 4, CUs: 40, CapacityMBps: 70, PeakRSSBytes: 6 * gb},
			{VCPU: 8, CUs: 80, CapacityMBps: 120, PeakRSSBytes: 12 * gb},
		},
	}
}

func TestRecommendExactPoint(t *testing.T) {
	r, err := Recommend(fixtureEntry(), 40, sizingCfg())
	require.NoError(t, err)
	require.Equal(t, 40, r.CUs)
	require.InDelta(t, 3.265, r.VCPU, 0.01)
	require.True(t, r.Estimated)
	require.Equal(t, "throughput", r.BoundBy)
	require.Equal(t, "20 CUs", r.MemoryBound)
	require.InDelta(t, 0.70, r.Headroom, 1e-9)
	require.Empty(t, r.Caveats)
}

func TestRecommendInterpolatesToThirty(t *testing.T) {
	r, err := Recommend(fixtureEntry(), 30, sizingCfg())
	require.NoError(t, err)
	require.Equal(t, 30, r.CUs)
	require.InDelta(t, 2.449, r.VCPU, 0.01)
}

func TestRecommendBelowFloor(t *testing.T) {
	r, err := Recommend(fixtureEntry(), 10, sizingCfg())
	require.NoError(t, err)
	require.True(t, r.BelowFloor)
	require.Zero(t, r.CUs)
}

func TestRecommendExceedsCeiling(t *testing.T) {
	r, err := Recommend(fixtureEntry(), 100, sizingCfg())
	require.NoError(t, err)
	require.True(t, r.ExceedsCeiling)
	require.InDelta(t, 120, r.CeilingMBps, 1e-9)
	require.Zero(t, r.CUs)
}

func TestRecommendNonMonotonic(t *testing.T) {
	e := fixtureEntry()
	e.Points[3].CapacityMBps = 60

	r, err := Recommend(e, 45, sizingCfg())
	require.NoError(t, err)
	require.Equal(t, 40, r.CUs)
	require.False(t, r.ExceedsCeiling)

	r, err = Recommend(e, 49.5, sizingCfg())
	require.NoError(t, err)
	require.True(t, r.ExceedsCeiling)
	require.InDelta(t, 70, r.CeilingMBps, 1e-9)
	require.Zero(t, r.CUs)
}

func TestRecommendUnsortedPoints(t *testing.T) {
	e := fixtureEntry()
	e.Points[0], e.Points[3] = e.Points[3], e.Points[0]
	r, err := Recommend(e, 40, sizingCfg())
	require.NoError(t, err)
	require.Equal(t, 40, r.CUs)
}

func TestRecommendMemoryBound(t *testing.T) {
	e := fixtureEntry()
	e.Points[2].PeakRSSBytes = 20_000_000_000
	r, err := Recommend(e, 40, sizingCfg())
	require.NoError(t, err)
	require.Equal(t, 50, r.CUs)
	require.Equal(t, "memory", r.BoundBy)
	require.Equal(t, "50 CUs", r.MemoryBound)
}

func TestRecommendMemoryNotMeasured(t *testing.T) {
	e := fixtureEntry()
	e.MemoryLimited = false
	e.Points[2].PeakRSSBytes = 20_000_000_000
	r, err := Recommend(e, 40, sizingCfg())
	require.NoError(t, err)
	require.Equal(t, "not_measured", r.MemoryBound)
	require.Contains(t, r.Caveats, "memory_not_limited")
	require.Equal(t, 40, r.CUs)
	require.Equal(t, "throughput", r.BoundBy)
}

func TestRecommendCaveats(t *testing.T) {
	e := fixtureEntry()
	e.SinkMeasured = false
	e.ExternalStubbed = true
	r, err := Recommend(e, 40, sizingCfg())
	require.NoError(t, err)
	require.Equal(t, []string{"sink_not_measured", "external_calls_stubbed"}, r.Caveats)
}

func TestRecommendRejectsBadTarget(t *testing.T) {
	for _, target := range []float64{0, -5} {
		_, err := Recommend(fixtureEntry(), target, sizingCfg())
		require.Error(t, err)
	}
}

func TestRecommendRejectsNoPoints(t *testing.T) {
	_, err := Recommend(Entry{}, 10, sizingCfg())
	require.Error(t, err)
}

func TestLoadSizingConfig(t *testing.T) {
	cfg, err := LoadSizingConfig("sizing.json")
	require.NoError(t, err)
	require.Equal(t, SizingConfig{Headroom: 0.70, CUsPerVCPU: 10, BytesPerCU: 400000000}, cfg)

	def, err := DefaultSizingConfig()
	require.NoError(t, err)
	require.Equal(t, cfg, def)

	for name, body := range map[string]string{
		"zero":    `{"headroom":0,"cus_per_vcpu":10,"bytes_per_cu":1}`,
		"over":    `{"headroom":1.5,"cus_per_vcpu":10,"bytes_per_cu":1}`,
		"cus":     `{"headroom":0.7,"cus_per_vcpu":0,"bytes_per_cu":1}`,
		"bytes":   `{"headroom":0.7,"cus_per_vcpu":10,"bytes_per_cu":0}`,
		"garbage": `nope`,
	} {
		p := filepath.Join(t.TempDir(), name+".json")
		require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
		_, err := LoadSizingConfig(p)
		require.Error(t, err, name)
	}
	_, err = LoadSizingConfig(filepath.Join(t.TempDir(), "missing.json"))
	require.Error(t, err)
}
