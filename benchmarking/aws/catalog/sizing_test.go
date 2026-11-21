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
	"math"
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
	require.InDelta(t, 4, r.VCPU, 1e-9)
	require.InDelta(t, 3.265, r.InterpolatedVCPU, 0.01)
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
	require.InDelta(t, 3, r.VCPU, 1e-9)
	require.InDelta(t, 2.449, r.InterpolatedVCPU, 0.01)
}

func TestRecommendBelowFloor(t *testing.T) {
	r, err := Recommend(fixtureEntry(), 10, sizingCfg())
	require.NoError(t, err)
	require.True(t, r.BelowFloor)
	require.Zero(t, r.CUs)
	require.Zero(t, r.VCPU)
}

func TestRecommendBelowFloorMemoryAboveFloor(t *testing.T) {
	e := fixtureEntry()
	e.Points[0].PeakRSSBytes = 7_000_000_000 // 17.5 CUs -> 20, above the 10 CU floor
	r, err := Recommend(e, 10, sizingCfg())
	require.NoError(t, err)
	require.False(t, r.BelowFloor)
	require.Equal(t, 20, r.CUs)
	require.Equal(t, "memory", r.BoundBy)
	require.InDelta(t, 2, r.VCPU, 1e-9)
	require.Equal(t, "20 CUs", r.MemoryBound)
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
	require.Equal(t, []string{"external_calls_stubbed", "sink_not_measured"}, r.Caveats)
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

func TestRecommendTargetOnMeasuredPoint(t *testing.T) {
	t.Run("top point of brief fixture", func(t *testing.T) {
		r, err := Recommend(fixtureEntry(), 84, sizingCfg())
		require.NoError(t, err)
		require.False(t, r.ExceedsCeiling)
		require.Equal(t, 80, r.CUs)
		require.InDelta(t, 8, r.VCPU, 1e-9)
		require.False(t, r.Estimated)
		require.Equal(t, "30 CUs", r.MemoryBound)
	})

	t.Run("middle point", func(t *testing.T) {
		e := Entry{MemoryLimited: true, SinkMeasured: true, Points: []Point{
			{VCPU: 1, CUs: 10, CapacityMBps: 15, PeakRSSBytes: 1_000_000_000},
			{VCPU: 2, CUs: 20, CapacityMBps: 30, PeakRSSBytes: 2_000_000_000},
			{VCPU: 4, CUs: 40, CapacityMBps: 60, PeakRSSBytes: 4_000_000_000},
		}}
		r, err := Recommend(e, 21, sizingCfg())
		require.NoError(t, err)
		require.Equal(t, 20, r.CUs)
		require.InDelta(t, 2, r.VCPU, 1e-9)
		require.False(t, r.Estimated)
		require.Equal(t, "10 CUs", r.MemoryBound)
	})

	t.Run("fractional points", func(t *testing.T) {
		e := Entry{MemoryLimited: true, SinkMeasured: true, Points: []Point{
			{VCPU: 0.5, CUs: 5, CapacityMBps: 10, PeakRSSBytes: 500_000_000},
			{VCPU: 1.5, CUs: 15, CapacityMBps: 33, PeakRSSBytes: 1_500_000_000},
			{VCPU: 3, CUs: 30, CapacityMBps: 66, PeakRSSBytes: 3_000_000_000},
		}}
		r, err := Recommend(e, 30.8, sizingCfg())
		require.NoError(t, err)
		require.Equal(t, 20, r.CUs)
	})
}

func TestRecommendFloorCUsAndBelowFloorMemory(t *testing.T) {
	r, err := Recommend(fixtureEntry(), 10, sizingCfg())
	require.NoError(t, err)
	require.True(t, r.BelowFloor)
	require.Equal(t, 10, r.FloorCUs)
	require.Equal(t, "10 CUs", r.MemoryBound) // 1.5 GB -> 4 -> 10
	require.NotContains(t, r.Caveats, "memory_not_limited")

	e := fixtureEntry()
	e.MemoryLimited = false
	e.SinkMeasured = false
	r, err = Recommend(e, 10, sizingCfg())
	require.NoError(t, err)
	require.Equal(t, "not_measured", r.MemoryBound)
	require.Equal(t, []string{"memory_not_limited", "sink_not_measured"}, r.Caveats)

	e = fixtureEntry()
	e.Points = e.Points[1:] // first point is 2 vCPU
	r, err = Recommend(e, 40, sizingCfg())
	require.NoError(t, err)
	require.Equal(t, 20, r.FloorCUs)
}

func TestRecommendRejectsNonFiniteTarget(t *testing.T) {
	for _, target := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		_, err := Recommend(fixtureEntry(), target, sizingCfg())
		require.Error(t, err)
	}
}

func TestRecommendCaveatsSerialiseAsEmptyArray(t *testing.T) {
	r, err := Recommend(fixtureEntry(), 40, sizingCfg())
	require.NoError(t, err)
	b, err := json.Marshal(r)
	require.NoError(t, err)
	require.Contains(t, string(b), `"caveats":[]`)
	require.Contains(t, string(b), `"floor_cus":10`)

	r, err = Recommend(fixtureEntry(), 100, sizingCfg()) // exceeds ceiling
	require.NoError(t, err)
	b, err = json.Marshal(r)
	require.NoError(t, err)
	require.Contains(t, string(b), `"caveats":[]`)
}

func TestRecommendSourceRateLimited(t *testing.T) {
	limited := func(vcpus ...float64) Entry {
		e := fixtureEntry()
		for i := range e.Points {
			for _, v := range vcpus {
				if e.Points[i].VCPU == v {
					e.Points[i].LimitedBy = "producer"
				}
			}
		}
		return e
	}
	for name, tc := range map[string]struct {
		e      Entry
		target float64
		want   bool
	}{
		"interpolated, upper point limited":    {limited(4), 40, true},
		"interpolated, lower point limited":    {limited(2), 40, true},
		"interpolated, other point limited":    {limited(8), 40, false},
		"exact point limited":                  {limited(4), 49, true},
		"exact point, lower neighbour limited": {limited(2), 49, false},
		"below floor, floor limited":           {limited(1), 10, true},
		"exceeds ceiling, ceiling limited":     {limited(8), 100, true},
		"nothing limited":                      {fixtureEntry(), 40, false},
	} {
		t.Run(name, func(t *testing.T) {
			r, err := Recommend(tc.e, tc.target, sizingCfg())
			require.NoError(t, err)
			if tc.want {
				require.Contains(t, r.Caveats, CaveatSourceRateLimited)
			} else {
				require.NotContains(t, r.Caveats, CaveatSourceRateLimited)
			}
		})
	}
	require.Equal(t, "source_rate_limited", CaveatSourceRateLimited)
}

func TestRecommendUnreleasedCaveat(t *testing.T) {
	e := fixtureEntry()
	e.ConnectVersion = "v4.50.0-3-gabc123"
	r, err := Recommend(e, 40, sizingCfg())
	require.NoError(t, err)
	require.Equal(t, []string{CaveatUnreleasedBuild}, r.Caveats)

	e.ConnectVersion = "v4.50.0"
	r, err = Recommend(e, 40, sizingCfg())
	require.NoError(t, err)
	require.Empty(t, r.Caveats)
}

// Entry caveats (from the plan) merge with Recommend's own, de-duplicated
// and sorted, on every return path.
func TestRecommendMergesPlanCaveats(t *testing.T) {
	e := fixtureEntry()
	e.SinkMeasured = false
	e.Caveats = []string{"source_approximated", "sink_not_measured", "external_calls_stubbed"}
	e.ExternalStubbed = true
	e.ConnectVersion = "v4.50.0-3-gabc123"
	want := []string{"external_calls_stubbed", "sink_not_measured", "source_approximated", "unreleased_build"}
	for _, target := range []float64{1, 40, 1000} { // below floor, interpolated, over ceiling
		r, err := Recommend(e, target, sizingCfg())
		require.NoError(t, err)
		require.Equal(t, want, r.Caveats, "target %v", target)
	}
}
