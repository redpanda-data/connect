// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func mkSamples(values ...float64) []Sample {
	out := make([]Sample, len(values))
	for i, v := range values {
		out[i] = Sample{T: i, MBPerSec: v}
	}
	return out
}

func TestDetectAnomalies_NoDips(t *testing.T) {
	samples := mkSamples(100, 100, 100, 100, 100)
	require.Empty(t, DetectAnomalies(samples, 100))
}

func TestDetectAnomalies_BriefDipIgnored(t *testing.T) {
	// 30s dip — below the 60s threshold, ignored.
	vals := []float64{}
	for i := 0; i < 100; i++ {
		vals = append(vals, 100)
	}
	for i := 100; i < 130; i++ {
		vals = append(vals, 50)
	}
	for i := 130; i < 200; i++ {
		vals = append(vals, 100)
	}
	require.Empty(t, DetectAnomalies(mkSamples(vals...), 100))
}

func TestDetectAnomalies_LongDipReported(t *testing.T) {
	// 73s dip at 0.6× median — should be detected.
	vals := []float64{}
	for i := 0; i < 100; i++ {
		vals = append(vals, 100)
	}
	for i := 0; i < 73; i++ {
		vals = append(vals, 60)
	}
	for i := 0; i < 100; i++ {
		vals = append(vals, 100)
	}
	anomalies := DetectAnomalies(mkSamples(vals...), 100)
	require.Len(t, anomalies, 1)
	require.Equal(t, 100, anomalies[0].StartT)
	require.Equal(t, 73, anomalies[0].DurationSec)
	require.InDelta(t, 0.6, anomalies[0].MinRatio, 0.001)
}

func TestDetectAnomalies_MultipleDips(t *testing.T) {
	// two separate long dips
	vals := []float64{}
	for i := 0; i < 50; i++ {
		vals = append(vals, 100)
	}
	for i := 0; i < 70; i++ {
		vals = append(vals, 50)
	}
	for i := 0; i < 50; i++ {
		vals = append(vals, 100)
	}
	for i := 0; i < 65; i++ {
		vals = append(vals, 60)
	}
	for i := 0; i < 50; i++ {
		vals = append(vals, 100)
	}
	anomalies := DetectAnomalies(mkSamples(vals...), 100)
	require.Len(t, anomalies, 2)
}

func TestDetectAnomaliesWithProm_AttachesContext(t *testing.T) {
	// Throughput dip from T=60..130s; samples below threshold.
	samples := []Sample{}
	for i := 0; i < 200; i++ {
		mb := 100.0
		if i >= 60 && i < 130 {
			mb = 50.0 // dip
		}
		samples = append(samples, Sample{T: i, MBPerSec: mb})
	}
	// Prom snapshots every 10s starting at T=0.
	prom := []PromPoint{
		{T: 0, Goroutines: 100, HeapInUseMB: 50, GCPauseTotalNS: 100_000},
		{T: 10, Goroutines: 100, HeapInUseMB: 51, GCPauseTotalNS: 110_000},
		{T: 20, Goroutines: 100, HeapInUseMB: 51, GCPauseTotalNS: 120_000},
		{T: 30, Goroutines: 100, HeapInUseMB: 52, GCPauseTotalNS: 130_000},
		{T: 40, Goroutines: 100, HeapInUseMB: 52, GCPauseTotalNS: 140_000},
		{T: 50, Goroutines: 100, HeapInUseMB: 52, GCPauseTotalNS: 150_000},
		{T: 60, Goroutines: 500, HeapInUseMB: 1400, GCPauseTotalNS: 1_000_000_000}, // spike at dip start
		{T: 70, Goroutines: 510, HeapInUseMB: 1410, GCPauseTotalNS: 1_500_000_000},
	}
	anomalies := DetectAnomaliesWithProm(samples, 100.0, prom)
	require.Len(t, anomalies, 1)
	a := anomalies[0]
	require.Equal(t, 60, a.StartT)
	require.Equal(t, 500, a.GoroutinesAtStart)
	require.InDelta(t, 1400.0, a.HeapInUseMBAtStart, 0.1)
	// GCPauseDeltaNS = pause_at_T60 - pause_at_T50 = 1_000_000_000 - 150_000 = 999_850_000
	require.Equal(t, uint64(999_850_000), a.GCPauseDeltaNS)
}

func TestDetectAnomaliesWithProm_NoPromKeepsZeros(t *testing.T) {
	samples := []Sample{}
	for i := 0; i < 200; i++ {
		mb := 100.0
		if i >= 60 && i < 130 {
			mb = 50.0
		}
		samples = append(samples, Sample{T: i, MBPerSec: mb})
	}
	anomalies := DetectAnomaliesWithProm(samples, 100.0, nil)
	require.Len(t, anomalies, 1)
	require.Equal(t, 0, anomalies[0].GoroutinesAtStart)
	require.Equal(t, uint64(0), anomalies[0].GCPauseDeltaNS)
}
