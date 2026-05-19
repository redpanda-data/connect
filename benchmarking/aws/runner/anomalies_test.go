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
