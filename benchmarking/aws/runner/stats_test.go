// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRollingStatsLine_OK(t *testing.T) {
	line := `INFO rolling stats: 99000 msg/sec, 204 MB/sec  @service=redpanda-connect bytes/sec=2.038e+08 label="" msg/sec=99000 path=root.output.processors.0`
	s, ok := ParseRollingStatsLine(line)
	require.True(t, ok)
	require.Equal(t, 99000.0, s.MsgPerSec)
	require.Equal(t, 204.0, s.MBPerSec)
}

func TestParseRollingStatsLine_IgnoresUnrelated(t *testing.T) {
	_, ok := ParseRollingStatsLine(`INFO starting input`)
	require.False(t, ok)
}

func TestParseRollingStatsLine_Float(t *testing.T) {
	line := `INFO rolling stats: 99316.5 msg/sec, 204.7 MB/sec`
	s, ok := ParseRollingStatsLine(line)
	require.True(t, ok)
	require.InDelta(t, 99316.5, s.MsgPerSec, 0.01)
	require.InDelta(t, 204.7, s.MBPerSec, 0.01)
}

// The benchmark processor emits bytes/sec via humanize.Bytes, which uses
// SI suffixes (B, kB, MB, GB). All units must be parsed and normalized to
// MB/sec so percentiles across a sweep are computed in the same unit.
func TestParseRollingStatsLine_BytesUnit(t *testing.T) {
	s, ok := ParseRollingStatsLine(`INFO rolling stats: 5 msg/sec, 500 B/sec`)
	require.True(t, ok)
	require.Equal(t, 5.0, s.MsgPerSec)
	require.InDelta(t, 0.0005, s.MBPerSec, 1e-9) // 500 B = 0.0005 MB
}

func TestParseRollingStatsLine_KilobytesUnit(t *testing.T) {
	// humanize.Bytes uses lowercase k: "204 kB/sec".
	s, ok := ParseRollingStatsLine(`INFO rolling stats: 250 msg/sec, 204 kB/sec`)
	require.True(t, ok)
	require.Equal(t, 250.0, s.MsgPerSec)
	require.InDelta(t, 0.204, s.MBPerSec, 1e-9) // 204 kB = 0.204 MB
}

func TestParseRollingStatsLine_GigabytesUnit(t *testing.T) {
	s, ok := ParseRollingStatsLine(`INFO rolling stats: 2000000 msg/sec, 1.5 GB/sec`)
	require.True(t, ok)
	require.Equal(t, 2000000.0, s.MsgPerSec)
	require.InDelta(t, 1500.0, s.MBPerSec, 1e-9) // 1.5 GB = 1500 MB
}

func TestSummarise_BasicPercentiles(t *testing.T) {
	samples := make([]Sample, 100)
	for i := 0; i < 100; i++ {
		samples[i] = Sample{T: i, MBPerSec: float64(i + 1)} // 1..100
	}
	sum := Summarise(samples)
	require.Equal(t, 50.5, sum.MedianMBPerSec)
	require.Equal(t, 5.5, sum.P5MBPerSec)
	require.Equal(t, 95.5, sum.P95MBPerSec)
	require.Equal(t, 100.0, sum.PeakMBPerSec)
}

func TestSummarise_EmptyInput(t *testing.T) {
	require.Equal(t, Summary{}, Summarise(nil))
	require.Equal(t, Summary{}, Summarise([]Sample{}))
}

func TestSummariseTopicPoints_Mean(t *testing.T) {
	// Bursty committer: mostly-zero per-interval rates with spikes.
	pts := []TopicPoint{{T: 10, MBPerSec: 0}, {T: 20, MBPerSec: 0}, {T: 30, MBPerSec: 10}, {T: 40, MBPerSec: 0}}
	s := SummariseTopicPoints(pts)
	if s.MedianMBPerSec != 0 {
		t.Errorf("median of bursty series = %v, want 0 (this is why median is unfair)", s.MedianMBPerSec)
	}
	if s.MeanMBPerSec < 2.49 || s.MeanMBPerSec > 2.51 {
		t.Errorf("mean = %v, want 2.5 (10/4)", s.MeanMBPerSec)
	}
}

func TestParseRollingStatsStream(t *testing.T) {
	in := strings.NewReader(`INFO startup
INFO rolling stats: 100 msg/sec, 1 MB/sec
INFO rolling stats: 200 msg/sec, 2 MB/sec
INFO unrelated
INFO rolling stats: 300 msg/sec, 3 MB/sec
`)
	samples, err := ParseRollingStatsStream(in)
	require.NoError(t, err)
	require.Len(t, samples, 3)
	require.Equal(t, 3.0, samples[2].MBPerSec)
	require.Equal(t, 0, samples[0].T)
	require.Equal(t, 1, samples[1].T)
	require.Equal(t, 2, samples[2].T)
}
