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
}
