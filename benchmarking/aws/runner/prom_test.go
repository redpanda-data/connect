// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSnapshots_TwoFrames(t *testing.T) {
	raw := `###timestamp=1747856130
go_goroutines 312
process_cpu_seconds_total 87.4
###timestamp=1747856140
go_goroutines 314
process_cpu_seconds_total 92.1
`
	snaps := parseSnapshots(strings.NewReader(raw))
	require.Len(t, snaps, 2)
	require.Equal(t, int64(1747856130), snaps[0].UnixTime)
	require.Contains(t, snaps[0].Body, "go_goroutines 312")
	require.False(t, snaps[0].Errored)
	require.Equal(t, int64(1747856140), snaps[1].UnixTime)
	require.Contains(t, snaps[1].Body, "go_goroutines 314")
}

func TestParseSnapshots_ErrorFrame(t *testing.T) {
	raw := `###timestamp=1747856130
go_goroutines 312
###timestamp=1747856140
###scrape_error
###timestamp=1747856150
go_goroutines 314
`
	snaps := parseSnapshots(strings.NewReader(raw))
	require.Len(t, snaps, 3)
	require.False(t, snaps[0].Errored)
	require.True(t, snaps[1].Errored, "frame with ###scrape_error marked")
	require.False(t, snaps[2].Errored)
}

func TestParseSnapshots_IgnoresLeadingNoise(t *testing.T) {
	raw := `noisy line
another
###timestamp=100
go_goroutines 1
`
	snaps := parseSnapshots(strings.NewReader(raw))
	require.Len(t, snaps, 1)
	require.Equal(t, int64(100), snaps[0].UnixTime)
}

func TestParseSnapshots_TruncatedLastFrame(t *testing.T) {
	raw := `###timestamp=100
go_goroutines 1
###timestamp=200
`
	snaps := parseSnapshots(strings.NewReader(raw))
	require.Len(t, snaps, 2)
	require.Empty(t, strings.TrimSpace(snaps[1].Body))
}

func TestParseSnapshots_Empty(t *testing.T) {
	snaps := parseSnapshots(strings.NewReader(""))
	require.Empty(t, snaps)
}

func TestExtractPromPoint_FromRealFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/prom-sample.txt")
	require.NoError(t, err)
	body := string(raw)

	pp, ok := extractPromPoint(promSnapshot{UnixTime: 1747856130, Body: body})
	require.True(t, ok)
	require.Greater(t, pp.Goroutines, 0)
	require.Greater(t, pp.HeapInUseMB, 0.0)
	require.GreaterOrEqual(t, pp.BytesTotal, 0.0)
	require.GreaterOrEqual(t, pp.CPUSeconds, 0.0)
	require.GreaterOrEqual(t, pp.GCPauseTotalNS, uint64(0))
}

func TestExtractPromPoint_SyntheticAllMetrics(t *testing.T) {
	body := `# HELP go_goroutines blah
# TYPE go_goroutines gauge
go_goroutines 312
go_memstats_heap_inuse_bytes 1.04857e+08
go_memstats_gc_pause_total_ns 4.2e+07
process_cpu_seconds_total 87.4
benchmark_bytes_total 4.12e+09
`
	pp, ok := extractPromPoint(promSnapshot{UnixTime: 100, Body: body})
	require.True(t, ok)
	require.Equal(t, 312, pp.Goroutines)
	require.InDelta(t, 104.857, pp.HeapInUseMB, 0.01) // 1.04857e+08 B / 1e6 = 104.857 MB
	require.InDelta(t, 4.12e+09, pp.BytesTotal, 1.0)
	require.InDelta(t, 87.4, pp.CPUSeconds, 0.01)
	require.Equal(t, uint64(42000000), pp.GCPauseTotalNS)
}

func TestExtractPromPoint_ErrorSnapshotSkipped(t *testing.T) {
	_, ok := extractPromPoint(promSnapshot{UnixTime: 100, Errored: true, Body: ""})
	require.False(t, ok)
}

func TestExtractPromPoint_PartialMetricsOK(t *testing.T) {
	body := `go_goroutines 50
go_memstats_heap_inuse_bytes 1.0485e+07
`
	pp, ok := extractPromPoint(promSnapshot{UnixTime: 1, Body: body})
	require.True(t, ok)
	require.Equal(t, 50, pp.Goroutines)
	require.InDelta(t, 10.485, pp.HeapInUseMB, 0.001) // 1.0485e+07 B / 1e6 = 10.485 MB
	require.Equal(t, 0.0, pp.CPUSeconds)        // missing — zero is OK
	require.Equal(t, uint64(0), pp.GCPauseTotalNS)
}

func TestParsePromStream_EndToEnd(t *testing.T) {
	raw := `noise
###timestamp=1000
go_goroutines 10
###timestamp=1010
go_goroutines 12
###timestamp=1020
###scrape_error
###timestamp=1030
go_goroutines 14
`
	pts, err := ParsePromStream(strings.NewReader(raw))
	require.NoError(t, err)
	require.Len(t, pts, 3) // error frame is dropped
	require.Equal(t, 0, pts[0].T)
	require.Equal(t, 10, pts[1].T) // 1010 - 1000
	require.Equal(t, 30, pts[2].T) // 1030 - 1000 (gap kept; the error frame's UnixTime is discarded with its data)
	require.Equal(t, 10, pts[0].Goroutines)
	require.Equal(t, 14, pts[2].Goroutines)
}

func TestParsePromStream_EmptyReader(t *testing.T) {
	pts, err := ParsePromStream(strings.NewReader(""))
	require.NoError(t, err)
	require.Empty(t, pts)
}
