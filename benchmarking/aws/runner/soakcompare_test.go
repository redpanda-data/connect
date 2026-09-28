// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// flatRSSProm builds rssSlopeMinSamples+ points at a constant RSS value, so
// rssSlopeBytesPerMin resolves to ok=true, slope~=0 — enough Prometheus
// history for the comparison table without a real slope obscuring the
// throughput/RSS-max assertions below.
func flatRSSProm(rssBytes uint64) []PromPoint {
	rss := make([]uint64, 12)
	for i := range rss {
		rss[i] = rssBytes
	}
	return promPointsAtRSS(rss...)
}

func TestBuildSoakComparisonMarkdown_OK(t *testing.T) {
	base := PointResult{
		Arm:     "base",
		Samples: make([]Sample, 100),
		Summary: Summary{MedianMBPerSec: 100, P5MBPerSec: 80, P95MBPerSec: 120, MedianMsgPerSec: 5000},
		Prom:    flatRSSProm(200_000_000),
	}
	pr := PointResult{
		Arm:     "pr",
		Samples: make([]Sample, 95),
		// 96% of base median throughput — above the 85% regression floor.
		Summary: Summary{MedianMBPerSec: 96, P5MBPerSec: 78, P95MBPerSec: 118, MedianMsgPerSec: 4900},
		// Same RSS max as base — well under the 130% regression ceiling.
		Prom: flatRSSProm(200_000_000),
	}

	md, err := BuildSoakComparisonMarkdown("postgres-orders-soak", []PointResult{base, pr}, "base", "pr")
	require.NoError(t, err)
	require.Contains(t, md, "postgres-orders-soak")
	require.Contains(t, md, "100 base samples, 95 PR samples")
	require.Contains(t, md, "| Median MB/s | 100.00 | 96.00 | -4.0% |")
	require.Contains(t, md, "| RSS max (MB) | 200.00 | 200.00 | +0.00 |")
	require.Contains(t, md, "**Verdict: OK**")
	require.NotContains(t, md, "REGRESSION")
}

func TestBuildSoakComparisonMarkdown_Regression(t *testing.T) {
	base := PointResult{
		Arm:     "base",
		Samples: make([]Sample, 100),
		Summary: Summary{MedianMBPerSec: 100, P5MBPerSec: 80, P95MBPerSec: 120, MedianMsgPerSec: 5000},
		Prom:    flatRSSProm(200_000_000),
	}
	pr := PointResult{
		Arm:     "pr",
		Samples: make([]Sample, 100),
		// 80% of base median throughput — below the 85% regression floor.
		Summary: Summary{MedianMBPerSec: 80, P5MBPerSec: 60, P95MBPerSec: 100, MedianMsgPerSec: 4000},
		// 150% of base RSS max — above the 130% regression ceiling.
		Prom: flatRSSProm(300_000_000),
	}

	md, err := BuildSoakComparisonMarkdown("postgres-orders-soak", []PointResult{base, pr}, "base", "pr")
	require.NoError(t, err)
	require.Contains(t, md, "**Verdict: REGRESSION**")
	require.Contains(t, md, "pr median throughput 80.00 MB/s is below 85% of base 100.00 MB/s")
	require.Contains(t, md, "pr RSS max 300 MB is above 130% of base 200 MB")
}

func TestBuildSoakComparisonMarkdown_MissingArmErrors(t *testing.T) {
	points := []PointResult{{Arm: "base", Summary: Summary{MedianMBPerSec: 100}}}

	_, err := BuildSoakComparisonMarkdown("x", points, "base", "pr")
	require.Error(t, err)
	require.Contains(t, err.Error(), `"pr"`)

	_, err = BuildSoakComparisonMarkdown("x", points, "not-base", "pr")
	require.Error(t, err)
	require.Contains(t, err.Error(), `"not-base"`)
}

// TestBuildSoakComparisonMarkdown_SlopeUnavailable covers both arms having
// too few Prometheus samples to fit a trend line: the row must render "n/a"
// rather than a fabricated zero.
func TestBuildSoakComparisonMarkdown_SlopeUnavailable(t *testing.T) {
	base := PointResult{Arm: "base", Summary: Summary{MedianMBPerSec: 10}, Prom: promPointsAtRSS(1, 2)}
	pr := PointResult{Arm: "pr", Summary: Summary{MedianMBPerSec: 10}, Prom: promPointsAtRSS(1, 2)}

	md, err := BuildSoakComparisonMarkdown("x", []PointResult{base, pr}, "base", "pr")
	require.NoError(t, err)
	require.Contains(t, md, "| RSS slope (MB/min) | n/a | n/a | n/a |")
}

func TestPctDelta_ZeroBaseIsNA(t *testing.T) {
	require.Equal(t, "n/a", pctDelta(0, 5))
}

func TestFindArmPoint(t *testing.T) {
	points := []PointResult{{Arm: "base"}, {Arm: "pr"}}
	p, ok := findArmPoint(points, "pr")
	require.True(t, ok)
	require.Equal(t, "pr", p.Arm)

	_, ok = findArmPoint(points, "missing")
	require.False(t, ok)
}

// TestBuildSoakComparisonMarkdown_TableShape is a light structural check
// that every advertised metric row is present, in a stable order, so a
// caller/workflow parsing this markdown doesn't need to re-derive the shape.
func TestBuildSoakComparisonMarkdown_TableShape(t *testing.T) {
	base := PointResult{Arm: "base", Summary: Summary{MedianMBPerSec: 10, P5MBPerSec: 8, P95MBPerSec: 12, MedianMsgPerSec: 500}}
	pr := PointResult{Arm: "pr", Summary: Summary{MedianMBPerSec: 10, P5MBPerSec: 8, P95MBPerSec: 12, MedianMsgPerSec: 500}}

	md, err := BuildSoakComparisonMarkdown("x", []PointResult{base, pr}, "base", "pr")
	require.NoError(t, err)
	wantOrder := []string{"Median MB/s", "P5 MB/s", "P95 MB/s", "Median records/s", "RSS max (MB)", "RSS slope (MB/min)", "Backlog max (s)"}
	lastIdx := -1
	for _, metric := range wantOrder {
		idx := strings.Index(md, metric)
		require.GreaterOrEqual(t, idx, 0, "missing row %q", metric)
		require.Greater(t, idx, lastIdx, "row %q out of order", metric)
		lastIdx = idx
	}
}
