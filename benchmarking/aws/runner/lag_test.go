// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComputeBacklog_NilWhenRateOrSeriesEmpty(t *testing.T) {
	series := []TopicPoint{{T: 10, MsgPerSec: 100, IntervalSec: 10}}
	require.Nil(t, ComputeBacklog(nil, 100))
	require.Nil(t, ComputeBacklog(series, 0))
	require.Nil(t, ComputeBacklog(series, -1))
	require.Nil(t, ComputeBacklog(nil, 0))
}

func TestComputeBacklog_TableDriven(t *testing.T) {
	tests := []struct {
		name           string
		series         []TopicPoint
		recordsPerSec  float64
		wantBacklog    []float64 // BacklogRecords per point, in order
		wantBacklogSec []float64
	}{
		{
			name: "on-pace: delivered exactly matches expected, backlog stays zero",
			// 100 rec/s delivered every 10s interval, matching a 100 rec/s target:
			// at T=10, delivered=1000, expected=1000, backlog=0. Same at T=20.
			series: []TopicPoint{
				{T: 10, MsgPerSec: 100, IntervalSec: 10},
				{T: 20, MsgPerSec: 100, IntervalSec: 10},
			},
			recordsPerSec:  100,
			wantBacklog:    []float64{0, 0},
			wantBacklogSec: []float64{0, 0},
		},
		{
			name: "source-behind: engine delivers half the target rate, backlog grows",
			// 50 rec/s delivered against a 100 rec/s target:
			// T=10: delivered=500, expected=1000, backlog=500, backlogSec=5.
			// T=20: delivered=1000, expected=2000, backlog=1000, backlogSec=10.
			series: []TopicPoint{
				{T: 10, MsgPerSec: 50, IntervalSec: 10},
				{T: 20, MsgPerSec: 50, IntervalSec: 10},
			},
			recordsPerSec:  100,
			wantBacklog:    []float64{500, 1000},
			wantBacklogSec: []float64{5, 10},
		},
		{
			name: "catch-up: engine falls behind then exceeds the target rate and drains the backlog to zero",
			// T=10: delivered=500 (50/s), expected=1000, backlog=500.
			// T=20: delivered=500+1500=2000 (150/s for the next 10s), expected=2000,
			// backlog=0 — fully caught up, clamped at zero rather than negative.
			series: []TopicPoint{
				{T: 10, MsgPerSec: 50, IntervalSec: 10},
				{T: 20, MsgPerSec: 150, IntervalSec: 10},
			},
			recordsPerSec:  100,
			wantBacklog:    []float64{500, 0},
			wantBacklogSec: []float64{5, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeBacklog(tt.series, tt.recordsPerSec)
			require.Len(t, got, len(tt.series))
			for i, p := range got {
				require.Equal(t, tt.series[i].T, p.T)
				require.InDelta(t, tt.wantBacklog[i], p.BacklogRecords, 0.001, "point %d BacklogRecords", i)
				require.InDelta(t, tt.wantBacklogSec[i], p.BacklogSec, 0.001, "point %d BacklogSec", i)
			}
		})
	}
}

func TestComputeBacklog_MissingRecordsMetricLeavesRateAtZero(t *testing.T) {
	// A point whose MsgPerSec is zero (records metric missing or reset —
	// see brokermetrics.go's ParseTopicSeries) contributes nothing to
	// delivered, so backlog grows exactly as if the engine stalled — the
	// behavior a soak run needs to catch.
	series := []TopicPoint{
		{T: 10, MsgPerSec: 100, IntervalSec: 10},
		{T: 20, MsgPerSec: 0, IntervalSec: 10}, // stall
	}
	got := ComputeBacklog(series, 100)
	require.Len(t, got, 2)
	require.InDelta(t, 0, got[0].BacklogRecords, 0.001)
	require.InDelta(t, 1000, got[1].BacklogRecords, 0.001) // expected=2000, delivered stuck at 1000
}
