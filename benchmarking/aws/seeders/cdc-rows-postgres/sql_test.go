// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkerRowCounts(t *testing.T) {
	tests := []struct {
		name    string
		rows    int64
		workers int
		want    int64 // expected sum, checked below alongside per-worker invariants
	}{
		{name: "evenly divisible", rows: 1_000_000, workers: 16, want: 1_000_000},
		{name: "fewer rows than workers", rows: 10, workers: 16, want: 10},
		{name: "single row", rows: 1, workers: 16, want: 1},
		{name: "zero rows", rows: 0, workers: 16, want: 0},
		{name: "not evenly divisible", rows: 1_000_001, workers: 16, want: 1_000_001},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			counts := workerRowCounts(tt.rows, tt.workers)
			require.Len(t, counts, tt.workers)

			var sum int64
			for _, c := range counts {
				require.GreaterOrEqual(t, c, int64(0))
				sum += c
			}
			require.Equal(t, tt.want, sum)

			// The spread must never exceed one row between the largest and
			// smallest share, otherwise the remainder wasn't distributed
			// evenly (e.g. it was dumped entirely on the first worker).
			var min, max int64 = counts[0], counts[0]
			for _, c := range counts {
				if c < min {
					min = c
				}
				if c > max {
					max = c
				}
			}
			require.LessOrEqual(t, max-min, int64(1))
		})
	}
}

func TestWorkerRowCounts_ExactShares(t *testing.T) {
	// rows=10, workers=16: the first 10 workers get exactly 1 row each and
	// the rest get 0 — this is the case that used to seed zero rows total.
	counts := workerRowCounts(10, 16)
	for w, c := range counts {
		if w < 10 {
			require.Equal(t, int64(1), c, "worker %d", w)
		} else {
			require.Equal(t, int64(0), c, "worker %d", w)
		}
	}
}

func TestPerWorkerRate(t *testing.T) {
	tests := []struct {
		name    string
		rate    int
		workers int
		want    int
	}{
		{name: "declared soak rate", rate: 10_000, workers: 16, want: 10_000},
		{name: "rate below worker count", rate: 3, workers: 16, want: 3},
		{name: "large rate", rate: 160_000, workers: 16, want: 160_000},
		{name: "zero rate", rate: 0, workers: 16, want: 0},
		{name: "not evenly divisible", rate: 10_001, workers: 16, want: 10_001},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rates := perWorkerRate(tt.rate, tt.workers)
			require.Len(t, rates, tt.workers)

			var sum int
			for _, r := range rates {
				require.GreaterOrEqual(t, r, 0)
				sum += r
			}
			require.Equal(t, tt.want, sum)
		})
	}
}

func TestTickCounts(t *testing.T) {
	tests := []struct {
		name          string
		ratePerWorker int
	}{
		{name: "evenly divisible by ticks", ratePerWorker: 625},
		{name: "less than one tick", ratePerWorker: 1},
		{name: "zero", ratePerWorker: 0},
		{name: "large per-worker rate", ratePerWorker: 10_000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			counts := tickCounts(tt.ratePerWorker)
			require.Len(t, counts, ticksPerSecond)

			var sum int
			for _, c := range counts {
				require.GreaterOrEqual(t, c, 0)
				sum += c
			}
			require.Equal(t, tt.ratePerWorker, sum)
		})
	}
}

// TestWorkloadRateEndToEnd pins the exact totals required by the soak
// scenarios: distributing a declared rate across workers and then across
// each worker's 10 ticks/sec must land on the declared rate exactly, with
// no compounding truncation.
func TestWorkloadRateEndToEnd(t *testing.T) {
	tests := []struct {
		name    string
		rate    int
		workers int
	}{
		{name: "shipped soak rate", rate: 10_000, workers: 16},
		{name: "rate below worker count", rate: 3, workers: 16},
		{name: "large rate", rate: 160_000, workers: 16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rates := perWorkerRate(tt.rate, tt.workers)

			var total int
			for _, ratePerWorker := range rates {
				counts := tickCounts(ratePerWorker)
				for _, c := range counts {
					total += c
				}
			}
			require.Equal(t, tt.rate, total)
		})
	}
}
