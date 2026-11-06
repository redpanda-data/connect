// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitChunkRanges(t *testing.T) {
	samplesOf := func(n int) []map[string]any {
		samples := make([]map[string]any, n)
		for i := range samples {
			samples[i] = map[string]any{"ID": i}
		}
		return samples
	}

	tests := []struct {
		name       string
		numSamples int
		maxWorkers int
		wantRanges int
	}{
		{name: "no samples", numSamples: 0, maxWorkers: 50, wantRanges: 1},
		{name: "single sample", numSamples: 1, maxWorkers: 50, wantRanges: 1},
		{name: "fewer samples than workers", numSamples: 3, maxWorkers: 50, wantRanges: 3},
		{name: "evenly divisible", numSamples: 1600, maxWorkers: 50, wantRanges: 50},
		{name: "rounding down would add a chunk", numSamples: 1599, maxWorkers: 50, wantRanges: 50},
		{name: "remainder just over a multiple", numSamples: 1601, maxWorkers: 50, wantRanges: 49},
		{name: "single worker", numSamples: 100, maxWorkers: 1, wantRanges: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ranges := splitChunkRanges(samplesOf(tt.numSamples), tt.maxWorkers)

			require.Len(t, ranges, tt.wantRanges)
			assert.LessOrEqual(t, len(ranges), tt.maxWorkers, "a table must never be split into more chunks than workers")
			assert.Nil(t, ranges[0].min, "the first range must be unbounded below")
			assert.Nil(t, ranges[len(ranges)-1].max, "the last range must be unbounded above")
			for i := 1; i < len(ranges); i++ {
				assert.Equal(t, ranges[i-1].max, ranges[i].min, "range %d must start where range %d ends", i, i-1)
			}
		})
	}
}
