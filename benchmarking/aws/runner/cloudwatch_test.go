// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAggregateSoakMinutes_NoDataReturnsUnchangedHighWater(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	noneYet := soakHighWaterMarks{Samples: -1, Prom: -1, Broker: -1, Backlog: -1}
	data, newMarks := aggregateSoakMinutes(nil, nil, nil, nil, base, noneYet)
	require.Nil(t, data)
	require.Equal(t, noneYet, newMarks)

	// A pre-existing high-water mark must survive untouched too — a cycle
	// that fetched nothing new (e.g. the checkpoint hasn't landed yet) must
	// never regress any family's mark.
	allAtSeven := soakHighWaterMarks{Samples: 7, Prom: 7, Broker: 7, Backlog: 7}
	data, newMarks = aggregateSoakMinutes(nil, nil, nil, nil, base, allAtSeven)
	require.Nil(t, data)
	require.Equal(t, allAtSeven, newMarks)
}

func TestAggregateSoakMinutes_BucketsMeansAndLastsExcludingIncompleteMinute(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Every family carries a point past minute 1 so minute 1 is complete
	// FROM EACH FAMILY'S OWN PERSPECTIVE — aggregateSoakMinutes now derives
	// "current incomplete minute" per family, not from a single shared
	// maximum across all four series (see soakHighWaterMarks).
	broker := []TopicPoint{
		{T: 0, MBPerSec: 1, MsgPerSec: 10},
		{T: 30, MBPerSec: 3, MsgPerSec: 30}, // minute 0: mean 2 / 20
		{T: 60, MBPerSec: 5, MsgPerSec: 50},
		{T: 90, MBPerSec: 7, MsgPerSec: 70},  // minute 1: mean 6 / 60
		{T: 120, MBPerSec: 9, MsgPerSec: 90}, // minute 2: broker's own current incomplete minute
	}
	samples := []Sample{
		{T: 0, MBPerSec: 100},
		{T: 30, MBPerSec: 300}, // minute 0: mean 200
		{T: 60, MBPerSec: 500},
		{T: 90, MBPerSec: 700},  // minute 1: mean 600
		{T: 120, MBPerSec: 900}, // minute 2: samples' own current incomplete minute
	}
	prom := []PromPoint{
		{T: 0, RSSBytes: 100, HeapInUseMB: 10, Goroutines: 5},
		{T: 50, RSSBytes: 200, HeapInUseMB: 20, Goroutines: 6},  // minute 0: last of these two
		{T: 65, RSSBytes: 300, HeapInUseMB: 30, Goroutines: 7},  // minute 1: last
		{T: 125, RSSBytes: 400, HeapInUseMB: 40, Goroutines: 8}, // minute 2: prom's own current incomplete minute
	}
	backlog := []BacklogPoint{
		{T: 0, BacklogSec: 5},    // minute 0
		{T: 60, BacklogSec: 15},  // minute 1
		{T: 120, BacklogSec: 25}, // minute 2: backlog's own current incomplete minute
	}

	noneYet := soakHighWaterMarks{Samples: -1, Prom: -1, Broker: -1, Backlog: -1}
	data, newMarks := aggregateSoakMinutes(samples, prom, broker, backlog, base, noneYet)
	require.Equal(t, soakHighWaterMarks{Samples: 1, Prom: 1, Broker: 1, Backlog: 1}, newMarks,
		"each family's own current incomplete minute (2) must never be emitted")

	// Datums are grouped per family (broker, samples, prom, backlog), not
	// interleaved per minute — a consequence of each family now walking its
	// own minute range independently.
	want := []MetricDatum{
		{Name: metricThroughputMBps, Value: 2, Unit: unitMegabytesPerSecond, At: base},
		{Name: metricRecordsPerSec, Value: 20, Unit: unitCountPerSecond, At: base},
		{Name: metricThroughputMBps, Value: 6, Unit: unitMegabytesPerSecond, At: base.Add(time.Minute)},
		{Name: metricRecordsPerSec, Value: 60, Unit: unitCountPerSecond, At: base.Add(time.Minute)},

		{Name: metricLogThroughputMBps, Value: 200, Unit: unitMegabytesPerSecond, At: base},
		{Name: metricLogThroughputMBps, Value: 600, Unit: unitMegabytesPerSecond, At: base.Add(time.Minute)},

		{Name: metricRSSBytes, Value: 200, Unit: unitBytes, At: base},
		{Name: metricHeapInUseBytes, Value: 20_000_000, Unit: unitBytes, At: base},
		{Name: metricGoroutines, Value: 6, Unit: unitCount, At: base},
		{Name: metricRSSBytes, Value: 300, Unit: unitBytes, At: base.Add(time.Minute)},
		{Name: metricHeapInUseBytes, Value: 30_000_000, Unit: unitBytes, At: base.Add(time.Minute)},
		{Name: metricGoroutines, Value: 7, Unit: unitCount, At: base.Add(time.Minute)},

		{Name: metricBacklogSeconds, Value: 5, Unit: unitSeconds, At: base},
		{Name: metricBacklogSeconds, Value: 15, Unit: unitSeconds, At: base.Add(time.Minute)},
	}
	require.Equal(t, want, data)
}

func TestAggregateSoakMinutes_SinceMinuteSkipsAlreadyEmitted(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	broker := []TopicPoint{
		{T: 0, MBPerSec: 1, MsgPerSec: 10},
		{T: 60, MBPerSec: 2, MsgPerSec: 20},
		{T: 120, MBPerSec: 3, MsgPerSec: 30}, // current incomplete minute (2)
	}
	// Broker's mark at 0 (minute 0 already emitted by an earlier cycle) must
	// only yield minute 1 this time, not a repeat of minute 0. The other
	// three families have never seen data, so their marks stay at -1.
	since := soakHighWaterMarks{Samples: -1, Prom: -1, Broker: 0, Backlog: -1}
	data, newMarks := aggregateSoakMinutes(nil, nil, broker, nil, base, since)
	require.Equal(t, soakHighWaterMarks{Samples: -1, Prom: -1, Broker: 1, Backlog: -1}, newMarks)
	require.Len(t, data, 2, "only ThroughputMBps and RecordsPerSec have data — no samples/prom/backlog in this call")
	for _, d := range data {
		require.Equal(t, base.Add(time.Minute), d.At, "must be minute 1, not a re-emit of minute 0")
	}
}

func TestAggregateSoakMinutes_BacklogOmittedWhenSeriesEmpty(t *testing.T) {
	// ComputeBacklog returns nil when ExpectedRecordsPerSec <= 0 — the
	// non-soak / no-expected-rate case. aggregateSoakMinutes must not
	// synthesize a BacklogSeconds datum out of nothing.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	broker := []TopicPoint{
		{T: 0, MBPerSec: 1, MsgPerSec: 10},
		{T: 120, MBPerSec: 2, MsgPerSec: 20},
	}
	noneYet := soakHighWaterMarks{Samples: -1, Prom: -1, Broker: -1, Backlog: -1}
	data, _ := aggregateSoakMinutes(nil, nil, broker, nil, base, noneYet)
	for _, d := range data {
		require.NotEqual(t, metricBacklogSeconds, d.Name)
	}
}

// TestAggregateSoakMinutes_PerFamilyMarksAvoidPermanentHoles is the
// regression test for Finding #D: a single shared high-water mark used to
// advance off the MAX minute across all four series, so a family that
// missed a cycle entirely (e.g. fetchBrokerSeries failing while
// fetchLog/fetchProm succeeded) found its own minutes already behind the
// shared mark once it recovered — permanently skipped, a silent hole in
// ThroughputMBps/RecordsPerSec that a CloudWatch alarm watching for gaps
// would miss. Per-family marks fix this: cycle 1 emits log+prom for
// minutes 0-3 with NO broker data (simulating the failed fetch); cycle 2
// brings broker data for minutes 0-3, which must now be emitted in full —
// while log/prom, whose own marks already cover 0-3, must NOT be
// re-emitted (CloudWatch treats a repeated (metric, timestamp) pair as a
// duplicate datum, not an idempotent update).
func TestAggregateSoakMinutes_PerFamilyMarksAvoidPermanentHoles(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Minutes 0-3 complete, minute 4 is the still-open one — for both
	// samples and prom.
	var samples []Sample
	var prom []PromPoint
	for i := 0; i <= 4; i++ {
		t := i * secondsPerMinute
		samples = append(samples, Sample{T: t, MBPerSec: float64(10 + i)})
		prom = append(prom, PromPoint{T: t, RSSBytes: uint64(100 + i)})
	}

	noneYet := soakHighWaterMarks{Samples: -1, Prom: -1, Broker: -1, Backlog: -1}

	// Cycle 1: broker fetch failed this cycle -> empty slice, non-fatal.
	data1, marks1 := aggregateSoakMinutes(samples, prom, nil, nil, base, noneYet)
	require.Equal(t, soakHighWaterMarks{Samples: 3, Prom: 3, Broker: -1, Backlog: -1}, marks1,
		"log/prom must advance on their own data even with no broker series this cycle")
	for _, d := range data1 {
		require.NotEqual(t, metricThroughputMBps, d.Name, "no broker data yet -> no broker datum")
		require.NotEqual(t, metricRecordsPerSec, d.Name)
	}
	require.NotEmpty(t, data1, "log/prom minutes 0-3 must still be emitted despite the broker gap")

	// Cycle 2: the broker fetch recovers with minutes 0-3 (and an open
	// minute 4), log/prom content is unchanged (same checkpoint, nothing
	// new to report).
	var broker []TopicPoint
	for i := 0; i <= 4; i++ {
		broker = append(broker, TopicPoint{T: i * secondsPerMinute, MBPerSec: 5, MsgPerSec: 50})
	}
	data2, marks2 := aggregateSoakMinutes(samples, prom, broker, nil, base, marks1)
	require.Equal(t, soakHighWaterMarks{Samples: 3, Prom: 3, Broker: 3, Backlog: -1}, marks2,
		"the recovered broker series must catch all the way up to minute 3, not stay stuck behind the earlier gap")

	var sawThroughputMinutes []time.Time
	for _, d := range data2 {
		require.NotEqual(t, metricLogThroughputMBps, d.Name, "log minutes 0-3 were already emitted in cycle 1")
		require.NotEqual(t, metricRSSBytes, d.Name, "prom minutes 0-3 were already emitted in cycle 1")
		if d.Name == metricThroughputMBps {
			sawThroughputMinutes = append(sawThroughputMinutes, d.At)
		}
	}
	require.ElementsMatch(t, []time.Time{base, base.Add(time.Minute), base.Add(2 * time.Minute), base.Add(3 * time.Minute)}, sawThroughputMinutes,
		"broker minutes 0-3 must be emitted in full once the fetch recovers, with no permanent hole")
}

// TestAggregateSoakMinutes_WarmupOffsetAlignment is the regression test for
// the subtlety flagged in the increment's brief: parseAndTrim reindexes a
// Connect log's samples so T=0 means "end of warmup" (wall-clock
// base+warmup), while prom/broker/backlog series all have T=0 mean
// wall-clock base. Calling aggregateSoakMinutes with raw (unshifted) samples
// alongside pointStart-based prom/broker data would silently misalign every
// rate pairing by exactly the warmup duration. offsetSampleT is what fixes
// that up before the call — this test proves both the bug (without the
// shift) and the fix (with it).
func TestAggregateSoakMinutes_WarmupOffsetAlignment(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const warmupSec = 90 // 1.5 minutes

	// Connect's log, post-parseAndTrim: T=0 is really base+90s (minute 1).
	rawSamples := []Sample{
		{T: 0, MBPerSec: 111},  // really base+90s -> minute 1
		{T: 30, MBPerSec: 222}, // really base+120s -> minute 2 (current incomplete)
	}
	// Broker series is never warmup-trimmed: T is already base-relative.
	broker := []TopicPoint{
		{T: 90, MBPerSec: 999, MsgPerSec: 9}, // minute 1, same wall-clock instant as rawSamples[0]
		{T: 120, MBPerSec: 1, MsgPerSec: 1},  // minute 2 (current incomplete)
	}

	noneYet := soakHighWaterMarks{Samples: -1, Prom: -1, Broker: -1, Backlog: -1}

	// Without the shift, the log sample lands in minute 0 — a minute the
	// broker series has no data for at all, and the wrong minute besides.
	unshifted, _ := aggregateSoakMinutes(rawSamples, nil, broker, nil, base, noneYet)
	for _, d := range unshifted {
		if d.Name == metricLogThroughputMBps {
			require.NotEqual(t, base.Add(time.Minute), d.At,
				"BUG reproduced: unshifted samples land a minute early")
		}
	}

	shifted := offsetSampleT(rawSamples, warmupSec)
	data, newMarks := aggregateSoakMinutes(shifted, nil, broker, nil, base, noneYet)
	require.Equal(t, soakHighWaterMarks{Samples: 1, Prom: -1, Broker: 1, Backlog: -1}, newMarks)

	var gotLog, gotThroughput bool
	for _, d := range data {
		if d.Name == metricLogThroughputMBps {
			gotLog = true
			require.Equal(t, float64(111), d.Value)
			require.Equal(t, base.Add(time.Minute), d.At,
				"log sample must land in minute 1, the SAME minute the broker series reports for this wall-clock instant")
		}
		if d.Name == metricThroughputMBps {
			gotThroughput = true
			require.Equal(t, base.Add(time.Minute), d.At)
		}
	}
	require.True(t, gotLog)
	require.True(t, gotThroughput)
}

func TestOffsetSampleT(t *testing.T) {
	in := []Sample{{T: 0, MBPerSec: 1}, {T: 10, MBPerSec: 2}}
	out := offsetSampleT(in, 90)
	require.Equal(t, []Sample{{T: 90, MBPerSec: 1}, {T: 100, MBPerSec: 2}}, out)
	// The input slice must not be mutated — offsetSampleT is called from the
	// mid-run emit loop on the SAME parsed samples the loop will keep using
	// for the next cycle's own re-parse of the growing checkpoint file, and
	// separately the final emit reuses Run's own `samples` var — either one
	// aliasing the original would silently corrupt the other.
	require.Equal(t, 0, in[0].T)
	require.Equal(t, 10, in[1].T)

	// offsetSampleT(0) is a no-op that returns the SAME slice (not a copy) —
	// exercised because the sweep (non-soak) path always calls with
	// offsetSec 0 today and must incur zero allocation overhead.
	same := offsetSampleT(in, 0)
	require.Same(t, &in[0], &same[0])
}

func TestFakeEmitter_RecordsAndFlattensCalls(t *testing.T) {
	f := &FakeEmitter{}
	require.NoError(t, f.Emit(nil, []MetricDatum{{Name: "a", Value: 1}}))
	require.NoError(t, f.Emit(nil, []MetricDatum{{Name: "b", Value: 2}, {Name: "c", Value: 3}}))
	require.Len(t, f.Calls, 2)
	require.Equal(t, []MetricDatum{{Name: "a", Value: 1}, {Name: "b", Value: 2}, {Name: "c", Value: 3}}, f.All())
}

func TestFakeEmitter_ErrShortCircuitsWithoutRecording(t *testing.T) {
	wantErr := fmt.Errorf("cloudwatch unavailable")
	f := &FakeEmitter{Err: wantErr}
	err := f.Emit(context.Background(), []MetricDatum{{Name: "a", Value: 1}})
	require.ErrorIs(t, err, wantErr)
	require.Empty(t, f.Calls, "a failed Emit must not be recorded as if it succeeded")
}

// promPointsAtRSS builds prom fixtures at a fixed scrape cadence (matching
// soakPromScrapeSec's real-world 60s), one RSSBytes value per point.
func promPointsAtRSS(rss ...uint64) []PromPoint {
	pts := make([]PromPoint, len(rss))
	for i, v := range rss {
		pts[i] = PromPoint{T: i * secondsPerMinute, RSSBytes: v}
	}
	return pts
}

func TestRSSSlopeBytesPerMin(t *testing.T) {
	tests := []struct {
		name      string
		prom      []PromPoint
		wantOK    bool
		wantSlope float64
		delta     float64
	}{
		{
			name:   "fewer than 10 samples is not ok",
			prom:   promPointsAtRSS(1, 2, 3, 4, 5, 6, 7, 8, 9),
			wantOK: false,
		},
		{
			name:      "flat series has ~zero slope",
			prom:      promPointsAtRSS(500, 500, 500, 500, 500, 500, 500, 500, 500, 500),
			wantOK:    true,
			wantSlope: 0,
			delta:     1e-6,
		},
		{
			name: "linear growth of 1MB/min fits to ~1e6 bytes/min",
			prom: promPointsAtRSS(
				0, 1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000,
				6_000_000, 7_000_000, 8_000_000, 9_000_000, 10_000_000, 11_000_000,
			),
			wantOK:    true,
			wantSlope: 1_000_000,
			delta:     1e-3,
		},
		{
			name: "sawtooth GC pattern around a flat mean is near zero",
			prom: promPointsAtRSS(
				100_000_000, 120_000_000, 90_000_000, 130_000_000, 95_000_000,
				125_000_000, 100_000_000, 115_000_000, 92_000_000, 128_000_000,
				98_000_000, 118_000_000,
			),
			wantOK:    true,
			wantSlope: 0,
			delta:     2_000_000, // near zero, not exactly zero for a noisy series
		},
		{
			name:   "too few samples",
			prom:   promPointsAtRSS(1, 2, 3),
			wantOK: false,
		},
		{
			name:   "empty",
			prom:   nil,
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := rssSlopeBytesPerMin(tt.prom)
			require.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				require.InDelta(t, tt.wantSlope, got, tt.delta)
			}
		})
	}
}

// TestRSSSlopeBytesPerMin_WindowLimitsToTrailing120Minutes pins that a run
// longer than rssSlopeMaxWindowMinutes fits its line against only the
// trailing window, not the whole history — an early, unrepresentative
// transient (e.g. page-cache warmup) must not drag the reported slope away
// from what is actually happening now.
func TestRSSSlopeBytesPerMin_WindowLimitsToTrailing120Minutes(t *testing.T) {
	var prom []PromPoint
	// Minutes 0..29: a steep 10MB/min climb (outside the trailing window
	// once the series passes 120 minutes).
	for i := 0; i < 30; i++ {
		prom = append(prom, PromPoint{T: i * secondsPerMinute, RSSBytes: uint64(i) * 10_000_000})
	}
	// Minutes 30..179 (150 more minutes): flat at the last climbed value.
	// Total series length is 180 minutes; only the trailing 120 (minutes
	// 60..179) should influence the fit, and that whole window is flat.
	flatValue := uint64(29) * 10_000_000
	for i := 30; i < 180; i++ {
		prom = append(prom, PromPoint{T: i * secondsPerMinute, RSSBytes: flatValue})
	}

	got, ok := rssSlopeBytesPerMin(prom)
	require.True(t, ok)
	require.InDelta(t, 0, got, 1e-6, "the early climb must have aged out of the trailing 120-minute window")
}
