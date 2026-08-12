// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

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
	data, newHW := aggregateSoakMinutes(nil, nil, nil, nil, base, -1)
	require.Nil(t, data)
	require.Equal(t, -1, newHW)

	// A pre-existing high-water mark must survive untouched too — a cycle
	// that fetched nothing new (e.g. the checkpoint hasn't landed yet) must
	// never regress the mark.
	data, newHW = aggregateSoakMinutes(nil, nil, nil, nil, base, 7)
	require.Nil(t, data)
	require.Equal(t, 7, newHW)
}

func TestAggregateSoakMinutes_BucketsMeansAndLastsExcludingIncompleteMinute(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	broker := []TopicPoint{
		{T: 0, MBPerSec: 1, MsgPerSec: 10},
		{T: 30, MBPerSec: 3, MsgPerSec: 30}, // minute 0: mean 2 / 20
		{T: 60, MBPerSec: 5, MsgPerSec: 50},
		{T: 90, MBPerSec: 7, MsgPerSec: 70},  // minute 1: mean 6 / 60
		{T: 120, MBPerSec: 9, MsgPerSec: 90}, // minute 2: the current incomplete minute
	}
	samples := []Sample{
		{T: 0, MBPerSec: 100},
		{T: 30, MBPerSec: 300}, // minute 0: mean 200
		{T: 60, MBPerSec: 500},
		{T: 90, MBPerSec: 700}, // minute 1: mean 600
	}
	prom := []PromPoint{
		{T: 0, RSSBytes: 100, HeapInUseMB: 10, Goroutines: 5},
		{T: 50, RSSBytes: 200, HeapInUseMB: 20, Goroutines: 6}, // minute 0: last of these two
		{T: 65, RSSBytes: 300, HeapInUseMB: 30, Goroutines: 7}, // minute 1: last (only point)
	}
	backlog := []BacklogPoint{
		{T: 0, BacklogSec: 5},   // minute 0
		{T: 60, BacklogSec: 15}, // minute 1
	}

	data, newHW := aggregateSoakMinutes(samples, prom, broker, backlog, base, -1)
	require.Equal(t, 1, newHW, "the current incomplete minute (2) must never be emitted")

	want := []MetricDatum{
		{Name: metricThroughputMBps, Value: 2, Unit: unitMegabytesPerSecond, At: base},
		{Name: metricRecordsPerSec, Value: 20, Unit: unitCountPerSecond, At: base},
		{Name: metricLogThroughputMBps, Value: 200, Unit: unitMegabytesPerSecond, At: base},
		{Name: metricRSSBytes, Value: 200, Unit: unitBytes, At: base},
		{Name: metricHeapInUseBytes, Value: 20_000_000, Unit: unitBytes, At: base},
		{Name: metricGoroutines, Value: 6, Unit: unitCount, At: base},
		{Name: metricBacklogSeconds, Value: 5, Unit: unitSeconds, At: base},

		{Name: metricThroughputMBps, Value: 6, Unit: unitMegabytesPerSecond, At: base.Add(time.Minute)},
		{Name: metricRecordsPerSec, Value: 60, Unit: unitCountPerSecond, At: base.Add(time.Minute)},
		{Name: metricLogThroughputMBps, Value: 600, Unit: unitMegabytesPerSecond, At: base.Add(time.Minute)},
		{Name: metricRSSBytes, Value: 300, Unit: unitBytes, At: base.Add(time.Minute)},
		{Name: metricHeapInUseBytes, Value: 30_000_000, Unit: unitBytes, At: base.Add(time.Minute)},
		{Name: metricGoroutines, Value: 7, Unit: unitCount, At: base.Add(time.Minute)},
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
	// sinceMinute=0 (minute 0 already emitted by an earlier cycle) must only
	// yield minute 1 this time, not a repeat of minute 0.
	data, newHW := aggregateSoakMinutes(nil, nil, broker, nil, base, 0)
	require.Equal(t, 1, newHW)
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
	data, _ := aggregateSoakMinutes(nil, nil, broker, nil, base, -1)
	for _, d := range data {
		require.NotEqual(t, metricBacklogSeconds, d.Name)
	}
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

	// Without the shift, the log sample lands in minute 0 — a minute the
	// broker series has no data for at all, and the wrong minute besides.
	unshifted, _ := aggregateSoakMinutes(rawSamples, nil, broker, nil, base, -1)
	for _, d := range unshifted {
		if d.Name == metricLogThroughputMBps {
			require.NotEqual(t, base.Add(time.Minute), d.At,
				"BUG reproduced: unshifted samples land a minute early")
		}
	}

	shifted := offsetSampleT(rawSamples, warmupSec)
	data, newHW := aggregateSoakMinutes(shifted, nil, broker, nil, base, -1)
	require.Equal(t, 1, newHW)

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
