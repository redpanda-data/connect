// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

// BacklogPoint is one sample of the end-to-end backlog proxy: how far the
// engine has fallen behind a source producing at a known, fixed rate. T
// mirrors TopicPoint.T (seconds since the first broker-metrics frame).
type BacklogPoint struct {
	T int `json:"t"`
	// BacklogRecords is max(0, expected - delivered) at time T, where
	// expected = recordsPerSec * T and delivered is the cumulative record
	// count the broker-side series has attributed to the engine so far.
	BacklogRecords float64 `json:"backlog_records"`
	// BacklogSec is BacklogRecords expressed as wall-clock seconds at the
	// source's own rate — how long it would take the engine to catch up if
	// the source stopped producing right now.
	BacklogSec float64 `json:"backlog_sec"`
}

// ComputeBacklog derives a backlog series from a broker-side throughput
// series and the workload's expected write rate.
//
// Records, not bytes: broker bytes are compressed on the wire (see the
// extractTopicProduceRecords comment in brokermetrics.go for the measured
// 11-17x gap this produces on identical-payload seeders), which would make
// an expected-vs-delivered subtraction meaningless. Records are one-row-in,
// one-record-out and directly comparable to the workload's
// records-per-second target.
//
// series carries TopicPoint.MsgPerSec, which is itself a RATE (records/sec
// for that inter-frame interval), not a cumulative count — ComputeBacklog
// integrates it (MsgPerSec * IntervalSec, summed over every point up to and
// including T) to reconstruct delivered(t). series is assumed to already be
// summed across topics: every caller in this codebase (AttributeByEngine's
// single-topic Connect case, mergeTopicSeries' per-table sum for KC) hands
// ComputeBacklog a single already-merged series, so there is no additional
// summing to do here — a caller holding several per-topic series must merge
// them (see mergeTopicSeries) before calling this.
//
// Returns nil when recordsPerSec <= 0 (no target to measure against, e.g.
// every non-soak sweep) or series is empty (nothing to compute from).
func ComputeBacklog(series []TopicPoint, recordsPerSec float64) []BacklogPoint {
	if recordsPerSec <= 0 || len(series) == 0 {
		return nil
	}
	out := make([]BacklogPoint, 0, len(series))
	var delivered float64
	for _, p := range series {
		delivered += p.MsgPerSec * float64(p.IntervalSec)
		expected := recordsPerSec * float64(p.T)
		backlog := expected - delivered
		if backlog < 0 {
			backlog = 0
		}
		var backlogSec float64
		if backlog > 0 {
			backlogSec = backlog / recordsPerSec
		}
		out = append(out, BacklogPoint{
			T:              p.T,
			BacklogRecords: backlog,
			BacklogSec:     backlogSec,
		})
	}
	return out
}
