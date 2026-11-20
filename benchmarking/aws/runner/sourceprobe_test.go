// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The testdata/rpk-*.txt files are captured rpk v25.3.4 output against a
// real broker: a 3-partition topic with 30 records on partition 0 and a
// group that committed offset 10 on partition 0 only.
func readCapture(t *testing.T, name string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	return strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
}

func TestParseTopicPartitionsCaptured(t *testing.T) {
	got, err := parseTopicPartitions(readCapture(t, "rpk-topic-describe-p.txt"))
	require.NoError(t, err)
	require.Equal(t, map[int]partitionOffsets{
		0: {LogStart: 0, HighWatermark: 30},
		1: {LogStart: 0, HighWatermark: 0},
		2: {LogStart: 0, HighWatermark: 0},
	}, got)

	_, err = parseTopicPartitions([]string{"PARTITION  LEADER  EPOCH  REPLICAS  LOG-START-OFFSET  HIGH-WATERMARK"})
	require.ErrorContains(t, err, "no partitions")
	_, err = parseTopicPartitions([]string{"unable to describe topic"})
	require.Error(t, err)
}

func TestParseGroupCommittedCaptured(t *testing.T) {
	got, err := parseGroupCommitted(readCapture(t, "rpk-group-describe.txt"), "zz_bench_capture_src")
	require.NoError(t, err)
	require.Equal(t, map[int]int64{0: 10}, got, "partitions showing '-' have no commit")

	got, err = parseGroupCommitted(readCapture(t, "rpk-group-describe-dead.txt"), "zz_bench_capture_src")
	require.NoError(t, err)
	require.Empty(t, got)

	_, err = parseGroupCommitted([]string{"unable to request metadata"}, "t")
	require.ErrorContains(t, err, "GROUP")
}

// probeLines frames captured describe output the way the bench script does.
func probeLines(t0, t1 int64, hwm0, hwm1, group []string) []string {
	var out []string
	out = append(out, fmt.Sprintf("%st0 %d", sourceProbePrefix, t0))
	for _, l := range hwm0 {
		out = append(out, sourceProbePrefix+"hwm0 "+l)
	}
	out = append(out, fmt.Sprintf("%st1 %d", sourceProbePrefix, t1))
	for _, l := range hwm1 {
		out = append(out, sourceProbePrefix+"hwm1 "+l)
	}
	for _, l := range group {
		out = append(out, sourceProbePrefix+"group "+l)
	}
	return out
}

func describeP(hwms ...int64) []string {
	lines := []string{"PARTITION  LEADER  EPOCH  REPLICAS  LOG-START-OFFSET  HIGH-WATERMARK"}
	for i, h := range hwms {
		lines = append(lines, fmt.Sprintf("%d          0       1      [0]       0                 %d", i, h))
	}
	return lines
}

func groupDescribe(topic string, committed ...int64) []string {
	lines := []string{
		"GROUP                  g", "COORDINATOR-NODE       0", "STATE                  Stable",
		"MEMBERS                1", "TOTAL-LAG              0", "",
		"TOPIC  PARTITION  CURRENT-OFFSET  LOG-START-OFFSET  LOG-END-OFFSET  LAG  MEMBER-ID  CLIENT-ID  HOST",
	}
	for i, c := range committed {
		cs := "-"
		if c >= 0 {
			cs = fmt.Sprint(c)
		}
		lines = append(lines, fmt.Sprintf("%s  %d  %s  0  0  0  m  c  h", topic, i, cs))
	}
	return lines
}

func TestParseSourceProbeCaptured(t *testing.T) {
	// Captured end state: lag is 20 on partition 0 (committed 10 of 30) and
	// 0 on the empty, uncommitted partitions.
	lines := probeLines(1000, 1010,
		describeP(0, 0, 0), readCapture(t, "rpk-topic-describe-p.txt"), readCapture(t, "rpk-group-describe.txt"))
	m, err := parseSourceProbe(lines, "zz_bench_capture_src")
	require.NoError(t, err)
	require.Equal(t, int64(0), m.HWMStart)
	require.Equal(t, int64(30), m.HWMEnd)
	require.InDelta(t, 10, m.WindowSec, 1e-9)
	require.InDelta(t, 3, m.ProducerRate(), 1e-9)
	require.Equal(t, int64(20), m.LagEnd)
}

func TestParseSourceProbeUncommittedPartitionCountsWholeLog(t *testing.T) {
	// rpk's TOTAL-LAG ignores partitions with no commit; a consumer that never
	// committed has read nothing there, so the whole retained log is lag.
	lines := probeLines(0, 100, describeP(0, 0), describeP(500, 700), groupDescribe("t", 450, -1))
	m, err := parseSourceProbe(lines, "t")
	require.NoError(t, err)
	require.Equal(t, int64(50+700), m.LagEnd)

	dead := probeLines(0, 100, describeP(0, 0, 0), readCapture(t, "rpk-topic-describe-p.txt"), readCapture(t, "rpk-group-describe-dead.txt"))
	m, err = parseSourceProbe(dead, "zz_bench_capture_src")
	require.NoError(t, err)
	require.Equal(t, int64(30), m.LagEnd, "a group with no commits lags by the whole log")
}

func TestParseSourceProbeMissingSection(t *testing.T) {
	full := probeLines(0, 100, describeP(0), describeP(10), groupDescribe("t", 10))
	for _, drop := range []string{"t0 ", "t1 ", "hwm0 ", "hwm1 ", "group "} {
		var lines []string
		for _, l := range full {
			if !strings.HasPrefix(l, sourceProbePrefix+drop) {
				lines = append(lines, l)
			}
		}
		_, err := parseSourceProbe(lines, "t")
		require.Error(t, err, "missing %q", drop)
	}
	_, err := parseSourceProbe(append(full, sourceProbePrefix+"error hwm1"), "t")
	require.ErrorContains(t, err, "hwm1")
	_, err = parseSourceProbe(probeLines(100, 100, describeP(0), describeP(10), groupDescribe("t", 10)), "t")
	require.ErrorContains(t, err, "window")
}

func TestSourceAnomalies(t *testing.T) {
	// Caught up: lag under 5 s of production.
	a, fail := sourceAnomalies(SourceMeasurement{WindowSec: 900, HWMEnd: 900_000, LagEnd: 4_000})
	require.False(t, fail)
	require.Len(t, a, 1)
	require.Equal(t, anomalyProducerBound, a[0].Kind)
	require.True(t, strings.HasPrefix(a[0].Note, "producer_bound"))
	require.Zero(t, a[0].MinRatio)

	// Lagging: the pipeline is the ceiling.
	a, fail = sourceAnomalies(SourceMeasurement{WindowSec: 900, HWMEnd: 900_000, LagEnd: 6_000})
	require.False(t, fail)
	require.Empty(t, a)

	// Nothing produced: the point fails.
	a, fail = sourceAnomalies(SourceMeasurement{WindowSec: 900, HWMStart: 5, HWMEnd: 5})
	require.True(t, fail)
	require.Len(t, a, 1)
	require.Equal(t, anomalyProducerUnreachable, a[0].Kind)
}

func TestRenderBenchScriptSourceProbe(t *testing.T) {
	script := renderBenchScript(benchScriptArgs{
		VCPU: 2, WarmupSec: 120, DurationSec: 900, Bucket: "b", SessionID: "s",
		SourceProbe: &SourceProbeArgs{Brokers: testBrokers, Topic: "bench_s_src", Group: "bench_s_p2"},
	})
	require.NotContains(t, script, "sleep 1020")
	warm := strings.Index(script, "sleep 120")
	t0 := strings.Index(script, sourceProbePrefix+"t0")
	hwm0 := strings.Index(script, "rpk topic describe -p \"bench_s_src\" -X brokers=\""+testBrokers+"\"")
	win := strings.Index(script, "sleep 900")
	grp := strings.Index(script, "rpk group describe \"bench_s_p2\" -X brokers=\""+testBrokers+"\"")
	kill := strings.Index(script, `kill -TERM "$PID"`)
	for _, i := range []int{warm, t0, hwm0, win, grp, kill} {
		require.GreaterOrEqual(t, i, 0, script)
	}
	require.Less(t, warm, t0)
	require.Less(t, t0, hwm0)
	require.Less(t, hwm0, win)
	require.Less(t, win, grp)
	require.Less(t, grp, kill, "lag is read while Connect is still consuming")
	require.NotContains(t, script, "localhost:9092")

	plain := renderBenchScript(benchScriptArgs{VCPU: 2, WarmupSec: 120, DurationSec: 900, Bucket: "b", SessionID: "s"})
	require.Contains(t, plain, "sleep 1020")
	require.NotContains(t, plain, sourceProbePrefix)
}

func runProbedPoint(t *testing.T, probe []string) ([]SweepPoint, *FakeSSM, error) {
	t.Helper()
	const sessionID = "bench-e2e-probe"
	prev := stdout
	stdout = &strings.Builder{}
	defer func() { stdout = prev }()
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": append(probe, "bench point complete")}}
	mr := &MatrixRunner{
		SSM: ssm,
		LogFetcher: &FakeLogFetcher{Contents: map[string]string{
			"runs/" + sessionID + "/sweep-1.log": makeLog(180, 7),
		}},
		RunnerInstance: "i-runner", Bucket: "b", SessionID: sessionID,
		Topology: e2eTopology{}, Names: newBenchNames(sessionID, "http_server"),
		SourceProbe: &SourceProbeArgs{Brokers: testBrokers, Topic: "t"},
	}
	pts, err := mr.Run(t.Context(), []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}}, 1, 60*time.Second, 120*time.Second, "", "")
	return pts, ssm, err
}

func TestMatrixSourceProbeCaughtUpIsProducerBound(t *testing.T) {
	// 120 s window, 120,000 produced (1000 msg/s), lag 10 < 5 s of production.
	pts, ssm, err := runProbedPoint(t, probeLines(0, 120, describeP(0), describeP(120_000), groupDescribe("t", 119_990)))
	require.NoError(t, err)
	require.Len(t, pts, 1)
	require.InDelta(t, 1000, pts[0].ProducerRateMsgPerSec, 1e-9)
	require.NotNil(t, pts[0].ConsumerLagEnd)
	require.Equal(t, int64(10), *pts[0].ConsumerLagEnd)
	require.Len(t, pts[0].Anomalies, 1)
	require.Equal(t, anomalyProducerBound, pts[0].Anomalies[0].Kind)
	// The point's own consumer group is the one the script describes.
	require.Contains(t, ssm.Scripts[0], `rpk group describe "bench_bench_e2e_probe_p1"`)
}

func TestMatrixSourceProbeFilteringPipelineNotFlagged(t *testing.T) {
	// Output measures 1000 msg/s (makeLog) while the producer achieved
	// 100,000 msg/s and the group is far behind: a filtering or slow
	// pipeline, not a producer-bound one.
	pts, _, err := runProbedPoint(t, probeLines(0, 120, describeP(0), describeP(12_000_000), groupDescribe("t", 6_000_000)))
	require.NoError(t, err)
	require.InDelta(t, 100_000, pts[0].ProducerRateMsgPerSec, 1e-9)
	require.Equal(t, int64(6_000_000), *pts[0].ConsumerLagEnd)
	require.Empty(t, pts[0].Anomalies)
}

func TestMatrixSourceProbeNothingProducedFails(t *testing.T) {
	pts, _, err := runProbedPoint(t, probeLines(0, 120, describeP(7), describeP(7), groupDescribe("t", 7)))
	require.ErrorContains(t, err, "producer_unreachable")
	require.Len(t, pts, 1, "the failed point is returned with its anomaly")
	require.Equal(t, anomalyProducerUnreachable, pts[0].Anomalies[0].Kind)
}

func TestMatrixSourceProbeMissingOutputFails(t *testing.T) {
	_, _, err := runProbedPoint(t, nil)
	require.ErrorContains(t, err, "source probe")
}

func TestParseSourceProbeFramedCapture(t *testing.T) {
	// testdata/rpk-probe-framed.txt is the stdout of renderSourceProbe's
	// start and end phases run against a real broker (rpk v25.3.4): 50
	// records before the window, none during it, a group committed at 20.
	m, err := parseSourceProbe(readCapture(t, "rpk-probe-framed.txt"), "zz_probe_src")
	require.NoError(t, err)
	require.InDelta(t, 2, m.WindowSec, 1e-9)
	require.Equal(t, int64(50), m.HWMStart)
	require.Equal(t, int64(50), m.HWMEnd)
	require.Zero(t, m.ProducerRate())
	require.Equal(t, int64(30), m.LagEnd)
	_, fail := sourceAnomalies(m)
	require.True(t, fail)
}
