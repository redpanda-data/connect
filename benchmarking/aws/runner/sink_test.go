// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	sinkTestSession = "bench-20261120-101010"
	sinkTestBrokers = "10.42.10.10:9092,10.42.10.11:9092,10.42.10.12:9092"
	sinkTestTopic   = "bench_bench_20261120_101010_dst"
)

// fakeRPK records every rpk invocation and answers from a script keyed by
// the first two args ("topic create", "topic describe", ...).
type fakeRPK struct {
	calls   []rpkCall
	answers map[string][]string
	errs    map[string]error
}

type rpkCall struct {
	Brokers string
	Args    []string
}

func (f *fakeRPK) RunRPK(_ context.Context, brokers string, args ...string) ([]string, error) {
	f.calls = append(f.calls, rpkCall{Brokers: brokers, Args: args})
	k := strings.Join(args[:min(2, len(args))], " ")
	return f.answers[k], f.errs[k]
}

func sinkTestDeps(rpk RPKRunner) SinkDeps {
	return SinkDeps{
		Region:    "us-east-2",
		SessionID: sinkTestSession,
		Outs: map[string]string{
			"redpanda_broker_endpoints": sinkTestBrokers,
			"results_bucket":            "bench-results-abc",
		},
		RPK: rpk,
	}
}

func TestSinkForKinds(t *testing.T) {
	deps := sinkTestDeps(&fakeRPK{})
	for kind, want := range map[string]any{
		"redpanda": &redpandaSink{},
		"s3":       &s3Sink{},
		"drop":     dropSink{},
	} {
		got, err := sinkFor(SinkSpec{Kind: kind}, deps)
		require.NoError(t, err, kind)
		require.IsType(t, want, got, kind)
	}
	for _, kind := range []string{"snowflake", "bigquery"} {
		_, err := sinkFor(SinkSpec{Kind: kind}, deps)
		require.ErrorContains(t, err, "sink "+kind+" not implemented yet")
	}
	_, err := sinkFor(SinkSpec{Kind: "bogus"}, deps)
	require.ErrorContains(t, err, `unknown sink kind "bogus"`)
}

func TestSinkForScenarioOnlyE2E(t *testing.T) {
	deps := sinkTestDeps(&fakeRPK{})
	src := &Scenario{Direction: DirectionSource, Soak: true}
	got, err := sinkForScenario(src, deps)
	require.NoError(t, err)
	require.Nil(t, got, "a source or soak scenario has no sink")

	e2e := &Scenario{Direction: DirectionE2E, Sink: &SinkSpec{Kind: "drop"}}
	got, err = sinkForScenario(e2e, deps)
	require.NoError(t, err)
	require.IsType(t, dropSink{}, got)
}

func TestSinkRedpandaProvisionCountTeardown(t *testing.T) {
	rpk := &fakeRPK{answers: map[string][]string{
		"topic describe": readCapture(t, "rpk-topic-describe-p.txt"),
	}}
	sink, err := sinkFor(SinkSpec{Kind: "redpanda"}, sinkTestDeps(rpk))
	require.NoError(t, err)

	ph, err := sink.Provision(t.Context(), sinkTestSession)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"BENCH_DST_TOPIC":        sinkTestTopic,
		"BENCH_REDPANDA_BROKERS": sinkTestBrokers,
	}, ph)

	n, err := sink.Count(t.Context(), "1")
	require.NoError(t, err)
	require.Equal(t, int64(30), n, "sum of high watermarks across partitions")

	require.NoError(t, sink.Teardown(t.Context(), sinkTestSession))

	require.Equal(t, []rpkCall{
		{sinkTestBrokers, []string{"topic", "create", sinkTestTopic, "-p", "12", "-r", "3", "-c", "retention.bytes=4294967296", "-c", "retention.ms=1800000"}},
		{sinkTestBrokers, []string{"topic", "describe", "-p", sinkTestTopic}},
		{sinkTestBrokers, []string{"topic", "delete", sinkTestTopic}},
	}, rpk.calls)
	for _, c := range rpk.calls {
		require.NotContains(t, c.Brokers, "localhost", "brokers are a separate cluster")
	}
}

func TestSinkRedpandaKeepsProvidedBrokers(t *testing.T) {
	deps := sinkTestDeps(&fakeRPK{})
	deps.Outs["bench_redpanda_brokers"] = "already-set:9092"
	sink, err := sinkFor(SinkSpec{Kind: "redpanda"}, deps)
	require.NoError(t, err)
	ph, err := sink.Provision(t.Context(), sinkTestSession)
	require.NoError(t, err)
	require.NotContains(t, ph, "BENCH_REDPANDA_BROKERS")
}

func TestSinkRedpandaNeedsBrokers(t *testing.T) {
	deps := sinkTestDeps(&fakeRPK{})
	delete(deps.Outs, "redpanda_broker_endpoints")
	_, err := sinkFor(SinkSpec{Kind: "redpanda"}, deps)
	require.ErrorContains(t, err, "redpanda_broker_endpoints")
}

func TestSinkRedpandaTeardownIdempotent(t *testing.T) {
	rpk := &fakeRPK{
		answers: map[string][]string{"topic delete": {
			"TOPIC                              STATUS",
			sinkTestTopic + "  UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition.",
		}},
		errs: map[string]error{"topic delete": errors.New("exit status 1")},
	}
	sink, err := sinkFor(SinkSpec{Kind: "redpanda"}, sinkTestDeps(rpk))
	require.NoError(t, err)
	require.NoError(t, sink.Teardown(t.Context(), sinkTestSession), "an absent topic is already torn down")

	rpk.answers["topic delete"] = []string{"unable to dial: connection refused"}
	require.ErrorContains(t, sink.Teardown(t.Context(), sinkTestSession), "connection refused")
}

func TestSinkS3AndDrop(t *testing.T) {
	rpk := &fakeRPK{}
	s3, err := sinkFor(SinkSpec{Kind: "s3"}, sinkTestDeps(rpk))
	require.NoError(t, err)
	ph, err := s3.Provision(t.Context(), sinkTestSession)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"BENCH_S3_BUCKET":  "bench-results-abc",
		"BENCH_S3_PREFIX":  "sink/" + sinkTestSession + "/",
		"BENCH_AWS_REGION": "us-east-2",
	}, ph)
	_, err = s3.Count(t.Context(), "1")
	require.ErrorIs(t, err, ErrCountUnsupported)
	require.NoError(t, s3.Teardown(t.Context(), sinkTestSession))

	drop, err := sinkFor(SinkSpec{Kind: "drop"}, sinkTestDeps(rpk))
	require.NoError(t, err)
	ph, err = drop.Provision(t.Context(), sinkTestSession)
	require.NoError(t, err)
	require.Empty(t, ph)
	_, err = drop.Count(t.Context(), "1")
	require.ErrorIs(t, err, ErrCountUnsupported)
	require.NoError(t, drop.Teardown(t.Context(), sinkTestSession))
	require.Empty(t, rpk.calls, "s3 and drop never call rpk")
}

func TestSinkS3NeedsResultsBucket(t *testing.T) {
	deps := sinkTestDeps(&fakeRPK{})
	delete(deps.Outs, "results_bucket")
	_, err := sinkFor(SinkSpec{Kind: "s3"}, deps)
	require.ErrorContains(t, err, "results_bucket")
}

// samplesTotalling returns n one-second samples summing to total msgs.
func samplesTotalling(n int, total float64) []Sample {
	out := make([]Sample, n)
	for i := range out {
		out[i] = Sample{T: i, MsgPerSec: total / float64(n)}
	}
	return out
}

func TestSinkCountCheck2Percent(t *testing.T) {
	// A 100 s point at 1000 msg/s: 3 s of median is 3000, 2% is 2000, so
	// the 3 s floor rules and these exercise the 2% side with a slow median.
	samples := samplesTotalling(100, 100_000)
	const slow = 100.0 // 3 s = 300 msgs, under 2%

	_, bad := sinkCountAnomaly(101_500, samples, slow)
	require.False(t, bad, "1.5% over is within tolerance")
	_, bad = sinkCountAnomaly(98_500, samples, slow)
	require.False(t, bad, "1.5% under is within tolerance")

	a, bad := sinkCountAnomaly(102_500, samples, slow)
	require.True(t, bad, "2.5% over is a mismatch")
	require.Equal(t, anomalySinkCountMismatch, a.Kind)
	require.Contains(t, a.Note, "102500")
	require.Contains(t, a.Note, "100000")

	_, bad = sinkCountAnomaly(97_500, samples, slow)
	require.True(t, bad, "2.5% under is a mismatch")

	_, bad = sinkCountAnomaly(0, nil, 0)
	require.False(t, bad, "nothing written, nothing measured")
	_, bad = sinkCountAnomaly(10, nil, 0)
	require.True(t, bad, "writes with no samples disagree")
}

// A short point at a steady rate: a second or two of samples lost at the
// edges (the last partial interval before SIGTERM) is more than 2% of the
// point but within the 3 s floor.
func TestSinkCountCheckShortWindowFloor(t *testing.T) {
	samples := samplesTotalling(60, 60_000) // 60 s at 1000 msg/s
	const median = 1000.0

	_, bad := sinkCountAnomaly(61_000, samples, median)
	require.False(t, bad, "1 s of extra records on a 60 s point passes")
	_, bad = sinkCountAnomaly(59_000, samples, median)
	require.False(t, bad, "1 s short on a 60 s point passes")
	_, bad = sinkCountAnomaly(62_500, samples, median)
	require.False(t, bad, "2.5 s (4.2%) is under the 3 s floor")
	_, bad = sinkCountAnomaly(63_500, samples, median)
	require.True(t, bad, "3.5 s is over the 3 s floor")
	_, bad = sinkCountAnomaly(54_000, samples, median)
	require.True(t, bad, "a 10% shortfall fails")
}

// fakeSink records lifecycle events into a shared log and returns counts
// from a queue, one per Count call.
type fakeSink struct {
	events      *[]string
	counts      []int64
	countErr    error
	provErr     error
	teardownErr error
}

func (f *fakeSink) Provision(_ context.Context, session string) (map[string]string, error) {
	*f.events = append(*f.events, "provision "+session)
	return map[string]string{"BENCH_DST_TOPIC": "t"}, f.provErr
}

func (f *fakeSink) Count(_ context.Context, point string) (int64, error) {
	*f.events = append(*f.events, "count "+point)
	if f.countErr != nil {
		return 0, f.countErr
	}
	n := f.counts[0]
	f.counts = f.counts[1:]
	return n, nil
}

func (f *fakeSink) Teardown(_ context.Context, session string) error {
	*f.events = append(*f.events, "teardown "+session)
	return f.teardownErr
}

// eventSSM appends "bench" to the shared log for every bench script, so a
// test can see where the points fall between sink calls.
type eventSSM struct {
	FakeSSM
	events *[]string
}

func (e *eventSSM) Run(ctx context.Context, instanceID, script string, onLine func(string)) error {
	*e.events = append(*e.events, "bench")
	return e.FakeSSM.Run(ctx, instanceID, script, onLine)
}

// e2eSinkRunner is a two-point e2e MatrixRunner whose logs carry
// samplesPerPoint rolling-stats lines at 1000 msg/sec each, warmup included.
func e2eSinkRunner(t *testing.T, sink Sink, ssm SSMExecutor, samplesPerPoint int) *MatrixRunner {
	t.Helper()
	prev := stdout
	stdout = &bytes.Buffer{}
	t.Cleanup(func() { stdout = prev })
	return &MatrixRunner{
		SSM: ssm,
		LogFetcher: &FakeLogFetcher{Contents: map[string]string{
			"runs/s/sweep-1.log": makeLog(samplesPerPoint, 5),
			"runs/s/sweep-2.log": makeLog(samplesPerPoint, 5),
		}},
		RunnerInstance: "i-runner",
		SessionID:      "s",
		Topology:       e2eTopology{},
		Names:          newBenchNames("s", "x"),
		Sink:           sink,
	}
}

var e2eSinkPlan = []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}, {VCPU: 2, GOMAXPROCS: 2, Streams: 1}}

func TestSinkCountCheckedPerPoint(t *testing.T) {
	// 10 samples x 1000 msg/sec = 10,000 per point; warmup (5 s) is
	// included because Count's delta spans the whole point.
	var events []string
	sink := &fakeSink{events: &events, counts: []int64{0, 10_150, 10_150, 20_300}}
	mr := e2eSinkRunner(t, sink, &eventSSM{FakeSSM: FakeSSM{}, events: &events}, 10)
	points, err := mr.Run(t.Context(), e2eSinkPlan, 1, 5*time.Second, 5*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 2)
	for _, p := range points {
		require.Empty(t, p.Anomalies)
	}
	require.Equal(t, []string{"count 1", "bench", "count 1", "count 2", "bench", "count 2"}, events,
		"Count brackets each point")
}

func TestSinkCountMismatchFailsPoint(t *testing.T) {
	var events []string
	// 10 s of 1000 msg/s: the 3 s floor (3000) outweighs 2% (200), so a
	// mismatch must exceed 3000.
	sink := &fakeSink{events: &events, counts: []int64{0, 14_000}}
	mr := e2eSinkRunner(t, sink, &FakeSSM{}, 10)
	points, err := mr.Run(t.Context(), e2eSinkPlan, 1, 5*time.Second, 5*time.Second, "", "")
	require.ErrorContains(t, err, anomalySinkCountMismatch)
	require.Len(t, points, 1, "the sweep stops at the failed point")
	require.Len(t, points[0].Anomalies, 1)
	require.Equal(t, anomalySinkCountMismatch, points[0].Anomalies[0].Kind)
	require.Contains(t, points[0].Anomalies[0].Note, "14000")
	require.Contains(t, points[0].Anomalies[0].Note, "10000")
}

func TestSinkCountUnsupportedSkipsCheck(t *testing.T) {
	var events []string
	sink := &fakeSink{events: &events, countErr: ErrCountUnsupported}
	mr := e2eSinkRunner(t, sink, &FakeSSM{}, 10)
	points, err := mr.Run(t.Context(), e2eSinkPlan, 1, 5*time.Second, 5*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 2)
	for _, p := range points {
		require.Empty(t, p.Anomalies, "no anomaly when the sink cannot count")
	}
}

func TestSinkCountErrorFailsSweep(t *testing.T) {
	var events []string
	sink := &fakeSink{events: &events, countErr: errors.New("rpk: connection refused")}
	mr := e2eSinkRunner(t, sink, &FakeSSM{}, 10)
	_, err := mr.Run(t.Context(), e2eSinkPlan, 1, 5*time.Second, 5*time.Second, "", "")
	require.ErrorContains(t, err, "connection refused")
}

func TestSinkLifecycleOrderAndTeardownOnSweepError(t *testing.T) {
	var events []string
	sink := &fakeSink{events: &events, counts: []int64{0, 10_000, 10_000, 20_000}}
	ssm := &eventSSM{FakeSSM: FakeSSM{Errs: map[string]error{"i-runner": errors.New("ssm boom")}}, events: &events}
	mr := e2eSinkRunner(t, sink, ssm, 10)

	err := withSink(t.Context(), sink, "sess", func(ph map[string]string) error {
		require.Equal(t, "t", ph["BENCH_DST_TOPIC"], "fn gets the sink's placeholders")
		_, err := mr.Run(t.Context(), e2eSinkPlan, 1, 5*time.Second, 5*time.Second, "", "")
		return err
	})
	require.ErrorContains(t, err, "ssm boom", "the sweep error is returned")
	require.Equal(t, []string{"provision sess", "count 1", "bench", "teardown sess"}, events)
}

func TestWithSinkTearsDownOnSuccess(t *testing.T) {
	var events []string
	sink := &fakeSink{events: &events}
	err := withSink(t.Context(), sink, "sess", func(map[string]string) error {
		events = append(events, "fn")
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"provision sess", "fn", "teardown sess"}, events)
}

func TestWithSinkTeardownErrorOnlyWhenFnSucceeded(t *testing.T) {
	var events []string
	sink := &fakeSink{events: &events, teardownErr: errors.New("delete failed")}

	fnErr := errors.New("sweep failed")
	err := withSink(t.Context(), sink, "sess", func(map[string]string) error { return fnErr })
	require.ErrorIs(t, err, fnErr)
	require.NotContains(t, err.Error(), "delete failed", "teardown never masks fn's error")

	err = withSink(t.Context(), sink, "sess", func(map[string]string) error { return nil })
	require.ErrorContains(t, err, "delete failed", "a teardown failure alone fails the run")
}

// ctxSink records whether Teardown saw a live context.
type ctxSink struct {
	fakeSink
	teardownCtxErr error
}

func (c *ctxSink) Teardown(ctx context.Context, session string) error {
	c.teardownCtxErr = ctx.Err()
	return c.fakeSink.Teardown(ctx, session)
}

func TestWithSinkTearsDownAfterCancel(t *testing.T) {
	var events []string
	sink := &ctxSink{fakeSink: fakeSink{events: &events}}
	ctx, cancel := context.WithCancel(t.Context())
	err := withSink(ctx, sink, "sess", func(map[string]string) error {
		cancel() // Ctrl-C mid-sweep
		return ctx.Err()
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []string{"provision sess", "teardown sess"}, events)
	require.NoError(t, sink.teardownCtxErr, "teardown runs on an uncancelled context")
}

func TestWithSinkProvisionFailure(t *testing.T) {
	var events []string
	called := false
	fn := func(map[string]string) error { called = true; return nil }

	// Nothing was created: no teardown.
	sink := &fakeSink{events: &events, provErr: fmt.Errorf("auth: %w", ErrNothingProvisioned)}
	err := withSink(t.Context(), sink, "sess", fn)
	require.ErrorIs(t, err, ErrNothingProvisioned)
	require.False(t, called)
	require.Equal(t, []string{"provision sess"}, events)

	// Part of the scratch may exist: tear down.
	events = nil
	sink = &fakeSink{events: &events, provErr: errors.New("create failed")}
	err = withSink(t.Context(), sink, "sess", fn)
	require.ErrorContains(t, err, "create failed")
	require.False(t, called)
	require.Equal(t, []string{"provision sess", "teardown sess"}, events, "a partial provision is torn down")
}

func TestSinkNilMakesNoCalls(t *testing.T) {
	called := false
	err := withSink(t.Context(), nil, "sess", func(ph map[string]string) error {
		called = true
		require.Empty(t, ph)
		return nil
	})
	require.NoError(t, err)
	require.True(t, called)

	// A source run's MatrixRunner has no sink: nothing brackets the point.
	mr := e2eSinkRunner(t, nil, &FakeSSM{}, 10)
	mr.Topology = sourceTopology{}
	mr.LogFetcher = &FakeLogFetcher{Contents: map[string]string{
		"runs/s/sweep-1.log": makeLog(10, 5),
	}}
	_, err = mr.Run(t.Context(), e2eSinkPlan[:1], 1, 5*time.Second, 5*time.Second, "", "")
	require.NoError(t, err)
}

// Fan-out or archive in output batching makes record counts diverge from
// message counts, so the count check is skipped.
func TestSinkCountSkippedForBatchingProcessors(t *testing.T) {
	plain := map[string]any{"kafka_franz": map[string]any{"topic": "t", "batching": map[string]any{"count": 100}}}
	archived := map[string]any{"aws_s3": map[string]any{"batching": map[string]any{
		"count": 100, "processors": []any{map[string]any{"archive": map[string]any{"format": "lines"}}},
	}}}
	nested := map[string]any{"broker": map[string]any{"outputs": []any{
		map[string]any{"drop": map[string]any{}},
		map[string]any{"kafka_franz": map[string]any{"batching": map[string]any{"processors": []any{}}}},
	}}}
	require.False(t, outputBatchesWithProcessors(plain))
	require.True(t, outputBatchesWithProcessors(archived))
	require.True(t, outputBatchesWithProcessors(nested), "any batching block in the output")

	var events []string
	inner := &fakeSink{events: &events, counts: []int64{0, 999_999}}
	s := &Scenario{Pipeline: map[string]any{"output": archived}}
	sink := countableSink(inner, s, e2eSinkPlan)
	mr := e2eSinkRunner(t, sink, &FakeSSM{}, 10)
	points, err := mr.Run(t.Context(), e2eSinkPlan, 1, 5*time.Second, 5*time.Second, "", "")
	require.NoError(t, err, "a 100x disagreement is not checked")
	for _, p := range points {
		require.Empty(t, p.Anomalies)
	}

	_, err = sink.Provision(t.Context(), "sess")
	require.NoError(t, err)
	require.NoError(t, sink.Teardown(t.Context(), "sess"))
	require.Equal(t, []string{"provision sess", "teardown sess"}, events, "lifecycle still runs; only Count is hidden")

	plainS := &Scenario{Pipeline: map[string]any{"output": plain}}
	require.Same(t, inner, countableSink(inner, plainS, e2eSinkPlan))

	// An arm's merged pipeline is checked too.
	armPlan := []sweepPoint{{VCPU: 1, ArmID: "a", Pipeline: map[string]any{"output": archived}}}
	_, isUncounted := countableSink(inner, plainS, armPlan).(uncountedSink)
	require.True(t, isUncounted)
	require.Nil(t, countableSink(nil, s, e2eSinkPlan))
}

func writeCfg(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

func TestSinkUnresolvedPlaceholderFailsFast(t *testing.T) {
	ok := writeCfg(t, "output:\n  aws_s3:\n    path: sink/x/${!counter()}.json\n    region: ${AWS_REGION_FROM_ENV}\n")
	bad := writeCfg(t, "output:\n  kafka_franz:\n    topic: ${BENCH_DST_TOPIC}\n")
	require.NoError(t, checkPlaceholdersResolved([]renderedPointConfigs{{Key: "1", Single: ok}}))
	err := checkPlaceholdersResolved([]renderedPointConfigs{{Key: "1", Single: ok}, {Key: "2", Root: ok, Streams: []string{bad}}})
	require.ErrorContains(t, err, "${BENCH_DST_TOPIC}")
	require.ErrorContains(t, err, `point "2"`)
}

// The golden redpanda-sink plan renders fully resolved only once the sink's
// placeholders are merged in.
func TestSinkGoldenRedpandaRendersResolved(t *testing.T) {
	s, err := LoadScenario(filepath.Join("..", "plan", "testdata", "bot-cccccccccccc.yaml"))
	require.NoError(t, err)
	outs := map[string]string{
		"redpanda_broker_endpoints": sinkTestBrokers,
		"results_bucket":            "b",
		"bench_session_id":          sinkTestSession,
	}
	names := newBenchNames(sinkTestSession, s.Connector)
	pt := buildSweepPlan(s)[0]

	set, err := renderPointConfigs(s, outs, e2eTopology{}, names, pt)
	require.NoError(t, err)
	require.ErrorContains(t, checkPlaceholdersResolved([]renderedPointConfigs{set}), "${BENCH_DST_TOPIC}")

	sink, err := sinkFor(*s.Sink, sinkTestDeps(&fakeRPK{}))
	require.NoError(t, err)
	merged := maps.Clone(outs)
	require.NoError(t, withSink(t.Context(), sink, sinkTestSession, func(ph map[string]string) error {
		maps.Copy(merged, ph)
		set, err = renderPointConfigs(s, merged, e2eTopology{}, names, pt)
		return err
	}))
	require.NoError(t, checkPlaceholdersResolved([]renderedPointConfigs{set}))
	raw, err := os.ReadFile(set.Single)
	require.NoError(t, err)
	require.Contains(t, string(raw), sinkTestTopic)
	require.NotContains(t, string(raw), "localhost")
}

func TestSSMRPKRendersBrokersAndCollectsLines(t *testing.T) {
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": {"PARTITION  HIGH-WATERMARK", "0  5"}}}
	r := ssmRPK{SSM: ssm, Instance: "i-runner"}
	lines, err := r.RunRPK(t.Context(), sinkTestBrokers, "topic", "describe", "-p", sinkTestTopic)
	require.NoError(t, err)
	require.Equal(t, []string{"PARTITION  HIGH-WATERMARK", "0  5"}, lines)
	require.Len(t, ssm.Scripts, 1)
	require.Contains(t, ssm.Scripts[0], fmt.Sprintf(`/usr/local/bin/rpk "topic" "describe" "-p" %q -X brokers=%q`, sinkTestTopic, sinkTestBrokers))

	ssm.Errs = map[string]error{"i-runner": errors.New("status Failed")}
	lines, err = r.RunRPK(t.Context(), sinkTestBrokers, "topic", "delete", sinkTestTopic)
	require.Error(t, err)
	require.NotEmpty(t, lines, "output is returned on failure so callers can read rpk's reason")
}
