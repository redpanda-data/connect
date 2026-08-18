// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// makeBrokerFrames produces a synthetic redpanda-<key>-connect.txt dump: a
// series of /public_metrics frames for one topic, starting at startUnix and
// stepping by intervalSec, with deltaBytes/deltaRecords accruing every
// frame. Constant per-interval deltas keep every derived TopicPoint's
// MBPerSec/MsgPerSec identical, which is what lets soak-emit tests assert
// exact per-minute means regardless of how many points land in a minute.
func makeBrokerFrames(topic string, startUnix, frames, intervalSec int, deltaBytes, deltaRecords int64) string {
	var sb strings.Builder
	var cumBytes, cumRecords int64
	for i := 0; i < frames; i++ {
		fmt.Fprintf(&sb, "###timestamp=%d\n", startUnix+i*intervalSec)
		fmt.Fprintf(&sb, `redpanda_kafka_request_bytes_total{redpanda_namespace="kafka",redpanda_request="produce",redpanda_topic="%s"} %d`+"\n", topic, cumBytes)
		fmt.Fprintf(&sb, `redpanda_kafka_records_produced_total{redpanda_namespace="kafka",redpanda_topic="%s"} %d`+"\n", topic, cumRecords)
		cumBytes += deltaBytes
		cumRecords += deltaRecords
	}
	return sb.String()
}

// makeLog produces a synthetic Connect log with `count` rolling-stats lines at
// the given throughput, plus a couple of startup info lines for realism.
func makeLog(count int, mbPerSec float64) string {
	var sb strings.Builder
	sb.WriteString("INFO starting redpanda-connect\n")
	sb.WriteString("INFO input connected\n")
	for i := 0; i < count; i++ {
		fmt.Fprintf(&sb, "INFO rolling stats: 1000 msg/sec, %.0f MB/sec\n", mbPerSec)
	}
	sb.WriteString("INFO benchmark processor stopped\n")
	return sb.String()
}

func TestMatrixRunner_RunSweepsEveryArm(t *testing.T) {
	// Two arms at one vCPU point must produce two SweepPoints carrying their
	// arm id and GOMAXPROCS, each launched from its own config paths.
	const sessionID = "sess-x"
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-2-a0.log", sessionID): makeLog(30, 60),
			fmt.Sprintf("runs/%s/sweep-2-b.log", sessionID):  makeLog(30, 90),
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": {"bench point complete"}}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM: ssm, LogFetcher: fetcher, RunnerInstance: "i-runner",
		Bucket: "b", SessionID: sessionID,
		ConfigPaths: map[string]pointConfigPaths{
			"2-a0": {Single: "/opt/bench/cfg/2-a0/config.yaml"},
			"2-b":  {Root: "/opt/bench/cfg/2-b/root.yaml", Dir: "/opt/bench/cfg/2-b/streams"},
		},
	}
	plan := []sweepPoint{
		{VCPU: 2, ArmID: "a0", GOMAXPROCS: 2, Streams: 1},
		{VCPU: 2, ArmID: "b", GOMAXPROCS: 4, Streams: 2},
	}
	points, err := mr.Run(context.Background(), plan, 2, 0, 30*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 2)
	require.Equal(t, "a0", points[0].ArmID)
	require.Equal(t, 2, points[0].GOMAXPROCS)
	require.Equal(t, "b", points[1].ArmID)
	require.Equal(t, 4, points[1].GOMAXPROCS)
	require.Equal(t, 2, points[0].VCPU, "every arm shares the vCPU pin")
	require.Equal(t, 2, points[1].VCPU)
	require.Equal(t, 1, points[0].Streams, "arm a0 launches a single pipeline")
	require.Equal(t, 2, points[1].Streams, "arm b launches two pipelines")

	// Each arm's script must reference that arm's own config paths and its own
	// GOMAXPROCS.
	scripts := strings.Join(ssm.Scripts, "\n---\n")
	require.Contains(t, scripts, "run /opt/bench/cfg/2-a0/config.yaml")
	require.Contains(t, scripts, "streams -o /opt/bench/cfg/2-b/root.yaml /opt/bench/cfg/2-b/streams")
	require.Contains(t, scripts, "GOMAXPROCS=2")
	require.Contains(t, scripts, "GOMAXPROCS=4")
}

func TestMatrixRunner_RejectsPlanPointMissingFromConfigPaths(t *testing.T) {
	// Finding #8: configPathsFor used to silently fall back to the legacy
	// ConfigPath on a key miss. For a multi-stream point that fallback
	// yields Root=="" -> a malformed `streams -o  <dir>` launch, which is a
	// confusing failure mode to debug. Run must now reject this loudly,
	// up front, before any AWS spend, naming the exact missing key.
	const sessionID = "sess-missing-key"
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": nil}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM: ssm, LogFetcher: &FakeLogFetcher{}, RunnerInstance: "i-runner",
		Bucket: "b", SessionID: sessionID,
		ConfigPaths: map[string]pointConfigPaths{
			"2-a0": {Single: "/opt/bench/cfg/2-a0/config.yaml"},
			// Deliberately missing "2-b".
		},
	}
	plan := []sweepPoint{
		{VCPU: 2, ArmID: "a0", GOMAXPROCS: 2, Streams: 1},
		{VCPU: 2, ArmID: "b", GOMAXPROCS: 4, Streams: 2},
	}
	_, err := mr.Run(context.Background(), plan, 2, 0, 30*time.Second, "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), `"2-b"`)
	require.Empty(t, ssm.Scripts, "the sweep must reject before issuing any SSM command, not partway through")
}

func TestMatrixRunner_ArmlessPlanUsesLegacyConfigPath(t *testing.T) {
	// nil ConfigPaths → every point launches the single staged config at the
	// historical path, exactly as the six existing scenarios do.
	const sessionID = "sess-y"
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID): makeLog(30, 50),
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": {"bench point complete"}}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM: ssm, LogFetcher: fetcher, RunnerInstance: "i-runner",
		Bucket: "b", SessionID: sessionID,
		ConfigPath: "/opt/bench/config.yaml",
	}
	points, err := mr.Run(context.Background(), []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}}, 2, 0, 30*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.Empty(t, points[0].ArmID)
	scripts := strings.Join(ssm.Scripts, "\n")
	require.Contains(t, scripts, "run /opt/bench/config.yaml")
	require.Contains(t, scripts, "s3://b/runs/sess-y/sweep-1.log",
		"arm-less artifact keys stay bare-vCPU")
}

func TestMatrixRunner_HappyPath(t *testing.T) {
	const sessionID = "bench-test"
	const bucket = "results-bucket"

	// 60s warmup + 120s window = first 60 samples discarded, 120 kept.
	logFor := func(vcpu int) string { return makeLog(180, float64(50+vcpu)) }

	// Broker frames for each point. The Summary is now derived from THESE, not
	// from the log above — the two carry deliberately different numbers so the
	// assertions below can tell which instrument was used. 2 MB over a 10s
	// interval = 0.2 MB/s; 20000 records over 10s = 2000 records/s.
	brokerFor := func(vcpu int) string {
		topic := fmt.Sprintf("bench_%s_pg_cdc_connect", sessionID)
		return fmt.Sprintf(`###timestamp=1000
redpanda_kafka_request_bytes_total{redpanda_namespace="kafka",redpanda_request="produce",redpanda_topic="%s"} 1000000
redpanda_kafka_records_produced_total{redpanda_namespace="kafka",redpanda_topic="%s"} 1000
###timestamp=1010
redpanda_kafka_request_bytes_total{redpanda_namespace="kafka",redpanda_request="produce",redpanda_topic="%s"} %d
redpanda_kafka_records_produced_total{redpanda_namespace="kafka",redpanda_topic="%s"} 21000
`, topic, topic, topic, 1000000+2000000*vcpu, topic)
	}
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID):            logFor(1),
			fmt.Sprintf("runs/%s/sweep-2.log", sessionID):            logFor(2),
			fmt.Sprintf("runs/%s/redpanda-1-connect.txt", sessionID): brokerFor(1),
			fmt.Sprintf("runs/%s/redpanda-2-connect.txt", sessionID): brokerFor(2),
		},
	}
	ssm := &FakeSSM{
		Transcripts: map[string][]string{
			"i-runner": {"starting bench: 1 vCPU", "bench point complete", "log uploaded"},
		},
	}

	// Silence the operator-facing prints during the test.
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM:            ssm,
		LogFetcher:     fetcher,
		RunnerInstance: "i-runner",
		Bucket:         bucket,
		SessionID:      sessionID,
		Topology:       sourceTopology{},
		Names:          newBenchNames(sessionID, "pg_cdc"),
	}
	points, err := mr.Run(context.Background(), []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}, {VCPU: 2, GOMAXPROCS: 2, Streams: 1}}, 1, 60*time.Second, 120*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 2)

	for i, p := range points {
		// Connect's rolling-stats log is still parsed and preserved in full, so
		// the log-derived view remains recomputable from any result file.
		require.Len(t, p.Samples, 120, "point %d should keep window-many samples", i)
		require.Equal(t, 0, p.Samples[0].T, "first kept sample re-indexed to T=0")
		require.Equal(t, 119, p.Samples[119].T)
		require.InDelta(t, float64(50+p.VCPU), p.Samples[0].MBPerSec, 1e-9,
			"samples keep the LOG's numbers")

		// ...but the Summary comes from the BROKER series, so both engines are
		// measured by one instrument and `Δ vs Connect` compares like with like.
		// Asserting the log value here is what this test used to do, and it is
		// exactly the asymmetry that made the head-to-head incomparable.
		expectedBrokerMB := float64(2*p.VCPU) / 10
		require.InDelta(t, expectedBrokerMB, p.Summary.MedianMBPerSec, 1e-9,
			"Summary must be broker-derived, not log-derived")
		require.InDelta(t, 2000, p.Summary.MedianMsgPerSec, 1e-9,
			"records/sec is the compression-independent comparison basis")
	}
}

func TestMatrixRunner_EarlyAbortOnZeroSamples(t *testing.T) {
	const sessionID = "bench-test"

	// Log has zero rolling-stats lines (Connect died immediately).
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID): "ERROR failed to start: license invalid\n",
		},
	}
	ssm := &FakeSSM{
		Transcripts: map[string][]string{"i-runner": {"starting bench: 1 vCPU"}},
	}

	buf := &bytes.Buffer{}
	prev := stdout
	stdout = buf
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM:            ssm,
		LogFetcher:     fetcher,
		RunnerInstance: "i-runner",
		Bucket:         "b",
		SessionID:      sessionID,
	}
	_, err := mr.Run(context.Background(), []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}, {VCPU: 2, GOMAXPROCS: 2, Streams: 1}}, 1, 1*time.Second, 5*time.Second, "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "0 samples")
	// Log tail should have been dumped so the operator can see the error.
	require.Contains(t, buf.String(), "license invalid")
}

func TestMatrixRunner_EarlyAbortFiresPerEngineAtFirstPoint(t *testing.T) {
	// Regression test for the early-abort guard's key. It must fire once per
	// engine at the FIRST plan point (pt.Key() == plan[0].Key()) — not once
	// per sweep (len(out) == 1, a bug introduced and then caught in this
	// task's first pass). A dual-engine sweep where only the SECOND engine's
	// first point produces zero throughput must still abort the whole sweep,
	// not silently continue to later points and burn the rest of a real
	// multi-hour bench on a broken engine.
	//
	// This is exercised under Direction: DirectionSink because that is the
	// only direction whose early-abort switch inspects every engine — its
	// first case (m.Direction == DirectionSink) matches unconditionally on
	// engine. The source-direction switch only has a case for
	// engine == "connect": under source direction, kafka_connect's own
	// zero-throughput signal is never inspected by this guard at all,
	// regardless of the guard's key. That is a pre-existing limitation of the
	// direction/engine switch's case list (frozen, out of scope for this
	// task — see the fix report for detail), not something this guard-key
	// fix can address, so a literal source-direction dual-engine version of
	// this test cannot be constructed to demonstrate an abort.
	const sessionID = "sess-guard"
	const connector = "iceberg"

	const icebergConnect = `###timestamp=1000
total_files_size_bytes 0
###timestamp=1010
total_files_size_bytes 500000000
`
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID):           "INFO starting redpanda-connect\nINFO output connected\n",
			fmt.Sprintf("runs/%s/iceberg-1-connect.txt", sessionID): icebergConnect,
			// Deliberately no iceberg-1-kc.txt: kafka_connect's first point
			// captures zero metric samples, simulating a broken KC connector.
			// No sweep-2.log / iceberg-2-*.txt either — if the guard fails to
			// abort, the run should blow up on THIS point (point 2 doesn't
			// exist for KC), not silently produce clean-looking output.
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": nil}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM:            ssm,
		LogFetcher:     fetcher,
		RunnerInstance: "i-runner",
		Bucket:         "b",
		SessionID:      sessionID,
		Topology:       sinkTopology{},
		Names:          newBenchNames(sessionID, connector),
		Engines:        []string{"connect", "kafka_connect"},
		Direction:      DirectionSink,
	}
	plan := []sweepPoint{
		{VCPU: 1, GOMAXPROCS: 1, Streams: 1},
		{VCPU: 2, GOMAXPROCS: 2, Streams: 1},
	}
	points, err := mr.Run(context.Background(), plan, 1, 60*time.Second, 120*time.Second, "", "")
	require.Error(t, err, "kafka_connect's empty first point must abort the sweep, not just connect's")
	require.Contains(t, err.Error(), "first sweep point at 1 vCPU captured 0 metric samples")
	require.Len(t, points, 2, "connect@1 and kafka_connect@1 were recorded before the abort fired")
	require.Equal(t, "connect", points[0].Engine)
	require.Equal(t, "kafka_connect", points[1].Engine)
	require.Empty(t, points[1].BrokerSeries, "kafka_connect's first point produced no metric samples")
}

func TestMatrixRunner_EarlyAbortFiresForLaterArmToo(t *testing.T) {
	// Regression test for finding #1 of the final whole-branch review: the
	// early-abort guard used to fire ONLY at plan[0] ("later points would
	// fail the same way — true when points differ only in vCPU, false when
	// they differ in launch mechanism"). Arm "a0" here launches via `run`
	// and succeeds; arm "b" launches via `streams` and produces zero
	// throughput. Before the fix (pt.Key() == plan[0].Key() only), arm b
	// would sail through with a 0 MB/s point and no error — the exact
	// "looks like an answer" failure mode this whole review exists to catch.
	const sessionID = "sess-guard-arm"
	const connector = "iceberg"

	const icebergA0 = `###timestamp=1000
total_files_size_bytes 0
###timestamp=1010
total_files_size_bytes 500000000
`
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/iceberg-2-a0-connect.txt", sessionID): icebergA0,
			fmt.Sprintf("runs/%s/sweep-2-a0.log", sessionID):           "INFO starting redpanda-connect\nINFO output connected\n",
			fmt.Sprintf("runs/%s/sweep-2-b.log", sessionID):            "INFO starting redpanda-connect\nINFO output connected\n",
			// Deliberately no iceberg-2-b-connect.txt: arm b's streams-mode
			// launch produced zero metric samples (e.g. it failed to start).
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": nil}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM:            ssm,
		LogFetcher:     fetcher,
		RunnerInstance: "i-runner",
		Bucket:         "b",
		SessionID:      sessionID,
		Topology:       sinkTopology{},
		Names:          newBenchNames(sessionID, connector),
		Engines:        []string{"connect"},
		Direction:      DirectionSink,
	}
	plan := []sweepPoint{
		{VCPU: 2, ArmID: "a0", GOMAXPROCS: 2, Streams: 1},
		{VCPU: 2, ArmID: "b", GOMAXPROCS: 4, Streams: 2},
	}
	points, err := mr.Run(context.Background(), plan, 1, 60*time.Second, 120*time.Second, "", "")
	require.Error(t, err, "arm b's empty metric series must abort the sweep, not just plan[0]'s arm")
	require.Contains(t, err.Error(), "first sweep point at 2 vCPU captured 0 metric samples")
	require.Len(t, points, 2, "both arms were recorded before the abort fired")
	require.Equal(t, "a0", points[0].ArmID)
	require.Equal(t, "b", points[1].ArmID)
	require.Empty(t, points[1].BrokerSeries, "arm b produced no metric samples")
}

func TestMatrixRunner_WarmupTrimsAndReindexes(t *testing.T) {
	const sessionID = "bench-test"
	// 5 samples total, warmup=2s → keep 3, T=0,1,2.
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID): makeLog(5, 10),
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": nil}}

	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM:            ssm,
		LogFetcher:     fetcher,
		RunnerInstance: "i-runner",
		Bucket:         "b",
		SessionID:      sessionID,
	}
	points, err := mr.Run(context.Background(), []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}}, 1, 2*time.Second, 3*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.Len(t, points[0].Samples, 3)
	require.Equal(t, []int{0, 1, 2}, []int{points[0].Samples[0].T, points[0].Samples[1].T, points[0].Samples[2].T})
}

func TestMatrixRunner_FetchesPromAlongsideLog(t *testing.T) {
	const sessionID = "bench-test"
	const bucket = "results-bucket"

	logFor := func(vcpu int) string { return makeLog(180, float64(50+vcpu)) }
	prom := `###timestamp=1000
go_goroutines 10
go_memstats_heap_inuse_bytes 1.0485e+08
###timestamp=1010
go_goroutines 12
go_memstats_heap_inuse_bytes 1.1e+08
`
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID): logFor(1),
			fmt.Sprintf("runs/%s/prom-1.txt", sessionID):  prom,
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": nil}}

	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM:            ssm,
		LogFetcher:     fetcher,
		RunnerInstance: "i-runner",
		Bucket:         bucket,
		SessionID:      sessionID,
	}
	points, err := mr.Run(context.Background(), []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}}, 1, 60*time.Second, 120*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.Len(t, points[0].Prom, 2)
	require.Equal(t, 10, points[0].Prom[0].Goroutines)
	require.Equal(t, 0, points[0].Prom[0].T)
	require.Equal(t, 10, points[0].Prom[1].T)
}

func TestMatrixRunner_MissingPromIsNonFatal(t *testing.T) {
	const sessionID = "bench-test"
	logFor := func(vcpu int) string { return makeLog(180, 50) }
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID): logFor(1),
			// no prom-1.txt
		},
		Errs: map[string]error{
			fmt.Sprintf("runs/%s/prom-1.txt", sessionID): fmt.Errorf("not found"),
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": nil}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{SSM: ssm, LogFetcher: fetcher, RunnerInstance: "i-runner", Bucket: "b", SessionID: sessionID}
	points, err := mr.Run(context.Background(), []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}}, 1, 60*time.Second, 120*time.Second, "", "")
	require.NoError(t, err, "missing prom dump must not fail the sweep point")
	require.Len(t, points, 1)
	require.Empty(t, points[0].Prom, "Prom stays nil/empty when fetch failed")
}

func TestRenderBenchScript_EmbedsBucketAndSession(t *testing.T) {
	got := renderBenchScript(benchScriptArgs{
		VCPU: 4, MemLimitGiB: 4, WarmupSec: 60, DurationSec: 300,
		ConfigPath: "/opt/bench/config.yaml", BinaryPath: "/opt/bench/redpanda-connect",
		Bucket: "my-bucket", SessionID: "sess-1",
	})
	require.Contains(t, got, "/tmp/bench-4.log")
	require.Contains(t, got, "/tmp/prom-4.txt")
	require.Contains(t, got, `s3://my-bucket/runs/sess-1/sweep-4.log`)
	require.Contains(t, got, `s3://my-bucket/runs/sess-1/prom-4.txt`)
	require.Contains(t, got, "taskset -c 2-5") // cores 2..(1+VCPU)
	require.Contains(t, got, "sleep 360")      // warmup+duration
	require.Contains(t, got, "GOMEMLIMIT=4GiB")
	require.Contains(t, got, "[heartbeat]")
	require.Contains(t, got, "###timestamp=")
	require.Contains(t, got, "###scrape_error")
}

func TestRenderBenchScript_DefaultsGOMAXPROCSToVCPU(t *testing.T) {
	// Zero-value GOMAXPROCS/Key/Streams must reproduce the pre-arms script.
	got := renderBenchScript(benchScriptArgs{
		VCPU: 4, MemLimitGiB: 8, WarmupSec: 0, DurationSec: 900,
		ConfigPath: "/opt/bench/config.yaml", BinaryPath: "/opt/bench/redpanda-connect",
		Bucket: "b", SessionID: "s",
	})
	require.Contains(t, got, "GOMAXPROCS=4")
	require.Contains(t, got, "taskset -c 2-5")
	require.Contains(t, got, "/opt/bench/redpanda-connect run /opt/bench/config.yaml")
	require.Contains(t, got, "/tmp/bench-4.log")
	require.Contains(t, got, "s3://b/runs/s/sweep-4.log")
	require.NotContains(t, got, "streams -o")
}

func TestRenderBenchScript_OversubscribesGOMAXPROCSWithoutWideningTaskset(t *testing.T) {
	got := renderBenchScript(benchScriptArgs{
		VCPU: 2, GOMAXPROCS: 4, Streams: 1, Key: "2-a1-1pipe-gmp4",
		MemLimitGiB: 4, DurationSec: 900,
		ConfigPath: "/opt/bench/cfg/2-a1-1pipe-gmp4/config.yaml",
		BinaryPath: "/opt/bench/redpanda-connect",
		Bucket:     "b", SessionID: "s",
	})
	require.Contains(t, got, "GOMAXPROCS=4")
	require.Contains(t, got, "taskset -c 2-3", "the core pin must still follow VCPU, not GOMAXPROCS")
	require.Contains(t, got, "GOMEMLIMIT=4GiB", "memory stays vCPU-derived so arms are memory-fair")
	require.Contains(t, got, "/tmp/bench-2-a1-1pipe-gmp4.log")
	require.Contains(t, got, "s3://b/runs/s/sweep-2-a1-1pipe-gmp4.log")
	require.Contains(t, got, "s3://b/runs/s/prom-2-a1-1pipe-gmp4.txt")
}

func TestRenderBenchScript_StreamsModeLaunch(t *testing.T) {
	got := renderBenchScript(benchScriptArgs{
		VCPU: 2, GOMAXPROCS: 4, Streams: 2, Key: "2-b-2pipe-gmp4",
		MemLimitGiB: 4, DurationSec: 900,
		RootConfigPath: "/opt/bench/cfg/2-b-2pipe-gmp4/root.yaml",
		StreamsDir:     "/opt/bench/cfg/2-b-2pipe-gmp4/streams",
		BinaryPath:     "/opt/bench/redpanda-connect",
		Bucket:         "b", SessionID: "s",
	})
	require.Contains(t, got,
		"/opt/bench/redpanda-connect streams -o /opt/bench/cfg/2-b-2pipe-gmp4/root.yaml /opt/bench/cfg/2-b-2pipe-gmp4/streams")
	require.NotContains(t, got, "redpanda-connect run ")
	require.Contains(t, got, "GOMAXPROCS=4")
	require.Contains(t, got, "taskset -c 2-3")
	require.Contains(t, got, "/tmp/bench-2-b-2pipe-gmp4.log")
}

func TestRenderBenchScript_RedpandaScraperWhenEndpointSet(t *testing.T) {
	// Back-compat path: only the legacy singular field is set. The
	// scraper still wraps it in the IFS-split shell construct (single-
	// element list) so the script shape matches the multi-broker case.
	sc := sourceTopology{}.MetricSidecar(MetricSidecarArgs{
		Engine: "connect", VCPU: 4,
		Bucket: "results-bucket", SessionID: "sess-abc",
		Outs: map[string]string{"redpanda_metrics_endpoint": "10.42.10.10:9644"},
	})
	out := renderBenchScript(benchScriptArgs{
		VCPU:                    4,
		MemLimitGiB:             4,
		WarmupSec:               60,
		DurationSec:             900,
		ConfigPath:              "/tmp/cfg.yaml",
		BinaryPath:              "/opt/bench/rpcn",
		Bucket:                  "results-bucket",
		SessionID:               "sess-abc",
		RedpandaMetricsEndpoint: "10.42.10.10:9644",
		ScrapeSetup:             sc.Setup,
		ScrapeUpload:            sc.Upload,
	})
	if !strings.Contains(out, "RP=/tmp/redpanda-4-connect.txt") {
		t.Errorf("expected RP path line for vcpu 4; got:\n%s", out)
	}
	if !strings.Contains(out, "10.42.10.10:9644") {
		t.Errorf("expected redpanda endpoint embedded in script; got:\n%s", out)
	}
	if !strings.Contains(out, "IFS=,") {
		t.Errorf("expected IFS=, multi-endpoint split even with one endpoint; got:\n%s", out)
	}
	if !strings.Contains(out, "RP_SCRAPER=$!") {
		t.Errorf("expected RP_SCRAPER pid capture; got:\n%s", out)
	}
	if !strings.Contains(out, `kill "$RP_SCRAPER" 2>/dev/null || true`) {
		t.Errorf("expected RP_SCRAPER kill on shutdown; got:\n%s", out)
	}
	if !strings.Contains(out, `aws s3 cp "$RP" "s3://results-bucket/runs/sess-abc/redpanda-4-connect.txt"`) {
		t.Errorf("expected redpanda upload to per-engine filename; got:\n%s", out)
	}
}

func TestRenderBenchScript_RedpandaScrapesAllBrokers(t *testing.T) {
	// Plan 3 fix: Redpanda's per-topic byte counter is per-broker, so
	// the scraper iterates over ALL brokers each interval. A topic
	// whose partition leader landed on broker 1 or 2 would be silently
	// absent from broker 0's scrape (verified live on the 2026-05-29
	// postgres real bench — KC's Debezium topic had 0 attribution
	// despite 2.9M records written).
	endpoints := "10.42.10.10:9644,10.42.11.10:9644,10.42.12.10:9644"
	sc := sourceTopology{}.MetricSidecar(MetricSidecarArgs{
		Engine: "connect", VCPU: 4,
		Bucket: "results-bucket", SessionID: "sess-abc",
		Outs: map[string]string{"redpanda_metrics_endpoints": endpoints},
	})
	out := renderBenchScript(benchScriptArgs{
		VCPU:                     4,
		MemLimitGiB:              4,
		WarmupSec:                60,
		DurationSec:              900,
		ConfigPath:               "/tmp/cfg.yaml",
		BinaryPath:               "/opt/bench/rpcn",
		Bucket:                   "results-bucket",
		SessionID:                "sess-abc",
		RedpandaMetricsEndpoints: endpoints,
		ScrapeSetup:              sc.Setup,
		ScrapeUpload:             sc.Upload,
	})
	if !strings.Contains(out, "IFS=,") {
		t.Errorf("expected IFS=, multi-endpoint split; got:\n%s", out)
	}
	for _, ep := range []string{"10.42.10.10:9644", "10.42.11.10:9644", "10.42.12.10:9644"} {
		if !strings.Contains(out, ep) {
			t.Errorf("expected endpoint %q in script; got:\n%s", ep, out)
		}
	}
}

func TestRenderBenchScript_PluralEndpointsTakePrecedence(t *testing.T) {
	// When both legacy and new fields are set (transition window),
	// the plural wins so we get cluster-wide scrape coverage.
	sc := sourceTopology{}.MetricSidecar(MetricSidecarArgs{
		Engine: "connect", VCPU: 1,
		Bucket: "results-bucket", SessionID: "sess-abc",
		Outs: map[string]string{
			"redpanda_metrics_endpoint":  "10.42.10.10:9644",
			"redpanda_metrics_endpoints": "10.42.10.10:9644,10.42.11.10:9644,10.42.12.10:9644",
		},
	})
	out := renderBenchScript(benchScriptArgs{
		VCPU:                     1,
		MemLimitGiB:              1,
		WarmupSec:                60,
		DurationSec:              60,
		ConfigPath:               "/tmp/cfg.yaml",
		BinaryPath:               "/opt/bench/rpcn",
		Bucket:                   "results-bucket",
		SessionID:                "sess-abc",
		RedpandaMetricsEndpoint:  "10.42.10.10:9644",
		RedpandaMetricsEndpoints: "10.42.10.10:9644,10.42.11.10:9644,10.42.12.10:9644",
		ScrapeSetup:              sc.Setup,
		ScrapeUpload:             sc.Upload,
	})
	if !strings.Contains(out, "10.42.11.10:9644") || !strings.Contains(out, "10.42.12.10:9644") {
		t.Errorf("plural endpoints must override singular; got:\n%s", out)
	}
}

func TestRenderBenchScript_RedpandaScraperOmittedWhenEmpty(t *testing.T) {
	out := renderBenchScript(benchScriptArgs{
		VCPU:                    1,
		MemLimitGiB:             1,
		WarmupSec:               60,
		DurationSec:             900,
		ConfigPath:              "/tmp/cfg.yaml",
		BinaryPath:              "/opt/bench/rpcn",
		Bucket:                  "results-bucket",
		SessionID:               "sess-abc",
		RedpandaMetricsEndpoint: "",
	})
	if strings.Contains(out, "/public_metrics") {
		t.Errorf("expected no redpanda scraper when endpoint is empty; got:\n%s", out)
	}
	if strings.Contains(out, "redpanda-1.txt") {
		t.Errorf("expected no redpanda upload when endpoint is empty; got:\n%s", out)
	}
}

func TestRenderBenchScript_ZeroCadenceArgsReproduceSweepDefault(t *testing.T) {
	// HeartbeatSec/PromScrapeSec/CheckpointSec are all new fields added for
	// soak; a zero value on every one of them must render byte-identical to
	// the script before those fields existed.
	base := benchScriptArgs{
		VCPU: 4, MemLimitGiB: 4, WarmupSec: 60, DurationSec: 300,
		ConfigPath: "/opt/bench/config.yaml", BinaryPath: "/opt/bench/redpanda-connect",
		Bucket: "my-bucket", SessionID: "sess-1",
	}
	withZeroFields := base
	withZeroFields.HeartbeatSec = 0
	withZeroFields.PromScrapeSec = 0
	withZeroFields.CheckpointSec = 0
	require.Equal(t, renderBenchScript(base), renderBenchScript(withZeroFields))
	require.Contains(t, renderBenchScript(base), "sleep 60")
	require.Contains(t, renderBenchScript(base), "sleep 10")
	require.NotContains(t, renderBenchScript(base), "CHECKPOINT")
}

func TestRenderBenchScript_SoakCadences(t *testing.T) {
	// A soak point widens the heartbeat and Prom scrape intervals and adds a
	// mid-run checkpoint upload — the shape main.go renders when Scenario.Soak
	// is set (see the soak-derived MatrixRunner fields in runBench).
	out := renderBenchScript(benchScriptArgs{
		VCPU: 2, MemLimitGiB: 2, WarmupSec: 300, DurationSec: 5400,
		Key:        "2",
		ConfigPath: "/opt/bench/config.yaml", BinaryPath: "/opt/bench/redpanda-connect",
		Bucket: "soak-bucket", SessionID: "sess-soak",
		HeartbeatSec: 95, PromScrapeSec: 60, CheckpointSec: 600,
	})
	require.Contains(t, out, "sleep 95", "heartbeat cadence widened for the long window")
	require.Contains(t, out, "sleep 60", "prom scrape cadence widened for the long window")
	require.Contains(t, out, "sleep 600", "checkpoint cadence rendered")
	require.Contains(t, out, "CHECKPOINT=$!")
	require.Contains(t, out, `kill "$CHECKPOINT" 2>/dev/null || true`)
	// The checkpoint subshell must re-upload to the exact same S3 keys the
	// end-of-script upload uses, so a mid-run crash's last checkpoint and a
	// clean run's final upload are indistinguishable to a downstream fetch.
	require.Contains(t, out, `aws s3 cp "$LOG" "s3://soak-bucket/runs/sess-soak/sweep-2.log" >/dev/null 2>&1 || true`)
	require.Contains(t, out, `aws s3 cp "$PROM" "s3://soak-bucket/runs/sess-soak/prom-2.txt" >/dev/null 2>&1 || true`)
	require.Contains(t, out, `aws s3 cp "$LOG" "s3://soak-bucket/runs/sess-soak/sweep-2.log" >/dev/null`)
	require.Contains(t, out, `aws s3 cp "$PROM" "s3://soak-bucket/runs/sess-soak/prom-2.txt" >/dev/null`)

	// Kill order: HEARTBEAT and PROM_SCRAPER first, then CHECKPOINT, all
	// before the final upload — a killed checkpoint subshell mid-upload
	// would otherwise race the final upload.
	heartbeatKillIdx := strings.Index(out, `kill "$HEARTBEAT"`)
	checkpointKillIdx := strings.Index(out, `kill "$CHECKPOINT"`)
	uploadIdx := strings.Index(out, `echo "bench point complete"`)
	require.True(t, heartbeatKillIdx > 0 && checkpointKillIdx > heartbeatKillIdx && uploadIdx > checkpointKillIdx,
		"expected HEARTBEAT kill, then CHECKPOINT kill, then final upload; got:\n%s", out)
}

func TestRenderBenchScript_CheckpointOmittedWhenZero(t *testing.T) {
	out := renderBenchScript(benchScriptArgs{
		VCPU: 1, MemLimitGiB: 1, WarmupSec: 60, DurationSec: 900,
		ConfigPath: "/opt/bench/config.yaml", BinaryPath: "/opt/bench/redpanda-connect",
		Bucket: "b", SessionID: "s", CheckpointSec: 0,
	})
	require.NotContains(t, out, "CHECKPOINT")
}

func TestMatrixRun_EngineInnerLoop_BothEngines(t *testing.T) {
	const sessionID = "sess"
	// Seed connect logs for both vCPUs. KC engine doesn't fetch.
	logFor := func(vcpu int) string { return makeLog(180, float64(50+vcpu)) }
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID): logFor(1),
			fmt.Sprintf("runs/%s/sweep-2.log", sessionID): logFor(2),
		},
		// Prom is fetched non-fatally — return an error so we don't need to seed it.
		Errs: map[string]error{
			fmt.Sprintf("runs/%s/prom-1.txt", sessionID): fmt.Errorf("not found"),
			fmt.Sprintf("runs/%s/prom-2.txt", sessionID): fmt.Errorf("not found"),
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": nil}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM:                   ssm,
		LogFetcher:            fetcher,
		RunnerInstance:        "i-runner",
		Bucket:                "b",
		SessionID:             sessionID,
		Engines:               []string{"connect", "kafka_connect"},
		KCConnectorName:       "bench_pg",
		KCConnectorConfigJSON: `{"connector.class":"x"}`,
	}
	points, err := mr.Run(context.Background(), []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}, {VCPU: 2, GOMAXPROCS: 2, Streams: 1}}, 1, 60*time.Second, 120*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 4, "expected 4 sweep points (2 vcpu × 2 engines)")

	wantOrder := []struct {
		vcpu   int
		engine string
	}{
		{1, "connect"},
		{1, "kafka_connect"},
		{2, "connect"},
		{2, "kafka_connect"},
	}
	for i, w := range wantOrder {
		require.Equal(t, w.vcpu, points[i].VCPU, "points[%d].VCPU", i)
		require.Equal(t, w.engine, points[i].Engine, "points[%d].Engine", i)
	}

	// Connect points have samples; KC points are empty (Plan 2 doesn't parse KC logs).
	require.NotEmpty(t, points[0].Samples, "connect at vcpu 1 should have samples")
	require.Empty(t, points[1].Samples, "kc at vcpu 1 should have no samples in Plan 2")
	require.NotEmpty(t, points[2].Samples, "connect at vcpu 2 should have samples")
	require.Empty(t, points[3].Samples, "kc at vcpu 2 should have no samples in Plan 2")
}

func TestMatrixRun_PopulatesBrokerSeriesForBothEngines(t *testing.T) {
	const sessionID = "sess1"
	const connector = "postgres_cdc"

	// Per-engine scrape files: each engine scrapes during its own window,
	// so the Connect file holds only Connect's topic and the KC file holds
	// only KC's topic. fetchBrokerSeriesForEngine reads the engine's own
	// file with no cross-engine merge.
	const rpConnect = `###timestamp=1000
redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="bench_sess1_postgres_cdc_connect"} 0
###timestamp=1010
redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="bench_sess1_postgres_cdc_connect"} 500000000
###timestamp=1020
redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="bench_sess1_postgres_cdc_connect"} 1000000000
`
	const rpKC = `###timestamp=2000
redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="bench_sess1_postgres_cdc_kc.public.orders"} 0
###timestamp=2010
redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="bench_sess1_postgres_cdc_kc.public.orders"} 300000000
###timestamp=2020
redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="bench_sess1_postgres_cdc_kc.public.orders"} 629145600
`

	connectLog := makeLog(180, 50)
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID):            connectLog,
			fmt.Sprintf("runs/%s/redpanda-1-connect.txt", sessionID): rpConnect,
			fmt.Sprintf("runs/%s/redpanda-1-kc.txt", sessionID):      rpKC,
		},
		Errs: map[string]error{
			fmt.Sprintf("runs/%s/prom-1.txt", sessionID): fmt.Errorf("not found"),
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": nil}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM:                      ssm,
		LogFetcher:               fetcher,
		RunnerInstance:           "i-runner",
		Bucket:                   "b",
		SessionID:                sessionID,
		Topology:                 sourceTopology{},
		Names:                    newBenchNames(sessionID, connector),
		Engines:                  []string{"connect", "kafka_connect"},
		KCConnectorName:          "bench_postgres_cdc",
		KCConnectorConfigJSON:    `{"connector.class":"x"}`,
		RedpandaMetricsEndpoints: "10.42.0.10:9644,10.42.1.10:9644,10.42.0.11:9644",
	}
	points, err := mr.Run(context.Background(), []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}}, 1, 60*time.Second, 120*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 2)

	connectPt := points[0]
	require.Equal(t, "connect", connectPt.Engine)
	require.NotEmpty(t, connectPt.BrokerSeries, "connect BrokerSeries must be populated from redpanda-1-connect.txt")
	// Connect produced 500 MB in 10s → 50 MB/s.
	require.InDelta(t, 50.0, connectPt.BrokerSeries[0].MBPerSec, 0.1)

	kcPt := points[1]
	require.Equal(t, "kafka_connect", kcPt.Engine)
	require.NotEmpty(t, kcPt.BrokerSeries, "kc BrokerSeries must be populated")
	// KC produced 300 MiB in 10s → 30 MB/s.
	require.InDelta(t, 30.0, kcPt.BrokerSeries[0].MBPerSec, 0.1)
	// KC's Summary should now have non-zero median (derived from broker bytes).
	require.Greater(t, kcPt.Summary.MedianMBPerSec, 0.0, "KC Summary should be derived from broker bytes")
}

func TestMatrixRun_SinkDerivesSummaryFromBrokerSeries(t *testing.T) {
	const sessionID = "sess-sink"
	const connector = "iceberg"

	// A sink's Connect pipeline has no benchmark processor, so there is no
	// rolling-stats log to fetch/parse — log samples are empty by design.
	// Throughput is the Iceberg committed-bytes series scraped into the
	// per-engine metric artifact (iceberg-1-connect.txt for vCPU 1).
	const iceberg = `###timestamp=1000
total_files_size_bytes 0
###timestamp=1010
total_files_size_bytes 500000000
###timestamp=1020
total_files_size_bytes 1000000000
`
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/iceberg-1-connect.txt", sessionID): iceberg,
			// A sink Connect run still produces a (rolling-stats-free) log;
			// parseAndTrim yields no samples from it.
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID): "INFO starting redpanda-connect\nINFO output connected\n",
		},
		Errs: map[string]error{
			fmt.Sprintf("runs/%s/prom-1.txt", sessionID): fmt.Errorf("not found"),
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": nil}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM:            ssm,
		LogFetcher:     fetcher,
		RunnerInstance: "i-runner",
		Bucket:         "b",
		SessionID:      sessionID,
		Topology:       sinkTopology{},
		Names:          newBenchNames(sessionID, connector),
		Engines:        []string{"connect"},
		Direction:      DirectionSink,
	}
	points, err := mr.Run(context.Background(), []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}}, 1, 60*time.Second, 120*time.Second, "", "")
	// No spurious early-abort even though log samples are empty: the sink's
	// metric series is non-empty.
	require.NoError(t, err)
	require.Len(t, points, 1)

	p := points[0]
	require.Equal(t, "connect", p.Engine)
	require.Empty(t, p.Samples, "sink Connect pipeline produces no rolling-stats samples")
	require.NotEmpty(t, p.BrokerSeries, "sink BrokerSeries must come from the Iceberg metric series")
	// 500 MB committed in 10s → 50 MB/s (decimal).
	require.Greater(t, p.Summary.MedianMBPerSec, 0.0, "Summary derived from brokerSeries, not empty log samples")
	require.InDelta(t, 50.0, p.Summary.MedianMBPerSec, 0.1)
}

func TestMatrixRunner_SidecarPollsEveryStreamTableForMultiStreamArm(t *testing.T) {
	// Regression test: MetricSidecar's Names must be scoped to the point's
	// own Streams count. Before the fix, MatrixRunner.Run passed m.Names
	// straight through (Streams == 0), so sinkTopology.MetricSidecar's
	// IcebergTables(engine) always returned only the unsuffixed base table —
	// even for a 2-stream arm whose two pipelines commit to ..._s0 and
	// ..._s1. The sidecar would poll a table nothing writes to (the base
	// table the reset union still creates), silently reporting ~0 MB/s with
	// no error raised anywhere: exactly the silent-corruption class this
	// plan exists to catch. Assert against the actual script text submitted
	// to FakeSSM — the sidecar's Setup is spliced into it — so this pins the
	// real end-to-end path from the plan point to the polled tables, not
	// just the Names.WithStreams call in isolation.
	const sessionID = "sess-multi"
	const connector = "iceberg"

	const iceberg = `###timestamp=1000
total_files_size_bytes 0
###timestamp=1010
total_files_size_bytes 500000000
###timestamp=1020
total_files_size_bytes 1000000000
`
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/iceberg-2-connect.txt", sessionID): iceberg,
			fmt.Sprintf("runs/%s/sweep-2.log", sessionID):           "INFO starting redpanda-connect\nINFO output connected\n",
		},
		Errs: map[string]error{
			fmt.Sprintf("runs/%s/prom-2.txt", sessionID): fmt.Errorf("not found"),
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": nil}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM:            ssm,
		LogFetcher:     fetcher,
		RunnerInstance: "i-runner",
		Bucket:         "b",
		SessionID:      sessionID,
		Topology:       sinkTopology{},
		Names:          newBenchNames(sessionID, connector),
		Engines:        []string{"connect"},
		Direction:      DirectionSink,
	}
	plan := []sweepPoint{{VCPU: 2, GOMAXPROCS: 4, Streams: 2}}
	_, err := mr.Run(context.Background(), plan, 1, 60*time.Second, 120*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, ssm.Scripts, 1)
	script := ssm.Scripts[0]
	require.Contains(t, script, "bench_sess_multi_iceberg_connect_s0",
		"sidecar must poll stream 0's table")
	require.Contains(t, script, "bench_sess_multi_iceberg_connect_s1",
		"sidecar must poll stream 1's table")
}

func TestMatrixRunner_SidecarPollsEveryTopicTableForMultiTopicScenario(t *testing.T) {
	// Regression test mirroring TestMatrixRunner_SidecarPollsEveryStreamTableForMultiStreamArm:
	// MatrixRunner.Topics must be chained onto Names when building the
	// sidecar's MetricSidecarArgs, exactly the way pt.Streams already is.
	// Before this wiring, a 7-topic scenario's sidecar would poll only the
	// unsuffixed base table (which nothing writes to when Topics > 1),
	// silently reporting ~0 MB/s with no error anywhere — the same class of
	// bug the streams precedent (2026-08-04) already caught once.
	const sessionID = "sess-topics"
	const connector = "iceberg"

	const iceberg = `###timestamp=1000
total_files_size_bytes 0
###timestamp=1010
total_files_size_bytes 500000000
###timestamp=1020
total_files_size_bytes 1000000000
`
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/iceberg-2-connect.txt", sessionID): iceberg,
			fmt.Sprintf("runs/%s/sweep-2.log", sessionID):           "INFO starting redpanda-connect\nINFO output connected\n",
		},
		Errs: map[string]error{
			fmt.Sprintf("runs/%s/prom-2.txt", sessionID): fmt.Errorf("not found"),
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": nil}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM:            ssm,
		LogFetcher:     fetcher,
		RunnerInstance: "i-runner",
		Bucket:         "b",
		SessionID:      sessionID,
		Topology:       sinkTopology{},
		Names:          newBenchNames(sessionID, connector),
		Topics:         7,
		Engines:        []string{"connect"},
		Direction:      DirectionSink,
	}
	plan := []sweepPoint{{VCPU: 2, GOMAXPROCS: 2, Streams: 1}}
	_, err := mr.Run(context.Background(), plan, 1, 60*time.Second, 120*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, ssm.Scripts, 1)
	script := ssm.Scripts[0]
	for i := 0; i < 7; i++ {
		require.Contains(t, script, fmt.Sprintf("bench_sess_topics_iceberg_connect_t%d", i),
			"sidecar must poll topic %d's table", i)
	}
}

func TestMatrixRun_EngineInnerLoop_ConnectOnly(t *testing.T) {
	const sessionID = "sess"
	logFor := func(vcpu int) string { return makeLog(180, float64(50+vcpu)) }
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID): logFor(1),
			fmt.Sprintf("runs/%s/sweep-2.log", sessionID): logFor(2),
			fmt.Sprintf("runs/%s/sweep-4.log", sessionID): logFor(4),
		},
		Errs: map[string]error{
			fmt.Sprintf("runs/%s/prom-1.txt", sessionID): fmt.Errorf("not found"),
			fmt.Sprintf("runs/%s/prom-2.txt", sessionID): fmt.Errorf("not found"),
			fmt.Sprintf("runs/%s/prom-4.txt", sessionID): fmt.Errorf("not found"),
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": nil}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM:            ssm,
		LogFetcher:     fetcher,
		RunnerInstance: "i-runner",
		Bucket:         "b",
		SessionID:      sessionID,
		Engines:        []string{"connect"},
	}
	points, err := mr.Run(context.Background(), []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}, {VCPU: 2, GOMAXPROCS: 2, Streams: 1}, {VCPU: 4, GOMAXPROCS: 4, Streams: 1}}, 1, 60*time.Second, 120*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 3, "expected 3 sweep points (connect-only)")
	for _, p := range points {
		require.Equal(t, "connect", p.Engine)
	}
}

// TestMatrixRunner_SoakEndToEnd_EmitsContractMetricsFromFakeInfra walks a
// full soak point through FakeSSM + FakeLogFetcher + FakeEmitter — the same
// wiring a real `runner bench` soak invocation uses, minus AWS — and asserts
// the emitted CloudWatch data matches the metric contract exactly: the
// fixed set of names, the mean/last aggregation semantics per metric, and
// the warmup-offset alignment between the log-derived and broker/prom-
// derived series (see offsetSampleT / aggregateSoakMinutes).
//
// CheckpointSec is left at 0, so only the deterministic FINAL post-point
// emit runs here — the mid-run loop's own timer-driven behavior is exercised
// separately (and without any real-time waiting) by
// TestMatrixRunner_EmitSoakCycle_DedupesAndAdvancesAcrossGrowingCheckpoints.
func TestMatrixRunner_SoakEndToEnd_EmitsContractMetricsFromFakeInfra(t *testing.T) {
	const sessionID = "soak-sess"
	const bucket = "soak-bucket"
	const connector = "pg_cdc"
	topic := fmt.Sprintf("bench_%s_%s_connect", sessionID, connector)

	// 60s warmup + 120s window: makeLog(180, 42) gives parseAndTrim exactly
	// 120 kept samples (T 0..119), all at a constant 42 MB/s.
	logContent := makeLog(180, 42)
	// 15 frames at a 10s cadence starting at unix 1000 -> 14 TopicPoints at
	// T=10,20,...,140. Constant deltas -> constant 0.2 MB/s / 2000 msg/s.
	brokerContent := makeBrokerFrames(topic, 1000, 15, 10, 2_000_000, 20_000)
	promContent := `###timestamp=2000
go_goroutines 5
go_memstats_heap_inuse_bytes 1.0e+07
process_resident_memory_bytes 100
###timestamp=2050
go_goroutines 6
go_memstats_heap_inuse_bytes 2.0e+07
process_resident_memory_bytes 200
###timestamp=2065
go_goroutines 7
go_memstats_heap_inuse_bytes 3.0e+07
process_resident_memory_bytes 300
###timestamp=2090
go_goroutines 8
go_memstats_heap_inuse_bytes 4.0e+07
process_resident_memory_bytes 400
###timestamp=2125
go_goroutines 9
go_memstats_heap_inuse_bytes 5.0e+07
process_resident_memory_bytes 500
`

	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID):            logContent,
			fmt.Sprintf("runs/%s/prom-1.txt", sessionID):             promContent,
			fmt.Sprintf("runs/%s/redpanda-1-connect.txt", sessionID): brokerContent,
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": {"bench point complete"}}}
	emitter := &FakeEmitter{}

	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM:            ssm,
		LogFetcher:     fetcher,
		RunnerInstance: "i-runner",
		Bucket:         bucket,
		SessionID:      sessionID,
		Topology:       sourceTopology{},
		Names:          newBenchNames(sessionID, connector),
		Emitter:        emitter,
		// 5000 > the fixture's constant 2000 msg/s delivered, so backlog
		// accrues instead of staying pinned at 0.
		ExpectedRecordsPerSec: 5000,
	}
	points, err := mr.Run(context.Background(), []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}}, 1, 60*time.Second, 120*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 1)

	require.Equal(t, 1, emitter.CallCount(), "CheckpointSec==0 disables the mid-run loop; only the final post-point emit runs")
	data := emitter.All()

	byMinute := map[time.Time]map[string]float64{}
	var runActiveCount int
	for _, d := range data {
		if d.Name == metricRunActive {
			runActiveCount++
			require.Equal(t, unitCount, d.Unit)
			require.Equal(t, float64(1), d.Value)
			continue
		}
		if byMinute[d.At] == nil {
			byMinute[d.At] = map[string]float64{}
		}
		_, dup := byMinute[d.At][d.Name]
		require.False(t, dup, "the same (minute, metric) pair must never be emitted twice: %s @ %s", d.Name, d.At)
		byMinute[d.At][d.Name] = d.Value
	}
	require.Equal(t, 1, runActiveCount, "exactly one RunActive datum per emit cycle")
	require.Len(t, byMinute, 2, "minutes 0 and 1 are complete; minute 2 is the current incomplete minute and must never appear")

	var minutes []time.Time
	for m := range byMinute {
		minutes = append(minutes, m)
	}
	sort.Slice(minutes, func(i, j int) bool { return minutes[i].Before(minutes[j]) })
	require.Equal(t, minutes[0].Add(time.Minute), minutes[1])
	minute0, minute1 := byMinute[minutes[0]], byMinute[minutes[1]]

	// Minute 0 falls entirely inside warmup: the broker/prom series (never
	// warmup-trimmed — see aggregateSoakMinutes' doc comment) DO cover it,
	// but Connect's own rolling-stats log (warmup-trimmed by parseAndTrim,
	// then shifted back by offsetSampleT) has no data there at all.
	require.InDelta(t, 0.2, minute0[metricThroughputMBps], 1e-9)
	require.InDelta(t, 2000, minute0[metricRecordsPerSec], 1e-9)
	require.NotContains(t, minute0, metricLogThroughputMBps)
	require.Equal(t, float64(200), minute0[metricRSSBytes])
	require.Equal(t, float64(20_000_000), minute0[metricHeapInUseBytes])
	require.Equal(t, float64(6), minute0[metricGoroutines])
	require.InDelta(t, 30, minute0[metricBacklogSeconds], 1e-9)

	// Minute 1 is past warmup: every contract metric, including the
	// log-derived one, is present — and this is the alignment the warmup-
	// offset handling exists for: LogThroughputMBps lands in the SAME
	// minute the broker series reports for the same wall-clock window, not
	// one minute early.
	require.InDelta(t, 0.2, minute1[metricThroughputMBps], 1e-9)
	require.InDelta(t, 2000, minute1[metricRecordsPerSec], 1e-9)
	require.InDelta(t, 42, minute1[metricLogThroughputMBps], 1e-9)
	require.Equal(t, float64(400), minute1[metricRSSBytes])
	require.Equal(t, float64(40_000_000), minute1[metricHeapInUseBytes])
	require.Equal(t, float64(8), minute1[metricGoroutines])
	require.InDelta(t, 66, minute1[metricBacklogSeconds], 1e-9)
}

// TestMatrixRunner_EmitSoakCycle_DedupesAndAdvancesAcrossGrowingCheckpoints
// exercises the mid-run loop's own per-cycle unit, emitSoakCycle, directly —
// avoiding any dependency on real wall-clock timer behavior (FakeSSM.Run
// returns synchronously, so driving this through the actual ticker inside
// runSoakEmitLoop would be racy to assert on in a test). It simulates three
// checkpoint fetches: the bench script's checkpoint upload overwrites the
// SAME S3 keys with ever-growing content, and the high-water mark is what
// keeps a re-fetch of unchanged content from re-emitting already-sent
// minutes.
func TestMatrixRunner_EmitSoakCycle_DedupesAndAdvancesAcrossGrowingCheckpoints(t *testing.T) {
	const sessionID = "soak-sess-2"
	const connector = "pg_cdc"
	const key = "1"
	topic := fmt.Sprintf("bench_%s_%s_connect", sessionID, connector)

	fetcher := &FakeLogFetcher{Contents: map[string]string{}}
	setCheckpoint := func(logLines, frames int) {
		fetcher.Contents[fmt.Sprintf("runs/%s/sweep-%s.log", sessionID, key)] = makeLog(logLines, 42)
		fetcher.Contents[fmt.Sprintf("runs/%s/redpanda-%s-connect.txt", sessionID, key)] = makeBrokerFrames(topic, 1000, frames, 10, 2_000_000, 20_000)
	}

	// First checkpoint: same shape as the end-to-end test above — minutes 0
	// and 1 are complete, minute 2 is still open.
	setCheckpoint(180, 15)

	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	emitter := &FakeEmitter{}
	mr := &MatrixRunner{
		LogFetcher: fetcher, Bucket: "b", SessionID: sessionID,
		Topology: sourceTopology{}, Names: newBenchNames(sessionID, connector),
		Emitter: emitter,
	}
	hw := newSoakHighWater()
	pointStart := time.Now()
	warmup := 60 * time.Second

	mr.emitSoakCycle(context.Background(), "connect", key, pointStart, warmup, hw)
	require.Equal(t, 1, hw.get())
	require.NotEmpty(t, emitter.All())

	// Second cycle against the IDENTICAL checkpoint content (as if the
	// bench script hasn't re-uploaded since the last cycle) must add
	// NOTHING new — the high-water mark is what prevents a duplicate
	// minute 0/1 emission.
	mr.emitSoakCycle(context.Background(), "connect", key, pointStart, warmup, hw)
	require.Equal(t, 1, hw.get(), "the high-water mark must not move when there is nothing new to emit")
	secondCallData := emitter.LastCall()
	for _, d := range secondCallData {
		require.Equal(t, metricRunActive, d.Name,
			"only the per-cycle RunActive heartbeat may repeat; every per-minute metric must be new")
	}

	// Third checkpoint: the run has progressed — minute 2 now has enough
	// data to be complete, and minute 3 is the new open minute.
	setCheckpoint(240, 21)
	mr.emitSoakCycle(context.Background(), "connect", key, pointStart, warmup, hw)
	require.Equal(t, 2, hw.get(), "the high-water mark must advance to the newly-completed minute 2")
	thirdCallData := emitter.LastCall()
	var sawMinute2Throughput bool
	for _, d := range thirdCallData {
		if d.Name == metricThroughputMBps {
			sawMinute2Throughput = true
		}
	}
	require.True(t, sawMinute2Throughput, "the newly-completed minute must actually be emitted, not just silently advance the mark")
}

// TestMatrixRunner_EmitAggregated_IncludesRSSSlopeOnceEnoughPromHistory
// exercises emitAggregated's own wiring of rssSlopeBytesPerMin (the
// aggregation contract itself is covered by TestRSSSlopeBytesPerMin in
// cloudwatch_test.go): fewer than 10 prom points must never publish the
// metric, and once there are enough, it must land stamped "now" like
// RunActive rather than backfilled to a past minute.
func TestMatrixRunner_EmitAggregated_IncludesRSSSlopeOnceEnoughPromHistory(t *testing.T) {
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	promFew := []PromPoint{{T: 0, RSSBytes: 100}, {T: 60, RSSBytes: 200}}
	var promEnough []PromPoint
	for i := 0; i < 12; i++ {
		promEnough = append(promEnough, PromPoint{T: i * secondsPerMinute, RSSBytes: uint64(i) * 1_000_000})
	}

	emitter := &FakeEmitter{}
	mr := &MatrixRunner{Emitter: emitter}
	pointStart := time.Now()

	mr.emitAggregated(context.Background(), nil, promFew, nil, nil, pointStart, 0, newSoakHighWater())
	for _, d := range emitter.LastCall() {
		require.NotEqual(t, metricRSSSlopeBytesPerMin, d.Name, "too few prom samples must never publish the slope metric")
	}

	mr.emitAggregated(context.Background(), nil, promEnough, nil, nil, pointStart, 0, newSoakHighWater())
	var found *MetricDatum
	for i, d := range emitter.LastCall() {
		if d.Name == metricRSSSlopeBytesPerMin {
			found = &emitter.LastCall()[i]
		}
	}
	require.NotNil(t, found, "expected an RSSSlopeBytesPerMin datum once prom has enough history")
	require.Equal(t, unitNone, found.Unit)
	require.InDelta(t, 1_000_000, found.Value, 1e-3)
}

// TestMatrixRunner_StartSoakEmitLoop_TicksAndStopsCleanly exercises the
// actual goroutine wiring startSoakEmitLoop returns — the one piece of this
// feature the other soak tests deliberately avoid driving through a real
// ticker (see the comment on TestMatrixRunner_SoakEndToEnd_...). soakEmitGrace
// is zeroed so the first tick doesn't require a real 30s wait.
func TestMatrixRunner_StartSoakEmitLoop_TicksAndStopsCleanly(t *testing.T) {
	prevGrace := soakEmitGrace
	soakEmitGrace = 0
	defer func() { soakEmitGrace = prevGrace }()

	const sessionID = "soak-loop"
	const connector = "pg_cdc"
	const key = "1"
	topic := fmt.Sprintf("bench_%s_%s_connect", sessionID, connector)

	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-%s.log", sessionID, key):            makeLog(180, 42),
			fmt.Sprintf("runs/%s/redpanda-%s-connect.txt", sessionID, key): makeBrokerFrames(topic, 1000, 15, 10, 2_000_000, 20_000),
		},
	}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	emitter := &FakeEmitter{}
	mr := &MatrixRunner{
		LogFetcher: fetcher, Bucket: "b", SessionID: sessionID,
		Topology: sourceTopology{}, Names: newBenchNames(sessionID, connector),
		Emitter: emitter, CheckpointSec: 1, // 1s interval + 0 grace -> ticks almost immediately
	}
	hw := newSoakHighWater()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := mr.startSoakEmitLoop(ctx, "connect", key, time.Now(), 60*time.Second, hw)

	require.Eventually(t, func() bool {
		return emitter.CallCount() > 0
	}, 5*time.Second, 20*time.Millisecond, "the loop must tick and emit at least once")

	stop()
	callsAtStop := emitter.CallCount()
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, callsAtStop, emitter.CallCount(),
		"stop() must block until the goroutine has actually exited — no more emits after it returns")
}
