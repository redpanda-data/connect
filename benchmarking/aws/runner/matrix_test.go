// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
