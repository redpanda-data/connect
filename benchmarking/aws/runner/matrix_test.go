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

func TestMatrixRunner_HappyPath(t *testing.T) {
	const sessionID = "bench-test"
	const bucket = "results-bucket"

	// 60s warmup + 120s window = first 60 samples discarded, 120 kept.
	logFor := func(vcpu int) string { return makeLog(180, float64(50+vcpu)) }
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID): logFor(1),
			fmt.Sprintf("runs/%s/sweep-2.log", sessionID): logFor(2),
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
	}
	points, err := mr.Run(context.Background(), []int{1, 2}, 1, 60*time.Second, 120*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 2)

	for i, p := range points {
		require.Len(t, p.Samples, 120, "point %d should keep window-many samples", i)
		require.Equal(t, 0, p.Samples[0].T, "first kept sample re-indexed to T=0")
		require.Equal(t, 119, p.Samples[119].T)
		expectedMB := float64(50 + p.VCPU)
		require.InDelta(t, expectedMB, p.Summary.MedianMBPerSec, 1e-9)
		require.InDelta(t, expectedMB, p.Summary.PeakMBPerSec, 1e-9)
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
	_, err := mr.Run(context.Background(), []int{1, 2}, 1, 1*time.Second, 5*time.Second, "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "0 samples")
	// Log tail should have been dumped so the operator can see the error.
	require.Contains(t, buf.String(), "license invalid")
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
	points, err := mr.Run(context.Background(), []int{1}, 1, 2*time.Second, 3*time.Second, "", "")
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
	points, err := mr.Run(context.Background(), []int{1}, 1, 60*time.Second, 120*time.Second, "", "")
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
	points, err := mr.Run(context.Background(), []int{1}, 1, 60*time.Second, 120*time.Second, "", "")
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

func TestRenderBenchScript_RedpandaScraperWhenEndpointSet(t *testing.T) {
	// Back-compat path: only the legacy singular field is set. The
	// scraper still wraps it in the IFS-split shell construct (single-
	// element list) so the script shape matches the multi-broker case.
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
	points, err := mr.Run(context.Background(), []int{1, 2}, 1, 60*time.Second, 120*time.Second, "", "")
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
redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="bench_sess1_postgres_cdc_connect"} 524288000
###timestamp=1020
redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="bench_sess1_postgres_cdc_connect"} 1048576000
`
	const rpKC = `###timestamp=2000
redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="bench_sess1_postgres_cdc_kc.public.orders"} 0
###timestamp=2010
redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="bench_sess1_postgres_cdc_kc.public.orders"} 314572800
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
	points, err := mr.Run(context.Background(), []int{1}, 1, 60*time.Second, 120*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 2)

	connectPt := points[0]
	require.Equal(t, "connect", connectPt.Engine)
	require.NotEmpty(t, connectPt.BrokerSeries, "connect BrokerSeries must be populated from redpanda-1-connect.txt")
	// Connect produced 500 MiB in 10s → 50 MiB/s.
	require.InDelta(t, 50.0, connectPt.BrokerSeries[0].MBPerSec, 0.1)

	kcPt := points[1]
	require.Equal(t, "kafka_connect", kcPt.Engine)
	require.NotEmpty(t, kcPt.BrokerSeries, "kc BrokerSeries must be populated")
	// KC produced 300 MiB in 10s → 30 MiB/s.
	require.InDelta(t, 30.0, kcPt.BrokerSeries[0].MBPerSec, 0.1)
	// KC's Summary should now have non-zero median (derived from broker bytes).
	require.Greater(t, kcPt.Summary.MedianMBPerSec, 0.0, "KC Summary should be derived from broker bytes")
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
	points, err := mr.Run(context.Background(), []int{1, 2, 4}, 1, 60*time.Second, 120*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 3, "expected 3 sweep points (connect-only)")
	for _, p := range points {
		require.Equal(t, "connect", p.Engine)
	}
}
