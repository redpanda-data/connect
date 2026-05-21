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
