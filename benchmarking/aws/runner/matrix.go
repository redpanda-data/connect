// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// stdout is the package-level writer used by streaming helpers.  main.go may
// override this for tests or structured logging; os.Stdout is the default.
var stdout io.Writer = os.Stdout

// MatrixRunner orchestrates the CPU sweep against the runner EC2.
type MatrixRunner struct {
	SSM             SSMExecutor
	RunnerInstance  string
	LoadGenInstance string
	ConfigPath      string // path on the runner host to benchmark_config.yaml
	BinaryPath      string // path on the runner host to redpanda-connect
}

// SweepPoint is the per-point measurement.
type SweepPoint struct {
	VCPU      int
	Samples   []Sample
	Summary   Summary
	Anomalies []Anomaly
}

// Run executes the full sweep. resetScript runs on the runner host between
// points (e.g. drop the CDC replication slot). workloadScript, if non-empty,
// runs on the load-gen host concurrently with the bench step.
func (m *MatrixRunner) Run(
	ctx context.Context,
	cpuPoints []int,
	memLimitPerVCPU int,
	warmup, duration time.Duration,
	resetScript string,
	workloadScript string,
) ([]SweepPoint, error) {
	out := make([]SweepPoint, 0, len(cpuPoints))
	for _, n := range cpuPoints {
		fmt.Printf("=== sweep point: %d vCPU (warmup %s, window %s) ===\n", n, warmup, duration)

		if resetScript != "" {
			if err := m.SSM.Run(ctx, m.RunnerInstance, resetScript, streamingOnLine(stdout, "reset")); err != nil {
				return nil, fmt.Errorf("reset at %d vCPU: %w", n, err)
			}
		}

		workloadCtx, cancelWorkload := context.WithCancel(ctx)
		workloadDone := make(chan error, 1)
		if workloadScript != "" {
			go func() {
				workloadDone <- m.SSM.Run(workloadCtx, m.LoadGenInstance, workloadScript, streamingOnLine(stdout, "load"))
			}()
		} else {
			close(workloadDone)
		}

		script := renderBenchScript(benchScriptArgs{
			VCPU:        n,
			MemLimitGiB: memLimitPerVCPU * n,
			WarmupSec:   int(warmup.Seconds()),
			DurationSec: int(duration.Seconds()),
			ConfigPath:  m.ConfigPath,
			BinaryPath:  m.BinaryPath,
		})

		var samples []Sample
		t := 0
		onLine := func(line string) {
			s, ok := ParseRollingStatsLine(line)
			if !ok {
				fmt.Fprintln(stdout, "[bench] "+line)
				return
			}
			elapsed := time.Duration(t) * time.Second
			t++
			if elapsed < warmup {
				return // discard warmup
			}
			s.T = int(elapsed.Seconds() - warmup.Seconds())
			samples = append(samples, s)
		}
		if err := m.SSM.Run(ctx, m.RunnerInstance, script, onLine); err != nil {
			cancelWorkload()
			return nil, fmt.Errorf("bench at %d vCPU: %w", n, err)
		}
		cancelWorkload()
		<-workloadDone

		summary := Summarise(samples)
		anomalies := DetectAnomalies(samples, summary.MedianMBPerSec)
		out = append(out, SweepPoint{
			VCPU:      n,
			Samples:   samples,
			Summary:   summary,
			Anomalies: anomalies,
		})
		fmt.Printf("  -> median %.0f MB/s (p5 %.0f, p95 %.0f, peak %.0f), %d anomalies\n",
			summary.MedianMBPerSec, summary.P5MBPerSec, summary.P95MBPerSec, summary.PeakMBPerSec, len(anomalies))
	}
	return out, nil
}

type benchScriptArgs struct {
	VCPU        int
	MemLimitGiB int
	WarmupSec   int
	DurationSec int
	ConfigPath  string
	BinaryPath  string
}

// renderBenchScript produces the shell script executed on the runner EC2 for
// one sweep point. The script pins Connect to the measured cores, runs it for
// warmup+duration seconds, then SIGTERMs cleanly so the benchmark processor
// flushes its final rolling-stats line.
func renderBenchScript(a benchScriptArgs) string {
	// Cores 0,1 reserved → measured set starts at core 2.
	cpusetHi := 1 + a.VCPU // inclusive
	return strings.Join([]string{
		`set -euo pipefail`,
		fmt.Sprintf(`echo "starting bench: %d vCPU, %d GiB, warmup %ds, window %ds"`,
			a.VCPU, a.MemLimitGiB, a.WarmupSec, a.DurationSec),
		fmt.Sprintf(`taskset -c 2-%d chrt --fifo 50 env GOMAXPROCS=%d GOMEMLIMIT=%dGiB REDPANDA_LICENSE_FILEPATH=/opt/bench/license.jwt %s run %s &`,
			cpusetHi, a.VCPU, a.MemLimitGiB, a.BinaryPath, a.ConfigPath),
		`PID=$!`,
		fmt.Sprintf(`sleep %d`, a.WarmupSec+a.DurationSec),
		`kill -TERM $PID || true`,
		`wait $PID 2>/dev/null || true`,
		`echo "bench point complete"`,
	}, "\n")
}
