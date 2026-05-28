// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bytes"
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
	LogFetcher      LogFetcher
	RunnerInstance  string
	LoadGenInstance string
	ConfigPath      string // path on the runner host to benchmark_config.yaml
	BinaryPath      string // path on the runner host to redpanda-connect
	Bucket          string // S3 bucket where per-point Connect logs are uploaded
	SessionID       string // run-scoped key prefix: runs/<SessionID>/sweep-<vcpu>.log
	// RedpandaMetricsEndpoint is the host:port pair (e.g. "10.42.10.10:9644") the
	// per-point scraper curls every 10s. Empty disables the scraper (Plan 1
	// safety net for environments without Redpanda yet).
	RedpandaMetricsEndpoint string
	// Engines lists the engines to sweep at each vCPU point, in order.
	// Default ["connect"] preserves the pre-Plan-2 behavior.
	Engines []string
	// KCConnectorName is the name to submit the KC connector under.
	// Empty when Engines does not include "kafka_connect".
	KCConnectorName string
	// KCConnectorConfigJSON is the rendered JSON config posted to KC's REST API.
	KCConnectorConfigJSON string
}

// SweepPoint is the per-point measurement.
type SweepPoint struct {
	VCPU      int
	Engine    string
	Samples   []Sample
	Summary   Summary
	Anomalies []Anomaly
	Prom      []PromPoint
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
	engines := m.Engines
	if len(engines) == 0 {
		engines = []string{"connect"}
	}
	out := make([]SweepPoint, 0, len(cpuPoints)*len(engines))
	for _, n := range cpuPoints {
		for _, engine := range engines {
			fmt.Printf("=== sweep point: %d vCPU, engine=%s (warmup %s, window %s) ===\n", n, engine, warmup, duration)

			if resetScript != "" {
				if err := m.SSM.Run(ctx, m.RunnerInstance, resetScript, streamingOnLine(stdout, "reset")); err != nil {
					return nil, fmt.Errorf("reset at %d vCPU (%s): %w", n, engine, err)
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

			var script string
			switch engine {
			case "connect":
				script = renderBenchScript(benchScriptArgs{
					VCPU:                    n,
					MemLimitGiB:             memLimitPerVCPU * n,
					WarmupSec:               int(warmup.Seconds()),
					DurationSec:             int(duration.Seconds()),
					ConfigPath:              m.ConfigPath,
					BinaryPath:              m.BinaryPath,
					Bucket:                  m.Bucket,
					SessionID:               m.SessionID,
					RedpandaMetricsEndpoint: m.RedpandaMetricsEndpoint,
				})
			case "kafka_connect":
				script = renderKCBenchScript(kcBenchScriptArgs{
					VCPU:                n,
					MemLimitGiB:         memLimitPerVCPU * n,
					WarmupSec:           int(warmup.Seconds()),
					DurationSec:         int(duration.Seconds()),
					ConnectorName:       m.KCConnectorName,
					ConnectorConfigJSON: m.KCConnectorConfigJSON,
					Bucket:              m.Bucket,
					SessionID:           m.SessionID,
				})
			default:
				cancelWorkload()
				if werr := <-workloadDone; werr != nil && werr != context.Canceled {
					fmt.Fprintf(stdout, "[bench] workload exited with error: %v\n", werr)
				}
				return nil, fmt.Errorf("unknown engine %q at vcpu %d", engine, n)
			}

			// The bench script writes the engine's stdout/stderr to a per-engine
			// log file on the runner host and uploads it to S3 after termination.
			// SSM stdout only carries the script's own status echos and a
			// per-minute heartbeat (well under the ~24KB SSM content cap), so
			// streaming every line is safe.
			if err := m.SSM.Run(ctx, m.RunnerInstance, script, streamingOnLine(stdout, fmt.Sprintf("bench-%s", engine))); err != nil {
				cancelWorkload()
				if werr := <-workloadDone; werr != nil && werr != context.Canceled {
					fmt.Fprintf(stdout, "[bench] workload exited with error: %v\n", werr)
				}
				return nil, fmt.Errorf("bench at %d vCPU (%s): %w", n, engine, err)
			}
			cancelWorkload()
			if werr := <-workloadDone; werr != nil && werr != context.Canceled {
				fmt.Fprintf(stdout, "[bench] workload exited with error: %v\n", werr)
			}

			// Per-engine fetch + parse. KC's broker-side metrics are scraped on
			// the runner and uploaded to S3 (Plan 3 parses them); the Plan 2
			// orchestrator does not read the KC log here.
			var samples []Sample
			var rawLog []byte
			if engine == "connect" {
				raw, err := m.fetchLog(ctx, n)
				if err != nil {
					return nil, fmt.Errorf("fetch log at %d vCPU (%s): %w", n, engine, err)
				}
				rawLog = raw
				samples = parseAndTrim(raw, warmup)
			}
			promPts := m.fetchProm(ctx, n)

			summary := Summarise(samples)
			anomalies := DetectAnomaliesWithProm(samples, summary.MedianMBPerSec, promPts)
			out = append(out, SweepPoint{
				VCPU:      n,
				Engine:    engine,
				Samples:   samples,
				Summary:   summary,
				Anomalies: anomalies,
				Prom:      promPts,
			})
			fmt.Printf("  -> %d samples; median %.2f MB/s (p5 %.2f, p95 %.2f, peak %.2f), %d anomalies\n",
				len(samples), summary.MedianMBPerSec, summary.P5MBPerSec, summary.P95MBPerSec, summary.PeakMBPerSec, len(anomalies))

			// Early-abort: if the first Connect sweep point captured no samples
			// (Connect failed to start or the connector errored out for the
			// whole window), the remaining points will almost certainly fail
			// the same way. Bail out, dump the tail of the log so the operator
			// can see the failure, and let the destroy defer reclaim the infra.
			//
			// Guarded by engine == "connect" because KC samples are intentionally
			// empty in Plan 2 (its log isn't parsed here) and would otherwise
			// always trigger early-abort.
			if engine == "connect" && n == cpuPoints[0] && len(samples) == 0 {
				const tailMax = 4 * 1024
				tail := rawLog
				if len(rawLog) > tailMax {
					tail = rawLog[len(rawLog)-tailMax:]
				}
				fmt.Fprintf(stdout, "[bench] connect log tail (last %d bytes):\n%s\n", len(tail), tail)
				return out, fmt.Errorf("first sweep point at %d vCPU captured 0 samples — see log tail above", n)
			}
		}
	}
	return out, nil
}

// fetchLog downloads the per-point Connect log uploaded by the bench script.
func (m *MatrixRunner) fetchLog(ctx context.Context, vcpu int) ([]byte, error) {
	if m.LogFetcher == nil {
		return nil, fmt.Errorf("LogFetcher not configured")
	}
	key := fmt.Sprintf("runs/%s/sweep-%d.log", m.SessionID, vcpu)
	body, err := m.LogFetcher.Fetch(ctx, m.Bucket, key)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	return io.ReadAll(body)
}

// fetchProm downloads the per-point Prometheus dump uploaded by the bench
// script. Failure is non-fatal — the sweep point is still useful without
// goroutine/heap context.
func (m *MatrixRunner) fetchProm(ctx context.Context, vcpu int) []PromPoint {
	if m.LogFetcher == nil {
		return nil
	}
	key := fmt.Sprintf("runs/%s/prom-%d.txt", m.SessionID, vcpu)
	body, err := m.LogFetcher.Fetch(ctx, m.Bucket, key)
	if err != nil {
		fmt.Fprintf(stdout, "[bench] fetch prom (non-fatal): %v\n", err)
		return nil
	}
	defer body.Close()
	pts, err := ParsePromStream(body)
	if err != nil {
		fmt.Fprintf(stdout, "[bench] parse prom (non-fatal): %v\n", err)
		return nil
	}
	return pts
}

// parseAndTrim parses the Connect log and discards the leading warmup samples,
// reindexing T so the first kept sample is T=0.
func parseAndTrim(raw []byte, warmup time.Duration) []Sample {
	all, _ := ParseRollingStatsStream(bytes.NewReader(raw))
	drop := int(warmup.Seconds())
	if drop >= len(all) {
		return nil
	}
	kept := make([]Sample, len(all)-drop)
	for i, s := range all[drop:] {
		s.T = i
		kept[i] = s
	}
	return kept
}

type benchScriptArgs struct {
	VCPU                    int
	MemLimitGiB             int
	WarmupSec               int
	DurationSec             int
	ConfigPath              string
	BinaryPath              string
	Bucket                  string
	SessionID               string
	RedpandaMetricsEndpoint string
}

// renderBenchScript produces the shell script executed on the runner EC2 for
// one sweep point. The script pins Connect to the measured cores, redirects
// Connect's stdout/stderr to /tmp/bench-N.log (SSM stdout is capped at ~24KB
// so streaming Connect's ~200KB of rolling-stats lines through it loses
// samples), runs for warmup+duration seconds, then SIGTERMs cleanly so the
// benchmark processor flushes its final rolling-stats line. After Connect
// exits the log and the Prometheus snapshot are both uploaded to S3 for the
// orchestrator to fetch and parse.
func renderBenchScript(a benchScriptArgs) string {
	// Cores 0,1 reserved → measured set starts at core 2.
	cpusetHi := 1 + a.VCPU // inclusive
	totalSec := a.WarmupSec + a.DurationSec
	lines := []string{
		`set -euo pipefail`,
		fmt.Sprintf(`echo "starting bench: %d vCPU, %d GiB, warmup %ds, window %ds"`,
			a.VCPU, a.MemLimitGiB, a.WarmupSec, a.DurationSec),
		fmt.Sprintf(`LOG=/tmp/bench-%d.log`, a.VCPU),
		fmt.Sprintf(`PROM=/tmp/prom-%d.txt`, a.VCPU),
		`: > "$LOG"`,
		`: > "$PROM"`,
		fmt.Sprintf(`taskset -c 2-%d chrt --fifo 50 env GOMAXPROCS=%d GOMEMLIMIT=%dGiB REDPANDA_LICENSE_FILEPATH=/opt/bench/license.jwt %s run %s >"$LOG" 2>&1 &`,
			cpusetHi, a.VCPU, a.MemLimitGiB, a.BinaryPath, a.ConfigPath),
		`PID=$!`,
		// Heartbeat: every 60s, echo the latest rolling-stats line so the
		// operator can see throughput live. Bounded output (~17 lines per
		// sweep point) keeps SSM stdout under its content cap.
		`(
  while kill -0 "$PID" 2>/dev/null; do
    sleep 60
    LATEST="$(grep -F 'rolling stats' "$LOG" 2>/dev/null | tail -n 1 || true)"
    if [ -n "$LATEST" ]; then
      echo "[heartbeat] $LATEST"
    else
      echo "[heartbeat] connect running, no samples yet"
    fi
  done
) &`,
		`HEARTBEAT=$!`,
		// Prom scraper — every 10s while Connect is alive, append a framed
		// /metrics snapshot to /tmp/prom-N.txt. ~17min × 6 scrapes/min ≈
		// 100 frames × ~50KB ≈ 5MB per point. Uploaded post-mortem.
		`(
  while kill -0 "$PID" 2>/dev/null; do
    {
      echo "###timestamp=$(date +%s)"
      curl -s --max-time 5 http://localhost:4195/metrics || echo "###scrape_error"
    } >> "$PROM"
    sleep 10
  done
) &`,
		`PROM_SCRAPER=$!`,
	}
	if a.RedpandaMetricsEndpoint != "" {
		lines = append(lines,
			fmt.Sprintf(`RP=/tmp/redpanda-%d.txt`, a.VCPU),
			`: > "$RP"`,
			fmt.Sprintf(`(
  while kill -0 "$PID" 2>/dev/null; do
    {
      echo "###timestamp=$(date +%%s)"
      curl -s --max-time 5 http://%s/public_metrics || echo "###scrape_error"
    } >> "$RP"
    sleep 10
  done
) &`, a.RedpandaMetricsEndpoint),
			`RP_SCRAPER=$!`,
		)
	}
	lines = append(lines,
		fmt.Sprintf(`sleep %d`, totalSec),
		`kill -TERM "$PID" 2>/dev/null || true`,
		`wait "$PID" 2>/dev/null || true`,
		`kill "$HEARTBEAT" 2>/dev/null || true`,
		`kill "$PROM_SCRAPER" 2>/dev/null || true`,
	)
	if a.RedpandaMetricsEndpoint != "" {
		lines = append(lines, `kill "$RP_SCRAPER" 2>/dev/null || true`)
	}
	lines = append(lines,
		`echo "bench point complete"`,
		fmt.Sprintf(`aws s3 cp "$LOG" "s3://%s/runs/%s/sweep-%d.log" >/dev/null`,
			a.Bucket, a.SessionID, a.VCPU),
		fmt.Sprintf(`aws s3 cp "$PROM" "s3://%s/runs/%s/prom-%d.txt" >/dev/null`,
			a.Bucket, a.SessionID, a.VCPU),
	)
	if a.RedpandaMetricsEndpoint != "" {
		lines = append(lines,
			fmt.Sprintf(`aws s3 cp "$RP" "s3://%s/runs/%s/redpanda-%d.txt" >/dev/null`,
				a.Bucket, a.SessionID, a.VCPU),
		)
	}
	lines = append(lines, `echo "log uploaded"`)
	return strings.Join(lines, "\n")
}
