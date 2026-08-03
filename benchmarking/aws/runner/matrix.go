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
	"strconv"
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
	// ConfigPaths maps sweepPoint.Key() to that point's launch config paths on
	// the runner host. Nil (the arm-less case) means every point launches
	// ConfigPath, which keeps existing scenarios on the historical
	// /opt/bench/config.yaml staging path.
	ConfigPaths map[string]pointConfigPaths
	BinaryPath  string // path on the runner host to redpanda-connect
	Bucket      string // S3 bucket where per-point Connect logs are uploaded
	SessionID   string // run-scoped key prefix: runs/<SessionID>/sweep-<vcpu>.log
	// RedpandaMetricsEndpoint is the host:port pair (e.g. "10.42.10.10:9644") the
	// per-point scraper curls every 10s. Empty disables the scraper, so callers
	// without a Redpanda cluster (e.g. an early-stack bring-up) won't fail.
	//
	// Deprecated: prefer RedpandaMetricsEndpoints. Redpanda emits per-topic byte
	// counters only on the broker that leads a partition — scraping a single
	// broker misses topics whose leader is elsewhere. Kept here for back-compat
	// with callers that haven't migrated yet.
	RedpandaMetricsEndpoint string
	// RedpandaMetricsEndpoints is a comma-separated list of all broker
	// host:9644 endpoints. Each bench script scrapes ALL brokers per
	// interval because Redpanda emits per-topic byte counters only on
	// the broker leading the partition.
	RedpandaMetricsEndpoints string
	// Engines lists the engines to sweep at each vCPU point, in order.
	// Default ["connect"] preserves the pre-Plan-2 behavior.
	Engines []string
	// KCConnectorName is the name to submit the KC connector under.
	// Empty when Engines does not include "kafka_connect".
	KCConnectorName string
	// KCConnectorConfigJSON is the rendered JSON config posted to KC's REST API.
	KCConnectorConfigJSON string
	// Topology supplies the direction-specific metric parser used by
	// fetchBrokerSeriesForEngine.
	Topology Topology
	// Names is the per-session naming value passed into Topology.EngineSeries.
	Names BenchNames
	// Outs is the TF output map, passed into Topology.MetricSidecar.
	Outs map[string]string
	// Direction selects how throughput is measured: source benches parse
	// Connect's rolling-stats log (samples); sink benches use the metric
	// series in brokerSeries (e.g. Iceberg committed bytes).
	Direction Direction
}

// pointConfigPaths locates one sweep point's launch config(s) on the runner
// host. Single is set for single-pipeline points; Root and Dir are set for
// streams-mode points.
type pointConfigPaths struct {
	Single string
	Root   string
	Dir    string
}

// SweepPoint is the per-point measurement.
type SweepPoint struct {
	VCPU int
	// ArmID is "" for arm-less sweeps; the matrix.arms id otherwise.
	ArmID string
	// GOMAXPROCS is the runtime P count measured at this point. Equal to VCPU
	// unless an arm oversubscribed it.
	GOMAXPROCS   int
	Engine       string
	Samples      []Sample
	Summary      Summary
	Anomalies    []Anomaly
	Prom         []PromPoint
	BrokerSeries []TopicPoint
}

// Run executes the full sweep. resetScript runs on the runner host between
// points (e.g. drop the CDC replication slot). workloadScript, if non-empty,
// runs on the load-gen host concurrently with the bench step.
func (m *MatrixRunner) Run(
	ctx context.Context,
	plan []sweepPoint,
	memLimitPerVCPU int,
	warmup, duration time.Duration,
	resetScript string,
	workloadScript string,
) ([]SweepPoint, error) {
	engines := m.Engines
	if len(engines) == 0 {
		engines = []string{"connect"}
	}
	out := make([]SweepPoint, 0, len(plan)*len(engines))
	for _, pt := range plan {
		n := pt.VCPU
		key := pt.Key()
		for _, engine := range engines {
			if pt.ArmID == "" {
				fmt.Printf("=== sweep point: %d vCPU, engine=%s (warmup %s, window %s) ===\n", n, engine, warmup, duration)
			} else {
				fmt.Printf("=== sweep point: %d vCPU, arm=%s (GOMAXPROCS %d, %d streams), engine=%s (warmup %s, window %s) ===\n",
					n, pt.ArmID, pt.GOMAXPROCS, pt.Streams, engine, warmup, duration)
			}

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

			// Topology may be nil in narrow unit tests that exercise only the
			// connect log-parsing path; those produce no scrape sidecar.
			var sidecar MetricSidecar
			if m.Topology != nil {
				sidecar = m.Topology.MetricSidecar(MetricSidecarArgs{
					Engine:    engine,
					VCPU:      n,
					Key:       key,
					Bucket:    m.Bucket,
					SessionID: m.SessionID,
					Outs:      m.Outs,
					// WithStreams scopes Names to this point's stream count so
					// sinkTopology.MetricSidecar's IcebergTables(engine) polls
					// every per-stream table (..._s0, ..._s1, ...) a multi-
					// stream arm actually writes to, not just the unsuffixed
					// base table (which the reset union creates but nothing
					// writes when Streams > 1). Without this, a 2-stream arm's
					// sidecar polls a table that never grows and the arm
					// reports ~0 MB/s with no error anywhere.
					Names: m.Names.WithStreams(pt.Streams),
				})
			}

			var script string
			switch engine {
			case "connect":
				cfg := m.configPathsFor(key)
				script = renderBenchScript(benchScriptArgs{
					VCPU:                     n,
					GOMAXPROCS:               pt.GOMAXPROCS,
					Streams:                  pt.Streams,
					Key:                      key,
					MemLimitGiB:              memLimitPerVCPU * n,
					WarmupSec:                int(warmup.Seconds()),
					DurationSec:              int(duration.Seconds()),
					ConfigPath:               cfg.Single,
					RootConfigPath:           cfg.Root,
					StreamsDir:               cfg.Dir,
					BinaryPath:               m.BinaryPath,
					Bucket:                   m.Bucket,
					SessionID:                m.SessionID,
					RedpandaMetricsEndpoint:  m.RedpandaMetricsEndpoint,
					RedpandaMetricsEndpoints: m.RedpandaMetricsEndpoints,
					ScrapeSetup:              sidecar.Setup,
					ScrapeUpload:             sidecar.Upload,
				})
			case "kafka_connect":
				// Per-vCPU connector name. KC stores Debezium offsets in
				// the _kc_offsets topic keyed by connector name; if every
				// sweep point reuses the same name, the connector at
				// vCPU=N+1 wakes up trying to resume from vCPU=N's LSN —
				// which Postgres has aged out of WAL between sweep points.
				// Verified live in the 2026-05-29 postgres real bench:
				// every KC point past the first produced 0 MB/s with
				// "redo log is no longer available" warnings.
				vcpuConnectorName := fmt.Sprintf("%s_v%d", m.KCConnectorName, n)
				script = renderKCBenchScript(kcBenchScriptArgs{
					VCPU:                     n,
					MemLimitGiB:              memLimitPerVCPU * n,
					WarmupSec:                int(warmup.Seconds()),
					DurationSec:              int(duration.Seconds()),
					ConnectorName:            vcpuConnectorName,
					ConnectorConfigJSON:      m.KCConnectorConfigJSON,
					Bucket:                   m.Bucket,
					SessionID:                m.SessionID,
					RedpandaMetricsEndpoint:  m.RedpandaMetricsEndpoint,
					RedpandaMetricsEndpoints: m.RedpandaMetricsEndpoints,
					ScrapeSetup:              sidecar.Setup,
					ScrapeUpload:             sidecar.Upload,
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
				raw, err := m.fetchLog(ctx, key)
				if err != nil {
					return nil, fmt.Errorf("fetch log at %d vCPU (%s): %w", n, engine, err)
				}
				rawLog = raw
				samples = parseAndTrim(raw, warmup)
			}
			promPts := m.fetchProm(ctx, key)

			// Broker-side: each engine scrapes /public_metrics during its
			// own window and uploads to a per-engine filename, so we fetch
			// only the matching engine's file here.
			brokerSeries := m.fetchBrokerSeriesForEngine(ctx, engine, key)

			var summary Summary
			if engine == "kafka_connect" || m.Direction == DirectionSink {
				// No rolling-stats log to parse (KC has none; a sink's
				// Connect pipeline has no benchmark processor). Derive the
				// Summary from the metric series attributed to this engine.
				summary = SummariseTopicPoints(brokerSeries)
			} else {
				summary = Summarise(samples)
			}
			anomalies := DetectAnomaliesWithProm(samples, summary.MedianMBPerSec, promPts)
			out = append(out, SweepPoint{
				VCPU:         n,
				ArmID:        pt.ArmID,
				GOMAXPROCS:   pt.GOMAXPROCS,
				Engine:       engine,
				Samples:      samples,
				Summary:      summary,
				Anomalies:    anomalies,
				Prom:         promPts,
				BrokerSeries: brokerSeries,
			})
			fmt.Printf("  -> %d samples; median %.2f MB/s (p5 %.2f, p95 %.2f, peak %.2f), %d anomalies\n",
				len(samples), summary.MedianMBPerSec, summary.P5MBPerSec, summary.P95MBPerSec, summary.PeakMBPerSec, len(anomalies))

			// Early-abort on the first sweep point if it produced no
			// throughput data — later points would fail the same way. The
			// signal differs by direction: source-Connect uses rolling-stats
			// log samples; a sink (no such log) uses its metric series.
			if pt.Key() == plan[0].Key() {
				var empty bool
				var what string
				switch {
				case m.Direction == DirectionSink:
					empty, what = len(brokerSeries) == 0, "metric samples"
				case engine == "connect":
					empty, what = len(samples) == 0, "samples"
				}
				if empty {
					const tailMax = 4 * 1024
					tail := rawLog
					if len(rawLog) > tailMax {
						tail = rawLog[len(rawLog)-tailMax:]
					}
					fmt.Fprintf(stdout, "[bench] connect log tail (last %d bytes):\n%s\n", len(tail), tail)
					return out, fmt.Errorf("first sweep point at %d vCPU captured 0 %s — see log tail above", n, what)
				}
			}
		}
	}
	return out, nil
}

// configPathsFor returns the launch config paths for a point key, falling back
// to the single staged ConfigPath when the scenario declares no arms.
func (m *MatrixRunner) configPathsFor(key string) pointConfigPaths {
	if cfg, ok := m.ConfigPaths[key]; ok {
		return cfg
	}
	return pointConfigPaths{Single: m.ConfigPath}
}

// fetchLog downloads the per-point Connect log uploaded by the bench script.
func (m *MatrixRunner) fetchLog(ctx context.Context, key string) ([]byte, error) {
	if m.LogFetcher == nil {
		return nil, fmt.Errorf("LogFetcher not configured")
	}
	s3Key := fmt.Sprintf("runs/%s/sweep-%s.log", m.SessionID, key)
	body, err := m.LogFetcher.Fetch(ctx, m.Bucket, s3Key)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	return io.ReadAll(body)
}

// fetchProm downloads the per-point Prometheus dump uploaded by the bench
// script. Failure is non-fatal — the sweep point is still useful without
// goroutine/heap context.
func (m *MatrixRunner) fetchProm(ctx context.Context, key string) []PromPoint {
	if m.LogFetcher == nil {
		return nil
	}
	s3Key := fmt.Sprintf("runs/%s/prom-%s.txt", m.SessionID, key)
	body, err := m.LogFetcher.Fetch(ctx, m.Bucket, s3Key)
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

// fetchBrokerSeriesForEngine downloads the per-engine, per-point broker
// metrics dump and returns the topic series attributed to that engine.
// Non-fatal: a missing or unparseable file logs and returns nil.
//
// Each engine writes its own scrape file (Connect → redpanda-N-connect.txt,
// KC → redpanda-N-kc.txt) covering only that engine's bench window, so
// we don't need to merge across engines — the file FOR an engine already
// contains only that engine's bytes (the other engine wasn't running).
func (m *MatrixRunner) fetchBrokerSeriesForEngine(ctx context.Context, engine, key string) []TopicPoint {
	if m.LogFetcher == nil {
		return nil
	}
	if m.Topology == nil {
		fmt.Fprintf(stdout, "[bench] no Topology configured; metric fetch skipped\n")
		return nil
	}
	s3Key := fmt.Sprintf("runs/%s/%s", m.SessionID, m.Topology.MetricArtifact(engine, key))
	body, err := m.LogFetcher.Fetch(ctx, m.Bucket, s3Key)
	if err != nil {
		fmt.Fprintf(stdout, "[bench] fetch broker metrics %s (non-fatal): %v\n", engine, err)
		return nil
	}
	defer body.Close()
	pts, err := m.Topology.EngineSeries(MetricInputs{Body: body, Names: m.Names}, engine)
	if err != nil {
		fmt.Fprintf(stdout, "[bench] EngineSeries(%s) failed: %v\n", engine, err)
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
	VCPU        int
	MemLimitGiB int
	WarmupSec   int
	DurationSec int
	ConfigPath  string
	BinaryPath  string
	Bucket      string
	SessionID   string
	// RedpandaMetricsEndpoint is the legacy single-broker scrape target.
	// Deprecated: prefer RedpandaMetricsEndpoints. Kept for back-compat with
	// callers that haven't migrated to the multi-broker output yet.
	RedpandaMetricsEndpoint string
	// RedpandaMetricsEndpoints is a comma-separated list of host:9644
	// endpoints, one per broker. The scraper iterates over all of them
	// each interval because Redpanda emits per-topic byte counters only
	// on the broker leading the partition. If both fields are set,
	// Endpoints wins.
	RedpandaMetricsEndpoints string
	// ScrapeSetup launches the metric poller; ScrapeUpload copies the artifact
	// to S3. Both come from Topology.MetricSidecar.
	ScrapeSetup  string
	ScrapeUpload string
	// GOMAXPROCS is the Go runtime's P count. 0 means "same as VCPU", which is
	// the pre-arms default. An arm may deliberately set it above VCPU: the
	// taskset core pin below always follows VCPU, so oversubscribing only
	// changes how many goroutines the runtime will schedule onto those cores.
	GOMAXPROCS int
	// Streams > 1 launches `redpanda-connect streams -o <RootConfigPath>
	// <StreamsDir>` instead of `run <ConfigPath>`.
	Streams        int
	RootConfigPath string
	StreamsDir     string
	// Key names this point's artifacts. Empty means the bare vCPU count.
	Key string
}

// artifactKey names this point's log and metric files: the bare vCPU count for
// arm-less scenarios (unchanged from before matrix.arms), "<vcpu>-<armID>" with
// arms.
func (a benchScriptArgs) artifactKey() string {
	if a.Key != "" {
		return a.Key
	}
	return strconv.Itoa(a.VCPU)
}

// gomaxprocs is the runtime P count for this point, defaulting to the pinned
// core count.
func (a benchScriptArgs) gomaxprocs() int {
	if a.GOMAXPROCS > 0 {
		return a.GOMAXPROCS
	}
	return a.VCPU
}

// launchCmd is the engine invocation: streams mode when the point runs more
// than one pipeline in the process, single-config run mode otherwise.
func (a benchScriptArgs) launchCmd() string {
	if a.Streams > 1 {
		return fmt.Sprintf("%s streams -o %s %s", a.BinaryPath, a.RootConfigPath, a.StreamsDir)
	}
	return fmt.Sprintf("%s run %s", a.BinaryPath, a.ConfigPath)
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
	key := a.artifactKey()
	totalSec := a.WarmupSec + a.DurationSec
	lines := []string{
		`set -euo pipefail`,
		fmt.Sprintf(`echo "starting bench: %d vCPU, GOMAXPROCS %d, %d streams, %d GiB, warmup %ds, window %ds"`,
			a.VCPU, a.gomaxprocs(), max(a.Streams, 1), a.MemLimitGiB, a.WarmupSec, a.DurationSec),
		fmt.Sprintf(`LOG=/tmp/bench-%s.log`, key),
		fmt.Sprintf(`PROM=/tmp/prom-%s.txt`, key),
		`: > "$LOG"`,
		`: > "$PROM"`,
		// chrt removed for scheduler parity with KC (it deadlocked the JVM
		// under single-core taskset; see traps reference in the
		// bench-framework Claude skill). taskset alone gives us CPU
		// isolation; SCHED_OTHER is what KC uses.
		//
		// The core pin follows VCPU while GOMAXPROCS is independent: an arm can
		// oversubscribe the runtime on a fixed core allocation. GOMEMLIMIT is
		// vCPU-derived by the caller, so it is constant across an A/B's arms.
		fmt.Sprintf(`taskset -c 2-%d env GOMAXPROCS=%d GOMEMLIMIT=%dGiB REDPANDA_LICENSE_FILEPATH=/opt/bench/license.jwt %s >"$LOG" 2>&1 &`,
			cpusetHi, a.gomaxprocs(), a.MemLimitGiB, a.launchCmd()),
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
	// The broker-scrape sidecar is computed by Topology.MetricSidecar and
	// passed in via ScrapeSetup. It defines $RP and ends with RP_SCRAPER=$!,
	// so it must be appended after $PID is live and before the bench process
	// is waited on. Empty when the topology has no scrape (or no endpoints).
	if a.ScrapeSetup != "" {
		lines = append(lines, a.ScrapeSetup)
	}
	lines = append(lines,
		fmt.Sprintf(`sleep %d`, totalSec),
		`kill -TERM "$PID" 2>/dev/null || true`,
		`wait "$PID" 2>/dev/null || true`,
		`kill "$HEARTBEAT" 2>/dev/null || true`,
		`kill "$PROM_SCRAPER" 2>/dev/null || true`,
	)
	if a.ScrapeSetup != "" {
		lines = append(lines, `kill "$RP_SCRAPER" 2>/dev/null || true`)
	}
	lines = append(lines,
		`echo "bench point complete"`,
		fmt.Sprintf(`aws s3 cp "$LOG" "s3://%s/runs/%s/sweep-%s.log" >/dev/null`,
			a.Bucket, a.SessionID, key),
		fmt.Sprintf(`aws s3 cp "$PROM" "s3://%s/runs/%s/prom-%s.txt" >/dev/null`,
			a.Bucket, a.SessionID, key),
	)
	if a.ScrapeUpload != "" {
		lines = append(lines, a.ScrapeUpload)
	}
	lines = append(lines, `echo "log uploaded"`)
	return strings.Join(lines, "\n")
}
