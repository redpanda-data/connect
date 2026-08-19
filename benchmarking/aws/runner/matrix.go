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
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// stdout is the package-level writer used by streaming helpers.  main.go may
// override this for tests or structured logging; os.Stdout is the default.
var stdout io.Writer = os.Stdout

// soakEmitGrace is the pause after a checkpoint interval elapses before the
// mid-run CloudWatch emit loop takes its first fetch, giving the bench
// script's own checkpoint upload (which fires on the same CheckpointSec
// cadence — see renderBenchScript) time to actually land in S3 before the
// orchestrator goes looking for it. A package var, not a const, so tests can
// zero it out instead of tolerating a real 30s wait.
var soakEmitGrace = 30 * time.Second

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
	// Topology supplies the direction-specific metric parser used by
	// fetchBrokerSeries.
	Topology Topology
	// Names is the per-session naming value passed into Topology.EngineSeries.
	Names BenchNames
	// Outs is the TF output map, passed into Topology.MetricSidecar.
	Outs map[string]string
	// HeartbeatSec overrides the bench script's per-minute heartbeat cadence.
	// 0 means 60 (the sweep default). A soak run widens this so the SSM
	// stdout content cap (~24KB) isn't exceeded over many hours — see
	// benchScriptArgs.heartbeatSec.
	HeartbeatSec int
	// PromScrapeSec overrides the bench script's /metrics scrape cadence.
	// 0 means 10 (the sweep default). A soak run widens this so the
	// Prometheus snapshot doesn't grow unboundedly over many hours — see
	// benchScriptArgs.promScrapeSec.
	PromScrapeSec int
	// CheckpointSec, when > 0, makes the bench script periodically re-upload
	// the in-progress log and Prometheus snapshot to their FINAL S3 keys
	// (overwriting each time) while the engine is still running, so a
	// mid-run crash doesn't lose the whole window's data. 0 disables this
	// (the sweep default — sweep points are short enough that only the
	// end-of-run upload matters).
	CheckpointSec int
	// ExpectedRecordsPerSec, when > 0, is the workload's target write rate.
	// Run uses it to compute a backlog series (ComputeBacklog) from each
	// point's broker-side series, tracking whether the engine is keeping
	// pace with the source over the run. 0 disables backlog computation.
	ExpectedRecordsPerSec float64
	// Emitter, when non-nil, makes Run publish per-minute CloudWatch metrics
	// for a soak point: a mid-run goroutine re-fetches the checkpointed log
	// and Prometheus/broker artifacts every CheckpointSec (see
	// startSoakEmitLoop) and, after the point completes, a final aggregate
	// emit covers whatever tail minutes the loop never got to. nil is the
	// sweep default — a short sweep point has no need for a live dashboard.
	Emitter MetricsEmitter
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
	GOMAXPROCS int
	// Streams is the number of pipelines launched for this point: 1 for a
	// single-config run, >1 for a streams-mode arm. Carried through so a
	// result JSON is re-analysable without inferring it from the arm-id
	// naming convention (e.g. "b-2pipe-gmp4").
	Streams int
	// Binary is the logical binary name (see Arm.Binary) this point
	// launched, or "" for the scenario's single default staged binary.
	// Carried through so a result JSON is re-analysable without inferring
	// which build an arm measured from its id.
	Binary       string
	Engine       string
	Samples      []Sample
	Summary      Summary
	Anomalies    []Anomaly
	Prom         []PromPoint
	BrokerSeries []TopicPoint
	// Backlog is the end-to-end backlog proxy computed from BrokerSeries
	// against MatrixRunner.ExpectedRecordsPerSec (see ComputeBacklog). Nil
	// when ExpectedRecordsPerSec is 0 (the sweep default — a sweep is
	// finding the ceiling, not tracking whether a fixed source rate is
	// being kept up with).
	Backlog []BacklogPoint
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
	// Validate up front, before any AWS spend in this sweep, that every plan
	// point has a staged config path. m.ConfigPaths and plan are always built
	// from the same list (runnerConfigPaths(sets) and buildSweepPlan(s) both
	// derive from the same scenario), so a miss here should be impossible —
	// but if it ever happened, configPathsFor's fallback to the single legacy
	// ConfigPath would launch the engine against the WRONG config on a
	// multi-stream point (Root=="" → `streams -o  <dir>`, a malformed launch
	// command), which combined with the early-abort guard would still fail
	// loud — but silently on the wrong point/config rather than obviously.
	// Failing here instead names the exact missing key immediately.
	if m.ConfigPaths != nil {
		for _, pt := range plan {
			if _, ok := m.ConfigPaths[pt.Key()]; !ok {
				return nil, fmt.Errorf("no staged config paths for sweep point %q: ConfigPaths and the sweep plan must derive from the same point list (this should be impossible — investigate before running)", pt.Key())
			}
		}
	}
	out := make([]SweepPoint, 0, len(plan))
	for _, pt := range plan {
		n := pt.VCPU
		key := pt.Key()
		if pt.ArmID == "" {
			fmt.Printf("=== sweep point: %d vCPU (warmup %s, window %s) ===\n", n, warmup, duration)
		} else {
			fmt.Printf("=== sweep point: %d vCPU, arm=%s (GOMAXPROCS %d, %d streams) (warmup %s, window %s) ===\n",
				n, pt.ArmID, pt.GOMAXPROCS, pt.Streams, warmup, duration)
		}

		if resetScript != "" {
			if err := m.SSM.Run(ctx, m.RunnerInstance, resetScript, streamingOnLine(stdout, "reset")); err != nil {
				return nil, fmt.Errorf("reset at %d vCPU: %w", n, err)
			}
		}

		// benchCtx is separate from workloadCtx: the workload goroutine
		// below cancels benchCtx (not the other way around) the moment
		// the workload script itself fails, so a crashed load generator
		// aborts the in-flight bench SSM command immediately instead of
		// running out the rest of the window measuring an idle engine.
		benchCtx, cancelBench := context.WithCancel(ctx)
		workloadCtx, cancelWorkload := context.WithCancel(ctx)
		workloadDone := make(chan error, 1)
		if workloadScript != "" {
			go func() {
				werr := m.SSM.Run(workloadCtx, m.LoadGenInstance, workloadScript, streamingOnLine(stdout, "load"))
				workloadDone <- werr
				if werr != nil && !errors.Is(werr, context.Canceled) {
					cancelBench()
				}
			}()
		} else {
			close(workloadDone)
		}

		// Topology may be nil in narrow unit tests that exercise only the
		// connect log-parsing path; those produce no scrape sidecar.
		var sidecar MetricSidecar
		if m.Topology != nil {
			sidecar = m.Topology.MetricSidecar(MetricSidecarArgs{
				VCPU:      n,
				Key:       key,
				Bucket:    m.Bucket,
				SessionID: m.SessionID,
				Outs:      m.Outs,
				Names:     m.Names.WithStreams(pt.Streams),
				// The sidecar's own scrape/checkpoint cadence reuses the SAME
				// fields the Connect-side scraper and log checkpoint already
				// use (see benchScriptArgs above) — a soak run wants both
				// pollers on the same cadence, and giving the sidecar its own
				// separate knobs would just be two places to keep in sync.
				ScrapeIntervalSec: m.PromScrapeSec,
				CheckpointSec:     m.CheckpointSec,
			})
		}

		cfg := m.configPathsFor(key)
		script := renderBenchScript(benchScriptArgs{
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
			BinaryPath:               m.binaryPathFor(pt),
			Bucket:                   m.Bucket,
			SessionID:                m.SessionID,
			RedpandaMetricsEndpoint:  m.RedpandaMetricsEndpoint,
			RedpandaMetricsEndpoints: m.RedpandaMetricsEndpoints,
			ScrapeSetup:              sidecar.Setup,
			ScrapeUpload:             sidecar.Upload,
			HeartbeatSec:             m.HeartbeatSec,
			PromScrapeSec:            m.PromScrapeSec,
			CheckpointSec:            m.CheckpointSec,
		})

		// pointStart is this point's wall-clock launch — the base every
		// CloudWatch minute bucket for this point is offset from (see
		// aggregateSoakMinutes). hwm is this point's high-water mark: the
		// last minute already emitted, shared between the mid-run loop
		// below and the final emit after the point completes so neither
		// re-emits a minute the other already sent.
		pointStart := time.Now()
		hwm := newSoakHighWater()
		stopEmit := m.startSoakEmitLoop(ctx, key, pointStart, warmup, hwm)

		// The bench script writes Connect's stdout/stderr to a log file on
		// the runner host and uploads it to S3 after termination. SSM
		// stdout only carries the script's own status echos and a
		// per-minute heartbeat (well under the ~24KB SSM content cap), so
		// streaming every line is safe. Run against benchCtx, not ctx
		// directly, so the workload goroutine above can cut this short.
		if err := m.SSM.Run(benchCtx, m.RunnerInstance, script, streamingOnLine(stdout, "bench")); err != nil {
			stopEmit()
			cancelWorkload()
			cancelBench()
			werr := <-workloadDone
			if werr != nil && !errors.Is(werr, context.Canceled) {
				// The workload error is almost always the real cause here:
				// the bench error on this path is usually nothing more
				// than the cancellation the workload goroutine itself
				// triggered via cancelBench(). Surface the workload
				// failure as the point's error, not the downstream
				// cancellation.
				return nil, fmt.Errorf("workload script failed during bench at %d vCPU: %w", n, werr)
			}
			return nil, fmt.Errorf("bench at %d vCPU: %w", n, err)
		}
		// Stop the mid-run loop now, before fetching this point's final
		// artifacts below — the loop and the final emit both read/write
		// hwm, and neither is safe to run concurrently with the other.
		stopEmit()
		cancelWorkload()
		cancelBench()
		if werr := <-workloadDone; werr != nil && !errors.Is(werr, context.Canceled) {
			// The bench window completed, but the workload that was
			// supposed to be driving it died partway through — the
			// engine spent some portion of the window measuring an idle
			// source. That's a failed point, not a footnote: return an
			// error instead of just logging it, so a load-gen crash
			// can't silently archive a near-zero result that poisons the
			// rolling soak baseline.
			return nil, fmt.Errorf("workload script failed during bench at %d vCPU: %w", n, werr)
		}

		raw, err := m.fetchLog(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("fetch log at %d vCPU: %w", n, err)
		}
		rawLog := raw
		samples := parseAndTrim(raw, warmup)
		promPts := m.fetchProm(ctx, key)

		// Broker-side: the bench script scrapes /public_metrics during its
		// window and uploads it to this point's artifact.
		brokerSeries := m.fetchBrokerSeries(ctx, key)

		// Summary is derived from the broker-side series — the canonical
		// fairness instrument — not Connect's own rolling-stats log.
		// Connect's log reports uncompressed logical bytes; the broker
		// reports what actually arrived. Records/sec (see brokermetrics.go)
		// is immune to compression effects.
		//
		// Connect's log is NOT discarded: Samples below still carries every
		// rolling-stats line, so the log-derived view can be recomputed
		// from any result file.
		summary := SummariseTopicPoints(brokerSeries)

		// Anomaly detection deliberately keeps using the LOG-derived median.
		// It judges the Connect log's internal consistency — how far
		// individual log samples stray from their own centre — so handing it
		// a broker-derived median would compare two instruments and flag
		// nearly every sample.
		logMedian := Summarise(samples).MedianMBPerSec
		anomalies := DetectAnomaliesWithProm(samples, logMedian, promPts)
		backlog := ComputeBacklog(brokerSeries, m.ExpectedRecordsPerSec)

		// Final emit: whatever tail minutes the mid-run loop above never
		// got to (it may never have ticked at all, e.g. a point shorter
		// than CheckpointSec+soakEmitGrace) land here, from the same
		// hwm — so a point never emits a minute twice regardless of how
		// many mid-run cycles ran before this.
		if m.Emitter != nil {
			m.emitAggregated(ctx, samples, promPts, brokerSeries, backlog, pointStart, warmup, hwm)
		}
		out = append(out, SweepPoint{
			VCPU:         n,
			ArmID:        pt.ArmID,
			GOMAXPROCS:   pt.GOMAXPROCS,
			Streams:      pt.Streams,
			Binary:       pt.Binary,
			Engine:       "connect",
			Samples:      samples,
			Summary:      summary,
			Anomalies:    anomalies,
			Prom:         promPts,
			BrokerSeries: brokerSeries,
			Backlog:      backlog,
		})
		fmt.Printf("  -> %d samples; median %.2f MB/s (p5 %.2f, p95 %.2f, peak %.2f), %d anomalies\n",
			len(samples), summary.MedianMBPerSec, summary.P5MBPerSec, summary.P95MBPerSec, summary.PeakMBPerSec, len(anomalies))

		// Early-abort if this point produced no throughput data. For the
		// very first plan point, "later points would fail the same way"
		// holds because every arm-less point shares the same launch
		// mechanism (run mode), so checking plan[0] once is sufficient.
		// That reasoning does NOT extend to arms: a0/a1 launch via
		// `run cfg.yaml` while b launches via `streams -o root.yaml
		// streams/` — a different launch mechanism entirely — so one
		// arm succeeding predicts nothing about another arm's launch.
		// Connect runs backgrounded (`… &`) in the bench script, so
		// `set -e` cannot see it die; a launch failure would otherwise
		// silently produce `median 0.00 MB/s` with no error. So: check
		// the first point unconditionally (pre-arms behaviour), AND
		// check every point that carries an arm, regardless of position
		// in the plan.
		if pt.Key() == plan[0].Key() || pt.ArmID != "" {
			if len(samples) == 0 {
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

// configPathsFor returns the launch config paths for a point key, falling back
// to the single staged ConfigPath when the scenario declares no arms.
func (m *MatrixRunner) configPathsFor(key string) pointConfigPaths {
	if cfg, ok := m.ConfigPaths[key]; ok {
		return cfg
	}
	return pointConfigPaths{Single: m.ConfigPath}
}

// binaryPathFor resolves the launch binary for one sweep point: an arm that
// set Arm.Binary launches the correspondingly-named staged binary
// (runnerBinaryPath — see main.go's stageArtefacts, which stages exactly
// this path for every --binary mapping); every other point (arm-less, or an
// arm that left Binary empty) launches the scenario's single default staged
// binary, m.BinaryPath — byte-identical to every pre-binary-arm scenario.
func (m *MatrixRunner) binaryPathFor(pt sweepPoint) string {
	if pt.Binary == "" {
		return m.BinaryPath
	}
	return runnerBinaryPath(pt.Binary)
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

// fetchBrokerSeries downloads this point's broker metrics dump and returns
// Connect's attributed topic series. Non-fatal: a missing or unparseable
// file logs and returns nil.
func (m *MatrixRunner) fetchBrokerSeries(ctx context.Context, key string) []TopicPoint {
	if m.LogFetcher == nil {
		return nil
	}
	if m.Topology == nil {
		fmt.Fprintf(stdout, "[bench] no Topology configured; metric fetch skipped\n")
		return nil
	}
	s3Key := fmt.Sprintf("runs/%s/%s", m.SessionID, m.Topology.MetricArtifact(key))
	body, err := m.LogFetcher.Fetch(ctx, m.Bucket, s3Key)
	if err != nil {
		fmt.Fprintf(stdout, "[bench] fetch broker metrics (non-fatal): %v\n", err)
		return nil
	}
	defer body.Close()
	pts, err := m.Topology.EngineSeries(MetricInputs{Body: body, Names: m.Names})
	if err != nil {
		fmt.Fprintf(stdout, "[bench] EngineSeries failed: %v\n", err)
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

// offsetSampleT returns a copy of samples with T shifted by offsetSec. It
// exists to undo parseAndTrim's own reindexing: a Sample's T=0 there means
// "end of warmup", i.e. wall-clock base+warmup, while every other soak
// series (PromPoint, TopicPoint, BacklogPoint) has T=0 mean wall-clock base
// (the point's own launch). aggregateSoakMinutes assumes all four series
// share one base, so this must run before samples are handed to it —
// skipping it would skew every rate/gauge pairing by exactly the warmup
// duration.
func offsetSampleT(samples []Sample, offsetSec int) []Sample {
	if offsetSec == 0 || len(samples) == 0 {
		return samples
	}
	out := make([]Sample, len(samples))
	for i, s := range samples {
		s.T += offsetSec
		out[i] = s
	}
	return out
}

// soakHighWater tracks the last CloudWatch minute already emitted for one
// sweep point, per artifact family (see soakHighWaterMarks). It is shared
// between the mid-run emit loop (runSoakEmitLoop) and the point's own final
// emit (see Run) so neither re-emits a minute the other already sent — the
// two never run concurrently by construction (startSoakEmitLoop's stop
// func blocks until the loop has exited before Run touches hwm again), but
// the type still encapsulates its own mutex rather than relying on that
// invariant holding forever. get/set always copy the whole marks struct so
// the four families stay consistent with each other across concurrent
// access, even though only aggregateSoakMinutes ever needs more than one
// at a time.
type soakHighWater struct {
	mu sync.Mutex
	v  soakHighWaterMarks
}

// newSoakHighWater starts every family at -1: "nothing emitted yet", so
// minute 0 becomes eligible for each family independently as soon as that
// family's own data shows it complete.
func newSoakHighWater() *soakHighWater {
	return &soakHighWater{v: soakHighWaterMarks{Samples: -1, Prom: -1, Broker: -1, Backlog: -1}}
}

func (h *soakHighWater) get() soakHighWaterMarks {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.v
}

func (h *soakHighWater) set(v soakHighWaterMarks) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.v = v
}

// startSoakEmitLoop launches the mid-run CloudWatch emit goroutine when the
// runner has both an Emitter and a checkpoint cadence configured (the soak
// profile — see main.go's runBench) and returns a stop func that cancels it
// and blocks until it has exited. Callers must call stop exactly once, on
// every exit path, before touching hw again.
//
// Disabled (no Emitter, or CheckpointSec <= 0) returns a no-op stop func so
// callers never need to branch on whether the loop is actually running.
func (m *MatrixRunner) startSoakEmitLoop(ctx context.Context, key string, pointStart time.Time, warmup time.Duration, hw *soakHighWater) (stop func()) {
	if m.Emitter == nil || m.CheckpointSec <= 0 {
		return func() {}
	}
	loopCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.runSoakEmitLoop(loopCtx, key, pointStart, warmup, hw)
	}()
	return func() {
		cancel()
		wg.Wait()
	}
}

// runSoakEmitLoop re-fetches the point's checkpointed artifacts every
// CheckpointSec (plus soakEmitGrace, so the bench script's own checkpoint
// upload has time to land first) and pushes newly-complete minutes to
// CloudWatch, so a dashboard watching a multi-hour run doesn't have to wait
// for the point to finish. Returns as soon as ctx is cancelled — see
// startSoakEmitLoop.
func (m *MatrixRunner) runSoakEmitLoop(ctx context.Context, key string, pointStart time.Time, warmup time.Duration, hw *soakHighWater) {
	interval := time.Duration(m.CheckpointSec) * time.Second
	timer := time.NewTimer(interval + soakEmitGrace)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			m.emitSoakCycle(ctx, key, pointStart, warmup, hw)
			timer.Reset(interval)
		}
	}
}

// emitSoakCycle fetches one checkpoint's worth of log/prom/broker data and
// hands it to emitAggregated. Fetch or parse failure is logged and
// swallowed, never fatal — a checkpoint that hasn't landed yet (or a
// transient S3 hiccup) must not fail a run that may have hours left to run;
// the next cycle simply tries again against the checkpoint's ever-growing
// content.
func (m *MatrixRunner) emitSoakCycle(ctx context.Context, key string, pointStart time.Time, warmup time.Duration, hw *soakHighWater) {
	raw, err := m.fetchLog(ctx, key)
	var samples []Sample
	if err != nil {
		fmt.Fprintf(stdout, "[soak] fetch log checkpoint (non-fatal): %v\n", err)
	} else {
		samples = parseAndTrim(raw, warmup)
	}
	promPts := m.fetchProm(ctx, key)
	brokerSeries := m.fetchBrokerSeries(ctx, key)
	backlog := ComputeBacklog(brokerSeries, m.ExpectedRecordsPerSec)
	m.emitAggregated(ctx, samples, promPts, brokerSeries, backlog, pointStart, warmup, hw)
}

// emitAggregated normalizes samples' warmup offset (see offsetSampleT),
// aggregates every newly-complete minute since hw's four per-family marks
// (see aggregateSoakMinutes and soakHighWaterMarks), appends a RunActive
// heartbeat datum stamped at call time, and emits the batch. Emit failure
// is logged and swallowed — never fatal, and ALL FOUR marks in hw are left
// unmoved together (never partially advanced) so the same range is retried
// in full on the next cycle instead of any of it being silently dropped.
func (m *MatrixRunner) emitAggregated(ctx context.Context, samples []Sample, prom []PromPoint, broker []TopicPoint, backlog []BacklogPoint, pointStart time.Time, warmup time.Duration, hw *soakHighWater) {
	normalized := offsetSampleT(samples, int(warmup.Seconds()))
	data, newMarks := aggregateSoakMinutes(normalized, prom, broker, backlog, pointStart, hw.get())
	data = append(data, MetricDatum{Name: metricRunActive, Value: 1, Unit: unitCount, At: time.Now()})
	// Per-cycle gauge, stamped "now" like RunActive rather than backfilled to
	// a past minute — see metricRSSSlopeBytesPerMin's doc comment. Skipped
	// (not appended at all) until there is enough history to fit a
	// meaningful trend line.
	if slope, ok := rssSlopeBytesPerMin(prom); ok {
		data = append(data, MetricDatum{Name: metricRSSSlopeBytesPerMin, Value: slope, Unit: unitNone, At: time.Now()})
	}
	if err := m.Emitter.Emit(ctx, data); err != nil {
		fmt.Fprintf(stdout, "[soak] emit metrics (non-fatal): %v\n", err)
		return
	}
	hw.set(newMarks)
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
	// HeartbeatSec overrides the heartbeat loop's sleep interval. <= 0 means
	// 60 (the pre-soak default) — see heartbeatSec.
	HeartbeatSec int
	// PromScrapeSec overrides the Prometheus scraper loop's sleep interval.
	// <= 0 means 10 (the pre-soak default) — see promScrapeSec.
	PromScrapeSec int
	// CheckpointSec, when > 0, adds a background subshell that periodically
	// re-uploads $LOG and $PROM to their final S3 keys (overwriting each
	// time) while the engine is still running, so a mid-run crash doesn't
	// lose the whole window's data. <= 0 disables it — no subshell is
	// rendered at all, keeping the script byte-identical to before this
	// field existed.
	CheckpointSec int
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

// defaultHeartbeatSec and defaultPromScrapeSec are the pre-soak sweep
// cadences: bounded output over a ~17min point (SSM's ~24KB stdout cap;
// ~5MB of Prometheus snapshots). A soak run overrides both — see
// heartbeatSec/promScrapeSec.
const (
	defaultHeartbeatSec  = 60
	defaultPromScrapeSec = 10
)

// heartbeatSec is the heartbeat loop's sleep interval, defaulting to the
// pre-soak sweep cadence.
func (a benchScriptArgs) heartbeatSec() int {
	if a.HeartbeatSec > 0 {
		return a.HeartbeatSec
	}
	return defaultHeartbeatSec
}

// promScrapeSec is the Prometheus scraper loop's sleep interval, defaulting
// to the pre-soak sweep cadence.
func (a benchScriptArgs) promScrapeSec() int {
	if a.PromScrapeSec > 0 {
		return a.PromScrapeSec
	}
	return defaultPromScrapeSec
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
	logKey := fmt.Sprintf("s3://%s/runs/%s/sweep-%s.log", a.Bucket, a.SessionID, key)
	promKey := fmt.Sprintf("s3://%s/runs/%s/prom-%s.txt", a.Bucket, a.SessionID, key)
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
		// Heartbeat: every heartbeatSec(), echo the latest rolling-stats line
		// so the operator can see throughput live. At the sweep default
		// (60s) this bounds output to ~17 lines per point, well under SSM's
		// stdout content cap; a soak run widens the interval (see
		// MatrixRunner.HeartbeatSec) so the same cap holds over many hours.
		fmt.Sprintf(`(
  while kill -0 "$PID" 2>/dev/null; do
    sleep %d
    LATEST="$(grep -F 'rolling stats' "$LOG" 2>/dev/null | tail -n 1 || true)"
    if [ -n "$LATEST" ]; then
      echo "[heartbeat] $LATEST"
    else
      echo "[heartbeat] connect running, no samples yet"
    fi
  done
) &`, a.heartbeatSec()),
		`HEARTBEAT=$!`,
		// Prom scraper — every promScrapeSec() while Connect is alive, append
		// a framed /metrics snapshot to /tmp/prom-N.txt. At the sweep default
		// (10s) a ~17min point accumulates ~5MB; a soak run widens the
		// interval (see MatrixRunner.PromScrapeSec) so a 24h run doesn't
		// accumulate gigabytes. Uploaded post-mortem (and, when CheckpointSec
		// is set, periodically mid-run too).
		fmt.Sprintf(`(
  while kill -0 "$PID" 2>/dev/null; do
    {
      echo "###timestamp=$(date +%%s)"
      curl -s --max-time 5 http://localhost:4195/metrics || echo "###scrape_error"
    } >> "$PROM"
    sleep %d
  done
) &`, a.promScrapeSec()),
		`PROM_SCRAPER=$!`,
	}
	// Checkpoint: for a long soak run, the end-of-script upload alone means a
	// crash anywhere before the final `aws s3 cp` loses the ENTIRE window's
	// data. When CheckpointSec > 0, periodically re-upload $LOG and $PROM to
	// their FINAL keys (the same ones the end-of-script upload uses),
	// overwriting each time, so the run's data survives a mid-run crash.
	// `|| true` on the uploads: a transient S3 error must not kill the run.
	// Disabled (CheckpointSec <= 0) renders no subshell at all, keeping the
	// script byte-identical to before this field existed.
	if a.CheckpointSec > 0 {
		lines = append(lines, fmt.Sprintf(`(
  while kill -0 "$PID" 2>/dev/null; do
    sleep %d
    aws s3 cp "$LOG" "%s" >/dev/null 2>&1 || true
    aws s3 cp "$PROM" "%s" >/dev/null 2>&1 || true
  done
) &`, a.CheckpointSec, logKey, promKey))
		lines = append(lines, `CHECKPOINT=$!`)
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
	if a.CheckpointSec > 0 {
		lines = append(lines, `kill "$CHECKPOINT" 2>/dev/null || true`)
	}
	if a.ScrapeSetup != "" {
		lines = append(lines, `kill "$RP_SCRAPER" 2>/dev/null || true`)
	}
	lines = append(lines,
		`echo "bench point complete"`,
		fmt.Sprintf(`aws s3 cp "$LOG" "%s" >/dev/null`, logKey),
		fmt.Sprintf(`aws s3 cp "$PROM" "%s" >/dev/null`, promKey),
	)
	if a.ScrapeUpload != "" {
		lines = append(lines, a.ScrapeUpload)
	}
	lines = append(lines, `echo "log uploaded"`)
	return strings.Join(lines, "\n")
}
