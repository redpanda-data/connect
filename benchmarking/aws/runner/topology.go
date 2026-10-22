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
	"io"
	"strconv"
)

// BenchNames is the source of truth for the per-session resource names a
// bench uses. The source path builds most names inline: a
// ${BENCH_SESSION_ID} topic literal in sourceTopology.Pipeline. Direction:
// sink named its table/topic/consumer-group names through this type too;
// that naming surface returns with the iceberg-sink stack PR.
type BenchNames struct {
	SessionID string
	Connector string
	// Streams is the arm's stream count. 0 and 1 both mean single-stream,
	// which is every scenario today — Validate rejects Streams > 1 for the
	// only direction that remains (source). Kept as part of the arms launch
	// mechanism (see MatrixRunner, renderPointConfigs) rather than removed
	// outright.
	Streams int
	// StreamIndex selects which stream's names to render. Only meaningful
	// when Streams > 1.
	StreamIndex int
}

func newBenchNames(sessionID, connector string) BenchNames {
	return BenchNames{SessionID: sessionID, Connector: connector}
}

// WithStreams returns a copy scoped to an arm's stream count, resetting the
// stream index to 0.
//
// Order matters: WithStreams always resets StreamIndex to 0, so
// n.WithStreams(2).WithStream(1) is correct but n.WithStream(1).WithStreams(2)
// silently loses the index — WithStreams runs second and stomps it back to 0
// with no error. Always call WithStreams before WithStream.
func (n BenchNames) WithStreams(count int) BenchNames {
	n.Streams = count
	n.StreamIndex = 0
	return n
}

// WithStream returns a copy scoped to one stream of a multi-stream arm.
//
// Call this AFTER WithStreams, not before: WithStreams resets StreamIndex to
// 0, so reversing the order (n.WithStream(1).WithStreams(2)) silently drops
// the index you just set. Every current call site uses the correct order;
// this is documented here so a future caller doesn't discover the footgun
// the hard way.
func (n BenchNames) WithStream(idx int) BenchNames {
	n.StreamIndex = idx
	return n
}

// ConnectTopic is the single topic Connect writes to in a source bench.
func (n BenchNames) ConnectTopic() string {
	return fmt.Sprintf("bench_%s_%s_connect", n.SessionID, n.Connector)
}

// MetricInputs carries everything EngineSeries needs to turn a metrics dump
// into a throughput series. Body is the raw dump for one vCPU point: the
// Redpanda /public_metrics scrape for a source bench.
type MetricInputs struct {
	Body  io.Reader
	Names BenchNames
}

// MetricSidecarArgs is the render context for MetricSidecar. Bucket/SessionID
// locate the S3 upload; Outs carries TF outputs (broker endpoints); Names
// supplies per-session resource names.
type MetricSidecarArgs struct {
	VCPU int
	// Key identifies the sweep point in artifact names: the bare vCPU count
	// for arm-less scenarios, "<vcpu>-<armID>" when arms are in play. Falls
	// back to VCPU when empty so narrow unit tests can omit it.
	Key       string
	Bucket    string
	SessionID string
	Outs      map[string]string
	Names     BenchNames
	// ScrapeIntervalSec overrides the sidecar poller's sleep interval.
	// <= 0 means 10 (the pre-soak sweep default) — see scrapeIntervalSec.
	ScrapeIntervalSec int
	// CheckpointSec, when > 0, adds a background subshell to the sidecar
	// that periodically re-uploads its scrape artifact ($RP) to its FINAL
	// S3 key (the same one Upload uses), overwriting each time, so a
	// mid-run crash doesn't lose the whole window's broker/Iceberg metrics.
	// <= 0 disables it — no subshell is rendered at all, keeping the
	// sidecar byte-identical to before this field existed.
	CheckpointSec int
}

// defaultScrapeIntervalSec is the sidecar poller's pre-soak sweep cadence:
// bounded output over a ~17min point. A soak run widens this — see
// scrapeIntervalSec.
const defaultScrapeIntervalSec = 10

// scrapeIntervalSec is the sidecar poller's sleep interval, defaulting to
// the pre-soak sweep cadence.
func (a MetricSidecarArgs) scrapeIntervalSec() int {
	if a.ScrapeIntervalSec > 0 {
		return a.ScrapeIntervalSec
	}
	return defaultScrapeIntervalSec
}

// renderSidecarCheckpoint renders the sidecar's mid-run checkpoint subshell
// (started right after RP_SCRAPER in Setup, under its own RP_CHECKPOINT
// PID) and the corresponding kill line prefixed onto Upload, or ("", "")
// when disabled. finalKey is the same S3 destination Upload already
// copies $RP to at end-of-run, so a mid-run crash still leaves the run's
// data at its final, expected location — matching the pattern
// renderBenchScript's own $LOG/$PROM checkpoint uses. `|| true` on the
// upload: a transient S3 error must not kill the run.
func renderSidecarCheckpoint(checkpointSec int, bucket, sessionID, artifact string) (setupSuffix, uploadPrefix string) {
	if checkpointSec <= 0 {
		return "", ""
	}
	finalKey := fmt.Sprintf("s3://%s/runs/%s/%s", bucket, sessionID, artifact)
	setupSuffix = fmt.Sprintf(`
(
  while kill -0 "$PID" 2>/dev/null; do
    sleep %d
    aws s3 cp "$RP" "%s" >/dev/null 2>&1 || true
  done
) &
RP_CHECKPOINT=$!`, checkpointSec, finalKey)
	uploadPrefix = "kill \"$RP_CHECKPOINT\" 2>/dev/null || true\n"
	return setupSuffix, uploadPrefix
}

// ArtifactKey is Key, or the bare vCPU count when Key was not set.
func (a MetricSidecarArgs) ArtifactKey() string {
	if a.Key != "" {
		return a.Key
	}
	return strconv.Itoa(a.VCPU)
}

// MetricSidecar is the bash a bench script splices in to sample throughput.
type MetricSidecar struct {
	Setup  string // background poller; defines $RP, ends with RP_SCRAPER=$!
	Upload string // aws s3 cp of $RP after the run
}

// Topology abstracts the direction-specific wiring of a bench. sourceTopology
// is the only implementation now that direction: sink has been cut (see
// topologyFor); callers (runBench, MatrixRunner) remain direction-blind
// against this interface so a future sink implementation slots back in
// without touching them.
type Topology interface {
	// Validate checks direction-specific scenario fields beyond the generic
	// checks in Scenario.Validate.
	Validate(s *Scenario) error
	// Pipeline returns the Connect input and output config maps.
	Pipeline(s *Scenario, n BenchNames) (input, output map[string]any, err error)
	// SeedScript renders the load-gen script that primes the system with data.
	SeedScript(s *Scenario, outs map[string]string, n BenchNames) (string, error)
	// WorkloadScript renders the sustained-load script, or "" when none.
	WorkloadScript(s *Scenario, outs map[string]string, n BenchNames) (string, error)
	// ResetScript renders the between-points reset script.
	ResetScript(s *Scenario, outs map[string]string, n BenchNames) (string, error)
	// EngineSeries turns a metrics dump into a throughput series.
	EngineSeries(in MetricInputs) ([]TopicPoint, error)
	// MetricArtifact is the per-point metrics dump basename that the bench
	// script uploads and EngineSeries later parses. key is the sweepPoint
	// key: a bare vCPU count without arms, "<vcpu>-<armID>" with.
	MetricArtifact(key string) string
	// MetricSidecar returns the bash that samples throughput during a bench
	// window. Setup launches a background poller (polling $PID every interval,
	// framing samples under "###timestamp=<unix>" into $RP) and ends by setting
	// RP_SCRAPER=$!. Upload copies $RP to S3 after the bench process exits.
	MetricSidecar(args MetricSidecarArgs) MetricSidecar
}

// topologyFor selects the implementation for a scenario's direction. An empty
// direction is treated as source (LoadScenario normalizes it, but direct
// struct construction in tests may leave it blank).
//
// direction: sink has no implementation here: the Iceberg-sink stack (sink
// topology, kafka_connect comparison, per-topic/per-stream Iceberg naming)
// was cut from this scope-reduced tree. It returns with the iceberg-sink
// stack PR, which re-adds a sinkTopology behind this same interface.
func topologyFor(d Direction) (Topology, error) {
	switch d {
	case DirectionSource, "":
		return sourceTopology{}, nil
	case DirectionSink:
		return nil, fmt.Errorf("direction %q is not supported in this build: sink scenarios return with the iceberg-sink stack PR", d)
	default:
		return nil, fmt.Errorf("unknown direction %q", d)
	}
}
