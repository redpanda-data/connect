// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"io"
	"strings"
)

// BenchNames is the source of truth for the per-session, per-engine resource
// names a bench uses. The sink path (sinkTopology) routes its table, topic, and
// consumer-group names through these helpers. The source path still builds names
// inline: a ${BENCH_SESSION_ID} topic literal in sourceTopology.Pipeline, plus
// buildKCRenderInputs and AttributeByEngine for Kafka Connect.
type BenchNames struct {
	SessionID string
	Connector string
	// Streams is the arm's stream count. 0 and 1 both mean single-stream, in
	// which case Iceberg table names are unsuffixed exactly as they were
	// before matrix.arms existed. > 1 suffixes each stream's table with
	// _s<StreamIndex> so concurrent streams commit to independent tables.
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
func (n BenchNames) WithStreams(count int) BenchNames {
	n.Streams = count
	n.StreamIndex = 0
	return n
}

// WithStream returns a copy scoped to one stream of a multi-stream arm.
func (n BenchNames) WithStream(idx int) BenchNames {
	n.StreamIndex = idx
	return n
}

// ConnectTopic is the single topic Connect writes to in a source bench.
func (n BenchNames) ConnectTopic() string {
	return fmt.Sprintf("bench_%s_%s_connect", n.SessionID, n.Connector)
}

// KCTopicPrefix is the Debezium topic.prefix for a source bench; KC emits
// <prefix>.<schema>.<table> topics under it.
func (n BenchNames) KCTopicPrefix() string {
	return fmt.Sprintf("bench_%s_%s_kc", n.SessionID, n.Connector)
}

// SourceTopic is the pre-seeded Redpanda topic a sink bench consumes.
func (n BenchNames) SourceTopic() string {
	return fmt.Sprintf("bench_%s_%s_src", n.SessionID, n.Connector)
}

// icebergTableBase is the unsuffixed per-engine Glue table name. Glue/SQL
// identifiers can't contain '-', so the session id's dashes become underscores.
func (n BenchNames) icebergTableBase(engine string) string {
	safe := strings.ReplaceAll(n.SessionID, "-", "_")
	return fmt.Sprintf("bench_%s_%s_%s", safe, n.Connector, engine)
}

// IcebergTable is the Glue table this stream writes. Unsuffixed for
// single-stream arms; _s<StreamIndex> when the arm runs multiple streams.
func (n BenchNames) IcebergTable(engine string) string {
	base := n.icebergTableBase(engine)
	if n.Streams <= 1 {
		return base
	}
	return fmt.Sprintf("%s_s%d", base, n.StreamIndex)
}

// IcebergTables is every table this arm writes, in stream order. Throughput for
// a multi-stream arm is the summed committed-bytes growth across all of them.
func (n BenchNames) IcebergTables(engine string) []string {
	if n.Streams <= 1 {
		return []string{n.icebergTableBase(engine)}
	}
	out := make([]string, 0, n.Streams)
	for i := 0; i < n.Streams; i++ {
		out = append(out, n.WithStream(i).IcebergTable(engine))
	}
	return out
}

// IcebergResetTables is the union of tables any arm in the plan might write:
// the base name (used by single-stream arms and by Kafka Connect) plus every
// per-stream name up to maxStreams. The between-points reset drops and
// pre-creates all of them, so a single precomputed reset script serves every
// arm and each arm still starts from zero committed bytes.
func (n BenchNames) IcebergResetTables(engine string, maxStreams int) []string {
	out := []string{n.icebergTableBase(engine)}
	if maxStreams <= 1 {
		return out
	}
	for i := 0; i < maxStreams; i++ {
		out = append(out, n.WithStreams(maxStreams).WithStream(i).IcebergTable(engine))
	}
	return out
}

// ConsumerGroup is the per-engine consumer group reading SourceTopic.
func (n BenchNames) ConsumerGroup(engine string) string {
	return fmt.Sprintf("bench_%s_%s_%s", n.SessionID, n.Connector, engine)
}

// MetricInputs carries everything EngineSeries needs to turn a per-engine
// metrics dump into a throughput series. Body is the raw dump for one
// (engine, vCPU) point: the Redpanda /public_metrics scrape for a source
// bench; the Iceberg snapshot poll for a sink bench (Plan 2).
type MetricInputs struct {
	Body  io.Reader
	Names BenchNames
}

// MetricSidecarArgs is the render context for MetricSidecar. Bucket/SessionID
// locate the S3 upload; Outs carries TF outputs (broker endpoints for source,
// Glue/warehouse for sink); Names supplies per-engine resource names.
type MetricSidecarArgs struct {
	Engine    string
	VCPU      int
	Bucket    string
	SessionID string
	Outs      map[string]string
	Names     BenchNames
}

// MetricSidecar is the bash a bench script splices in to sample throughput.
type MetricSidecar struct {
	Setup  string // background poller; defines $RP, ends with RP_SCRAPER=$!
	Upload string // aws s3 cp of $RP after the run
}

// KCRenderResult is the Kafka Connect connector to submit for an engine run.
type KCRenderResult struct {
	ConnectorName string // e.g. bench_postgres_cdc
	ConfigJSON    string // rendered connector config posted to the KC REST API
}

// Topology abstracts the direction-specific wiring of a bench. One
// implementation exists per Direction. All source-vs-sink branching lives
// behind this interface; callers (runBench, MatrixRunner) are direction-blind.
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
	// EngineSeries turns a per-engine metrics dump into a throughput series.
	EngineSeries(in MetricInputs, engine string) ([]TopicPoint, error)
	// MetricArtifact is the per-engine, per-vCPU metrics dump basename that the
	// bench script uploads and EngineSeries later parses.
	MetricArtifact(engine string, vcpu int) string
	// MetricSidecar returns the bash that samples throughput during a bench
	// window. Setup launches a background poller (polling $PID every interval,
	// framing samples under "###timestamp=<unix>" into $RP) and ends by setting
	// RP_SCRAPER=$!. Upload copies $RP to S3 after the bench process exits.
	MetricSidecar(args MetricSidecarArgs) MetricSidecar
	// KCConfig renders the Kafka Connect connector (name + JSON) for this
	// scenario, or returns ok=false when the direction has no KC counterpart.
	KCConfig(s *Scenario, outs map[string]string, n BenchNames) (res KCRenderResult, ok bool, err error)
}

// topologyFor selects the implementation for a scenario's direction. An empty
// direction is treated as source (LoadScenario normalizes it, but direct
// struct construction in tests may leave it blank).
func topologyFor(d Direction) (Topology, error) {
	switch d {
	case DirectionSource, "":
		return sourceTopology{}, nil
	case DirectionSink:
		return sinkTopology{}, nil
	default:
		return nil, fmt.Errorf("unknown direction %q", d)
	}
}
