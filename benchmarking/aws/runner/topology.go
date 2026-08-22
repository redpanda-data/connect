// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"io"
	"strconv"
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
	// Topics is a scenario's multi-topic sink bench topic count: N pre-seeded
	// source topics instead of one, each with its own Iceberg table. 0 and 1
	// both mean single-topic, in which case the source topic, table, and
	// consumer group are all unsuffixed exactly as they were before Topics
	// existed. > 1 suffixes each per-topic name with _t<TopicIndex>.
	//
	// Topics > 1 and Streams > 1 are mutually exclusive — validation enforces
	// this (dataset.topics and matrix.arms[].streams are independent knobs,
	// but a scenario should only exercise one at a time). If both are somehow
	// set, IcebergTable prefers the _t<i> suffix and never emits both.
	Topics int
	// TopicIndex selects which topic's names to render. Only meaningful when
	// Topics > 1.
	TopicIndex int
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

// WithTopics returns a copy scoped to a scenario's topic count, resetting the
// topic index to 0.
//
// Order matters: WithTopics always resets TopicIndex to 0, so
// n.WithTopics(7).WithTopic(3) is correct but n.WithTopic(3).WithTopics(7)
// silently loses the index — WithTopics runs second and stomps it back to 0
// with no error. Always call WithTopics before WithTopic (the same footgun
// WithStreams/WithStream document).
func (n BenchNames) WithTopics(count int) BenchNames {
	n.Topics = count
	n.TopicIndex = 0
	return n
}

// WithTopic returns a copy scoped to one topic of a multi-topic scenario.
//
// Call this AFTER WithTopics, not before: WithTopics resets TopicIndex to 0,
// so reversing the order (n.WithTopic(3).WithTopics(7)) silently drops the
// index you just set.
func (n BenchNames) WithTopic(idx int) BenchNames {
	n.TopicIndex = idx
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
// Unsuffixed when Topics <= 1 (unchanged); else suffixed with
// _t<TopicIndex> so each of a multi-topic scenario's N topics is distinct.
func (n BenchNames) SourceTopic() string {
	base := fmt.Sprintf("bench_%s_%s_src", n.SessionID, n.Connector)
	if n.Topics <= 1 {
		return base
	}
	return fmt.Sprintf("%s_t%d", base, n.TopicIndex)
}

// icebergTableBase is the unsuffixed per-engine Glue table name. Glue/SQL
// identifiers can't contain '-', so the session id's dashes become underscores.
func (n BenchNames) icebergTableBase(engine string) string {
	safe := strings.ReplaceAll(n.SessionID, "-", "_")
	return fmt.Sprintf("bench_%s_%s_%s", safe, n.Connector, engine)
}

// IcebergTable is the Glue table this stream/topic writes. Unsuffixed for
// single-stream, single-topic arms; _s<StreamIndex> when the arm runs
// multiple streams; _t<TopicIndex> when the scenario runs multiple topics.
// Topics > 1 and Streams > 1 are mutually exclusive (validation enforces
// this); if both are somehow set, the _t<i> suffix wins and _s<i> is never
// also emitted.
func (n BenchNames) IcebergTable(engine string) string {
	base := n.icebergTableBase(engine)
	if n.Topics > 1 {
		return fmt.Sprintf("%s_t%d", base, n.TopicIndex)
	}
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

// IcebergTablesForTopics is every table a multi-topic scenario writes, in
// topic order. Single-topic scenarios (Topics <= 1) yield a one-element list
// with the unsuffixed base name, so callers get an identical shape either
// way. Used by the sidecar (throughput is the summed growth across all N
// topic tables) and by ResetScript (pre-create/drop the union).
func (n BenchNames) IcebergTablesForTopics(engine string) []string {
	if n.Topics <= 1 {
		return []string{n.icebergTableBase(engine)}
	}
	out := make([]string, 0, n.Topics)
	for i := 0; i < n.Topics; i++ {
		out = append(out, n.WithTopic(i).IcebergTable(engine))
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
// Unsuffixed when Topics <= 1 (unchanged). Suffixed with _t<TopicIndex> when
// Topics > 1: in streams mode each stream reads a distinct topic under its
// own group so it independently consumes that whole topic (unlike the
// Streams-only case, where multiple streams intentionally SHARE one group to
// split one topic's partitions). Fan-in instead needs the unsuffixed group
// with one subscription per topic — it gets that by calling ConsumerGroup on
// an unscoped (Topics <= 1) BenchNames, which is guaranteed to reproduce
// today's exact name.
func (n BenchNames) ConsumerGroup(engine string) string {
	base := fmt.Sprintf("bench_%s_%s_%s", n.SessionID, n.Connector, engine)
	if n.Topics > 1 {
		return fmt.Sprintf("%s_t%d", base, n.TopicIndex)
	}
	return base
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
	Engine string
	VCPU   int
	// Key identifies the sweep point in artifact names: the bare vCPU count
	// for arm-less scenarios, "<vcpu>-<armID>" when arms are in play. Falls
	// back to VCPU when empty so narrow unit tests can omit it.
	Key       string
	Bucket    string
	SessionID string
	Outs      map[string]string
	Names     BenchNames
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
	// MetricArtifact is the per-engine, per-point metrics dump basename that
	// the bench script uploads and EngineSeries later parses. key is the
	// sweepPoint key: a bare vCPU count without arms, "<vcpu>-<armID>" with.
	// connector selects the sink connector's artifact prefix; the source
	// topology ignores it (every source dump is a Redpanda scrape).
	MetricArtifact(connector, engine, key string) string
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
