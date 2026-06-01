// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"io"
)

// BenchNames is intended to become the single source of truth for the
// per-session, per-engine resource names a bench uses. Today those names are
// still computed independently in renderPipelineConfig/sourceTopology.Pipeline,
// combineReset, buildKCRenderInputs, and AttributeByEngine; the ConnectTopic /
// KCTopicPrefix helpers below exist so Plan 2 can route those sites through a
// single authority. EngineSeries already consumes Names via MetricInputs.
type BenchNames struct {
	SessionID string
	Connector string
}

func newBenchNames(sessionID, connector string) BenchNames {
	return BenchNames{SessionID: sessionID, Connector: connector}
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

// MetricInputs carries everything EngineSeries needs to turn a per-engine
// metrics dump into a throughput series. Body is the raw dump for one
// (engine, vCPU) point: the Redpanda /public_metrics scrape for a source
// bench; the Iceberg snapshot poll for a sink bench (Plan 2).
type MetricInputs struct {
	Body  io.Reader
	Names BenchNames
}

// Topology abstracts the direction-specific wiring of a bench. One
// implementation exists per Direction. All source-vs-sink branching lives
// behind this interface; callers (runBench, MatrixRunner) are direction-blind.
type Topology interface {
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
}

// topologyFor selects the implementation for a scenario's direction. An empty
// direction is treated as source (LoadScenario normalizes it, but direct
// struct construction in tests may leave it blank).
func topologyFor(d Direction) (Topology, error) {
	switch d {
	case DirectionSource, "":
		return sourceTopology{}, nil
	case DirectionSink:
		return nil, fmt.Errorf("sink topology not yet implemented (lands in Plan 2)")
	default:
		return nil, fmt.Errorf("unknown direction %q", d)
	}
}
