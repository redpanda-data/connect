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
)

// e2eTopology runs the customer's whole pipeline and measures throughput at
// the output via the benchmark processor's rolling stats in the Connect log
// rather than broker-side topic metrics.
//
// Measurement point: the benchmark processor is the last entry of
// output.processors, so it counts messages handed to the output, before the
// output's own batching and before sink acks. Transaction channels are
// unbuffered and backpressured, so in steady state its rate tracks the sink's
// acceptance rate. The overstatement is bounded by one pending batch plus
// max_in_flight batches, which is negligible over a 15-minute window.
type e2eTopology struct{}

func (e2eTopology) Validate(s *Scenario) error {
	if err := s.validateSink(); err != nil {
		return err
	}
	// Stack-backed sources still need an engine; stack none has no source.
	if s.Stack != stackNone {
		if _, ok := engineSpecFor(s.Connector); !ok {
			return fmt.Errorf("connector %q has no engineSpec entry; add one to engineSpecs in scenario.go", s.Connector)
		}
	}
	return nil
}

// Pipeline returns the scenario input unchanged and a copy of the scenario
// output with the benchmark processor appended last. The scenario's own maps
// are never mutated, so it is safe to call once per point.
func (e2eTopology) Pipeline(s *Scenario, _ BenchNames) (input, output map[string]any, err error) {
	in, ok := s.Pipeline["input"].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("e2e scenario %q: pipeline.input must be a map", s.Connector)
	}
	orig, ok := s.Pipeline["output"].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("e2e scenario %q: pipeline.output must be a map", s.Connector)
	}
	out := make(map[string]any, len(orig)+1)
	for k, v := range orig {
		out[k] = v
	}
	var procs []any
	switch p := orig["processors"].(type) {
	case nil:
	case []any:
		procs = append(procs, p...)
	default:
		return nil, nil, fmt.Errorf("e2e scenario %q: pipeline.output.processors must be a list", s.Connector)
	}
	procs = append(procs, map[string]any{
		"benchmark": map[string]any{"interval": "1s", "count_bytes": true},
	})
	out["processors"] = procs
	return in, out, nil
}

func (e2eTopology) SeedScript(s *Scenario, outs map[string]string, _ BenchNames) (string, error) {
	if s.Stack == stackNone {
		return "", nil
	}
	return renderSeedScript(s, outs, "stage/"+s.Dataset.Seeder)
}

func (e2eTopology) WorkloadScript(s *Scenario, outs map[string]string, _ BenchNames) (string, error) {
	if s.Stack == stackNone {
		return "", nil
	}
	return renderWorkloadScript(s, outs)
}

func (e2eTopology) ResetScript(s *Scenario, outs map[string]string, _ BenchNames) (string, error) {
	if s.Stack == stackNone {
		return "", nil
	}
	return combineReset(s.Connector, s.Reset, outs)
}

// EngineSeries is nil: e2e has no broker-side series.
func (e2eTopology) EngineSeries(MetricInputs) ([]TopicPoint, error) { return nil, nil }

// MetricArtifact is "": e2e uploads no broker metrics dump.
func (e2eTopology) MetricArtifact(string) string { return "" }

func (e2eTopology) MetricSidecar(MetricSidecarArgs) MetricSidecar { return MetricSidecar{} }

func (e2eTopology) SummaryFromSamples() bool { return true }
