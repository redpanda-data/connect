// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "fmt"

// sourceTopology is the CDC bench path: the connector-under-test reads an
// external system and writes into Redpanda; throughput is measured on the
// broker produce side. Every method delegates to the pre-existing free
// functions so behavior is byte-identical to the pre-refactor runner.
type sourceTopology struct{}

func (sourceTopology) Pipeline(s *Scenario, n BenchNames) (input, output map[string]any, err error) {
	in, ok := s.Pipeline["input"].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("source scenario %q: pipeline.input must be a map", s.Connector)
	}
	// Per-engine output topic so broker-side metrics attribute cleanly. The
	// ${BENCH_SESSION_ID} placeholder is substituted later by
	// substitutePlaceholders — preserved verbatim from renderPipelineConfig.
	out := map[string]any{
		"processors": []any{
			map[string]any{
				"benchmark": map[string]any{"interval": "1s", "count_bytes": true},
			},
		},
		"redpanda": map[string]any{
			"topic": fmt.Sprintf("bench_${BENCH_SESSION_ID}_%s_connect", s.Connector),
		},
	}
	return in, out, nil
}

func (sourceTopology) SeedScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) {
	return renderSeedScript(s, outs, "stage/"+s.Dataset.Seeder)
}

func (sourceTopology) WorkloadScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) {
	return renderWorkloadScript(s, outs)
}

func (sourceTopology) ResetScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) {
	return combineReset(s.Connector, s.Reset, outs)
}

func (sourceTopology) EngineSeries(in MetricInputs, engine string) ([]TopicPoint, error) {
	series, err := ParseTopicSeries(in.Body)
	if err != nil {
		return nil, err
	}
	byEngine, err := AttributeByEngine(series, in.Names.SessionID, in.Names.Connector)
	if err != nil {
		return nil, err
	}
	return byEngine[engine], nil
}
