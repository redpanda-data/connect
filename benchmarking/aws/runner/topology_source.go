// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"encoding/json"
	"fmt"
)

// sourceTopology is the CDC bench path: the connector-under-test reads an
// external system and writes into Redpanda; throughput is measured on the
// broker produce side. Every method delegates to the pre-existing free
// functions so behavior is byte-identical to the pre-refactor runner.
type sourceTopology struct{}

func (sourceTopology) Validate(s *Scenario) error {
	if _, ok := engineSpecFor(s.Connector); !ok {
		return fmt.Errorf("connector %q has no engineSpec entry; add one to engineSpecs in scenario.go", s.Connector)
	}
	return nil
}

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

func (sourceTopology) MetricArtifact(engine string, vcpu int) string {
	suffix := engine
	if engine == "kafka_connect" {
		suffix = "kc"
	}
	return fmt.Sprintf("redpanda-%d-%s.txt", vcpu, suffix)
}

func (sourceTopology) KCConfig(s *Scenario, outs map[string]string, n BenchNames) (KCRenderResult, bool, error) {
	es, ok := engineSpecFor(s.Connector)
	if !ok {
		return KCRenderResult{}, false, fmt.Errorf("no engineSpec for %q", s.Connector)
	}
	in, err := buildKCRenderInputs(s, es, outs, n.SessionID)
	if err != nil {
		return KCRenderResult{}, false, fmt.Errorf("build KC render inputs: %w", err)
	}
	cfg, err := renderKCConfig(s, in)
	if err != nil {
		return KCRenderResult{}, false, fmt.Errorf("render KC config: %w", err)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return KCRenderResult{}, false, err
	}
	return KCRenderResult{
		ConnectorName: fmt.Sprintf("bench_%s", s.Connector),
		ConfigJSON:    string(raw),
	}, true, nil
}

func (t sourceTopology) MetricSidecar(args MetricSidecarArgs) MetricSidecar {
	artifact := t.MetricArtifact(args.Engine, args.VCPU)
	endpoints := args.Outs["redpanda_metrics_endpoints"]
	if endpoints == "" {
		endpoints = args.Outs["redpanda_metrics_endpoint"]
	}
	if endpoints == "" {
		// Match the pre-refactor renderer, which omitted the scrape entirely
		// when no broker endpoints were configured.
		return MetricSidecar{}
	}
	setup := fmt.Sprintf(`RP=/tmp/%s
: > "$RP"
ENDPOINTS=%q
(
  while kill -0 "$PID" 2>/dev/null; do
    {
      echo "###timestamp=$(date +%%s)"
      IFS=, read -ra EPS <<< "$ENDPOINTS"
      for EP in "${EPS[@]}"; do
        curl -s --max-time 5 "http://$EP/public_metrics" || echo "###scrape_error_$EP"
      done
    } >> "$RP"
    sleep 10
  done
) &
RP_SCRAPER=$!`, artifact, endpoints)
	upload := fmt.Sprintf(`aws s3 cp "$RP" "s3://%s/runs/%s/%s" >/dev/null`,
		args.Bucket, args.SessionID, artifact)
	return MetricSidecar{Setup: setup, Upload: upload}
}
