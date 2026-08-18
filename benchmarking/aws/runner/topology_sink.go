// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"strings"
)

// sinkTopology is the sink bench path: the connector-under-test reads a
// pre-seeded Redpanda topic and writes into an external system. Everything
// connector-specific (output decoration, table reset, committed-truth
// polling) lives behind the sinkSpec hooks; this file owns the shared shell:
// the redpanda input, topic seeding, and the metric frame plumbing.
type sinkTopology struct{}

func (sinkTopology) Validate(s *Scenario) error {
	if _, ok := sinkSpecFor(s.Connector); !ok {
		return fmt.Errorf("connector %q has no sinkSpec entry; add one to sinkSpecs in sinkspecs.go", s.Connector)
	}
	return nil
}

// Pipeline injects the redpanda INPUT (consuming the pre-seeded topic) and
// hands the scenario-supplied OUTPUT component config to the connector's
// DecorateOutput hook, which fills endpoints from TF output placeholders
// (resolved by substitutePlaceholders) and names from BenchNames literals.
func (sinkTopology) Pipeline(s *Scenario, n BenchNames) (input, output map[string]any, err error) {
	sp, ok := sinkSpecFor(s.Connector)
	if !ok {
		return nil, nil, fmt.Errorf("no sinkSpec for %q", s.Connector)
	}
	out, ok := s.Pipeline["output"].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("sink scenario %q: pipeline.output must be a map", s.Connector)
	}
	cfg, ok := out[sp.OutputComponent].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("sink scenario %q: pipeline.output.%s must be a map", s.Connector, sp.OutputComponent)
	}
	sp.DecorateOutput(s, n, cfg)
	redpandaIn := map[string]any{
		"seed_brokers":      []string{"${REDPANDA_BROKER_ENDPOINTS}"},
		"topics":            []any{n.SourceTopic()},
		"consumer_group":    n.ConsumerGroup("connect"),
		"start_from_oldest": true,
	}
	// A scenario may supply extra redpanda-input tuning via pipeline.input_options
	// (e.g. unordered_processing for input-side batching, which lets the input
	// assemble large batches from its read-ahead fetch buffer so each sink
	// commit carries far more records). Merge those in, but never let them
	// clobber the bench-managed connection fields (brokers/topic/group) above.
	if opts, ok := s.Pipeline["input_options"].(map[string]any); ok {
		for k, v := range opts {
			if _, managed := redpandaIn[k]; !managed {
				redpandaIn[k] = v
			}
		}
	}
	input = map[string]any{"redpanda": redpandaIn}
	output = map[string]any{sp.OutputComponent: cfg}
	return input, output, nil
}

func (sinkTopology) SeedScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) {
	key := "stage/" + s.Dataset.Seeder
	var sb strings.Builder
	fmt.Fprintf(&sb, "\nset -euo pipefail\naws s3 cp s3://%s/%s /opt/bench/%s\nchmod +x /opt/bench/%s\n",
		outs["results_bucket"], key, s.Dataset.Seeder, s.Dataset.Seeder)

	brokers := outs["redpanda_broker_endpoints"]
	if s.Dataset.Topics <= 1 {
		fmt.Fprintf(&sb, "REDPANDA_BROKERS=%q /opt/bench/%s seed \\\n  --topic=%s --rows=%d --row-size=%d\n",
			brokers, s.Dataset.Seeder, n.SourceTopic(), s.Dataset.InitialRows, s.Dataset.RowSizeBytes)
		return sb.String(), nil
	}

	// Multi-topic: one seeder invocation per topic. InitialRows splits evenly
	// (Validate guarantees InitialRows % Topics == 0) and each topic is
	// pre-created with PartitionsPerTopic partitions (default 4).
	rowsPerTopic := s.Dataset.InitialRows / int64(s.Dataset.Topics)
	partitions := s.Dataset.partitionsPerTopic()
	scoped := n.WithTopics(s.Dataset.Topics)
	for i := 0; i < s.Dataset.Topics; i++ {
		fmt.Fprintf(&sb, "REDPANDA_BROKERS=%q /opt/bench/%s seed \\\n  --topic=%s --rows=%d --row-size=%d --partitions=%d\n",
			brokers, s.Dataset.Seeder, scoped.WithTopic(i).SourceTopic(), rowsPerTopic, s.Dataset.RowSizeBytes, partitions)
	}
	return sb.String(), nil
}

func (sinkTopology) WorkloadScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) {
	return "", nil
}

func (sinkTopology) ResetScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) {
	sp, _ := sinkSpecFor(s.Connector) // ok ignored: Validate guarantees the sinkSpec exists
	return sp.ResetScript(s, outs, n), nil
}

func (sinkTopology) EngineSeries(in MetricInputs, engine string) ([]TopicPoint, error) {
	return ParseIcebergSeries(in.Body)
}

func (sinkTopology) MetricArtifact(connector, engine, key string) string {
	sp, _ := sinkSpecFor(connector) // unknown connector yields an empty prefix; Validate prevents that upstream
	suffix := engine
	if engine == "kafka_connect" {
		suffix = "kc"
	}
	return fmt.Sprintf("%s-%s-%s.txt", sp.ArtifactPrefix, key, suffix)
}

func (t sinkTopology) MetricSidecar(args MetricSidecarArgs) MetricSidecar {
	artifact := t.MetricArtifact(args.Names.Connector, args.Engine, args.ArtifactKey())
	sp, _ := sinkSpecFor(args.Names.Connector) // ok ignored: Validate guarantees the sinkSpec exists
	upload := fmt.Sprintf(`aws s3 cp "$RP" "s3://%s/runs/%s/%s" >/dev/null`,
		args.Bucket, args.SessionID, artifact)
	return MetricSidecar{Setup: sp.SidecarSetup(args, artifact), Upload: upload}
}

func (sinkTopology) KCConfig(s *Scenario, outs map[string]string, n BenchNames) (KCRenderResult, bool, error) {
	sp, ok := sinkSpecFor(s.Connector)
	if !ok {
		return KCRenderResult{}, false, fmt.Errorf("no sinkSpec for %q", s.Connector)
	}
	if sp.KCConfig == nil {
		// Connect-only sink: no KC counterpart wired. main.go turns this into
		// a hard error if --engines includes kafka_connect.
		return KCRenderResult{}, false, nil
	}
	res, err := sp.KCConfig(s, outs, n)
	if err != nil {
		return KCRenderResult{}, false, err
	}
	return res, true, nil
}
