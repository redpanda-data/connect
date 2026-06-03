// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

// sinkTopology is the sink bench path: the connector-under-test reads a
// pre-seeded Redpanda topic and writes into an external system (Iceberg/Glue).
// Throughput is the Iceberg table's committed-bytes growth, polled from Glue.
type sinkTopology struct{}

func (sinkTopology) Validate(s *Scenario) error {
	if _, ok := sinkSpecFor(s.Connector); !ok {
		return fmt.Errorf("connector %q has no sinkSpec entry; add one to sinkSpecs in sinkspecs.go", s.Connector)
	}
	return nil
}

// Pipeline injects the redpanda INPUT (consuming the pre-seeded topic) and the
// scenario-supplied OUTPUT component, filling catalog/storage from TF output
// placeholders (resolved by substitutePlaceholders). The topic, consumer group,
// and Iceberg table route through the BenchNames helpers (literals built from
// the real SessionID) so the per-engine table matches exactly what
// ResetScript/MetricSidecar poll — IcebergTable dash-sanitizes for Glue while
// SourceTopic keeps dashes, which the ${BENCH_SESSION_ID} placeholder could not
// do consistently.
func (sinkTopology) Pipeline(s *Scenario, n BenchNames) (input, output map[string]any, err error) {
	sp, ok := sinkSpecFor(s.Connector)
	if !ok {
		return nil, nil, fmt.Errorf("no sinkSpec for %q", s.Connector)
	}
	out, ok := s.Pipeline["output"].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("sink scenario %q: pipeline.output must be a map", s.Connector)
	}
	icfg, ok := out[sp.OutputComponent].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("sink scenario %q: pipeline.output.%s must be a map", s.Connector, sp.OutputComponent)
	}
	icfg["catalog"] = map[string]any{
		"url":       "${GLUE_REST_URI}",
		"warehouse": "${WAREHOUSE_ACCOUNT_ID}",
		"auth": map[string]any{
			"aws_sigv4": map[string]any{
				"region":  "${AWS_REGION}",
				"service": "glue",
			},
		},
	}
	icfg["namespace"] = sp.Namespace
	icfg["table"] = n.IcebergTable("connect")
	icfg["storage"] = map[string]any{
		"aws_s3": map[string]any{
			"bucket": "${S3_BUCKET}",
			"region": "${AWS_REGION}",
		},
	}
	icfg["schema_evolution"] = map[string]any{
		"enabled":        true,
		"table_location": "${WAREHOUSE_S3_URI}/",
	}
	input = map[string]any{
		"redpanda": map[string]any{
			"seed_brokers":      []string{"${REDPANDA_BROKER_ENDPOINTS}"},
			"topics":            []any{n.SourceTopic()},
			"consumer_group":    n.ConsumerGroup("connect"),
			"start_from_oldest": true,
		},
	}
	output = map[string]any{sp.OutputComponent: icfg}
	return input, output, nil
}

func (sinkTopology) SeedScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) {
	key := "stage/" + s.Dataset.Seeder
	return fmt.Sprintf(`
set -euo pipefail
aws s3 cp s3://%s/%s /opt/bench/%s
chmod +x /opt/bench/%s
REDPANDA_BROKERS=%q /opt/bench/%s seed \
  --topic=%s --rows=%d --row-size=%d
`,
		outs["results_bucket"], key, s.Dataset.Seeder, s.Dataset.Seeder,
		outs["redpanda_broker_endpoints"], s.Dataset.Seeder,
		n.SourceTopic(), s.Dataset.InitialRows, s.Dataset.RowSizeBytes,
	), nil
}

func (sinkTopology) WorkloadScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) {
	return "", nil
}

func (sinkTopology) ResetScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) {
	sp, _ := sinkSpecFor(s.Connector) // ok ignored: Validate guarantees the sinkSpec exists
	region := outs["aws_region"]
	db := sp.Namespace
	brokers := outs["redpanda_broker_endpoints"]
	catalogURI := outs["glue_rest_uri"]
	warehouse := outs["warehouse_account_id"]
	whBase := outs["warehouse_s3_uri"] // no trailing slash
	var sb strings.Builder
	w := func(format string, a ...any) { fmt.Fprintf(&sb, format+"\n", a...) }
	w("set -euo pipefail")
	for _, eng := range []string{"connect", "kafka_connect"} {
		table := n.IcebergTable(eng)
		// Drop the per-engine table so total-files-size restarts at 0.
		w(`aws glue delete-table --region %q --database-name %q --name %q 2>/dev/null || true`,
			region, db, table)
		// Pre-create the table with an explicit location: the Glue REST catalog
		// requires one on create and the KC Tabular sink does not supply it.
		w(`/opt/bench/iceberg-tablegen --catalog-uri=%s --warehouse=%s --region=%s --namespace=%s --table=%s --location=%s`,
			catalogURI, warehouse, region, db, table, fmt.Sprintf("%s/%s/%s", whBase, db, table))
		// Reset the per-engine consumer group to re-read the whole topic.
		w(`/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server %q --group %q --reset-offsets --to-earliest --all-topics --execute 2>/dev/null || true`,
			brokers, n.ConsumerGroup(eng))
	}
	w(`curl -fsS -X DELETE "http://localhost:8083/connectors/bench_%s" || true`, s.Connector)
	return sb.String(), nil
}

func (sinkTopology) EngineSeries(in MetricInputs, engine string) ([]TopicPoint, error) {
	return ParseIcebergSeries(in.Body)
}

func (sinkTopology) MetricArtifact(engine string, vcpu int) string {
	suffix := engine
	if engine == "kafka_connect" {
		suffix = "kc"
	}
	return fmt.Sprintf("iceberg-%d-%s.txt", vcpu, suffix)
}

func (t sinkTopology) MetricSidecar(args MetricSidecarArgs) MetricSidecar {
	artifact := t.MetricArtifact(args.Engine, args.VCPU)
	sp, _ := sinkSpecFor(args.Names.Connector) // ok ignored: Validate guarantees the sinkSpec exists
	region := args.Outs["aws_region"]
	db := sp.Namespace
	table := args.Names.IcebergTable(args.Engine)
	setup := fmt.Sprintf(`RP=/tmp/%s
: > "$RP"
(
  while kill -0 "$PID" 2>/dev/null; do
    {
      echo "###timestamp=$(date +%%s)"
      META=$(aws glue get-table --region %q --database-name %q --name %q \
              --query 'Table.Parameters.metadata_location' --output text 2>/dev/null || echo "")
      if [ -n "$META" ] && [ "$META" != "None" ]; then
        SNAP=$(aws s3 cp "$META" - 2>/dev/null || echo '{}')
        SIZE=$(echo "$SNAP" | jq -r '[.snapshots[]?."summary"."total-files-size" // "0" | tonumber] | last // 0' 2>/dev/null || echo 0)
        RECS=$(echo "$SNAP" | jq -r '[.snapshots[]?."summary"."total-records" // "0" | tonumber] | last // 0' 2>/dev/null || echo 0)
      else
        SIZE=0
        RECS=0
      fi
      echo "total_files_size_bytes ${SIZE:-0}"
      echo "total_records ${RECS:-0}"
    } >> "$RP"
    sleep 10
  done
) &
RP_SCRAPER=$!`, artifact, region, db, table)
	upload := fmt.Sprintf(`aws s3 cp "$RP" "s3://%s/runs/%s/%s" >/dev/null`,
		args.Bucket, args.SessionID, artifact)
	return MetricSidecar{Setup: setup, Upload: upload}
}

func (sinkTopology) KCConfig(s *Scenario, outs map[string]string, n BenchNames) (KCRenderResult, bool, error) {
	sp, ok := sinkSpecFor(s.Connector)
	if !ok {
		return KCRenderResult{}, false, fmt.Errorf("no sinkSpec for %q", s.Connector)
	}
	in := kcRenderInputs{
		GlueRESTURI:   outs["glue_rest_uri"],
		Warehouse:     outs["warehouse_account_id"],
		Region:        outs["aws_region"],
		Namespace:     sp.Namespace,
		Table:         n.IcebergTable("kafka_connect"),
		Topic:         n.SourceTopic(),
		ConsumerGroup: n.ConsumerGroup("kafka_connect"),
	}
	cfg, err := renderKCConfig(s, in)
	if err != nil {
		return KCRenderResult{}, false, fmt.Errorf("render KC iceberg config: %w", err)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return KCRenderResult{}, false, err
	}
	return KCRenderResult{ConnectorName: fmt.Sprintf("bench_%s", s.Connector), ConfigJSON: string(raw)}, true, nil
}
