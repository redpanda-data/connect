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
	redpandaIn := map[string]any{
		"seed_brokers":      []string{"${REDPANDA_BROKER_ENDPOINTS}"},
		"topics":            []any{n.SourceTopic()},
		"consumer_group":    n.ConsumerGroup("connect"),
		"start_from_oldest": true,
	}
	// A scenario may supply extra redpanda-input tuning via pipeline.input_options
	// (e.g. unordered_processing for input-side batching, which lets the input
	// assemble large batches from its read-ahead fetch buffer so each iceberg
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
	output = map[string]any{sp.OutputComponent: icfg}
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
		// Multi-topic scenarios write N topic-derived tables instead of the
		// stream union; Topics > 1 and Streams > 1 are mutually exclusive
		// (validation enforces this), so exactly one of these branches ever
		// applies for a given scenario.
		var tables []string
		if s.Dataset.Topics > 1 {
			tables = n.WithTopics(s.Dataset.Topics).IcebergTablesForTopics(eng)
		} else {
			// Reset the union of every arm's tables: the base name plus each
			// per-stream name up to the plan's max stream count. n.Streams
			// carries that max (see planMaxStreams), which lets this one
			// precomputed script serve every arm — each arm's own tables
			// start at zero committed bytes and the extras sit empty.
			tables = n.IcebergResetTables(eng, n.Streams)
		}
		for _, table := range tables {
			// Drop the table so total-files-size restarts at 0.
			w(`aws glue delete-table --region %q --database-name %q --name %q 2>/dev/null || true`,
				region, db, table)
			// Pre-create with an explicit location: the Glue REST catalog
			// requires one on create and the KC Tabular sink does not supply it.
			//
			// Retried, because this script runs under `set -euo pipefail` and
			// iceberg-tablegen exits non-zero on transient Glue/IAM errors (it
			// already treats "already exists" as success). The table union turns
			// one unguarded call per engine into N, and a single throttled call
			// would otherwise abort the whole sweep at reset time. Three
			// attempts, then fail LOUD: a missing table must never be silently
			// tolerated, because the stream that needed it would commit nothing
			// and deflate its arm's throughput instead of erroring.
			//
			// `if cmd; then` (not `cmd && break`) because the `if` condition is
			// explicitly exempt from `-e`, making the retry's semantics
			// unambiguous.
			w(`for attempt in 1 2 3; do`)
			w(`  if /opt/bench/iceberg-tablegen --catalog-uri=%s --warehouse=%s --region=%s --namespace=%s --table=%s --location=%s; then break; fi`,
				catalogURI, warehouse, region, db, table, fmt.Sprintf("%s/%s/%s", whBase, db, table))
			w(`  if [ "$attempt" = 3 ]; then echo "iceberg-tablegen failed for %s after 3 attempts" >&2; exit 1; fi`, table)
			w(`  sleep 5`)
			w(`done`)
		}
		if s.Dataset.Topics > 1 {
			// Each topic is a distinct Kafka topic (not partitions of one
			// topic, unlike the multi-stream case), so each gets its own
			// consumer group reset.
			scoped := n.WithTopics(s.Dataset.Topics)
			for i := 0; i < s.Dataset.Topics; i++ {
				w(`/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server %q --group %q --reset-offsets --to-earliest --all-topics --execute 2>/dev/null || true`,
					brokers, scoped.WithTopic(i).ConsumerGroup(eng))
			}
		} else {
			// Reset the per-engine consumer group to re-read the whole topic. Both
			// streams of a multi-stream arm share this group — that is what splits
			// the partitions between them instead of doubling the work.
			w(`/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server %q --group %q --reset-offsets --to-earliest --all-topics --execute 2>/dev/null || true`,
				brokers, n.ConsumerGroup(eng))
		}
	}
	w(`curl -fsS -X DELETE "http://localhost:8083/connectors/bench_%s" || true`, s.Connector)
	return sb.String(), nil
}

func (sinkTopology) EngineSeries(in MetricInputs, engine string) ([]TopicPoint, error) {
	return ParseIcebergSeries(in.Body)
}

func (sinkTopology) MetricArtifact(engine, key string) string {
	suffix := engine
	if engine == "kafka_connect" {
		suffix = "kc"
	}
	return fmt.Sprintf("iceberg-%s-%s.txt", key, suffix)
}

func (t sinkTopology) MetricSidecar(args MetricSidecarArgs) MetricSidecar {
	artifact := t.MetricArtifact(args.Engine, args.ArtifactKey())
	sp, _ := sinkSpecFor(args.Names.Connector) // ok ignored: Validate guarantees the sinkSpec exists
	region := args.Outs["aws_region"]
	db := sp.Namespace
	// A multi-stream arm writes one table per stream; a multi-topic scenario
	// writes one table per topic. Either way the throughput is the summed
	// committed-bytes growth across all of them, and single-stream/
	// single-topic points yield a one-element list, so the shell shape is
	// identical either way. Topics > 1 and Streams > 1 are mutually
	// exclusive, so at most one of these ever differs from the base table.
	tables := args.Names.IcebergTables(args.Engine)
	if args.Names.Topics > 1 {
		tables = args.Names.IcebergTablesForTopics(args.Engine)
	}
	setup := fmt.Sprintf(`RP=/tmp/%s
: > "$RP"
(
  while kill -0 "$PID" 2>/dev/null; do
    {
      echo "###timestamp=$(date +%%s)"
      SIZE=0
      RECS=0
      for T in %s; do
        META=$(aws glue get-table --region %q --database-name %q --name "$T" \
                --query 'Table.Parameters.metadata_location' --output text 2>/dev/null || echo "")
        if [ -n "$META" ] && [ "$META" != "None" ]; then
          SNAP=$(aws s3 cp "$META" - 2>/dev/null || echo '{}')
          S=$(echo "$SNAP" | jq -r '[.snapshots[]?."summary"."total-files-size" // "0" | tonumber] | last // 0' 2>/dev/null || echo 0)
          R=$(echo "$SNAP" | jq -r '[.snapshots[]?."summary"."total-records" // "0" | tonumber] | last // 0' 2>/dev/null || echo 0)
          SIZE=$((SIZE + ${S:-0}))
          RECS=$((RECS + ${R:-0}))
          # Per-table line, live evidence for the plan's own acceptance check
          # ("did BOTH of arm B's tables grow, or did the rebalance starve
          # one stream of partitions"). The summed total_files_size_bytes
          # line below cannot distinguish a healthy 8/8 split from a
          # degenerate 16/0 one, and by the time anyone looks at
          # runs/<sess>/iceberg-*.txt after teardown the Glue database and
          # warehouse bucket are already gone — this line is the only
          # record. table_files_size_bytes is a DISTINCT prefix from
          # total_files_size_bytes/total_records (see ParseIcebergSeries),
          # so it is inert to the parser: unmatched lines fall through its
          # switch default and are silently dropped.
          echo "table_files_size_bytes $T ${S:-0}"
        fi
      done
      echo "total_files_size_bytes ${SIZE:-0}"
      echo "total_records ${RECS:-0}"
    } >> "$RP"
    sleep 10
  done
) &
RP_SCRAPER=$!`, artifact, strings.Join(tables, " "), region, db)
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
