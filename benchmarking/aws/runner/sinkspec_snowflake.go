// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"strings"
)

// snowflakeKeyPath is where the reset script materializes the RSA private key
// on the runner host, and what the rendered pipeline's private_key_file
// points at.
const snowflakeKeyPath = "/opt/bench/snowflake_rsa_key.p8"

// snowflakeKeySSMParam is the SecureString parameter holding the PEM key,
// created once by hand (see scenarios/snowflake/README.md). The runner host
// reads it with the ssm:GetParameter its AmazonSSMManagedInstanceCore role
// policy already grants — the key never enters Terraform state or the repo.
const snowflakeKeySSMParam = "/bench/snowflake/private_key"

// snowflakeSinkSpec benches the snowflake_streaming output (Snowpipe
// Streaming) Connect-only: no Kafka Connect counterpart is wired (KCConfig
// nil), so --engines must stay [connect].
//
// The Snowflake account is external to AWS. Non-secret connection facts
// (account, user, role, database, schema) arrive as outputs of the snowflake
// TF stack, which reads them from plain SSM parameters under /bench/snowflake/
// — hence the ${SNOWFLAKE_*} placeholders in DecorateOutput. Throughput is
// server-side committed truth: the sidecar polls SHOW TABLES (metadata-only,
// no warehouse credits) via snowflake-tablegen and emits the same frame
// format the iceberg sidecar does, so ParseIcebergSeries serves both.
var snowflakeSinkSpec = sinkSpec{
	OutputComponent: "snowflake_streaming",
	HelperBinary:    "snowflake-tablegen",
	ArtifactPrefix:  "snowflake",
	DecorateOutput:  snowflakeDecorateOutput,
	ResetScript:     snowflakeResetScript,
	SidecarSetup:    snowflakeSidecarSetup,
}

// snowflakeDecorateOutput fills connection fields from TF output placeholders
// and the table from BenchNames (IcebergTable is the generic sink-table name
// despite the Iceberg name: bench_<session>_<connector>_<engine>, dashes
// sanitized to underscores — which Snowflake unquoted identifiers need too).
// Batching, max_in_flight, and schema_evolution stay scenario-owned.
func snowflakeDecorateOutput(_ *Scenario, n BenchNames, cfg map[string]any) {
	cfg["account"] = "${SNOWFLAKE_ACCOUNT}"
	cfg["user"] = "${SNOWFLAKE_USER}"
	cfg["role"] = "${SNOWFLAKE_ROLE}"
	cfg["database"] = "${SNOWFLAKE_DATABASE}"
	cfg["schema"] = "${SNOWFLAKE_SCHEMA}"
	cfg["table"] = n.IcebergTable("connect")
	cfg["private_key_file"] = snowflakeKeyPath
}

// snowflakeTablegenAuthFlags renders the connection flags shared by every
// snowflake-tablegen invocation. Values are real TF outputs, not placeholders:
// reset and sidecar scripts render after terraform apply.
func snowflakeTablegenAuthFlags(outs map[string]string) string {
	return fmt.Sprintf("--account=%q --user=%q --role=%q --database=%q --schema=%q --key-file=%s",
		outs["snowflake_account"], outs["snowflake_user"], outs["snowflake_role"],
		outs["snowflake_database"], outs["snowflake_schema"], snowflakeKeyPath)
}

func snowflakeResetScript(s *Scenario, outs map[string]string, n BenchNames) string {
	region := outs["aws_region"]
	brokers := outs["redpanda_broker_endpoints"]
	// Same union rule as iceberg, connect engine only: multi-topic scenarios
	// write N topic-derived tables; otherwise the base name plus every
	// per-stream name up to the plan's max stream count (n.Streams, see
	// planMaxStreams), so one precomputed script serves every arm.
	var tables []string
	if s.Dataset.Topics > 1 {
		tables = n.WithTopics(s.Dataset.Topics).IcebergTablesForTopics("connect")
	} else {
		tables = n.IcebergResetTables("connect", n.Streams)
	}
	var sb strings.Builder
	w := func(format string, a ...any) { fmt.Fprintf(&sb, format+"\n", a...) }
	w("set -euo pipefail")
	// Materialize the private key for both this script's tablegen calls and
	// the pipeline's private_key_file. Re-fetched every reset (idempotent),
	// so a key rotation in SSM takes effect at the next sweep point.
	w(`aws ssm get-parameter --region %q --name %q --with-decryption --query Parameter.Value --output text > %s`,
		region, snowflakeKeySSMParam, snowflakeKeyPath)
	w(`chmod 0600 %s`, snowflakeKeyPath)
	auth := snowflakeTablegenAuthFlags(outs)
	for _, table := range tables {
		// CREATE OR REPLACE TABLE: one statement drops the old rows and
		// guarantees the table exists, so SHOW TABLES restarts at 0 rows.
		// Retried like iceberg-tablegen, and for the same reason: this script
		// runs under `set -euo pipefail`, and a transient auth/API failure
		// must not abort the whole sweep — but after three attempts fail
		// LOUD, because a stream writing to a missing table would commit
		// nothing and silently deflate its arm's throughput.
		w(`for attempt in 1 2 3; do`)
		w(`  if /opt/bench/snowflake-tablegen reset %s --table=%q; then break; fi`, auth, table)
		w(`  if [ "$attempt" = 3 ]; then echo "snowflake-tablegen reset failed for %s after 3 attempts" >&2; exit 1; fi`, table)
		w(`  sleep 5`)
		w(`done`)
	}
	if s.Dataset.Topics > 1 {
		// Each topic is a distinct Kafka topic, so each gets its own consumer
		// group reset (see ConsumerGroup's Topics > 1 naming rule).
		scoped := n.WithTopics(s.Dataset.Topics)
		for i := 0; i < s.Dataset.Topics; i++ {
			w(`/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server %q --group %q --reset-offsets --to-earliest --all-topics --execute 2>/dev/null || true`,
				brokers, scoped.WithTopic(i).ConsumerGroup("connect"))
		}
	} else {
		// Rewind the consumer group so the next point re-reads the whole
		// topic. Both streams of a multi-stream arm share this group — that
		// is what splits the partitions between them.
		w(`/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server %q --group %q --reset-offsets --to-earliest --all-topics --execute 2>/dev/null || true`,
			brokers, n.ConsumerGroup("connect"))
	}
	return sb.String()
}

func snowflakeSidecarSetup(args MetricSidecarArgs, artifact string) string {
	// Same table-union rule as the iceberg sidecar: streams and topics are
	// mutually exclusive, and single-stream/single-topic points yield a
	// one-element list, so the shape is identical either way.
	tables := args.Names.IcebergTables(args.Engine)
	if args.Names.Topics > 1 {
		tables = args.Names.IcebergTablesForTopics(args.Engine)
	}
	auth := snowflakeTablegenAuthFlags(args.Outs)
	// One helper invocation per frame prints the whole frame body (per-table
	// lines plus totals), keeping the shell dumb. `|| true` because a failed
	// poll leaves a timestamp-only frame, which ParseIcebergSeries skips —
	// one lost sample, not a dead scraper under a transient Snowflake error.
	return fmt.Sprintf(`RP=/tmp/%s
: > "$RP"
(
  while kill -0 "$PID" 2>/dev/null; do
    {
      echo "###timestamp=$(date +%%s)"
      /opt/bench/snowflake-tablegen poll %s --tables=%s || true
    } >> "$RP"
    sleep 10
  done
) &
RP_SCRAPER=$!`, artifact, auth, strings.Join(tables, ","))
}
