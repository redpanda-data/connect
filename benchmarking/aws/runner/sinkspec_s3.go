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

// s3PathField is the aws_s3 output's filename-template field. The scenario
// YAML owns its value (e.g. "${!uuid_v4()}.ndjson.gz"); s3DecorateOutput
// prefixes it with a bench-managed, session-unique folder so ResetScript can
// delete exactly this run's objects between sweep points without touching
// other concurrent sessions in the same shared bucket.
const s3PathField = "path"

// s3DefaultPathTemplate is the filename template used when a scenario
// doesn't set pipeline.output.aws_s3.path at all.
const s3DefaultPathTemplate = "${!uuid_v4()}.ndjson.gz"

// s3PagedSizeSumAwk sums every numeric field across every line of `aws
// s3api list-objects-v2 --query "Contents[].Size" --output text` output for
// one prefix. An earlier version of this query used
// `sum(Contents[].Size)` so the CLI would hand back a single pre-summed
// number, but that had two problems: the CLI applies --query PER PAGE, not
// once over the whole paginated result, so a prefix with more than 1000
// objects printed one sum per page as a whitespace-separated token, not a
// single grand total; and a jmespath sum() over an absent Contents key on
// an EMPTY prefix is a type error, making the CLI call fail outright on
// every sweep point's first poll (see s3SidecarSetup's AWSFAIL handling).
// Querying the raw per-object sizes instead fixes both: an empty prefix's
// missing Contents key just projects to nothing (a true, error-free zero,
// not a failure), and a populated prefix -- one page or many -- prints
// every object's size as its own token, which this awk program sums
// regardless of how many pages or lines they're spread across. Looping
// over every field on every line and accumulating into one running total
// is correct whether the call returned zero, one, or many objects' worth
// of sizes; a non-numeric or missing token coerces to 0 in awk's numeric
// context rather than corrupting the sum. Exported as a constant (rather
// than left inline in s3SidecarSetup's template) so tests can exercise the
// exact same awk program the sidecar runs, instead of a hand-copied
// duplicate that could silently drift out of sync.
const s3PagedSizeSumAwk = `{for(i=1;i<=NF;i++) s+=$i} END{print s+0}`

// s3SinkSpec drains a pre-seeded Redpanda topic into raw S3 objects (batched
// gzip'd NDJSON via the aws_s3 output on the Connect side, gzip'd JSON-lines
// via the Aiven S3 Sink connector on the Kafka Connect side) — a head-to-head
// against a KC S3 Sink replacement. Both engines can drain the same
// pre-seeded topic concurrently into the same shared results bucket, so every
// prefix and consumer group is scoped per engine (see s3Prefix). Throughput
// is server-side committed truth from two independent sources per frame (see
// s3SidecarSetup): `total_files_size_bytes` comes from `aws s3api
// list-objects-v2` (real S3-written, gzip-compressed bytes — useful for
// storage-cost sizing, not for throughput comparison, since it varies with
// whatever compression ratio the seeder's synthetic data happens to produce);
// `total_records` comes from the engine's Kafka consumer group offset (real
// consumed-message count, compression- and format-independent, comparable
// across engines/runs and against a customer's real topic
// produce-throughput). ParseIcebergSeries (unchanged) turns consecutive
// `total_records` deltas into MsgPerSec; the raw uncompressed MB/s
// equivalent to a customer's produce-throughput dashboards is `MsgPerSec *
// dataset.row_size_bytes / 1e6`, computed downstream by whoever reads the
// results JSON — NOT the MBPerSec field, which stays S3-compressed-bytes
// based.
var s3SinkSpec = sinkSpec{
	OutputComponent: "aws_s3",
	ArtifactPrefix:  "s3",
	DecorateOutput:  s3DecorateOutput,
	ResetScript:     s3ResetScript,
	SidecarSetup:    s3SidecarSetup,
	KCConfig:        s3KCConfig,
}

// s3Prefix is the run-scoped, engine-scoped S3 key prefix objects for one
// topic land under. SourceTopic already embeds the session id, connector,
// and (for multi-topic scenarios) the topic index; adding the engine segment
// on top keeps Connect's and Kafka Connect's writes into the same shared
// results bucket from co-mingling, since both drain the same pre-seeded
// topic (via separate consumer groups) into disjoint namespaces that
// ResetScript can safely wipe independently.
func s3Prefix(n BenchNames, engine string) string {
	return fmt.Sprintf("raw/%s/%s/", n.SourceTopic(), engine)
}

// s3Prefixes is every prefix one engine's sink writes to, one per topic for
// multi-topic scenarios, else a single-element list — mirroring the
// IcebergTablesForTopics shape the iceberg/snowflake sinkSpecs use.
func s3Prefixes(n BenchNames, engine string) []string {
	if n.Topics <= 1 {
		return []string{s3Prefix(n, engine)}
	}
	out := make([]string, 0, n.Topics)
	for i := 0; i < n.Topics; i++ {
		out = append(out, s3Prefix(n.WithTopic(i), engine))
	}
	return out
}

// s3ConsumerGroups is every consumer group the Connect engine's sink reads
// from, one per topic for multi-topic scenarios, else a single-element list
// — mirroring s3Prefixes/IcebergTablesForTopics. Each element pairs 1:1 with
// the same-index element of s3Prefixes(n, "connect"): both are derived from
// the same n.WithTopic(i) scoping, so s3SidecarSetup can sum offsets across
// exactly the topics whose objects it's also summing bytes for.
//
// This is only correct for the "connect" engine: n.ConsumerGroup("connect")
// is a name Redpanda Connect's redpanda input is explicitly configured with
// (see topology_sink.go), so it is genuinely under our control. Kafka
// Connect sink connectors do NOT take a configurable consumer group for
// their data-consuming task — the framework derives one internally, always
// named "connect-<connector-name>" — so callers must use
// s3KafkaConnectConsumerGroups for the "kafka_connect" engine instead of
// calling this with engine="kafka_connect".
func s3ConsumerGroups(n BenchNames, engine string) []string {
	if n.Topics <= 1 {
		return []string{n.ConsumerGroup(engine)}
	}
	out := make([]string, 0, n.Topics)
	for i := 0; i < n.Topics; i++ {
		out = append(out, n.WithTopic(i).ConsumerGroup(engine))
	}
	return out
}

// s3KafkaConnectConnectorName is the exact name matrix.go submits Kafka
// Connect's S3 sink connector under for a given vCPU sweep point (see
// matrix.go's vcpuConnectorName := fmt.Sprintf("%s_v%d", m.KCConnectorName,
// n), and s3KCConfig's KCConnectorName = fmt.Sprintf("bench_%s",
// s.Connector)). That half of the naming is fully under our control.
//
// What is NOT under our control is the internal consumer-group id Kafka
// Connect's framework derives for that connector's data-consuming task —
// sink connectors don't take a configurable group id. An earlier version of
// this file hardcoded a guess at the framework's derivation
// ("connect-<connector-name>"), confirmed once against a single live worker
// log ("[Consumer clientId=connector-consumer-bench_s3_v1-0,
// groupId=connect-bench_s3_v1]") but never re-verified, and fragile to any
// framework/version change in that convention. s3SidecarSetup instead uses
// this connector-name string as a substring filter against
// `kafka-consumer-groups.sh --list` output at poll time, discovering
// whatever the real group name turns out to be rather than guessing it.
//
// TODO: multi-topic kafka_connect naming is unverified — matrix.go does not
// version the KC connector name per topic today, and both s3 scenarios
// (orders-sink.yaml, orders-sink-smoke.yaml) are single-topic, so this has
// only been confirmed for the single-topic case.
func s3KafkaConnectConnectorName(n BenchNames, vcpu int) string {
	return fmt.Sprintf("bench_%s_v%d", n.Connector, vcpu)
}

// s3DecorateOutput fills the bucket/region from TF output placeholders
// (resolved by substitutePlaceholders) and rewrites path to prefix the
// scenario-owned filename template with the run-scoped, Connect-scoped
// folder. n is already scoped to the topic this config set is for
// (renderPointConfigs calls topo.Pipeline once per topic for multi-topic
// scenarios, same as iceberg/snowflake), so s3Prefix(n, "connect") is exact
// whether Topics is 0, 1, or greater. DecorateOutput only ever renders the
// Redpanda Connect pipeline (the Kafka Connect counterpart is rendered
// separately by s3KCConfig), so the engine is always "connect" here.
// Batching, content_type, content_encoding, and max_in_flight stay
// scenario-owned.
func s3DecorateOutput(_ *Scenario, n BenchNames, cfg map[string]any) {
	cfg["bucket"] = "${RESULTS_BUCKET}"
	cfg["region"] = "${AWS_REGION}"

	template, _ := cfg[s3PathField].(string)
	if template == "" {
		template = s3DefaultPathTemplate
	}
	cfg[s3PathField] = s3Prefix(n, "connect") + template
}

// s3ResetScript resets BOTH engines' state unconditionally (mirroring
// icebergResetScript's dual-engine shape), since Connect and Kafka Connect
// can each run against the same shared results bucket and must each start a
// sweep point at zero committed bytes: it wipes each engine's S3 prefix(es),
// then either rewinds each engine's consumer group (bounded-backlog-drain
// scenarios) or deletes and recreates the source topic itself (live-stream
// scenarios — see the liveWorkload branch below for why), then tears down
// any lingering KC connector from a previous point.
func s3ResetScript(s *Scenario, outs map[string]string, n BenchNames) string {
	bucket := outs["results_bucket"]
	region := outs["aws_region"]
	brokers := outs["redpanda_broker_endpoints"]
	var sb strings.Builder
	w := func(format string, a ...any) { fmt.Fprintf(&sb, format+"\n", a...) }
	w("set -euo pipefail")
	// Under continuous production (workload.write_rate_per_sec > 0), the live
	// producer keeps feeding the topic across every sweep point. A first
	// attempt at isolating each point's own window just reset consumer-group
	// offsets --to-latest between points instead of replaying from
	// --to-earliest — but that only rewinds where a consumer group *starts
	// reading from*, it never removes anything from the topic itself. Kafka
	// Connect's Aiven S3 Sink drains slower than the producer feeds it (its
	// bursty, sub-mean throughput is documented elsewhere in this codebase),
	// so every point where kafka_connect was the active engine left a large
	// unconsumed tail sitting permanently on the brokers' disks — an
	// --to-latest reset just means the NEXT point's consumer skips over that
	// tail, it never goes away. That accumulating on-disk backlog is what
	// throttled the *producer* itself via broker-side backpressure on every
	// point after the first in a real run (point 1 hit ~100% of target;
	// every later point, regardless of which engine/vCPU was active,
	// plateaued at ~37-40%). The fix is the same one CDC source scenarios
	// already use for their own between-point reset (see
	// orders-cdc.yaml's TRUNCATE-based reset: block): delete and recreate
	// the topic itself so each point starts from a genuinely empty topic,
	// not just rewound offsets. Once the topic is fresh there are no stale
	// offsets to reset for either engine, so the per-engine
	// kafka-consumer-groups.sh --reset-offsets loop below is skipped
	// entirely for this branch. Bounded-backlog-drain scenarios (Workload
	// nil, or Workload set with no write_rate_per_sec — e.g. orders-sink.yaml's
	// KC-cold-start warmup-only block) are unaffected and keep resetting to
	// earliest against the same static pre-seeded topic, byte-identical to
	// before this existed.
	liveWorkload := s.Workload != nil && s.Workload.WriteRatePerSec > 0
	for _, eng := range []string{"connect", "kafka_connect"} {
		// ResetScript's n arrives unscoped for topics (see matrix.go/main.go:
		// it is only ever WithStreams-scoped before this call), so the
		// prefix union is driven off s.Dataset.Topics directly, same as
		// snowflakeResetScript/icebergResetScript.
		var prefixes []string
		if s.Dataset.Topics > 1 {
			prefixes = s3Prefixes(n.WithTopics(s.Dataset.Topics), eng)
		} else {
			prefixes = s3Prefixes(n, eng)
		}
		for _, prefix := range prefixes {
			// `|| true` because an empty/nonexistent prefix on the first
			// sweep point isn't an error. `--quiet` matters more here than in
			// the bounded-backlog-drain case this script also serves: under
			// live-stream production (see the liveWorkload branch below) a
			// single engine's window can leave thousands of objects to
			// delete, and --recursive's default per-object "delete: ..."
			// echo easily exceeds the SSM RunCommand output cap (~24KB) —
			// observed live as an indefinite hang with zero CPU activity,
			// not a truncation error, so it's silent until you notice the
			// run stopped progressing. --quiet keeps this script's own
			// output small regardless of how many objects it deletes.
			w(`aws s3 rm "s3://%s/%s" --recursive --quiet --region %q || true`, bucket, prefix, region)
		}
		// The live-stream branch deletes and recreates the topic itself once
		// below (outside this per-engine loop), which implicitly invalidates
		// both engines' stale consumer-group offsets — resetting them here on
		// top would be redundant, and the resulting group.id wouldn't even
		// exist against the fresh topic until each engine's consumer rejoins.
		if liveWorkload {
			continue
		}
		if s.Dataset.Topics > 1 {
			// Each topic is a distinct Kafka topic, so each gets its own
			// consumer group reset (same rule as
			// snowflakeResetScript/icebergResetScript).
			scoped := n.WithTopics(s.Dataset.Topics)
			for i := 0; i < s.Dataset.Topics; i++ {
				w(`/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server %q --group %q --reset-offsets --to-earliest --all-topics --execute 2>/dev/null || true`,
					brokers, scoped.WithTopic(i).ConsumerGroup(eng))
			}
		} else {
			w(`/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server %q --group %q --reset-offsets --to-earliest --all-topics --execute 2>/dev/null || true`,
				brokers, n.ConsumerGroup(eng))
		}
	}
	if liveWorkload {
		// Delete and recreate the source topic itself, once per topic (not
		// per engine — a single delete+recreate clears both engines' stale
		// consumer-group state implicitly). This is the live-stream
		// counterpart to the --to-earliest offset reset above: instead of
		// rewinding where a consumer starts reading, it makes the topic
		// genuinely empty again, so the accumulating on-disk backlog that
		// throttled the producer via broker backpressure can't build up
		// across the sweep. The partition count is read back from the
		// live topic rather than hardcoded, since a scenario's
		// partitions_per_topic (or a future change to json-orders seed's own
		// --partitions default) could differ from whatever default this file
		// might otherwise assume; 16 is only a last-resort fallback matching
		// that default, used if --describe fails (e.g. the topic doesn't
		// exist yet on the very first sweep point). Deletion is asynchronous
		// in both Kafka and Redpanda, so the loop below polls --list until
		// the topic is actually gone before recreating it — recreating too
		// early would race the async delete (mirrors the bounded polling
		// loop kcscript.go uses while waiting for port 8083 to free up
		// before spawning a new worker). max.message.bytes=67108864 (64
		// MiB) matches json-orders seed()'s own topic-config override: the
		// producer batches up to 16 MiB, and the broker's default
		// max.message.bytes is far below that, so a freshly recreated topic
		// needs the same override or large batches get rejected.
		var topics []string
		if s.Dataset.Topics > 1 {
			scoped := n.WithTopics(s.Dataset.Topics)
			for i := 0; i < s.Dataset.Topics; i++ {
				topics = append(topics, scoped.WithTopic(i).SourceTopic())
			}
		} else {
			topics = []string{n.SourceTopic()}
		}
		for _, topic := range topics {
			w(`PARTS=$(/opt/kafka/bin/kafka-topics.sh --bootstrap-server %q --describe --topic %q 2>/dev/null | head -1 | grep -oE 'PartitionCount:[[:space:]]*[0-9]+' | grep -oE '[0-9]+$' || true)
[ -z "$PARTS" ] && PARTS=16
/opt/kafka/bin/kafka-topics.sh --bootstrap-server %q --delete --topic %q 2>/dev/null || true
for i in $(seq 1 30); do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server %q --list 2>/dev/null | grep -qx %q || break
  sleep 2
done
/opt/kafka/bin/kafka-topics.sh --bootstrap-server %q --create --topic %q --partitions "$PARTS" --replication-factor 3 --config max.message.bytes=67108864 2>/dev/null || true`,
				brokers, topic, brokers, topic, brokers, topic, brokers, topic)
		}
	}
	// Tear down any KC connector left running from the previous sweep point
	// (mirrors icebergResetScript).
	w(`curl -fsS -X DELETE "http://localhost:8083/connectors/bench_%s" || true`, s.Connector)
	return sb.String()
}

// s3SidecarSetup polls two independent, compression-agnostic-vs-compressed
// sources per frame, on the same 10s cadence:
//
//   - total_files_size_bytes: summed `aws s3api list-objects-v2` object
//     sizes under the engine-scoped prefix(es) — real S3-written,
//     gzip-compressed bytes. Useful for storage-cost sizing; NOT comparable
//     across runs/engines, since it moves with whatever compression ratio
//     the seeder's synthetic data happens to produce. The query is
//     `Contents[].Size` (every object's raw size, not a pre-summed total —
//     see s3PagedSizeSumAwk for why), and `--query` is applied PER PAGE by
//     the CLI (see the loop below), so a prefix with more than 1000 objects
//     prints one page's worth of sizes per line, which the loop adds
//     together. An empty prefix has no Contents key at all, so the query
//     projects to nothing and the CLI exits 0 with empty/`None` output —
//     summed as a true zero below, not treated as a failure. If a page's
//     call genuinely fails (bad credentials, missing bucket, etc.), that
//     line is omitted for the frame entirely (see SIZE_OK below) instead of
//     being reported as a false 0, so a real AWS API error can't be
//     confused downstream with a genuinely idle/empty interval.
//
// This whole sidecar body is spliced into renderBenchScript's script, which
// runs under `set -euo pipefail` (matrix.go), and that shell option is
// inherited by the `( while ... ) &` background subshell this function's
// output lives inside. Every command substitution and pipeline below must
// therefore force its own zero exit status (via `|| <fallback>`) rather than
// relying on a subsequent `if [ $? -ne 0 ]` check: under `set -e`, a bare
// assignment `VAR=$(cmd)` whose command substitution exits non-zero kills
// the shell on that line, before the next line even runs, so `$?` is never
// reached there. This guard is load-bearing history, not theoretical: an
// earlier version of this query used `aws s3api list-objects-v2 --query
// "sum(Contents[].Size)"`, and a jmespath sum() over an absent Contents key
// on an empty prefix is a type error — a guaranteed CLI failure at the
// start of every sweep point (KC's cold start alone can leave a prefix
// empty for ~10 minutes), which silently killed the whole poll loop after
// emitting exactly one frame. Querying `Contents[].Size` instead of
// `sum(Contents[].Size)` fixes that specific failure at the source (an
// empty prefix is now a real, error-free zero — see s3PagedSizeSumAwk), but
// every assignment/pipeline here still ends in `|| <sentinel-or-zero>` so a
// genuinely failed call (bad credentials, missing bucket, transient AWS API
// error) still can't kill this loop: failure is detected by inspecting the
// captured value afterward instead of by letting the shell die.
//   - total_records: summed CURRENT-OFFSET across every partition of the
//     engine-scoped consumer group(s), read via `kafka-consumer-groups.sh
//     --describe`. This is the exact count of Kafka records actually
//     consumed, independent of engine, output format, or compression — the
//     same axis a customer's real topic produce-throughput dashboards
//     report. For the kafka_connect engine the group itself is discovered
//     at poll time (see below) rather than guessed. ParseIcebergSeries
//     (unchanged) turns consecutive deltas of this field into MsgPerSec;
//     multiplying that by dataset.row_size_bytes gives the raw uncompressed
//     MB/s figure to compare against a customer's numbers (that
//     multiplication happens downstream, when reading the results JSON —
//     not here).
//
// NOTE ON total_files_size_bytes omission: ParseIcebergSeries gates an
// entire frame-pair's output (both MBPerSec AND MsgPerSec) on
// total_files_size_bytes being present on both frames (its hasB check) — it
// does not compute MsgPerSec independently when only the bytes line is
// missing. So omitting total_files_size_bytes here on a transient failure
// also costs that interval's total_records sample, not just its bytes
// sample. That coupling lives in ParseIcebergSeries, not here; decoupling
// it (so a bytes-unavailable frame still contributes a records-only point)
// is a larger change than this fix and is left for a follow-up rather than
// forced in here.
func s3SidecarSetup(args MetricSidecarArgs, artifact string) string {
	bucket := args.Outs["results_bucket"]
	region := args.Outs["aws_region"]
	brokers := args.Outs["redpanda_broker_endpoints"]
	// One sidecar instance runs per engine per sweep point, so args.Engine
	// scopes the poll to exactly that engine's prefix(es)/group(s) —
	// polling the other engine's would double-count or attribute the wrong
	// engine's writes/consumption.
	prefixes := s3Prefixes(args.Names, args.Engine)

	var recordsLoop string
	if args.Engine == "kafka_connect" {
		// The Kafka Connect worker doesn't take a configurable consumer
		// group for its data-consuming task, and the exact string the
		// framework derives for a given connector isn't something this
		// codebase controls (see s3KafkaConnectConnectorName). Guessing
		// that string and polling it directly (an earlier version of this
		// file did exactly that) fails silently: a wrong guess just
		// returns nothing from --describe, which is why every
		// kafka_connect-engine run's total_records/msg_per_sec samples had
		// at one point been reporting 0 despite real throughput happening.
		// Discover the real group(s) at poll time instead: list every
		// registered group and keep whatever contains our own connector
		// name as a substring — that substring is exactly what matrix.go
		// submitted the connector as, so it is not a guess.
		pattern := s3KafkaConnectConnectorName(args.Names, args.VCPU)
		recordsLoop = fmt.Sprintf(`      KC_GROUPS=$(/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server %q --list 2>/dev/null | grep -F %q || true)
      if [ -z "$KC_GROUPS" ]; then
        # Loud on purpose: a silently empty match here is exactly the
        # failure mode that let total_records read 0 for an entire run
        # undetected before. This goes to the sidecar's own stderr, which
        # is captured separately from $RP, so it can't corrupt the metric
        # dump this loop is writing.
        echo "###WARN no kafka_connect consumer group found matching connector %s -- total_records/msg_per_sec will read 0 for this engine for the rest of this run" >&2
      fi
      for G in $KC_GROUPS; do
        DESC=$(/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server %q --describe --group "$G" 2>/dev/null || true)
        SUM=$(echo "$DESC" | awk -v grp="$G" '$1==grp && $4 ~ /^[0-9]+$/ {s+=$4} END{print s+0}') || SUM=0
        RECS=$((RECS + ${SUM:-0}))
      done
`, brokers, pattern, pattern, brokers)
	} else {
		groups := s3ConsumerGroups(args.Names, args.Engine)
		recordsLoop = fmt.Sprintf(`      for G in %s; do
        # This runs unattended every 10s for 15 minutes, so a not-yet-joined
        # consumer, a mid-rebalance blip, or the group not existing yet must
        # never crash the poll: "|| true" survives a nonzero exit, and the
        # awk sum defaults every row to 0 unless CURRENT-OFFSET (column 4)
        # is a bare non-negative integer, so a header row, a missing offset,
        # or garbage output all just contribute 0.
        DESC=$(/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server %q --describe --group "$G" 2>/dev/null || true)
        SUM=$(echo "$DESC" | awk -v grp="$G" '$1==grp && $4 ~ /^[0-9]+$/ {s+=$4} END{print s+0}') || SUM=0
        RECS=$((RECS + ${SUM:-0}))
      done
`, strings.Join(groups, " "), brokers)
	}

	return fmt.Sprintf(`RP=/tmp/%s
: > "$RP"
# Sentinel used below to tell a genuinely failed aws s3api call (bad
# credentials, missing bucket, a real transient AWS API error) apart from a
# real (possibly empty/"None") result -- an empty prefix is now a
# legitimate zero, not a failure -- without relying on $? surviving past a
# set -e-fatal command substitution -- see the S_RAW comment in the loop.
AWSFAIL="__AWSFAIL__"
(
  while kill -0 "$PID" 2>/dev/null; do
    {
      echo "###timestamp=$(date +%%s)"
      SIZE=0
      SIZE_OK=1
      RECS=0
      for P in %s; do
        # The query is "Contents[].Size" (every object's raw size), not
        # "sum(Contents[].Size)": aws s3api applies --query PER PAGE, not
        # once over the whole paginated result, so once a prefix holds more
        # than 1000 objects --output text prints one page's sizes per line
        # -- a whitespace-separated multi-token string, not a single grand
        # total either way. Summing the raw sizes ourselves (the awk loop
        # below adds every numeric field on every line) is correct whether
        # the call returned 0, 1, or many pages' worth of numbers, and a
        # non-numeric or missing token (e.g. "None"/empty output for a
        # prefix with zero objects) contributes 0 rather than corrupting
        # the sum.
        #
        # This whole subshell inherits the caller script's "set -euo
        # pipefail". A bare assignment "VAR=$(cmd)" whose command
        # substitution exits non-zero is fatal under -e *on that same
        # line* -- a later "if [ $? -ne 0 ]" is never reached, because the
        # shell is already gone by then. An earlier version of this query
        # used "sum(Contents[].Size)", which is exactly what turned this
        # into a real, live failure: jmespath sum() over an absent
        # Contents key on an empty prefix is a type error, so the CLI
        # exited non-zero on every single sweep point's first poll (not a
        # transient blip -- e.g. Kafka Connect's cold start alone can leave
        # a prefix empty for ~10 minutes), which killed the whole poll
        # loop after exactly one frame. Querying the raw sizes instead
        # fixes that at the source -- an empty prefix's missing Contents
        # key just projects to nothing, so the CLI exits 0 -- but
        # "|| S_RAW=$AWSFAIL" stays regardless, forcing the assignment's
        # own exit status to 0 no matter what a *genuine* failure (bad
        # credentials, missing bucket, a real transient AWS API error)
        # does, so the shell survives and the sentinel value (rather than
        # $?, which the next line can no longer rely on) is what
        # distinguishes that real failure from a legitimate empty/zero
        # result below.
        S_RAW=$(aws s3api list-objects-v2 --bucket %q --prefix "$P" --region %q \
                --query "Contents[].Size" --output text 2>/dev/null) || S_RAW=$AWSFAIL
        if [ "$S_RAW" = "$AWSFAIL" ]; then
          # A failed call means this frame's SIZE can no longer be trusted
          # as the true cumulative total across every prefix -- mark it
          # unknown rather than silently treating the missing prefix as
          # zero, which downstream is indistinguishable from a genuinely
          # idle (zero-byte) interval.
          SIZE_OK=0
        else
          # "|| S=0" is the same set -e safeguard as S_RAW above, applied
          # to the summing pipeline itself (echo | awk): if awk were ever
          # unavailable or errored, the pipeline's own failure must not
          # kill the poll loop either.
          S=$(printf '%%s' "$S_RAW" | awk '%s') || S=0
          SIZE=$((SIZE + S))
        fi
      done
%s      if [ "$SIZE_OK" = "1" ]; then
        echo "total_files_size_bytes ${SIZE:-0}"
      fi
      echo "total_records ${RECS:-0}"
    } >> "$RP"
    sleep 10
  done
) &
RP_SCRAPER=$!`, artifact, strings.Join(prefixes, " "), bucket, region, s3PagedSizeSumAwk, recordsLoop)
}

// s3KCConfig renders the Aiven S3 Sink connector counterpart, mirroring
// icebergKCConfig. The kafka_connect-scoped prefix keeps its objects
// disjoint from Connect's own writes into the same shared results bucket
// (see s3Prefix).
func s3KCConfig(s *Scenario, outs map[string]string, n BenchNames) (KCRenderResult, error) {
	in := kcRenderInputs{
		Bucket:        outs["results_bucket"],
		Region:        outs["aws_region"],
		Prefix:        s3Prefix(n, "kafka_connect"),
		Topic:         n.SourceTopic(),
		ConsumerGroup: n.ConsumerGroup("kafka_connect"),
	}
	cfg, err := renderKCConfig(s, in)
	if err != nil {
		return KCRenderResult{}, fmt.Errorf("render KC s3 config: %w", err)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return KCRenderResult{}, err
	}
	return KCRenderResult{ConnectorName: fmt.Sprintf("bench_%s", s.Connector), ConfigJSON: string(raw)}, nil
}
