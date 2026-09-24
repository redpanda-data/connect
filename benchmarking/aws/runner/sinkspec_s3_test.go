// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func s3Outs() map[string]string {
	return map[string]string{
		"aws_region":                "us-east-2",
		"redpanda_broker_endpoints": "10.0.0.1:9092",
		"results_bucket":            "rpcn-bench-results",
	}
}

func TestSinkSpecFor_S3(t *testing.T) {
	sp, ok := sinkSpecFor("s3")
	if !ok {
		t.Fatal("s3 sinkSpec must be registered")
	}
	if sp.OutputComponent != "aws_s3" {
		t.Errorf("OutputComponent = %q", sp.OutputComponent)
	}
	if sp.HelperBinary != "" {
		t.Errorf("HelperBinary = %q, want empty (s3 needs no table/schema helper)", sp.HelperBinary)
	}
	if sp.KCConfig == nil {
		t.Error("s3 now has a Kafka Connect counterpart (Aiven S3 Sink); KCConfig must not be nil")
	}
}

func TestSinkTopology_Pipeline_S3(t *testing.T) {
	s := &Scenario{Connector: "s3", Direction: DirectionSink, Pipeline: map[string]any{
		"output": map[string]any{"aws_s3": map[string]any{
			"path":          "${!uuid_v4()}.ndjson.gz",
			"content_type":  "application/x-ndjson",
			"max_in_flight": 32,
		}},
	}}
	in, out, err := (sinkTopology{}).Pipeline(s, newBenchNames("sess", "s3"))
	if err != nil {
		t.Fatalf("Pipeline: %v", err)
	}
	rp, ok := in["redpanda"].(map[string]any)
	if !ok {
		t.Fatalf("input must be redpanda; got %#v", in)
	}
	topics, _ := rp["topics"].([]any)
	if len(topics) != 1 || topics[0] != "bench_sess_s3_src" {
		t.Errorf("input topics = %#v", rp["topics"])
	}
	cfg, ok := out["aws_s3"].(map[string]any)
	if !ok {
		t.Fatalf("output must be aws_s3; got %#v", out)
	}
	if cfg["bucket"] != "${RESULTS_BUCKET}" {
		t.Errorf("output bucket = %v, want ${RESULTS_BUCKET}", cfg["bucket"])
	}
	if cfg["region"] != "${AWS_REGION}" {
		t.Errorf("output region = %v, want ${AWS_REGION}", cfg["region"])
	}
	// The Connect side's DecorateOutput always writes under the
	// engine-scoped "connect" prefix so its objects never co-mingle with
	// the Kafka Connect counterpart's writes into the same shared bucket.
	if want := "raw/bench_sess_s3_src/connect/${!uuid_v4()}.ndjson.gz"; cfg["path"] != want {
		t.Errorf("output path = %v, want %v (run-scoped, engine-scoped prefix + scenario template)", cfg["path"], want)
	}
	// Scenario-owned tuning must survive decoration.
	if cfg["content_type"] != "application/x-ndjson" {
		t.Errorf("content_type = %v; DecorateOutput must not clobber scenario tuning", cfg["content_type"])
	}
	if cfg["max_in_flight"] != 32 {
		t.Errorf("max_in_flight = %v; DecorateOutput must not clobber scenario tuning", cfg["max_in_flight"])
	}
}

func TestSinkTopology_Pipeline_S3_DefaultPathTemplate(t *testing.T) {
	// A scenario that omits pipeline.output.aws_s3.path entirely still gets a
	// safe default filename template under the run-scoped, engine-scoped
	// prefix.
	s := &Scenario{Connector: "s3", Direction: DirectionSink, Pipeline: map[string]any{
		"output": map[string]any{"aws_s3": map[string]any{}},
	}}
	_, out, err := (sinkTopology{}).Pipeline(s, newBenchNames("sess", "s3"))
	if err != nil {
		t.Fatalf("Pipeline: %v", err)
	}
	cfg := out["aws_s3"].(map[string]any)
	if want := "raw/bench_sess_s3_src/connect/" + s3DefaultPathTemplate; cfg["path"] != want {
		t.Errorf("output path = %v, want %v", cfg["path"], want)
	}
}

func TestSinkTopology_Pipeline_S3_MultiTopicPrefixesPerTopic(t *testing.T) {
	s := &Scenario{Connector: "s3", Direction: DirectionSink, Dataset: DatasetSpec{Topics: 3}, Pipeline: map[string]any{
		"output": map[string]any{"aws_s3": map[string]any{"path": "${!uuid_v4()}.ndjson.gz"}},
	}}
	names := newBenchNames("sess", "s3").WithTopics(3)
	for i := 0; i < 3; i++ {
		// A fresh Pipeline call per topic, mirroring how renderPointConfigs
		// drives a multi-topic scenario in streams mode (one stream per
		// topic; see main.go's renderPointConfigs loop).
		fresh := map[string]any{"output": map[string]any{"aws_s3": map[string]any{"path": "${!uuid_v4()}.ndjson.gz"}}}
		s.Pipeline = fresh
		_, out, err := (sinkTopology{}).Pipeline(s, names.WithTopic(i))
		if err != nil {
			t.Fatalf("Pipeline topic %d: %v", i, err)
		}
		cfg := out["aws_s3"].(map[string]any)
		want := fmt.Sprintf("raw/bench_sess_s3_src_t%d/connect/${!uuid_v4()}.ndjson.gz", i)
		if cfg["path"] != want {
			t.Errorf("topic %d path = %v, want %v", i, cfg["path"], want)
		}
	}
}

func TestS3Prefix_EngineScoped(t *testing.T) {
	n := newBenchNames("sess", "s3")
	if got, want := s3Prefix(n, "connect"), "raw/bench_sess_s3_src/connect/"; got != want {
		t.Errorf("s3Prefix(connect) = %q, want %q", got, want)
	}
	if got, want := s3Prefix(n, "kafka_connect"), "raw/bench_sess_s3_src/kafka_connect/"; got != want {
		t.Errorf("s3Prefix(kafka_connect) = %q, want %q", got, want)
	}
}

func TestS3Prefixes_MultiTopic_EngineScoped(t *testing.T) {
	n := newBenchNames("sess", "s3").WithTopics(2)
	for _, eng := range []string{"connect", "kafka_connect"} {
		got := s3Prefixes(n, eng)
		want := []string{
			fmt.Sprintf("raw/bench_sess_s3_src_t0/%s/", eng),
			fmt.Sprintf("raw/bench_sess_s3_src_t1/%s/", eng),
		}
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Errorf("s3Prefixes(%s) = %v, want %v", eng, got, want)
		}
	}
}

func TestS3ResetScript(t *testing.T) {
	s := &Scenario{Connector: "s3", Direction: DirectionSink}
	got, err := (sinkTopology{}).ResetScript(s, s3Outs(), newBenchNames("sess", "s3"))
	if err != nil {
		t.Fatalf("ResetScript: %v", err)
	}
	// Dual-engine shape (matches icebergResetScript): both engines' prefixes
	// get wiped and both engines' consumer groups get rewound, since both
	// can drain the same pre-seeded topic into the same shared bucket.
	if !strings.Contains(got, `aws s3 rm "s3://rpcn-bench-results/raw/bench_sess_s3_src/connect/" --recursive --quiet --region "us-east-2" || true`) {
		t.Errorf("reset must wipe the connect-scoped prefix with a tolerant, quiet rm:\n%s", got)
	}
	if !strings.Contains(got, `aws s3 rm "s3://rpcn-bench-results/raw/bench_sess_s3_src/kafka_connect/" --recursive --quiet --region "us-east-2" || true`) {
		t.Errorf("reset must wipe the kafka_connect-scoped prefix with a tolerant, quiet rm:\n%s", got)
	}
	if !strings.Contains(got, `--group "bench_sess_s3_connect"`) {
		t.Errorf("must rewind the connect consumer group:\n%s", got)
	}
	if !strings.Contains(got, `--group "bench_sess_s3_kafka_connect"`) {
		t.Errorf("must rewind the kafka_connect consumer group:\n%s", got)
	}
	if !strings.Contains(got, `curl -fsS -X DELETE "http://localhost:8083/connectors/bench_s3" || true`) {
		t.Errorf("must tear down any lingering KC connector from a previous point:\n%s", got)
	}
}

// TestS3ResetScript_ToEarliestWithoutWorkload locks in the byte-identical
// default: no Workload, or a Workload that only carries duration/warmup with
// no write_rate_per_sec (orders-sink.yaml's shape), must keep resetting to
// earliest so every point replays the same static pre-seeded backlog.
func TestS3ResetScript_ToEarliestWithoutWorkload(t *testing.T) {
	for name, s := range map[string]*Scenario{
		"nil workload":       {Connector: "s3", Direction: DirectionSink},
		"zero-rate workload": {Connector: "s3", Direction: DirectionSink, Workload: &WorkloadSpec{Duration: 15 * time.Minute, Warmup: 10 * time.Minute}},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := (sinkTopology{}).ResetScript(s, s3Outs(), newBenchNames("sess", "s3"))
			if err != nil {
				t.Fatalf("ResetScript: %v", err)
			}
			if !strings.Contains(got, "--reset-offsets --to-earliest") {
				t.Errorf("must reset to earliest without a live workload:\n%s", got)
			}
			if strings.Contains(got, "--to-latest") {
				t.Errorf("must not reset to latest without a live workload:\n%s", got)
			}
			if strings.Contains(got, "kafka-topics.sh") {
				t.Errorf("must not delete/recreate the topic without a live workload; offset reset alone is sufficient:\n%s", got)
			}
		})
	}
}

// TestS3ResetScript_TopicRecreateUnderLiveWorkload covers the live-stream
// case after the real orders-live.yaml run exposed why a plain offset reset
// isn't enough: --to-latest only rewinds where a consumer group starts
// reading, it never removes anything from the topic, so kafka_connect's
// slower-than-produced drain left an ever-growing unconsumed tail on disk
// that throttled the producer itself via broker backpressure on every point
// after the first. The fix deletes and recreates the topic itself instead of
// resetting offsets, so this branch must contain the delete/recreate
// commands (with the partition-count fallback) and must NOT contain any
// --reset-offsets invocation at all.
func TestS3ResetScript_TopicRecreateUnderLiveWorkload(t *testing.T) {
	s := &Scenario{
		Connector: "s3", Direction: DirectionSink,
		Workload: &WorkloadSpec{WriteRatePerSec: 300000, Duration: 15 * time.Minute, Warmup: 10 * time.Minute},
	}
	got, err := (sinkTopology{}).ResetScript(s, s3Outs(), newBenchNames("sess", "s3"))
	if err != nil {
		t.Fatalf("ResetScript: %v", err)
	}
	if !strings.Contains(got, `kafka-topics.sh --bootstrap-server "10.0.0.1:9092" --delete --topic "bench_sess_s3_src" 2>/dev/null || true`) {
		t.Errorf("must delete the source topic under a live workload:\n%s", got)
	}
	if !strings.Contains(got, `kafka-topics.sh --bootstrap-server "10.0.0.1:9092" --create --topic "bench_sess_s3_src" --partitions "$PARTS" --replication-factor 3 --config max.message.bytes=67108864 2>/dev/null || true`) {
		t.Errorf("must recreate the source topic with the captured partition count under a live workload:\n%s", got)
	}
	if !strings.Contains(got, "[ -z \"$PARTS\" ] && PARTS=16") {
		t.Errorf("must fall back to 16 partitions if --describe can't determine the current count:\n%s", got)
	}
	if strings.Contains(got, "--reset-offsets") {
		t.Errorf("must not reset consumer-group offsets under a live workload; the topic delete+recreate makes that redundant:\n%s", got)
	}
	if strings.Contains(got, "--to-latest") {
		t.Errorf("must not reset to latest under a live workload (superseded by topic delete+recreate):\n%s", got)
	}
}

func TestS3ResetScript_MultiTopic(t *testing.T) {
	s := &Scenario{Connector: "s3", Direction: DirectionSink, Dataset: DatasetSpec{Topics: 3}}
	got, err := (sinkTopology{}).ResetScript(s, s3Outs(), newBenchNames("sess", "s3"))
	if err != nil {
		t.Fatalf("ResetScript: %v", err)
	}
	for _, eng := range []string{"connect", "kafka_connect"} {
		for i := 0; i < 3; i++ {
			prefix := fmt.Sprintf("raw/bench_sess_s3_src_t%d/%s/", i, eng)
			if !strings.Contains(got, prefix) {
				t.Errorf("missing engine %s topic %d prefix wipe %q:\n%s", eng, i, prefix, got)
			}
			group := fmt.Sprintf(`--group "bench_sess_s3_%s_t%d"`, eng, i)
			if !strings.Contains(got, group) {
				t.Errorf("missing engine %s topic %d consumer-group reset %q:\n%s", eng, i, group, got)
			}
		}
	}
	if n := strings.Count(got, "aws s3 rm"); n != 6 {
		t.Errorf("expected 6 prefix wipes (2 engines x 3 topics), got %d:\n%s", n, got)
	}
}

func TestS3MetricSidecar(t *testing.T) {
	sc := (sinkTopology{}).MetricSidecar(MetricSidecarArgs{
		Engine:    "connect",
		VCPU:      4,
		Bucket:    "rpcn-bench-results",
		SessionID: "sess",
		Outs:      s3Outs(),
		Names:     newBenchNames("sess", "s3"),
	})
	if !strings.Contains(sc.Setup, "RP=/tmp/s3-4-connect.txt") {
		t.Errorf("sidecar must write the s3-prefixed artifact:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "aws s3api list-objects-v2") {
		t.Errorf("sidecar must poll via list-objects-v2:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "raw/bench_sess_s3_src/connect/") {
		t.Errorf("sidecar must poll the run-scoped, connect-scoped prefix:\n%s", sc.Setup)
	}
	if strings.Contains(sc.Setup, "raw/bench_sess_s3_src/kafka_connect/") {
		t.Errorf("connect-engine sidecar must not poll the kafka_connect prefix:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "total_files_size_bytes") || !strings.Contains(sc.Setup, "total_records") {
		t.Errorf("sidecar must emit both totals:\n%s", sc.Setup)
	}
	if strings.Contains(sc.Setup, "written_files_size_bytes") || strings.Contains(sc.Setup, "written_records") {
		t.Errorf("s3 objects are pure-append; sidecar must not emit written_* lines:\n%s", sc.Setup)
	}
	// total_records now comes from the engine's real consumer-group offset
	// progress (compression-independent), not from S3 object count, so
	// list-objects-v2's --query must no longer ask for length(Contents).
	if strings.Contains(sc.Setup, "length(Contents)") {
		t.Errorf("total_records must no longer be derived from S3 object count:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "for G in bench_sess_s3_connect;") {
		t.Errorf("sidecar must iterate the connect-engine consumer group:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, `/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "10.0.0.1:9092" --describe --group "$G"`) {
		t.Errorf("sidecar must describe each iterated consumer group to derive total_records:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "RP_SCRAPER=$!") {
		t.Errorf("sidecar setup must end by exporting RP_SCRAPER:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Upload, "s3://rpcn-bench-results/runs/sess/s3-4-connect.txt") {
		t.Errorf("upload key mismatch:\n%s", sc.Upload)
	}
}

func TestS3MetricSidecar_KafkaConnectEngine(t *testing.T) {
	sc := (sinkTopology{}).MetricSidecar(MetricSidecarArgs{
		Engine:    "kafka_connect",
		VCPU:      4,
		Key:       "4",
		Bucket:    "rpcn-bench-results",
		SessionID: "sess",
		Outs:      s3Outs(),
		Names:     newBenchNames("sess", "s3"),
	})
	if !strings.Contains(sc.Setup, "raw/bench_sess_s3_src/kafka_connect/") {
		t.Errorf("kafka_connect-engine sidecar must poll the kafka_connect-scoped prefix:\n%s", sc.Setup)
	}
	if strings.Contains(sc.Setup, "raw/bench_sess_s3_src/connect/") {
		t.Errorf("kafka_connect-engine sidecar must not poll the connect prefix:\n%s", sc.Setup)
	}
	// Regression: Kafka Connect sink connectors don't use a configurable
	// consumer group for their data-consuming task — the framework derives
	// one internally, and the exact derivation isn't something this
	// codebase controls or can predict with certainty (an earlier version
	// of this file hardcoded a guess, "connect-<connector-name>", confirmed
	// once against a single live worker log but never re-verified). Rather
	// than polling a guessed name directly (which silently returns nothing
	// on a wrong guess), the sidecar must discover the real group at poll
	// time by listing every registered group and keeping whatever matches
	// our own connector name as a substring.
	if !strings.Contains(sc.Setup, `kafka-consumer-groups.sh --bootstrap-server "10.0.0.1:9092" --list`) {
		t.Errorf("kafka_connect-engine sidecar must discover groups via --list rather than guessing a name:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, `grep -F "bench_s3_v4"`) {
		t.Errorf("kafka_connect-engine sidecar must filter --list output on the exact connector name matrix.go submits (bench_s3_v4):\n%s", sc.Setup)
	}
	if strings.Contains(sc.Setup, "connect-bench_s3_v4") {
		t.Errorf("kafka_connect-engine sidecar must not hardcode a guessed group name:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, `for G in $KC_GROUPS; do`) {
		t.Errorf("kafka_connect-engine sidecar must describe every discovered group:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "no kafka_connect consumer group found matching connector") {
		t.Errorf("kafka_connect-engine sidecar must emit a visible diagnostic when no group is discovered:\n%s", sc.Setup)
	}
	if strings.Contains(sc.Setup, "bench_sess_s3_kafka_connect") {
		t.Errorf("kafka_connect-engine sidecar must not poll the nonexistent n.ConsumerGroup(\"kafka_connect\") name:\n%s", sc.Setup)
	}
	if strings.Contains(sc.Setup, "bench_sess_s3_connect;") {
		t.Errorf("kafka_connect-engine sidecar must not iterate the connect-scoped consumer group:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "RP=/tmp/s3-4-kc.txt") {
		t.Errorf("sidecar must write the kc-suffixed artifact:\n%s", sc.Setup)
	}
}

// TestS3MetricSidecar_ConsumerGroupOffsetDrivesRecords locks in the
// data-quality fix: total_records must come from the engine's real
// consumer-group CURRENT-OFFSET progress (exact, compression-independent
// consumed-message count), not from S3 object count, so the reported
// throughput is comparable across runs/engines and against a customer's
// real topic produce-throughput numbers.
func TestS3MetricSidecar_ConsumerGroupOffsetDrivesRecords(t *testing.T) {
	sc := (sinkTopology{}).MetricSidecar(MetricSidecarArgs{
		Engine:    "connect",
		VCPU:      4,
		Bucket:    "rpcn-bench-results",
		SessionID: "sess",
		Outs:      s3Outs(),
		Names:     newBenchNames("sess", "s3"),
	})
	if !strings.Contains(sc.Setup, "for G in bench_sess_s3_connect;") {
		t.Errorf("sidecar must iterate the engine-scoped consumer group:\n%s", sc.Setup)
	}
	want := `/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "10.0.0.1:9092" --describe --group "$G"`
	if !strings.Contains(sc.Setup, want) {
		t.Errorf("sidecar must describe each iterated consumer group %q:\n%s", want, sc.Setup)
	}
	// Column 4 (CURRENT-OFFSET) is what gets summed into RECS; the awk must
	// defend against a header row, a missing/non-numeric offset, or a
	// mid-rebalance blip rather than let a bad poll crash the subshell.
	if !strings.Contains(sc.Setup, `$1==grp && $4 ~ /^[0-9]+$/ {s+=$4}`) {
		t.Errorf("sidecar must defensively sum CURRENT-OFFSET (column 4) per group:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "2>/dev/null || true") {
		t.Errorf("describe call must tolerate a not-yet-joined group without killing the poll:\n%s", sc.Setup)
	}
}

// TestS3MetricSidecar_MultiTopicDescribesEveryConsumerGroup mirrors
// TestS3MetricSidecar_MultiTopicPollsEveryPrefix for the consumer-group
// side: each topic in a multi-topic scenario gets its own per-topic
// consumer group, and the sidecar must describe every one of them (same
// summed-across-topics shape as s3Prefixes).
func TestS3MetricSidecar_MultiTopicDescribesEveryConsumerGroup(t *testing.T) {
	sc := (sinkTopology{}).MetricSidecar(MetricSidecarArgs{
		Engine: "connect", VCPU: 2, Key: "2",
		Bucket: "b", SessionID: "sess-x",
		Outs:  s3Outs(),
		Names: newBenchNames("sess-x", "s3").WithTopics(3),
	})
	if !strings.Contains(sc.Setup, "--describe --group \"$G\"") {
		t.Errorf("sidecar must describe each group iterated in the for-loop:\n%s", sc.Setup)
	}
	for i := 0; i < 3; i++ {
		// Each topic's group name must appear in the `for G in ...` word
		// list the describe loop iterates over.
		group := fmt.Sprintf("bench_sess-x_s3_connect_t%d", i)
		if !strings.Contains(sc.Setup, group) {
			t.Errorf("sidecar missing describe loop entry for topic %d group %q:\n%s", i, group, sc.Setup)
		}
	}
	if got := strings.Count(sc.Setup, `--describe --group "$G"`); got != 1 {
		t.Errorf("expected exactly one describe call inside the loop, got %d:\n%s", got, sc.Setup)
	}
}

func TestS3MetricSidecar_MultiTopicPollsEveryPrefix(t *testing.T) {
	sc := (sinkTopology{}).MetricSidecar(MetricSidecarArgs{
		Engine: "connect", VCPU: 2, Key: "2",
		Bucket: "b", SessionID: "sess-x",
		Outs:  s3Outs(),
		Names: newBenchNames("sess-x", "s3").WithTopics(3),
	})
	for i := 0; i < 3; i++ {
		// SourceTopic keeps the session id's dashes (unlike IcebergTable,
		// which sanitizes them); "sess-x" stays "sess-x" here.
		prefix := fmt.Sprintf("raw/bench_sess-x_s3_src_t%d/connect/", i)
		if !strings.Contains(sc.Setup, prefix) {
			t.Errorf("sidecar missing topic %d prefix %q:\n%s", i, prefix, sc.Setup)
		}
	}
	if got := strings.Count(sc.Setup, `echo "total_files_size_bytes`); got != 1 {
		t.Errorf("expected exactly one summed size emission per frame, got %d:\n%s", got, sc.Setup)
	}
}

// TestS3MetricSidecar_SizePagination locks in the fix for the
// total_files_size_bytes-goes-silently-to-0-past-1000-objects bug:
// `aws s3api list-objects-v2 --query "Contents[].Size"` applies --query PER
// PAGE, so a prefix with more than 1000 objects prints one page's sizes per
// line, not a single grand total. The generated script must sum those
// tokens (s3PagedSizeSumAwk) rather than reject them via the old
// `case ... *[!0-9]*) S=0` guard, which silently zeroed SIZE for the rest of
// the run the moment any prefix crossed 1000 objects.
func TestS3MetricSidecar_SizePagination(t *testing.T) {
	for _, engine := range []string{"connect", "kafka_connect"} {
		t.Run(engine, func(t *testing.T) {
			sc := (sinkTopology{}).MetricSidecar(MetricSidecarArgs{
				Engine:    engine,
				VCPU:      4,
				Bucket:    "rpcn-bench-results",
				SessionID: "sess",
				Outs:      s3Outs(),
				Names:     newBenchNames("sess", "s3"),
			})
			if !strings.Contains(sc.Setup, "S_RAW=$(aws s3api list-objects-v2") {
				t.Errorf("sidecar must capture the raw (possibly multi-page) list-objects-v2 output:\n%s", sc.Setup)
			}
			if !strings.Contains(sc.Setup, "awk '"+s3PagedSizeSumAwk+"'") {
				t.Errorf("sidecar must sum every page's value via s3PagedSizeSumAwk:\n%s", sc.Setup)
			}
			// The old guard's executable form, not just its name in an
			// explanatory comment above the new code.
			if strings.Contains(sc.Setup, `case "$S" in ''|None|*[!0-9]*) S=0 ;; esac`) {
				t.Errorf("sidecar must not reject multi-token output via the old case guard:\n%s", sc.Setup)
			}
		})
	}
}

// TestS3PagedSizeSumAwk exercises the exact awk program s3SidecarSetup
// embeds (s3PagedSizeSumAwk) against synthetic aws-cli output, so a future
// edit to that program can't silently break pagination summing without a
// unit test catching it.
func TestS3PagedSizeSumAwk(t *testing.T) {
	if _, err := exec.LookPath("awk"); err != nil {
		t.Skip("awk not available on PATH")
	}
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty prefix, no aws-cli output at all", input: "", want: "0"},
		{name: "empty prefix, aws-cli reports None", input: "None", want: "0"},
		{name: "single page under the 1000-object threshold", input: "12345", want: "12345"},
		{
			name:  "multi-page output: one sum(Contents[].Size) token per page",
			input: "12345 6789\n111",
			want:  "19245",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command("awk", s3PagedSizeSumAwk)
			cmd.Stdin = strings.NewReader(tt.input)
			out, err := cmd.Output()
			if err != nil {
				t.Fatalf("awk failed: %v", err)
			}
			if got := strings.TrimSpace(string(out)); got != tt.want {
				t.Errorf("awk %q = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestSinkTopology_KCConfig_S3(t *testing.T) {
	s := &Scenario{Connector: "s3", Direction: DirectionSink}
	res, ok, err := (sinkTopology{}).KCConfig(s, s3Outs(), newBenchNames("sess", "s3"))
	if err != nil {
		t.Fatalf("KCConfig: %v", err)
	}
	if !ok {
		t.Fatal("s3 now has a Kafka Connect counterpart (Aiven S3 Sink); KCConfig must report ok=true")
	}
	if res.ConnectorName != "bench_s3" {
		t.Errorf("ConnectorName = %q, want bench_s3", res.ConnectorName)
	}
	var cfg map[string]any
	if err := json.Unmarshal([]byte(res.ConfigJSON), &cfg); err != nil {
		t.Fatalf("ConfigJSON did not unmarshal: %v\n%s", err, res.ConfigJSON)
	}
	if cfg["connector.class"] != "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector" {
		t.Errorf("connector.class = %v", cfg["connector.class"])
	}
	if cfg["topics"] != "bench_sess_s3_src" {
		t.Errorf("topics = %v", cfg["topics"])
	}
	if cfg["aws.s3.bucket.name"] != "rpcn-bench-results" {
		t.Errorf("aws.s3.bucket.name = %v", cfg["aws.s3.bucket.name"])
	}
	if cfg["aws.s3.region"] != "us-east-2" {
		t.Errorf("aws.s3.region = %v", cfg["aws.s3.region"])
	}
	// file.name.prefix is not a real, currently-valid connector property
	// (Kafka Connect silently ignores it), so the only real way to scope the
	// object key is file.name.template with the Aiven placeholders appended
	// verbatim after our run-scoped prefix.
	if want := "raw/bench_sess_s3_src/kafka_connect/{{topic}}-{{partition}}-{{start_offset}}"; cfg["file.name.template"] != want {
		t.Errorf("file.name.template = %v, want %v", cfg["file.name.template"], want)
	}
	if _, present := cfg["file.name.prefix"]; present {
		t.Error("file.name.prefix must not be set: it is not a real connector property (silently ignored by Kafka Connect)")
	}
	if cfg["aws.access.key.id"] != nil || cfg["aws.secret.access.key"] != nil {
		t.Error("must not set explicit credentials; relies on the IAM instance profile via the default credential chain")
	}
}

func TestSinkTopology_MetricArtifact_S3(t *testing.T) {
	if got := (sinkTopology{}).MetricArtifact("s3", "connect", "2"); got != "s3-2-connect.txt" {
		t.Errorf("MetricArtifact = %q, want s3-2-connect.txt", got)
	}
	if got := (sinkTopology{}).MetricArtifact("s3", "kafka_connect", "2"); got != "s3-2-kc.txt" {
		t.Errorf("MetricArtifact = %q, want s3-2-kc.txt", got)
	}
}

func TestValidateEngines_S3HasKafkaConnectCounterpart(t *testing.T) {
	s := &Scenario{Connector: "s3", Direction: DirectionSink}
	if err := validateEngines(s, []string{"connect", "kafka_connect"}); err != nil {
		t.Errorf("default engine list must now pass for s3 (KCConfig is wired): %v", err)
	}
	if err := validateEngines(s, []string{"connect"}); err != nil {
		t.Errorf("engines=connect must pass: %v", err)
	}
	if err := validateEngines(s, []string{"kafka_connect"}); err != nil {
		t.Errorf("engines=kafka_connect must pass: %v", err)
	}
}

func TestSinkHelperBinaries_OmitsS3(t *testing.T) {
	for _, name := range sinkHelperBinaries() {
		if name == "" {
			t.Error("sinkHelperBinaries must never include an empty name")
		}
	}
	// s3 has no HelperBinary; it must never appear in the staged-binary list.
	for _, name := range sinkHelperBinaries() {
		if name == "s3" {
			t.Error("s3 has no HelperBinary and must not appear in sinkHelperBinaries()")
		}
	}
}

// End-to-end render over the real scenario files: LoadScenario validates
// them, renderPointConfigs proves every ${RESULTS_BUCKET}/${AWS_REGION}
// placeholder resolves against the s3 stack's outputs and the run-scoped,
// engine-scoped path prefix survives to the final YAML.
func TestRenderPointConfigs_S3Scenarios(t *testing.T) {
	for _, path := range []string{
		"../scenarios/s3/orders-sink-smoke.yaml",
		"../scenarios/s3/orders-sink.yaml",
	} {
		s, err := LoadScenario(path)
		if err != nil {
			t.Fatalf("LoadScenario(%s): %v", path, err)
		}
		topo, err := topologyFor(s.Direction)
		if err != nil {
			t.Fatalf("topologyFor: %v", err)
		}
		names := newBenchNames("sess-x", s.Connector)
		got, err := renderPointConfigs(s, s3Outs(), topo, names, buildSweepPlan(s)[0])
		if err != nil {
			t.Fatalf("renderPointConfigs(%s): %v", path, err)
		}
		cfg := readYAML(t, got.Single)
		s3Cfg := cfg["output"].(map[string]any)["aws_s3"].(map[string]any)
		if s3Cfg["bucket"] != "rpcn-bench-results" {
			t.Errorf("%s: bucket = %v; ${RESULTS_BUCKET} must resolve from TF outputs", path, s3Cfg["bucket"])
		}
		if s3Cfg["region"] != "us-east-2" {
			t.Errorf("%s: region = %v; ${AWS_REGION} must resolve from TF outputs", path, s3Cfg["region"])
		}
		// SourceTopic keeps the session id's dashes (unlike IcebergTable).
		if want := "raw/bench_sess-x_s3_src/connect/${!uuid_v4()}.ndjson.gz"; s3Cfg["path"] != want {
			t.Errorf("%s: path = %v, want %v", path, s3Cfg["path"], want)
		}
	}
}
