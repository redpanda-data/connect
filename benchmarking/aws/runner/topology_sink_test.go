// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"strings"
	"testing"
)

func sinkOuts() map[string]string {
	return map[string]string{
		"glue_rest_uri":             "https://glue.us-east-2.amazonaws.com/iceberg",
		"warehouse_account_id":      "123456789012",
		"warehouse_s3_uri":          "s3://rpcn-bench-ice/wh",
		"s3_bucket":                 "rpcn-bench-ice",
		"aws_region":                "us-east-2",
		"redpanda_broker_endpoints": "10.0.0.1:9092",
		"results_bucket":            "rpcn-bench-results",
	}
}

func TestSinkTopology_Validate(t *testing.T) {
	if err := (sinkTopology{}).Validate(&Scenario{Connector: "iceberg", Direction: DirectionSink}); err != nil {
		t.Fatalf("iceberg must validate: %v", err)
	}
	if err := (sinkTopology{}).Validate(&Scenario{Connector: "nope", Direction: DirectionSink}); err == nil {
		t.Fatal("unknown sink connector must fail validation")
	}
}

func TestSinkTopology_Pipeline_RedpandaInIcebergOut(t *testing.T) {
	s := &Scenario{Connector: "iceberg", Direction: DirectionSink, Pipeline: map[string]any{
		"output": map[string]any{"iceberg": map[string]any{"batching": map[string]any{"count": 5000}}},
	}}
	in, out, err := (sinkTopology{}).Pipeline(s, newBenchNames("sess", "iceberg"))
	if err != nil {
		t.Fatalf("Pipeline: %v", err)
	}
	rp, ok := in["redpanda"].(map[string]any)
	if !ok {
		t.Fatalf("input must be redpanda; got %#v", in)
	}
	topics, _ := rp["topics"].([]any)
	if len(topics) != 1 || topics[0] != "bench_sess_iceberg_src" {
		t.Errorf("input topics = %#v", rp["topics"])
	}
	if _, ok := out["iceberg"]; !ok {
		t.Errorf("output must be iceberg; got %#v", out)
	}
	icfg, _ := out["iceberg"].(map[string]any)
	if icfg["table"] != "bench_sess_iceberg_connect" {
		t.Errorf("output table = %v, want bench_sess_iceberg_connect (must match IcebergTable/ResetScript/MetricSidecar)", icfg["table"])
	}
	if rp["consumer_group"] != "bench_sess_iceberg_connect" {
		t.Errorf("consumer_group = %v, want bench_sess_iceberg_connect", rp["consumer_group"])
	}
}

func TestSinkTopology_Pipeline_MergesInputOptions(t *testing.T) {
	// pipeline.input_options is merged into the redpanda input (e.g. input-side
	// batching via unordered_processing), but must NOT clobber the bench-managed
	// connection fields (topics / consumer_group / seed_brokers).
	s := &Scenario{Connector: "iceberg", Direction: DirectionSink, Pipeline: map[string]any{
		"input_options": map[string]any{
			"unordered_processing": map[string]any{
				"enabled":          true,
				"checkpoint_limit": 100000,
				"batching":         map[string]any{"count": 50000, "period": "10s"},
			},
			// Attempt to clobber a managed field — must be ignored.
			"topics": []any{"hacker_topic"},
		},
		"output": map[string]any{"iceberg": map[string]any{"batching": map[string]any{"count": 50000}}},
	}}
	in, _, err := (sinkTopology{}).Pipeline(s, newBenchNames("sess", "iceberg"))
	if err != nil {
		t.Fatalf("Pipeline: %v", err)
	}
	rp, _ := in["redpanda"].(map[string]any)
	if _, ok := rp["unordered_processing"]; !ok {
		t.Errorf("expected unordered_processing merged into input; got %#v", rp)
	}
	// Managed field must survive the merge attempt.
	topics, _ := rp["topics"].([]any)
	if len(topics) != 1 || topics[0] != "bench_sess_iceberg_src" {
		t.Errorf("input_options must not clobber managed topics; got %#v", rp["topics"])
	}
}

func TestSinkTopology_SeedScript_SingleTopicUnchanged(t *testing.T) {
	s := &Scenario{
		Connector: "iceberg", Direction: DirectionSink,
		Dataset: DatasetSpec{Seeder: "json-orders", InitialRows: 1000, RowSizeBytes: 1200},
	}
	outs := map[string]string{"results_bucket": "bucket", "redpanda_broker_endpoints": "10.0.0.1:9092"}
	got, err := (sinkTopology{}).SeedScript(s, outs, newBenchNames("sess", "iceberg"))
	if err != nil {
		t.Fatalf("SeedScript: %v", err)
	}
	want := `
set -euo pipefail
aws s3 cp s3://bucket/stage/json-orders /opt/bench/json-orders
chmod +x /opt/bench/json-orders
REDPANDA_BROKERS="10.0.0.1:9092" /opt/bench/json-orders seed \
  --topic=bench_sess_iceberg_src --rows=1000 --row-size=1200
`
	if got != want {
		t.Errorf("single-topic SeedScript changed:\n got: %q\nwant: %q", got, want)
	}
	if strings.Count(got, "seed \\") != 1 {
		t.Errorf("single-topic must emit exactly one seed invocation, got:\n%s", got)
	}
	if strings.Contains(got, "--partitions") {
		t.Errorf("single-topic must not pass --partitions (seeder's own default applies); got:\n%s", got)
	}
}

func TestSinkTopology_SeedScript_SevenTopicsSevenInvocations(t *testing.T) {
	// 119,000,000 rows / 7 topics = 17,000,000 rows per topic.
	s := &Scenario{
		Connector: "iceberg", Direction: DirectionSink,
		Dataset: DatasetSpec{Seeder: "json-orders", InitialRows: 119000000, RowSizeBytes: 1200, Topics: 7},
	}
	outs := map[string]string{"results_bucket": "bucket", "redpanda_broker_endpoints": "10.0.0.1:9092"}
	got, err := (sinkTopology{}).SeedScript(s, outs, newBenchNames("sess", "iceberg"))
	if err != nil {
		t.Fatalf("SeedScript: %v", err)
	}
	if n := strings.Count(got, "seed \\"); n != 7 {
		t.Fatalf("expected 7 seed invocations, got %d:\n%s", n, got)
	}
	if n := strings.Count(got, "--rows=17000000"); n != 7 {
		t.Errorf("expected 7 invocations with --rows=17000000, got %d:\n%s", n, got)
	}
	if n := strings.Count(got, "--partitions=4"); n != 7 {
		t.Errorf("expected 7 invocations with the default --partitions=4, got %d:\n%s", n, got)
	}
	for i := 0; i < 7; i++ {
		want := fmt.Sprintf("--topic=bench_sess_iceberg_src_t%d", i)
		if !strings.Contains(got, want) {
			t.Errorf("missing topic %d invocation %q:\n%s", i, want, got)
		}
	}
}

func TestSinkTopology_SeedScript_CustomPartitionsPerTopic(t *testing.T) {
	s := &Scenario{
		Connector: "iceberg", Direction: DirectionSink,
		Dataset: DatasetSpec{Seeder: "json-orders", InitialRows: 700, RowSizeBytes: 1200, Topics: 7, PartitionsPerTopic: 8},
	}
	outs := map[string]string{"results_bucket": "bucket", "redpanda_broker_endpoints": "10.0.0.1:9092"}
	got, err := (sinkTopology{}).SeedScript(s, outs, newBenchNames("sess", "iceberg"))
	if err != nil {
		t.Fatalf("SeedScript: %v", err)
	}
	if n := strings.Count(got, "--partitions=8"); n != 7 {
		t.Errorf("expected 7 invocations with --partitions=8, got %d:\n%s", n, got)
	}
}

func TestSinkTopology_MetricArtifact(t *testing.T) {
	if got := (sinkTopology{}).MetricArtifact("iceberg", "connect", "4"); got != "iceberg-4-connect.txt" {
		t.Errorf("artifact = %q", got)
	}
	if got := (sinkTopology{}).MetricArtifact("iceberg", "kafka_connect", "2"); got != "iceberg-2-kc.txt" {
		t.Errorf("kc artifact = %q", got)
	}
}

func TestSinkTopology_MetricSidecar_GluePoll(t *testing.T) {
	sc := (sinkTopology{}).MetricSidecar(MetricSidecarArgs{
		Engine: "connect", VCPU: 1, Bucket: "rpcn-bench-results", SessionID: "sess",
		Outs: sinkOuts(), Names: newBenchNames("sess", "iceberg"),
	})
	if !strings.Contains(sc.Setup, "aws glue get-table") {
		t.Errorf("sink sidecar must poll Glue; got:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "total_files_size_bytes") {
		t.Errorf("sink sidecar must emit total_files_size_bytes; got:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "iceberg-1-connect.txt") {
		t.Errorf("sink sidecar must write the iceberg artifact; got:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Upload, "iceberg-1-connect.txt") {
		t.Errorf("upload must reference the artifact; got:\n%s", sc.Upload)
	}
}

func TestSinkTopology_MetricSidecar_EmitsPerTableLine(t *testing.T) {
	// Finding #2: the sidecar must emit a per-table
	// "table_files_size_bytes <table> <bytes>" line inside the for-loop, so
	// the plan's own acceptance check (did BOTH of arm B's tables grow) has
	// live evidence before the Glue database and warehouse bucket are torn
	// down. The summed total_files_size_bytes line survives in
	// runs/<sess>/iceberg-*.txt but cannot distinguish a healthy 8/8 split
	// from a degenerate 16/0 one.
	sc := sinkTopology{}.MetricSidecar(MetricSidecarArgs{
		Engine: "connect", VCPU: 2, Key: "2-b-2pipe-gmp4",
		Bucket: "b", SessionID: "sess-x",
		Outs:  map[string]string{"aws_region": "us-east-2"},
		Names: newBenchNames("sess-x", "iceberg").WithStreams(2),
	})
	if !strings.Contains(sc.Setup, `echo "table_files_size_bytes $T ${S:-0}"`) {
		t.Errorf("sidecar must emit a per-table line inside the for loop; got:\n%s", sc.Setup)
	}
	// The load-bearing invariant from Task 5 must still hold: exactly one
	// summed total_files_size_bytes / total_records emission per frame.
	if got := strings.Count(sc.Setup, `echo "total_files_size_bytes`); got != 1 {
		t.Errorf("expected exactly one summed size emission per frame, got %d:\n%s", got, sc.Setup)
	}
	if got := strings.Count(sc.Setup, `echo "total_records`); got != 1 {
		t.Errorf("expected exactly one summed records emission per frame, got %d:\n%s", got, sc.Setup)
	}
}

func TestSinkTopology_MetricSidecar_SumsAcrossAllSevenTopicTables(t *testing.T) {
	// The failure mode this exists to catch: a 7-topic sidecar that polls
	// one table, or six, instead of all seven — which would silently hide a
	// starved topic (or under-report throughput) with no error anywhere.
	sc := sinkTopology{}.MetricSidecar(MetricSidecarArgs{
		Engine: "connect", VCPU: 2, Key: "2",
		Bucket: "b", SessionID: "sess-x",
		Outs:  map[string]string{"aws_region": "us-east-2"},
		Names: newBenchNames("sess-x", "iceberg").WithTopics(7),
	})
	for i := 0; i < 7; i++ {
		table := fmt.Sprintf("bench_sess_x_iceberg_connect_t%d", i)
		if !strings.Contains(sc.Setup, table) {
			t.Errorf("sidecar missing topic %d table %q:\n%s", i, table, sc.Setup)
		}
	}
	// Exactly 7 per-table lines, not 1 and not 6.
	if n := strings.Count(sc.Setup, `echo "table_files_size_bytes $T`); n != 1 {
		// The per-table echo is inside the for-loop template (one echo
		// statement, executed once per table at runtime); this asserts the
		// template shape, and the "for T in <7 names>" list above asserts
		// the actual count of tables iterated.
		t.Errorf("expected the per-table echo template exactly once, got %d:\n%s", n, sc.Setup)
	}
	if got := strings.Count(sc.Setup, "bench_sess_x_iceberg_connect_t"); got != 7 {
		t.Errorf("expected exactly 7 topic-table references, got %d:\n%s", got, sc.Setup)
	}
	// Still exactly one summed emission per frame.
	if got := strings.Count(sc.Setup, `echo "total_files_size_bytes`); got != 1 {
		t.Errorf("expected exactly one summed size emission per frame, got %d:\n%s", got, sc.Setup)
	}
}

func TestSinkTopology_KCConfig_Iceberg(t *testing.T) {
	res, ok, err := (sinkTopology{}).KCConfig(&Scenario{Connector: "iceberg", Direction: DirectionSink}, sinkOuts(), newBenchNames("sess", "iceberg"))
	if err != nil || !ok {
		t.Fatalf("KCConfig: ok=%v err=%v", ok, err)
	}
	if res.ConnectorName != "bench_iceberg" {
		t.Errorf("connector name = %q", res.ConnectorName)
	}
	if !strings.Contains(res.ConfigJSON, "io.tabular.iceberg.connect.IcebergSinkConnector") {
		t.Errorf("config must be the iceberg sink; got:\n%s", res.ConfigJSON)
	}
}

func TestSinkTopology_ResetScript_DropsTableAndResetsOffset(t *testing.T) {
	sc, err := (sinkTopology{}).ResetScript(&Scenario{Connector: "iceberg", Direction: DirectionSink}, sinkOuts(), newBenchNames("sess", "iceberg"))
	if err != nil {
		t.Fatalf("ResetScript: %v", err)
	}
	if !strings.Contains(sc, "aws glue delete-table") {
		t.Errorf("reset must drop the per-engine Glue table; got:\n%s", sc)
	}
	if !strings.Contains(sc, "/opt/bench/iceberg-tablegen") {
		t.Errorf("reset must invoke iceberg-tablegen to pre-create tables; got:\n%s", sc)
	}
	if !strings.Contains(sc, "--table=bench_sess_iceberg_connect") || !strings.Contains(sc, "--table=bench_sess_iceberg_kafka_connect") {
		t.Errorf("reset must pre-create both per-engine tables; got:\n%s", sc)
	}
	if !strings.Contains(sc, "s3://rpcn-bench-ice/wh/bench/bench_sess_iceberg_connect") {
		t.Errorf("reset must pass the per-table S3 location; got:\n%s", sc)
	}
}

func TestSinkTopology_ResetScript_CreatesUnionForSevenTopics(t *testing.T) {
	s := &Scenario{Connector: "iceberg", Direction: DirectionSink, Dataset: DatasetSpec{Topics: 7}}
	got, err := (sinkTopology{}).ResetScript(s, sinkOuts(), newBenchNames("sess", "iceberg"))
	if err != nil {
		t.Fatalf("ResetScript: %v", err)
	}
	for i := 0; i < 7; i++ {
		table := fmt.Sprintf("--table=bench_sess_iceberg_connect_t%d", i)
		if !strings.Contains(got, table) {
			t.Errorf("missing topic %d table pre-create %q:\n%s", i, table, got)
		}
		group := fmt.Sprintf("--group %q", fmt.Sprintf("bench_sess_iceberg_connect_t%d", i))
		if !strings.Contains(got, group) {
			t.Errorf("missing topic %d consumer-group reset %q:\n%s", i, group, got)
		}
	}
	// Only Connect is in play here (KC gets its own union too, but the count
	// check below only wants Connect's 7 + KC's 7).
	if n := strings.Count(got, "aws glue delete-table"); n != 14 {
		t.Errorf("expected 14 delete-table calls (7 topics x 2 engines), got %d:\n%s", n, got)
	}
	if strings.Contains(got, "_s0") || strings.Contains(got, "_s1") {
		t.Errorf("multi-topic reset must not mention stream-suffixed tables:\n%s", got)
	}
}

func TestSinkTopology_WorkloadScript_Empty(t *testing.T) {
	got, err := (sinkTopology{}).WorkloadScript(&Scenario{Connector: "iceberg"}, sinkOuts(), newBenchNames("sess", "iceberg"))
	if err != nil || got != "" {
		t.Fatalf("bounded sink has no workload: got %q err %v", got, err)
	}
}

func TestSinkTopology_EngineSeries_ParsesIcebergDump(t *testing.T) {
	dump := "###timestamp=1000\ntotal_files_size_bytes 0\n###timestamp=1010\ntotal_files_size_bytes 104857600\n"
	pts, err := (sinkTopology{}).EngineSeries(MetricInputs{Body: strings.NewReader(dump), Names: newBenchNames("sess", "iceberg")}, "connect")
	if err != nil {
		t.Fatalf("EngineSeries: %v", err)
	}
	if len(pts) != 1 || pts[0].MBPerSec < 9.9 {
		t.Errorf("want ~10 MB/s point, got %#v", pts)
	}
}

func TestTopologyFor_SinkResolves(t *testing.T) {
	topo, err := topologyFor(DirectionSink)
	if err != nil {
		t.Fatalf("sink must resolve now: %v", err)
	}
	if _, ok := topo.(sinkTopology); !ok {
		t.Errorf("DirectionSink must yield sinkTopology, got %T", topo)
	}
}
