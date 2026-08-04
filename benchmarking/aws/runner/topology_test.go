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

func TestBenchNames_SourceTopicConventions(t *testing.T) {
	n := newBenchNames("sess-abc", "postgres_cdc")
	if got := n.ConnectTopic(); got != "bench_sess-abc_postgres_cdc_connect" {
		t.Errorf("ConnectTopic = %q, want bench_sess-abc_postgres_cdc_connect", got)
	}
	if got := n.KCTopicPrefix(); got != "bench_sess-abc_postgres_cdc_kc" {
		t.Errorf("KCTopicPrefix = %q, want bench_sess-abc_postgres_cdc_kc", got)
	}
}

func TestBenchNames_SinkConventions(t *testing.T) {
	n := newBenchNames("sess-x", "iceberg")
	if got := n.SourceTopic(); got != "bench_sess-x_iceberg_src" {
		t.Errorf("SourceTopic = %q", got)
	}
	if got := n.IcebergTable("connect"); got != "bench_sess_x_iceberg_connect" {
		t.Errorf("IcebergTable(connect) = %q (dashes must become underscores)", got)
	}
	if got := n.ConsumerGroup("kafka_connect"); got != "bench_sess-x_iceberg_kafka_connect" {
		t.Errorf("ConsumerGroup = %q", got)
	}
}

func TestBenchNames_SingleStreamTableNamesUnchanged(t *testing.T) {
	// Streams 0 (zero value) and 1 must both yield the historical unsuffixed
	// name — this is what keeps the six existing scenarios untouched.
	for _, n := range []BenchNames{
		newBenchNames("sess-x", "iceberg"),
		newBenchNames("sess-x", "iceberg").WithStreams(1),
	} {
		if got := n.IcebergTable("connect"); got != "bench_sess_x_iceberg_connect" {
			t.Errorf("IcebergTable(connect) = %q, want unsuffixed bench_sess_x_iceberg_connect", got)
		}
		if got := n.IcebergTables("connect"); len(got) != 1 || got[0] != "bench_sess_x_iceberg_connect" {
			t.Errorf("IcebergTables(connect) = %v, want one unsuffixed name", got)
		}
	}
}

func TestBenchNames_MultiStreamTableNamesSuffixed(t *testing.T) {
	n := newBenchNames("sess-x", "iceberg").WithStreams(2)
	if got := n.WithStream(0).IcebergTable("connect"); got != "bench_sess_x_iceberg_connect_s0" {
		t.Errorf("stream 0 table = %q", got)
	}
	if got := n.WithStream(1).IcebergTable("connect"); got != "bench_sess_x_iceberg_connect_s1" {
		t.Errorf("stream 1 table = %q", got)
	}
	want := []string{"bench_sess_x_iceberg_connect_s0", "bench_sess_x_iceberg_connect_s1"}
	got := n.IcebergTables("connect")
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("IcebergTables = %v, want %v", got, want)
	}
}

func TestBenchNames_SharedIdentifiersAreStreamIndependent(t *testing.T) {
	// Both streams join the same consumer group and read the same topic — that
	// is what splits the partitions 8/8 instead of doubling the work.
	n := newBenchNames("sess-x", "iceberg").WithStreams(2)
	if a, b := n.WithStream(0).ConsumerGroup("connect"), n.WithStream(1).ConsumerGroup("connect"); a != b {
		t.Errorf("consumer group must be shared across streams: %q vs %q", a, b)
	}
	if a, b := n.WithStream(0).SourceTopic(), n.WithStream(1).SourceTopic(); a != b {
		t.Errorf("source topic must be shared across streams: %q vs %q", a, b)
	}
}

func TestBenchNames_IcebergResetTablesIsUnionAcrossArms(t *testing.T) {
	n := newBenchNames("sess-x", "iceberg")
	// maxStreams 1: just the base table, exactly as before arms existed.
	got := n.IcebergResetTables("connect", 1)
	if len(got) != 1 || got[0] != "bench_sess_x_iceberg_connect" {
		t.Errorf("maxStreams=1 reset tables = %v, want [base]", got)
	}
	// maxStreams 2: base (for single-stream arms and for KC) plus both
	// per-stream tables, so one reset script serves every arm.
	got = n.IcebergResetTables("connect", 2)
	want := []string{
		"bench_sess_x_iceberg_connect",
		"bench_sess_x_iceberg_connect_s0",
		"bench_sess_x_iceberg_connect_s1",
	}
	if len(got) != 3 {
		t.Fatalf("maxStreams=2 reset tables = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("reset table[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestBenchNames_SingleTopicNamesUnchanged(t *testing.T) {
	// Topics 0 (zero value) and 1 must both yield the historical unsuffixed
	// names — the parity guard for every existing scenario.
	for _, n := range []BenchNames{
		newBenchNames("sess-x", "iceberg"),
		newBenchNames("sess-x", "iceberg").WithTopics(1),
	} {
		if got := n.SourceTopic(); got != "bench_sess-x_iceberg_src" {
			t.Errorf("SourceTopic = %q, want unsuffixed", got)
		}
		if got := n.IcebergTable("connect"); got != "bench_sess_x_iceberg_connect" {
			t.Errorf("IcebergTable(connect) = %q, want unsuffixed", got)
		}
		if got := n.ConsumerGroup("connect"); got != "bench_sess-x_iceberg_connect" {
			t.Errorf("ConsumerGroup = %q, want unsuffixed", got)
		}
		if got := n.IcebergTablesForTopics("connect"); len(got) != 1 || got[0] != "bench_sess_x_iceberg_connect" {
			t.Errorf("IcebergTablesForTopics = %v, want one unsuffixed name", got)
		}
	}
}

func TestBenchNames_MultiTopicNamesSuffixed(t *testing.T) {
	n := newBenchNames("sess-x", "iceberg").WithTopics(7)
	if got := n.WithTopic(0).SourceTopic(); got != "bench_sess-x_iceberg_src_t0" {
		t.Errorf("topic 0 source topic = %q", got)
	}
	if got := n.WithTopic(6).SourceTopic(); got != "bench_sess-x_iceberg_src_t6" {
		t.Errorf("topic 6 source topic = %q", got)
	}
	if got := n.WithTopic(0).IcebergTable("connect"); got != "bench_sess_x_iceberg_connect_t0" {
		t.Errorf("topic 0 table = %q", got)
	}
	if got := n.WithTopic(6).IcebergTable("connect"); got != "bench_sess_x_iceberg_connect_t6" {
		t.Errorf("topic 6 table = %q", got)
	}
	if got := n.WithTopic(0).ConsumerGroup("connect"); got != "bench_sess-x_iceberg_connect_t0" {
		t.Errorf("topic 0 consumer group = %q", got)
	}
	if a, b := n.WithTopic(0).ConsumerGroup("connect"), n.WithTopic(1).ConsumerGroup("connect"); a == b {
		t.Errorf("distinct topics must get distinct consumer groups, got %q for both", a)
	}
}

func TestBenchNames_IcebergTablesForTopics_AllSeven(t *testing.T) {
	// The failure mode this exists to catch: a 7-topic sidecar/reset script
	// that references one table, or six, instead of all seven.
	n := newBenchNames("sess-x", "iceberg").WithTopics(7)
	got := n.IcebergTablesForTopics("connect")
	if len(got) != 7 {
		t.Fatalf("IcebergTablesForTopics returned %d tables, want 7: %v", len(got), got)
	}
	for i := 0; i < 7; i++ {
		want := fmt.Sprintf("bench_sess_x_iceberg_connect_t%d", i)
		if got[i] != want {
			t.Errorf("table[%d] = %q, want %q", i, got[i], want)
		}
	}
}

func TestBenchNames_TopicsAndStreamsMutuallyExclusive_TopicSuffixWins(t *testing.T) {
	// Validation elsewhere rejects Topics>1 and Streams>1 together, but
	// IcebergTable must still behave sanely (and never emit both suffixes)
	// if it somehow happens.
	n := newBenchNames("sess-x", "iceberg").WithStreams(2).WithStream(1).WithTopics(3).WithTopic(2)
	got := n.IcebergTable("connect")
	if got != "bench_sess_x_iceberg_connect_t2" {
		t.Errorf("IcebergTable with both set = %q, want _t2 suffix only", got)
	}
	if strings.Contains(got, "_s1") {
		t.Errorf("must not also emit the stream suffix: %q", got)
	}
}

func TestBenchNames_WithTopicsResetsTopicIndex(t *testing.T) {
	n := newBenchNames("sess-x", "iceberg").WithTopics(7).WithTopic(5)
	if n.TopicIndex != 5 {
		t.Fatalf("setup: TopicIndex = %d, want 5", n.TopicIndex)
	}
	n = n.WithTopics(3)
	if n.TopicIndex != 0 {
		t.Errorf("WithTopics must reset TopicIndex to 0, got %d", n.TopicIndex)
	}
}

func TestTopologyFor(t *testing.T) {
	if _, err := topologyFor(DirectionSource); err != nil {
		t.Errorf("source topology must resolve, got %v", err)
	}
	if _, err := topologyFor(Direction("")); err != nil {
		t.Errorf("empty direction must resolve to source, got %v", err)
	}
	if _, err := topologyFor(DirectionSink); err != nil {
		t.Errorf("sink topology must resolve, got %v", err)
	}
	if _, err := topologyFor(Direction("sideways")); err == nil {
		t.Errorf("unknown direction must error")
	}
}

func TestSourceTopology_Validate_RejectsUnknownConnector(t *testing.T) {
	s := &Scenario{Connector: "nope_cdc", Direction: DirectionSource}
	if err := (sourceTopology{}).Validate(s); err == nil {
		t.Fatal("expected error for connector with no engineSpec")
	}
}

func TestSourceTopology_Validate_AcceptsKnown(t *testing.T) {
	s := &Scenario{Connector: "postgres_cdc", Direction: DirectionSource}
	if err := (sourceTopology{}).Validate(s); err != nil {
		t.Fatalf("postgres_cdc must validate, got %v", err)
	}
}

func TestSourceTopology_SeedScript_MatchesRenderSeedScript(t *testing.T) {
	s := &Scenario{
		Connector: "postgres_cdc",
		Dataset:   DatasetSpec{Tables: []string{"orders"}, RowSizeBytes: 1200, Seeder: "cdc-rows-postgres", InitialRows: 1000},
	}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@host:5432/db", "results_bucket": "bucket"}
	n := newBenchNames("sess", "postgres_cdc")

	want, err := renderSeedScript(s, outs, "stage/cdc-rows-postgres")
	if err != nil {
		t.Fatalf("renderSeedScript: %v", err)
	}
	got, err := sourceTopology{}.SeedScript(s, outs, n)
	if err != nil {
		t.Fatalf("SeedScript: %v", err)
	}
	if got != want {
		t.Errorf("SeedScript diverged from renderSeedScript:\n got: %q\nwant: %q", got, want)
	}
}

func TestSourceTopology_ResetScript_MatchesCombineReset(t *testing.T) {
	s := &Scenario{Connector: "postgres_cdc", Reset: []ResetStep{{SQL: "SELECT 1"}}}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@host/db"}
	n := newBenchNames("sess", "postgres_cdc")

	want, err := combineReset(s.Connector, s.Reset, outs)
	if err != nil {
		t.Fatalf("combineReset: %v", err)
	}
	got, err := sourceTopology{}.ResetScript(s, outs, n)
	if err != nil {
		t.Fatalf("ResetScript: %v", err)
	}
	if got != want {
		t.Errorf("ResetScript diverged from combineReset:\n got: %q\nwant: %q", got, want)
	}
}

func TestSourceTopology_WorkloadScript_NilIsEmpty(t *testing.T) {
	s := &Scenario{Connector: "postgres_cdc", Workload: nil}
	got, err := sourceTopology{}.WorkloadScript(s, map[string]string{}, newBenchNames("sess", "postgres_cdc"))
	if err != nil {
		t.Fatalf("WorkloadScript: %v", err)
	}
	if got != "" {
		t.Errorf("nil workload must render empty, got %q", got)
	}
}

func TestSourceTopology_Pipeline_InputAndOutput(t *testing.T) {
	s := &Scenario{
		Connector: "postgres_cdc",
		Pipeline: map[string]any{
			"input": map[string]any{"postgres_cdc": map[string]any{"dsn": "x"}},
		},
	}
	in, out, err := sourceTopology{}.Pipeline(s, newBenchNames("sess", "postgres_cdc"))
	if err != nil {
		t.Fatalf("Pipeline: %v", err)
	}
	if _, ok := in["postgres_cdc"]; !ok {
		t.Errorf("input must pass through scenario pipeline.input; got %#v", in)
	}
	rp, ok := out["redpanda"].(map[string]any)
	if !ok {
		t.Fatalf("output must contain a redpanda block; got %#v", out)
	}
	if rp["topic"] != "bench_${BENCH_SESSION_ID}_postgres_cdc_connect" {
		t.Errorf("output topic = %v, want bench_${BENCH_SESSION_ID}_postgres_cdc_connect", rp["topic"])
	}
}

func TestSourceTopology_KCConfig_Postgres(t *testing.T) {
	s := &Scenario{
		Connector: "postgres_cdc",
		Pipeline:  map[string]any{"input": map[string]any{"postgres_cdc": map[string]any{"schema": "public", "tables": []any{"orders"}}}},
	}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@host:5432/db"}
	res, ok, err := (sourceTopology{}).KCConfig(s, outs, newBenchNames("sess", "postgres_cdc"))
	if err != nil || !ok {
		t.Fatalf("KCConfig: ok=%v err=%v", ok, err)
	}
	if res.ConnectorName != "bench_postgres_cdc" {
		t.Errorf("connector name = %q", res.ConnectorName)
	}
	if !strings.Contains(res.ConfigJSON, "io.debezium.connector.postgresql.PostgresConnector") {
		t.Errorf("config must be the Debezium postgres connector; got:\n%s", res.ConfigJSON)
	}
}

func TestSourceTopology_MetricArtifact(t *testing.T) {
	if got := (sourceTopology{}).MetricArtifact("connect", "2"); got != "redpanda-2-connect.txt" {
		t.Errorf("connect artifact = %q", got)
	}
	if got := (sourceTopology{}).MetricArtifact("kafka_connect", "4"); got != "redpanda-4-kc.txt" {
		t.Errorf("kc artifact = %q", got)
	}
}

func TestMetricArtifact_KeyedByPointKey(t *testing.T) {
	// A bare vCPU key reproduces the historical filenames exactly.
	if got := (sourceTopology{}).MetricArtifact("connect", "4"); got != "redpanda-4-connect.txt" {
		t.Errorf("source MetricArtifact = %q, want redpanda-4-connect.txt", got)
	}
	if got := (sourceTopology{}).MetricArtifact("kafka_connect", "4"); got != "redpanda-4-kc.txt" {
		t.Errorf("source MetricArtifact(kc) = %q, want redpanda-4-kc.txt", got)
	}
	if got := (sinkTopology{}).MetricArtifact("connect", "2"); got != "iceberg-2-connect.txt" {
		t.Errorf("sink MetricArtifact = %q, want iceberg-2-connect.txt", got)
	}
	// An arm key flows straight through, giving each arm its own artifact.
	if got := (sinkTopology{}).MetricArtifact("connect", "2-b-2pipe-gmp4"); got != "iceberg-2-b-2pipe-gmp4-connect.txt" {
		t.Errorf("sink MetricArtifact(arm) = %q", got)
	}
}

func TestSourceTopology_MetricSidecar_BrokerScrape(t *testing.T) {
	args := MetricSidecarArgs{
		Engine: "connect", VCPU: 2,
		Bucket: "rpcn-bench-results", SessionID: "sess",
		Outs:  map[string]string{"redpanda_metrics_endpoints": "10.0.0.1:9644,10.0.0.2:9644"},
		Names: newBenchNames("sess", "postgres_cdc"),
	}
	sc := (sourceTopology{}).MetricSidecar(args)
	if !strings.Contains(sc.Setup, "/public_metrics") {
		t.Errorf("source sidecar must scrape /public_metrics; got:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "10.0.0.1:9644,10.0.0.2:9644") {
		t.Errorf("source sidecar must embed broker endpoints; got:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, `RP=/tmp/redpanda-2-connect.txt`) {
		t.Errorf("source sidecar must write the connect artifact; got:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Upload, "redpanda-2-connect.txt") {
		t.Errorf("upload must reference the artifact; got:\n%s", sc.Upload)
	}
}

func TestSourceTopology_MetricSidecar_EmptyWhenNoEndpoints(t *testing.T) {
	sc := (sourceTopology{}).MetricSidecar(MetricSidecarArgs{
		Engine: "connect", VCPU: 1, Bucket: "b", SessionID: "s",
		Outs:  map[string]string{}, // no broker endpoints
		Names: newBenchNames("s", "postgres_cdc"),
	})
	if sc.Setup != "" || sc.Upload != "" {
		t.Errorf("no endpoints must yield an empty sidecar (scrape omitted), got Setup=%q Upload=%q", sc.Setup, sc.Upload)
	}
}

func TestSourceTopology_EngineSeries_ParsesBrokerDump(t *testing.T) {
	// One topic owned by Connect, sampled across two frames so a single
	// throughput point is produced.
	dump := strings.Join([]string{
		"###timestamp=1000",
		`redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_namespace="kafka",redpanda_topic="bench_sess_postgres_cdc_connect"} 0`,
		"###timestamp=1001",
		`redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_namespace="kafka",redpanda_topic="bench_sess_postgres_cdc_connect"} 1048576`,
	}, "\n")
	in := MetricInputs{Body: strings.NewReader(dump), Names: newBenchNames("sess", "postgres_cdc")}
	pts, err := sourceTopology{}.EngineSeries(in, "connect")
	if err != nil {
		t.Fatalf("EngineSeries: %v", err)
	}
	if len(pts) != 1 {
		t.Fatalf("expected 1 throughput point, got %d (%#v)", len(pts), pts)
	}
	if pts[0].MBPerSec <= 0 {
		t.Errorf("expected positive MB/s, got %v", pts[0].MBPerSec)
	}
}

func TestSinkMetricSidecar_SingleTableShapeUnchanged(t *testing.T) {
	sc := sinkTopology{}.MetricSidecar(MetricSidecarArgs{
		Engine: "connect", VCPU: 2, Key: "2",
		Bucket: "b", SessionID: "sess-x",
		Outs:  map[string]string{"aws_region": "us-east-2"},
		Names: newBenchNames("sess-x", "iceberg"),
	})
	if !strings.Contains(sc.Setup, "RP=/tmp/iceberg-2-connect.txt") {
		t.Errorf("artifact path missing:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "bench_sess_x_iceberg_connect") {
		t.Errorf("base table missing:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "total_files_size_bytes") || !strings.Contains(sc.Setup, "total_records") {
		t.Errorf("sidecar must still emit both metric lines:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Upload, "s3://b/runs/sess-x/iceberg-2-connect.txt") {
		t.Errorf("upload target wrong:\n%s", sc.Upload)
	}
}

func TestSinkMetricSidecar_SumsAcrossStreamTables(t *testing.T) {
	sc := sinkTopology{}.MetricSidecar(MetricSidecarArgs{
		Engine: "connect", VCPU: 2, Key: "2-b-2pipe-gmp4",
		Bucket: "b", SessionID: "sess-x",
		Outs:  map[string]string{"aws_region": "us-east-2"},
		Names: newBenchNames("sess-x", "iceberg").WithStreams(2),
	})
	for _, want := range []string{
		"RP=/tmp/iceberg-2-b-2pipe-gmp4-connect.txt",
		"bench_sess_x_iceberg_connect_s0",
		"bench_sess_x_iceberg_connect_s1",
	} {
		if !strings.Contains(sc.Setup, want) {
			t.Errorf("sidecar missing %q:\n%s", want, sc.Setup)
		}
	}
	// The poller must accumulate, not overwrite, or a 2-stream arm reports
	// only one stream's bytes and the whole A/B is wrong.
	if !strings.Contains(sc.Setup, "SIZE=$((SIZE + ") || !strings.Contains(sc.Setup, "RECS=$((RECS + ") {
		t.Errorf("sidecar must accumulate across tables:\n%s", sc.Setup)
	}
	// Still exactly two emitted metric lines per frame, so ParseIcebergSeries
	// needs no change.
	if got := strings.Count(sc.Setup, `echo "total_files_size_bytes`); got != 1 {
		t.Errorf("expected exactly one size emission per frame, got %d:\n%s", got, sc.Setup)
	}
}

func TestSinkResetScript_SingleStreamUnchanged(t *testing.T) {
	s := &Scenario{Connector: "iceberg", Direction: DirectionSink}
	outs := map[string]string{
		"aws_region": "us-east-2", "redpanda_broker_endpoints": "b:9092",
		"glue_rest_uri": "https://glue", "warehouse_account_id": "1234",
		"warehouse_s3_uri": "s3://wh",
	}
	got, err := sinkTopology{}.ResetScript(s, outs, newBenchNames("sess-x", "iceberg"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"bench_sess_x_iceberg_connect",
		"bench_sess_x_iceberg_kafka_connect",
		"kafka-consumer-groups.sh",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("reset missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "_s0") {
		t.Errorf("single-stream reset must not mention per-stream tables:\n%s", got)
	}
	// One delete + one pre-create per engine.
	if n := strings.Count(got, "aws glue delete-table"); n != 2 {
		t.Errorf("expected 2 delete-table calls (one per engine), got %d", n)
	}
}

func TestSinkResetScript_CreatesUnionForMultiStreamPlan(t *testing.T) {
	s := &Scenario{Connector: "iceberg", Direction: DirectionSink}
	outs := map[string]string{
		"aws_region": "us-east-2", "redpanda_broker_endpoints": "b:9092",
		"glue_rest_uri": "https://glue", "warehouse_account_id": "1234",
		"warehouse_s3_uri": "s3://wh",
	}
	// Streams here is the plan max, so the reset serves every arm.
	got, err := sinkTopology{}.ResetScript(s, outs, newBenchNames("sess-x", "iceberg").WithStreams(2))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"bench_sess_x_iceberg_connect",    // base: used by the single-stream arms
		"bench_sess_x_iceberg_connect_s0", // arm B stream 0
		"bench_sess_x_iceberg_connect_s1", // arm B stream 1
		"bench_sess_x_iceberg_kafka_connect",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("reset missing %q:\n%s", want, got)
		}
	}
	// 3 connect tables + 3 kafka_connect tables, each dropped and pre-created.
	if n := strings.Count(got, "aws glue delete-table"); n != 6 {
		t.Errorf("expected 6 delete-table calls, got %d:\n%s", n, got)
	}
	if n := strings.Count(got, "/opt/bench/iceberg-tablegen"); n != 6 {
		t.Errorf("expected 6 tablegen pre-creates, got %d:\n%s", n, got)
	}
	// Each pre-create is wrapped in a bounded retry that still fails loud.
	if n := strings.Count(got, "for attempt in 1 2 3; do"); n != 6 {
		t.Errorf("expected each tablegen wrapped in a retry loop, got %d:\n%s", n, got)
	}
	if !strings.Contains(got, "after 3 attempts") {
		t.Errorf("retry must fail loud after 3 attempts:\n%s", got)
	}
	// The consumer group is shared by both streams, so it is reset once per
	// engine, not once per table.
	if n := strings.Count(got, "kafka-consumer-groups.sh"); n != 2 {
		t.Errorf("expected 2 consumer-group resets (one per engine), got %d:\n%s", n, got)
	}
}
