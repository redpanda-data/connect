// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
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
		Dataset:   DatasetSpec{Tables: []string{"orders"}, RowSizeBytes: 1200, Seeder: "cdc-rows", InitialRows: 1000},
	}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@host:5432/db", "results_bucket": "bucket"}
	n := newBenchNames("sess", "postgres_cdc")

	want, err := renderSeedScript(s, outs, "stage/cdc-rows")
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
	if got := (sourceTopology{}).MetricArtifact("connect", 2); got != "redpanda-2-connect.txt" {
		t.Errorf("connect artifact = %q", got)
	}
	if got := (sourceTopology{}).MetricArtifact("kafka_connect", 4); got != "redpanda-4-kc.txt" {
		t.Errorf("kc artifact = %q", got)
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
