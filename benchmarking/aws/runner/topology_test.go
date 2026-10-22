// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

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
}

// TestTopologyFor pins the seam: direction: sink has no implementation in
// this scope-reduced tree — it returns an error naming the future PR that
// restores it, rather than resolving to a sinkTopology.
func TestTopologyFor(t *testing.T) {
	if _, err := topologyFor(DirectionSource); err != nil {
		t.Errorf("source topology must resolve, got %v", err)
	}
	if _, err := topologyFor(Direction("")); err != nil {
		t.Errorf("empty direction must resolve to source, got %v", err)
	}
	if _, err := topologyFor(DirectionSink); err == nil {
		t.Error("sink direction must not resolve in this scope-reduced tree")
	} else if !strings.Contains(err.Error(), "iceberg-sink stack PR") {
		t.Errorf("sink rejection must name the future PR; got: %v", err)
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

func TestSourceTopology_MetricArtifact(t *testing.T) {
	if got := (sourceTopology{}).MetricArtifact("2"); got != "redpanda-2-connect.txt" {
		t.Errorf("connect artifact = %q", got)
	}
}

func TestMetricArtifact_KeyedByPointKey(t *testing.T) {
	// A bare vCPU key reproduces the historical filename exactly.
	if got := (sourceTopology{}).MetricArtifact("4"); got != "redpanda-4-connect.txt" {
		t.Errorf("MetricArtifact = %q, want redpanda-4-connect.txt", got)
	}
	// An arm key flows straight through, giving each arm its own artifact.
	if got := (sourceTopology{}).MetricArtifact("2-b-2pipe-gmp4"); got != "redpanda-2-b-2pipe-gmp4-connect.txt" {
		t.Errorf("MetricArtifact(arm) = %q", got)
	}
}

func TestSourceTopology_MetricSidecar_BrokerScrape(t *testing.T) {
	args := MetricSidecarArgs{
		VCPU:   2,
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
		VCPU: 1, Bucket: "b", SessionID: "s",
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
	pts, err := sourceTopology{}.EngineSeries(in)
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
