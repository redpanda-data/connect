// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
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

func TestSinkTopology_MetricArtifact(t *testing.T) {
	if got := (sinkTopology{}).MetricArtifact("connect", 4); got != "iceberg-4-connect.txt" {
		t.Errorf("artifact = %q", got)
	}
	if got := (sinkTopology{}).MetricArtifact("kafka_connect", 2); got != "iceberg-2-kc.txt" {
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
