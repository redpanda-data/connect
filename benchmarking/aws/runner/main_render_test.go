// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"os"
	"strings"
	"testing"
)

func TestRenderPipelineConfig_PostgresCDC(t *testing.T) {
	s := &Scenario{
		Name:      "postgres-test",
		Connector: "postgres_cdc",
		Stack:     "postgres",
		Pipeline: map[string]any{
			"input": map[string]any{
				"postgres_cdc": map[string]any{
					"dsn":             "${POSTGRES_DSN}",
					"stream_snapshot": false,
					"tables":          []string{"orders"},
					"slot_name":       "bench_slot",
				},
			},
		},
	}
	outs := map[string]string{
		"bench_session_id":          "sess-abc",
		"postgres_dsn":              "postgres://user:pw@host/db",
		"redpanda_broker_endpoints": "10.42.10.10:9092",
	}

	path, err := renderPipelineConfig(s, outs, sourceTopology{}, BenchNames{})
	if err != nil {
		t.Fatalf("renderPipelineConfig: %v", err)
	}
	t.Cleanup(func() { os.Remove(path) })

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read rendered config: %v", err)
	}
	body := string(raw)

	if !strings.Contains(body, "seed_brokers:") {
		t.Errorf("expected top-level redpanda.seed_brokers in rendered config; got:\n%s", body)
	}
	if !strings.Contains(body, "10.42.10.10:9092") {
		t.Errorf("expected substituted broker endpoint; got:\n%s", body)
	}
	if !strings.Contains(body, "topic: bench_sess-abc_postgres_cdc_connect") {
		t.Errorf("expected per-engine topic name; got:\n%s", body)
	}
	if !strings.Contains(body, "postgres_cdc:") {
		t.Errorf("expected postgres_cdc input block; got:\n%s", body)
	}
	if !strings.Contains(body, "benchmark:") {
		t.Errorf("expected benchmark processor; got:\n%s", body)
	}
	if strings.Contains(body, "drop:") {
		t.Errorf("output should be redpanda not drop; got:\n%s", body)
	}
}

func TestRenderPipelineConfig_ThreadsCacheResources(t *testing.T) {
	s := &Scenario{
		Name:      "mysql-test",
		Connector: "mysql_cdc",
		Stack:     "mysql",
		Pipeline: map[string]any{
			"cache_resources": []any{
				map[string]any{"label": "bench_checkpoint", "memory": map[string]any{}},
			},
			"input": map[string]any{
				"mysql_cdc": map[string]any{
					"dsn": "${MYSQL_DSN}",
				},
			},
		},
	}
	outs := map[string]string{
		"bench_session_id":          "sess-xyz",
		"mysql_dsn":                 "user:pw@tcp(host)/db",
		"redpanda_broker_endpoints": "10.42.10.10:9092",
	}

	path, err := renderPipelineConfig(s, outs, sourceTopology{}, BenchNames{})
	if err != nil {
		t.Fatalf("renderPipelineConfig: %v", err)
	}
	t.Cleanup(func() { os.Remove(path) })

	raw, _ := os.ReadFile(path)
	body := string(raw)
	if !strings.Contains(body, "cache_resources:") {
		t.Errorf("expected cache_resources to be threaded through; got:\n%s", body)
	}
	if !strings.Contains(body, "bench_checkpoint") {
		t.Errorf("expected cache_resources content; got:\n%s", body)
	}
}

func TestRenderPipelineConfig_ThreadsBuffer(t *testing.T) {
	// A scenario may declare a top-level buffer (e.g. to decouple a fast input
	// from a commit-latency-bound sink output like iceberg). It must be
	// threaded through to the Connect config root.
	s := &Scenario{
		Name:      "pg-test",
		Connector: "postgres_cdc",
		Stack:     "postgres",
		Pipeline: map[string]any{
			"buffer": map[string]any{
				"memory": map[string]any{"limit": 536870912},
			},
			"input": map[string]any{
				"postgres_cdc": map[string]any{
					"dsn":    "${POSTGRES_DSN}",
					"tables": []any{"orders"},
				},
			},
		},
	}
	outs := map[string]string{
		"bench_session_id":          "sess-xyz",
		"postgres_dsn":              "postgres://u:p@host/db",
		"redpanda_broker_endpoints": "10.42.10.10:9092",
	}

	path, err := renderPipelineConfig(s, outs, sourceTopology{}, BenchNames{})
	if err != nil {
		t.Fatalf("renderPipelineConfig: %v", err)
	}
	t.Cleanup(func() { os.Remove(path) })

	raw, _ := os.ReadFile(path)
	body := string(raw)
	if !strings.Contains(body, "buffer:") {
		t.Errorf("expected buffer to be threaded through; got:\n%s", body)
	}
	if !strings.Contains(body, "memory:") {
		t.Errorf("expected buffer memory content; got:\n%s", body)
	}
}
