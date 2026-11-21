// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/catalog"
	"github.com/redpanda-data/connect/v4/benchmarking/aws/plan"
)

// A source scenario's entry is byte-identical to the one recorded before
// e2e catalogue identity existed (testdata/catalog-entry-source.json).
func TestCatalogEntrySourceScenarioUnchanged(t *testing.T) {
	s, r := catalogFixture(t)
	e, err := catalogEntryFromResult(s, r, catalogMeta{SessionID: "bench-1", RunURL: "https://x/run/1", Now: time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)})
	require.NoError(t, err)
	got, err := json.MarshalIndent(e, "", "  ")
	require.NoError(t, err)
	path := filepath.Join("testdata", "catalog-entry-source.json")
	if os.Getenv("UPDATE_CATALOG_GOLDEN") != "" {
		require.NoError(t, os.WriteFile(path, append(got, '\n'), 0o644))
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, string(want), string(got)+"\n")
}

// pgToSnowflakeConfig is the postgres -> snowflake customer config of the
// plan package's Task 1 fixture (plan/plan_test.go pgToSnowflake).
func pgToSnowflakeConfig() map[string]any {
	return map[string]any{
		"input": map[string]any{
			"postgres_cdc": map[string]any{
				"dsn":             "postgres://admin:s3cretpw@db.customer-corp.internal:5432/prod",
				"stream_snapshot": false,
				"schema":          "public",
				"tables":          []any{"customers"},
				"slot_name":       "cust_slot",
				"batching":        map[string]any{"count": 5000, "period": "1s"},
			},
		},
		"pipeline": map[string]any{"processors": []any{
			map[string]any{"mapping": "root = this"},
		}},
		"output": map[string]any{
			"snowflake_streaming": map[string]any{
				"account":          "CUSTORG-ACCT",
				"user":             "svc_loader",
				"role":             "LOADER",
				"database":         "PROD",
				"schema":           "PUBLIC",
				"table":            "CUSTOMERS",
				"private_key":      "-----BEGIN PRIVATE KEY-----MIIEvQ-----END PRIVATE KEY-----",
				"schema_evolution": map[string]any{"enabled": true},
			},
		},
	}
}

// renderedScenario builds a plan from cfg, renders it and loads the scenario
// the runner would run.
func renderedScenario(t *testing.T, cfg map[string]any) (plan.Plan, *Scenario) {
	t.Helper()
	p, err := plan.Build(cfg, plan.Workload{MessageSizeBytes: 1000, TargetMBps: 10}, plan.SlackRef{}, time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.True(t, p.Runnable, p.Reason)
	out, err := plan.Render(p)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "bot-"+p.ID+".yaml")
	require.NoError(t, os.WriteFile(path, out, 0o644))
	s, err := LoadScenario(path)
	require.NoError(t, err)
	return p, s
}

func e2eResult(t *testing.T, name string) *Result {
	t.Helper()
	_, r := catalogFixture(t)
	r.Scenario = name
	return r
}

// The e2e entry's fingerprint is the customer's ORIGINAL config's, not the
// identity-swapped pipeline's.
func TestCatalogEntryE2EFingerprintMatchesCustomerConfig(t *testing.T) {
	p, s := renderedScenario(t, pgToSnowflakeConfig())
	e, err := catalogEntryFromResult(s, e2eResult(t, s.Name), catalogMeta{SessionID: "bench-1"})
	require.NoError(t, err)
	require.Equal(t, p.Fingerprint, e.Fingerprint)

	// The customer re-asks with the same pipeline pasted as a whole config,
	// carrying top-level keys that do not change throughput.
	reask := pgToSnowflakeConfig()
	reask["logger"] = map[string]any{"level": "DEBUG"}
	reask["http"] = map[string]any{"address": "0.0.0.0:4195"}
	reask["metrics"] = map[string]any{"prometheus": map[string]any{}}
	reask["tracer"] = map[string]any{"none": map[string]any{}}
	reask["cache_resources"] = []any{map[string]any{"label": "c", "memory": map[string]any{}}}
	reask["rate_limit_resources"] = []any{map[string]any{"label": "r", "local": map[string]any{"count": 10}}}
	want, err := catalog.ConfigIdentity(reask)
	require.NoError(t, err)
	require.Equal(t, want.Fingerprint, e.Fingerprint)
	require.Equal(t, "postgres_cdc", e.Input)
	require.Equal(t, []string{"mapping"}, e.Processors)
	require.Equal(t, "snowflake_streaming", e.Output)
	require.True(t, e.SinkMeasured)
	require.False(t, e.SourceApproximated)
	require.Empty(t, e.Caveats)
}

// Plan caveats reach the entry: an approximated source into a fallback
// (drop) sink with an external call stubbed.
func TestCatalogEntryE2EPlanCaveats(t *testing.T) {
	cfg := map[string]any{
		"input": map[string]any{"http_server": map[string]any{"path": "/post"}},
		"pipeline": map[string]any{"processors": []any{
			map[string]any{"http": map[string]any{"url": "https://api.customer-corp.com", "verb": "POST"}},
		}},
		"output": map[string]any{"elasticsearch_v8": map[string]any{"urls": []any{"https://es.customer-corp.internal"}}},
	}
	want, err := catalog.Fingerprint(cfg)
	require.NoError(t, err)
	p, s := renderedScenario(t, cfg)
	require.Equal(t, []string{"external_calls_stubbed", "sink_not_measured", "source_approximated"}, p.Caveats)

	e, err := catalogEntryFromResult(s, e2eResult(t, s.Name), catalogMeta{SessionID: "bench-1"})
	require.NoError(t, err)
	require.Equal(t, want, e.Fingerprint)
	require.Equal(t, "http_server", e.Input)
	require.Equal(t, []string{"http"}, e.Processors)
	require.Equal(t, "elasticsearch_v8", e.Output)
	require.Equal(t, "bulk", e.Workload.Mode)
	require.True(t, e.SourceApproximated)
	require.False(t, e.SinkMeasured)
	require.True(t, e.ExternalStubbed)
	require.Equal(t, p.Caveats, e.Caveats)
}

// A hand-written e2e scenario has no catalog block: the entry fingerprints
// the scenario pipeline without the harness's benchmark processor.
func TestCatalogEntryE2EFallbackExcludesBenchmark(t *testing.T) {
	customer := map[string]any{
		"input":    map[string]any{"redpanda": map[string]any{"topics": []any{"t"}}},
		"pipeline": map[string]any{"processors": []any{map[string]any{"mapping": "root = this"}}},
		"output": map[string]any{
			"drop":       map[string]any{},
			"processors": []any{map[string]any{"mapping": "root.x = 1"}},
		},
	}
	want, err := catalog.Fingerprint(customer)
	require.NoError(t, err)

	pipeline := map[string]any{
		"input":    customer["input"],
		"pipeline": customer["pipeline"],
		"output": map[string]any{
			"drop": map[string]any{},
			"processors": []any{
				map[string]any{"mapping": "root.x = 1"},
				map[string]any{"benchmark": map[string]any{"interval": "1s", "count_bytes": true}},
			},
		},
	}
	s := &Scenario{Name: "hand", Direction: DirectionE2E, Connector: "redpanda", Stack: stackNone, Sink: &SinkSpec{Kind: "drop"}, Workload: &WorkloadSpec{WriteRatePerSec: 1000}, Pipeline: pipeline}
	e, err := catalogEntryFromResult(s, e2eResult(t, "hand"), catalogMeta{SessionID: "bench-1"})
	require.NoError(t, err)
	require.Equal(t, want, e.Fingerprint)
	require.Equal(t, []string{"mapping", "mapping"}, e.Processors)
	require.Equal(t, "drop", e.Output)
	require.False(t, e.SinkMeasured, "a drop sink measures nothing")
	require.Empty(t, e.Caveats)
	// The scenario is not mutated.
	require.Len(t, pipeline["output"].(map[string]any)["processors"], 2)
}

func TestScenarioCatalogBlockValidation(t *testing.T) {
	_, s := renderedScenario(t, pgToSnowflakeConfig())
	require.NotNil(t, s.Catalog)
	require.NoError(t, s.Validate())

	bad := *s.Catalog
	bad.Caveats = []string{"made_up"}
	s.Catalog = &bad
	require.ErrorContains(t, s.Validate(), "made_up")

	bad.Caveats = nil
	bad.Fingerprint = "nothex"
	require.ErrorContains(t, s.Validate(), "catalog.fingerprint")

	src, _ := catalogFixture(t)
	src.Catalog = &CatalogSpec{Fingerprint: s.Catalog.Fingerprint, Input: "x", Output: "y"}
	require.ErrorContains(t, src.Validate(), "catalog")
}
