// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cdcPipeline() map[string]any {
	return map[string]any{
		"input": map[string]any{
			"postgres_cdc": map[string]any{
				"dsn":             "postgres://a",
				"tables":          []any{"t1"},
				"slot_name":       "slot_a",
				"consumer_group":  "group_a",
				"stream_snapshot": true,
				"tls":             map[string]any{"skip_cert_verify": true},
				"batching":        map[string]any{"count": 5000},
			},
		},
		"pipeline": map[string]any{
			"processors": []any{map[string]any{"mapping": "root = this"}},
		},
		"output": map[string]any{
			"redpanda": map[string]any{"topic": "out_a", "compression": "lz4"},
		},
	}
}

func TestFingerprintIgnoresIdentity(t *testing.T) {
	a := cdcPipeline()
	b := cdcPipeline()
	in := b["input"].(map[string]any)["postgres_cdc"].(map[string]any)
	in["dsn"] = "postgres://b"
	in["tables"] = []any{"t2", "t3"}
	in["slot_name"] = "slot_b"
	in["tls"] = map[string]any{"skip_cert_verify": false}
	in["consumer_group"] = "group_b"
	b["output"].(map[string]any)["redpanda"].(map[string]any)["topic"] = "out_b"

	fa, err := Fingerprint(a)
	require.NoError(t, err)
	fb, err := Fingerprint(b)
	require.NoError(t, err)
	assert.Equal(t, fa, fb)
	assert.Len(t, fa, 64)
}

func TestFingerprintSeesPerformanceKnobs(t *testing.T) {
	base, err := Fingerprint(cdcPipeline())
	require.NoError(t, err)

	tests := map[string]func(p map[string]any){
		"batching count": func(p map[string]any) {
			p["input"].(map[string]any)["postgres_cdc"].(map[string]any)["batching"] = map[string]any{"count": 1000}
		},
		"mapping text": func(p map[string]any) {
			p["pipeline"].(map[string]any)["processors"] = []any{map[string]any{"mapping": "root = null"}}
		},
		"stream_snapshot": func(p map[string]any) {
			p["input"].(map[string]any)["postgres_cdc"].(map[string]any)["stream_snapshot"] = false
		},
		"mutation body": func(p map[string]any) {
			p["pipeline"].(map[string]any)["processors"] = []any{map[string]any{"mutation": "root.a = 1"}}
		},
		"branch request_map": func(p map[string]any) {
			p["pipeline"].(map[string]any)["processors"] = []any{
				map[string]any{"branch": map[string]any{"request_map": "root = this.a"}},
			}
		},
		"compression": func(p map[string]any) {
			p["output"].(map[string]any)["redpanda"].(map[string]any)["compression"] = "zstd"
		},
		"added processor": func(p map[string]any) {
			procs := p["pipeline"].(map[string]any)["processors"].([]any)
			p["pipeline"].(map[string]any)["processors"] = append(procs, map[string]any{"branch": map[string]any{}})
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			p := cdcPipeline()
			mutate(p)
			got, err := Fingerprint(p)
			require.NoError(t, err)
			assert.NotEqual(t, base, got)
		})
	}
}

func TestComponents(t *testing.T) {
	in, procs, out, err := Components(map[string]any{
		"input": map[string]any{"postgres_cdc": map[string]any{}},
		"pipeline": map[string]any{"processors": []any{
			map[string]any{"mapping": "x"},
			map[string]any{"branch": map[string]any{}},
		}},
		"output": map[string]any{"snowflake_streaming": map[string]any{}},
	})
	require.NoError(t, err)
	assert.Equal(t, "postgres_cdc", in)
	assert.Equal(t, []string{"mapping", "branch"}, procs)
	assert.Equal(t, "snowflake_streaming", out)
}

func TestComponentsNestedProcessors(t *testing.T) {
	in, procs, out, err := Components(map[string]any{
		"input": map[string]any{
			"postgres_cdc": map[string]any{},
			"processors":   []any{map[string]any{"mapping": "a"}},
		},
		"pipeline": map[string]any{"processors": []any{map[string]any{"branch": map[string]any{}}}},
		"output": map[string]any{
			"processors": []any{map[string]any{"benchmark": map[string]any{}}},
			"redpanda":   map[string]any{},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "postgres_cdc", in)
	assert.Equal(t, []string{"mapping", "branch", "benchmark"}, procs)
	assert.Equal(t, "redpanda", out)
}

func TestComponentsErrors(t *testing.T) {
	good := map[string]any{"x": map[string]any{}}
	tests := map[string]map[string]any{
		"missing input":  {"output": good},
		"missing output": {"input": good},
		"two input keys": {"input": map[string]any{"a": 1, "b": 2}, "output": good},
		"only processors": {
			"input":  map[string]any{"processors": []any{}},
			"output": good,
		},
	}
	for name, p := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, _, err := Components(p)
			require.Error(t, err)
		})
	}
}
